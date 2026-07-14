package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (s *Store) AuthorizeAccount(ctx context.Context, ownerSub string) error {
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO account_quotas(owner_sub) VALUES ($1)
		ON CONFLICT (owner_sub) DO NOTHING`, ownerSub); err != nil {
		return err
	}
	return s.CheckAccount(ctx, ownerSub)
}

func (s *Store) CheckAccount(ctx context.Context, ownerSub string) error {
	var suspendedAt *time.Time
	err := s.pool.QueryRow(ctx, `SELECT suspended_at FROM account_quotas WHERE owner_sub=$1`, ownerSub).Scan(&suspendedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if suspendedAt != nil {
		return fmt.Errorf("%w: account suspended", ErrQuotaExceeded)
	}
	return nil
}

func (s *Store) CreateUpload(ctx context.Context, ownerSub, idempotencyKey, requestHash, bucket string, req CreateUploadRequest) (Upload, bool, error) {
	if req.ExpectedBytes < 1 || req.ExpectedBytes > 20*1024*1024*1024 || !validSHA256Hex(req.ChecksumSHA256) {
		return Upload{}, false, ErrConflict
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Upload{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var existingID *uuid.UUID
	var existingHash string
	err = tx.QueryRow(ctx, `
		SELECT resource_id, request_hash FROM idempotency_keys
		WHERE owner_sub=$1 AND operation='create_upload' AND idempotency_key=$2 AND expires_at>now()
		FOR UPDATE`, ownerSub, idempotencyKey).Scan(&existingID, &existingHash)
	if err == nil {
		if existingID == nil || existingHash != requestHash {
			return Upload{}, false, ErrConflict
		}
		upload, getErr := scanUpload(tx.QueryRow(ctx, uploadSelect+` WHERE id=$1 AND owner_sub=$2`, *existingID, ownerSub))
		if getErr != nil {
			return Upload{}, false, getErr
		}
		if err = tx.Commit(ctx); err != nil {
			return Upload{}, false, err
		}
		return upload, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Upload{}, false, err
	}
	var active, activeLimit int
	err = tx.QueryRow(ctx, `
		SELECT count(u.id), COALESCE(q.active_multipart_uploads,2)
		FROM (SELECT $1::text AS owner_sub) x
		LEFT JOIN uploads u ON u.owner_sub=x.owner_sub AND u.status IN ('initiated','uploading') AND u.expires_at>now()
		LEFT JOIN account_quotas q ON q.owner_sub=x.owner_sub
		GROUP BY q.active_multipart_uploads`, ownerSub).Scan(&active, &activeLimit)
	if err != nil {
		return Upload{}, false, err
	}
	if active >= activeLimit {
		return Upload{}, false, ErrQuotaExceeded
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO quota_usage_daily(owner_sub,usage_date) VALUES ($1,CURRENT_DATE)
		ON CONFLICT (owner_sub,usage_date) DO NOTHING`, ownerSub); err != nil {
		return Upload{}, false, err
	}
	var uploadedToday, uploadLimit int64
	if err = tx.QueryRow(ctx, `
		SELECT u.uploaded_bytes, COALESCE(q.upload_bytes_per_day,21474836480)
		FROM quota_usage_daily u LEFT JOIN account_quotas q ON q.owner_sub=u.owner_sub
		WHERE u.owner_sub=$1 AND u.usage_date=CURRENT_DATE FOR UPDATE OF u`, ownerSub).
		Scan(&uploadedToday, &uploadLimit); err != nil {
		return Upload{}, false, err
	}
	if uploadedToday+req.ExpectedBytes > uploadLimit {
		return Upload{}, false, ErrQuotaExceeded
	}
	id := uuid.New()
	objectKey := fmt.Sprintf("uploads/%s/%s/source", ownerKey(ownerSub), id)
	partSize := int64(64 * 1024 * 1024)
	maxParts := int((req.ExpectedBytes + partSize - 1) / partSize)
	expires := time.Now().UTC().Add(24 * time.Hour)
	_, err = tx.Exec(ctx, `
		INSERT INTO uploads(id, owner_sub, bucket, object_key, expected_bytes, expected_checksum,
			part_size_bytes, max_parts, expires_at)
		VALUES ($1,$2,$3,$4,$5,lower($6),$7,$8,$9)`, id, ownerSub, bucket, objectKey,
		req.ExpectedBytes, req.ChecksumSHA256, partSize, maxParts, expires)
	if err != nil {
		return Upload{}, false, err
	}
	if _, err = tx.Exec(ctx, `UPDATE quota_usage_daily SET uploaded_bytes=uploaded_bytes+$2 WHERE owner_sub=$1 AND usage_date=CURRENT_DATE`, ownerSub, req.ExpectedBytes); err != nil {
		return Upload{}, false, err
	}
	if _, err = tx.Exec(ctx, `SELECT append_audit_event($1,'user','upload.initiate','upload',$2,NULL,jsonb_build_object('expected_bytes',$3))`, ownerSub, id.String(), req.ExpectedBytes); err != nil {
		return Upload{}, false, err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO idempotency_keys(owner_sub, operation, idempotency_key, request_hash, resource_id, expires_at)
		VALUES ($1,'create_upload',$2,$3,$4,now()+interval '24 hours')`, ownerSub, idempotencyKey, requestHash, id)
	if err != nil {
		return Upload{}, false, err
	}
	upload, err := scanUpload(tx.QueryRow(ctx, uploadSelect+` WHERE id=$1`, id))
	if err != nil {
		return Upload{}, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Upload{}, false, err
	}
	return upload, false, nil
}

func ownerKey(ownerSub string) string {
	return RequestHash([]byte(ownerSub))[:24]
}

const uploadSelect = `SELECT id, bucket, object_key, status::text, expected_bytes, expected_checksum,
	part_size_bytes, max_parts, expires_at, provider_upload_id FROM uploads`

func scanUpload(row rowScanner) (Upload, error) {
	var upload Upload
	err := row.Scan(&upload.ID, &upload.Bucket, &upload.ObjectKey, &upload.Status,
		&upload.ExpectedBytes, &upload.ExpectedChecksum, &upload.PartSizeBytes,
		&upload.MaxParts, &upload.ExpiresAt, &upload.ProviderUploadID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Upload{}, ErrNotFound
	}
	return upload, err
}

func (s *Store) GetUpload(ctx context.Context, ownerSub string, id uuid.UUID) (Upload, error) {
	return scanUpload(s.pool.QueryRow(ctx, uploadSelect+` WHERE id=$1 AND owner_sub=$2`, id, ownerSub))
}

func (s *Store) SetMultipartProviderID(ctx context.Context, ownerSub string, id uuid.UUID, providerID string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE uploads SET provider_upload_id=$3, status='uploading'
		WHERE id=$1 AND owner_sub=$2 AND status='initiated' AND expires_at>now()`, id, ownerSub, providerID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	return nil
}

func (s *Store) RecordSignedPart(ctx context.Context, ownerSub string, uploadID uuid.UUID, partNumber int, sizeBytes int64, checksumBase64 string) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `
		INSERT INTO quota_usage_daily(owner_sub,usage_date) VALUES ($1,CURRENT_DATE)
		ON CONFLICT (owner_sub,usage_date) DO NOTHING`, ownerSub); err != nil {
		return err
	}
	var used, limit int
	if err = tx.QueryRow(ctx, `
		SELECT u.signed_part_requests, COALESCE(q.signed_parts_per_day,10000)
		FROM quota_usage_daily u LEFT JOIN account_quotas q ON q.owner_sub=u.owner_sub
		WHERE u.owner_sub=$1 AND u.usage_date=CURRENT_DATE FOR UPDATE OF u`, ownerSub).Scan(&used, &limit); err != nil {
		return err
	}
	if used >= limit {
		return ErrQuotaExceeded
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO multipart_parts(upload_id, part_number, size_bytes, checksum_sha256_base64)
		SELECT u.id, $3, $4, $5 FROM uploads u
		WHERE u.id=$1 AND u.owner_sub=$2 AND u.status='uploading' AND u.expires_at>now()
		ON CONFLICT (upload_id,part_number) DO UPDATE SET size_bytes=EXCLUDED.size_bytes,
			checksum_sha256_base64=EXCLUDED.checksum_sha256_base64,signed_at=now()`,
		uploadID, ownerSub, partNumber, sizeBytes, checksumBase64)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	if _, err = tx.Exec(ctx, `UPDATE quota_usage_daily SET signed_part_requests=signed_part_requests+1 WHERE owner_sub=$1 AND usage_date=CURRENT_DATE`, ownerSub); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) ValidateMultipartCompletion(ctx context.Context, ownerSub string, uploadID uuid.UUID, parts []MultipartCompletionPart) error {
	rows, err := s.pool.Query(ctx, `SELECT p.part_number,p.size_bytes,p.checksum_sha256_base64,u.expected_bytes,u.part_size_bytes,u.max_parts
		FROM multipart_parts p JOIN uploads u ON u.id=p.upload_id
		WHERE p.upload_id=$1 AND u.owner_sub=$2 AND u.status='uploading' ORDER BY p.part_number`, uploadID, ownerSub)
	if err != nil {
		return err
	}
	defer rows.Close()
	type signedPart struct {
		number           int32
		size             int64
		checksum         string
		expected, policy int64
		maxParts         int
	}
	var signed []signedPart
	for rows.Next() {
		var item signedPart
		if err = rows.Scan(&item.number, &item.size, &item.checksum, &item.expected, &item.policy, &item.maxParts); err != nil {
			return err
		}
		signed = append(signed, item)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if len(signed) == 0 || len(signed) != len(parts) || len(parts) > signed[0].maxParts {
		return ErrConflict
	}
	total := int64(0)
	for index, item := range signed {
		part := parts[index]
		if item.number != int32(index+1) || part.PartNumber != item.number || part.ETag == "" || part.ChecksumBase64 != item.checksum {
			return ErrConflict
		}
		if index < len(signed)-1 && item.size != item.policy {
			return ErrConflict
		}
		total += item.size
	}
	if total != signed[0].expected {
		return ErrConflict
	}
	return nil
}

func (s *Store) CompleteUpload(ctx context.Context, ownerSub string, id uuid.UUID, idempotencyKey, requestHash string, actualBytes int64, verifiedChecksum string) (bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var existingID *uuid.UUID
	var existingHash string
	err = tx.QueryRow(ctx, `SELECT resource_id,request_hash FROM idempotency_keys
		WHERE owner_sub=$1 AND operation='complete_upload' AND idempotency_key=$2 AND expires_at>now()
		FOR UPDATE`, ownerSub, idempotencyKey).Scan(&existingID, &existingHash)
	if err == nil {
		if existingID == nil || *existingID != id || existingHash != requestHash {
			return false, ErrConflict
		}
		var completed bool
		if err = tx.QueryRow(ctx, `SELECT status='completed' FROM uploads WHERE id=$1 AND owner_sub=$2`, id, ownerSub).Scan(&completed); err != nil {
			return false, err
		}
		if !completed {
			return false, ErrConflict
		}
		if err = tx.Commit(ctx); err != nil {
			return false, err
		}
		return true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE uploads SET status='completed', actual_bytes=$3, verified_checksum=lower($4), completed_at=now()
		WHERE id=$1 AND owner_sub=$2 AND status='uploading' AND expires_at>now()
			AND expected_bytes=$3 AND expected_checksum=lower($4)`, id, ownerSub, actualBytes, verifiedChecksum)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() != 1 {
		return false, ErrConflict
	}
	if _, err = tx.Exec(ctx, `UPDATE multipart_parts SET completed_at=now() WHERE upload_id=$1`, id); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `SELECT append_audit_event($1,'user','upload.complete','upload',$2,NULL,jsonb_build_object('bytes',$3))`, ownerSub, id.String(), actualBytes); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO idempotency_keys(owner_sub,operation,idempotency_key,request_hash,resource_id,expires_at)
		VALUES ($1,'complete_upload',$2,$3,$4,now()+interval '24 hours')`, ownerSub, idempotencyKey, requestHash, id); err != nil {
		return false, err
	}
	return false, tx.Commit(ctx)
}

func (s *Store) CancelJob(ctx context.Context, ownerSub string, id uuid.UUID, expectedVersion int64, idempotencyKey, requestHash string) (Job, bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Job{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var existingID *uuid.UUID
	var existingHash string
	err = tx.QueryRow(ctx, `SELECT resource_id,request_hash FROM idempotency_keys
		WHERE owner_sub=$1 AND operation='cancel_job' AND idempotency_key=$2 AND expires_at>now()
		FOR UPDATE`, ownerSub, idempotencyKey).Scan(&existingID, &existingHash)
	if err == nil {
		if existingID == nil || *existingID != id || existingHash != requestHash {
			return Job{}, false, ErrConflict
		}
		job, getErr := scanJob(tx.QueryRow(ctx, jobSelect+` WHERE id=$1 AND owner_sub=$2`, id, ownerSub))
		if getErr != nil {
			return Job{}, false, getErr
		}
		if err = tx.Commit(ctx); err != nil {
			return Job{}, false, err
		}
		return job, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, err
	}
	if _, err = tx.Exec(ctx, `SELECT set_config('app.actor',$1,true)`, ownerSub); err != nil {
		return Job{}, false, err
	}
	var status string
	if err = tx.QueryRow(ctx, `SELECT status::text FROM jobs WHERE id=$1 AND owner_sub=$2 FOR UPDATE`, id, ownerSub).Scan(&status); errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, ErrNotFound
	} else if err != nil {
		return Job{}, false, err
	}
	target := "cancelled"
	if status == "running" {
		target = "cancel_requested"
	} else if status == "succeeded" || status == "failed" || status == "cancelled" || status == "expired" {
		return Job{}, false, ErrConflict
	}
	tag, err := tx.Exec(ctx, `UPDATE jobs SET status=$4,
		terminal_at=CASE WHEN $4='cancelled' THEN now() ELSE terminal_at END,
		expires_at=CASE WHEN $4='cancelled' THEN now()+interval '7 days' ELSE expires_at END
		WHERE id=$1 AND owner_sub=$2 AND optimistic_version=$3`, id, ownerSub, expectedVersion, target)
	if err != nil {
		return Job{}, false, err
	}
	if tag.RowsAffected() != 1 {
		return Job{}, false, ErrConflict
	}
	if target == "cancelled" {
		if err = stopSiblingTasksTx(ctx, tx, id, uuid.Nil); err != nil {
			return Job{}, false, err
		}
		if err = retainJobStorageTx(ctx, tx, id); err != nil {
			return Job{}, false, err
		}
	}
	eventType := "job.cancel_requested"
	if target == "cancelled" {
		eventType = "job.cancelled"
	}
	if _, err = tx.Exec(ctx, `SELECT append_progress_event($1,$2,$3,jsonb_build_object('status',$3))`, id, eventType, target); err != nil {
		return Job{}, false, err
	}
	if _, err = tx.Exec(ctx, `SELECT append_audit_event($1,'user','job.cancel','job',$2,NULL,jsonb_build_object('result_status',$3))`, ownerSub, id.String(), target); err != nil {
		return Job{}, false, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO idempotency_keys(owner_sub,operation,idempotency_key,request_hash,resource_id,expires_at)
		VALUES ($1,'cancel_job',$2,$3,$4,now()+interval '24 hours')`, ownerSub, idempotencyKey, requestHash, id); err != nil {
		return Job{}, false, err
	}
	job, err := scanJob(tx.QueryRow(ctx, jobSelect+` WHERE id=$1`, id))
	if err != nil {
		return Job{}, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Job{}, false, err
	}
	return job, false, nil
}

func (s *Store) RegisterWorker(ctx context.Context, id uuid.UUID, name, identity, gpuUUID, gpuType, imageDigest, driver string, capabilities []string, maxConcurrency int, freeScratchBytes int64, preflight json.RawMessage) error {
	if len(preflight) == 0 {
		preflight = json.RawMessage(`{}`)
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO workers(id,name,identity,gpu_uuid,gpu_type,image_digest,driver_version,capabilities,max_concurrency,free_scratch_bytes,last_seen_at,last_preflight)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,now(),$11)
		ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, identity=EXCLUDED.identity,
			gpu_uuid=EXCLUDED.gpu_uuid, gpu_type=EXCLUDED.gpu_type, image_digest=EXCLUDED.image_digest,
			driver_version=EXCLUDED.driver_version, capabilities=EXCLUDED.capabilities, max_concurrency=EXCLUDED.max_concurrency,
			free_scratch_bytes=EXCLUDED.free_scratch_bytes,
			last_seen_at=now(), last_preflight=EXCLUDED.last_preflight`,
		id, name, identity, gpuUUID, gpuType, imageDigest, driver, capabilities, maxConcurrency, freeScratchBytes, preflight)
	return err
}

func (s *Store) RecordDeliveryDeferral(ctx context.Context, taskID, workerID uuid.UUID, reason string) error {
	delay, allowed := capacityDeferralDelay(reason)
	if !allowed {
		return ErrConflict
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var jobID uuid.UUID
	var capability, jobStage string
	err = tx.QueryRow(ctx, `SELECT t.job_id,t.required_capability,j.status::text FROM tasks t
		JOIN jobs j ON j.id=t.job_id JOIN workers w ON w.id=$2
		WHERE t.id=$1 AND t.status IN ('pending','queued','retry_wait') FOR UPDATE OF t`, taskID, workerID).
		Scan(&jobID, &capability, &jobStage)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrConflict
	}
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE tasks SET delivery_deferral_count=delivery_deferral_count+1,
		available_at=now()+$2::interval WHERE id=$1`, taskID, delay.String()); err != nil {
		return err
	}
	payload, _ := json.Marshal(map[string]any{"task_id": taskID, "job_id": jobID, "required_capability": capability})
	if _, err = tx.Exec(ctx, `INSERT INTO outbox_events(aggregate_type,aggregate_id,event_type,subject,payload,available_at)
		VALUES ('task',$1,'task.capacity_ready',$2,$3,now()+$4::interval)`, taskID, "tasks."+capability, payload, delay.String()); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `SELECT append_progress_event($1,'task.capacity_deferred',$2,jsonb_build_object('reason',$3))`, jobID, jobStage, reason); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func capacityDeferralDelay(reason string) (time.Duration, bool) {
	switch reason {
	case "blocked_external_gpu", "worker_capacity", "retry_other_worker":
		return 30 * time.Second, true
	case "account_active_limit":
		return 2 * time.Minute, true
	case "daily_gpu_quota":
		return time.Hour, true
	default:
		return 0, false
	}
}

func (s *Store) ReleaseClaimForCapacity(ctx context.Context, taskID, attemptID, workerID uuid.UUID, fencingToken int64, reason string) error {
	if reason != "blocked_external_gpu" {
		return ErrConflict
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var jobID uuid.UUID
	var capability string
	var activeAttempt *uuid.UUID
	var activeToken int64
	if err = tx.QueryRow(ctx, `SELECT job_id,required_capability,active_attempt_id,fencing_token FROM tasks WHERE id=$1 FOR UPDATE`, taskID).
		Scan(&jobID, &capability, &activeAttempt, &activeToken); errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	if activeAttempt == nil || *activeAttempt != attemptID || activeToken != fencingToken {
		return ErrLeaseLost
	}
	var attemptWorker uuid.UUID
	if err = tx.QueryRow(ctx, `SELECT worker_id FROM task_attempts WHERE id=$1 AND task_id=$2 FOR UPDATE`, attemptID, taskID).Scan(&attemptWorker); err != nil {
		return err
	}
	if attemptWorker != workerID {
		return ErrLeaseLost
	}
	if _, err = tx.Exec(ctx, `UPDATE tasks SET status='queued',active_attempt_id=NULL,lease_expires_at=NULL,last_heartbeat_at=NULL,
		fencing_token=fencing_token+1,optimistic_version=optimistic_version+1,
		execution_attempt_count=GREATEST(0,execution_attempt_count-1),delivery_deferral_count=delivery_deferral_count+1,
		available_at=now() WHERE id=$1`, taskID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE storage_reservations SET released_at=now()
		WHERE task_id=$1 AND scope='host' AND released_at IS NULL`, taskID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE task_attempts SET status='capacity_deferred',blocked_reason=$3,finished_at=now()
		WHERE id=$1 AND task_id=$2`, attemptID, taskID, reason); err != nil {
		return err
	}
	payload, _ := json.Marshal(map[string]any{"task_id": taskID, "job_id": jobID, "required_capability": capability})
	if _, err = tx.Exec(ctx, `INSERT INTO outbox_events(aggregate_type,aggregate_id,event_type,subject,payload,available_at)
		VALUES ('task',$1,'task.capacity_ready',$2,$3,now()+interval '30 seconds')`, taskID, "tasks."+capability, payload); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `SELECT append_progress_event($1,'task.capacity_deferred','running',jsonb_build_object('reason',$2))`, jobID, reason); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) AuthorizeWorkerIdentity(ctx context.Context, workerID uuid.UUID, identity string) error {
	var allowed bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM workers WHERE id=$1 AND identity=$2 AND scheduling_enabled)`, workerID, identity).Scan(&allowed); err != nil {
		return err
	}
	if !allowed {
		return ErrNotFound
	}
	return nil
}

func (s *Store) PublishOutboxBatch(ctx context.Context, limit int, publish func(context.Context, OutboxEvent) error) (int, error) {
	if limit < 1 || limit > 100 {
		limit = 25
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `
		SELECT id, aggregate_type, aggregate_id, event_type, subject, payload, publish_attempts
		FROM outbox_events
		WHERE status IN ('pending','failed') AND available_at<=now()
		ORDER BY created_at
		LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, err
	}
	var events []OutboxEvent
	for rows.Next() {
		var event OutboxEvent
		if err = rows.Scan(&event.ID, &event.AggregateType, &event.AggregateID, &event.EventType,
			&event.Subject, &event.Payload, &event.PublishAttempts); err != nil {
			rows.Close()
			return 0, err
		}
		events = append(events, event)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return 0, err
	}
	delivered := 0
	var publishErr error
	for _, event := range events {
		if _, err = tx.Exec(ctx, `UPDATE outbox_events SET status='publishing', locked_at=now(), publish_attempts=publish_attempts+1 WHERE id=$1`, event.ID); err != nil {
			return 0, err
		}
		if err = publish(ctx, event); err != nil {
			message := err.Error()
			if len(message) > 1024 {
				message = message[:1024]
			}
			delay := outboxRetryDelay(event.PublishAttempts + 1)
			if _, updateErr := tx.Exec(ctx, `UPDATE outbox_events SET status='failed',locked_at=NULL,
				available_at=now()+$2::interval,last_error=$3 WHERE id=$1`, event.ID, delay.String(), message); updateErr != nil {
				return delivered, updateErr
			}
			publishErr = err
			break
		}
		if _, err = tx.Exec(ctx, `UPDATE outbox_events SET status='delivered', delivered_at=now(), last_error=NULL WHERE id=$1`, event.ID); err != nil {
			return delivered, err
		}
		delivered++
	}
	if err = tx.Commit(ctx); err != nil {
		return delivered, err
	}
	return delivered, publishErr
}

func outboxRetryDelay(attempt int) time.Duration {
	delays := []time.Duration{time.Second, 5 * time.Second, 30 * time.Second, 2 * time.Minute}
	if attempt < 1 {
		attempt = 1
	}
	if attempt > len(delays) {
		attempt = len(delays)
	}
	return delays[attempt-1]
}

func (s *Store) RebuildRunnableOutbox(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO outbox_events(aggregate_type,aggregate_id,event_type,subject,payload,available_at)
		SELECT 'task', t.id, 'task.ready', 'tasks.'||t.required_capability,
			jsonb_build_object('task_id',t.id,'job_id',t.job_id,'required_capability',t.required_capability),
			GREATEST(t.available_at,now())
		FROM tasks t
		WHERE t.status IN ('pending','queued','retry_wait')`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *Store) ReapExpiredLeases(ctx context.Context, batch int) (int, error) {
	if batch < 1 || batch > 500 {
		batch = 100
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `
		SELECT t.id, t.job_id, t.active_attempt_id, t.execution_attempt_count, t.max_execution_attempts, j.status::text, t.stage
		FROM tasks t JOIN jobs j ON j.id=t.job_id
		WHERE t.status='running' AND t.lease_expires_at<=now()
		ORDER BY t.lease_expires_at LIMIT $1 FOR UPDATE OF j,t SKIP LOCKED`, batch)
	if err != nil {
		return 0, err
	}
	type expired struct {
		taskID, jobID, attemptID uuid.UUID
		attempts, maxAttempts    int
		jobStatus                string
		stage                    string
	}
	var expiredTasks []expired
	for rows.Next() {
		var item expired
		if err = rows.Scan(&item.taskID, &item.jobID, &item.attemptID, &item.attempts, &item.maxAttempts, &item.jobStatus, &item.stage); err != nil {
			rows.Close()
			return 0, err
		}
		expiredTasks = append(expiredTasks, item)
	}
	rows.Close()
	for _, item := range expiredTasks {
		if _, err = tx.Exec(ctx, `UPDATE storage_reservations SET released_at=now() WHERE task_id=$1 AND scope='host' AND released_at IS NULL`, item.taskID); err != nil {
			return 0, err
		}
		_, err = tx.Exec(ctx, `UPDATE task_attempts SET status='stale', finished_at=now(), error_code='lease_expired'
			WHERE id=$1 AND status IN ('claimed','running')`, item.attemptID)
		if err != nil {
			return 0, err
		}
		if item.stage == "conformer" {
			if err = chargeGPUTimeTx(ctx, tx, item.jobID, item.attemptID); err != nil {
				return 0, err
			}
		}
		if item.jobStatus == "cancel_requested" {
			tag, updateErr := tx.Exec(ctx, `UPDATE tasks SET status='cancelled',active_attempt_id=NULL,lease_expires_at=NULL,
				fencing_token=fencing_token+1,optimistic_version=optimistic_version+1
				WHERE id=$1 AND status='running' AND active_attempt_id=$2`, item.taskID, item.attemptID)
			if updateErr != nil {
				return 0, updateErr
			}
			if tag.RowsAffected() != 1 {
				continue
			}
			if _, err = tx.Exec(ctx, `UPDATE jobs SET status='cancelled',terminal_at=now(),expires_at=now()+interval '7 days'
				WHERE id=$1 AND status='cancel_requested'`, item.jobID); err != nil {
				return 0, err
			}
			if err = stopSiblingTasksTx(ctx, tx, item.jobID, item.taskID); err != nil {
				return 0, err
			}
			if err = retainJobStorageTx(ctx, tx, item.jobID); err != nil {
				return 0, err
			}
			if _, err = tx.Exec(ctx, `SELECT append_progress_event($1,'job.cancelled','cancelled','{}'::jsonb)`, item.jobID); err != nil {
				return 0, err
			}
			continue
		}
		if item.attempts >= item.maxAttempts {
			tag, updateErr := tx.Exec(ctx, `UPDATE tasks SET status='failed', active_attempt_id=NULL, lease_expires_at=NULL, fencing_token=fencing_token+1, optimistic_version=optimistic_version+1
				WHERE id=$1 AND status='running' AND active_attempt_id=$2`, item.taskID, item.attemptID)
			if updateErr != nil {
				return 0, updateErr
			}
			if tag.RowsAffected() != 1 {
				continue
			}
			_, _ = tx.Exec(ctx, `SELECT set_config('app.reason_code','execution_attempts_exhausted',true)`)
			_, err = tx.Exec(ctx, `UPDATE jobs SET status='failed', failure_code='execution_attempts_exhausted',
				terminal_at=now(),expires_at=now()+interval '7 days'
				WHERE id=$1 AND status IN ('queued','running','finalizing','cancel_requested')`, item.jobID)
			if err != nil {
				return 0, err
			}
			if err = stopSiblingTasksTx(ctx, tx, item.jobID, item.taskID); err != nil {
				return 0, err
			}
			if err = retainJobStorageTx(ctx, tx, item.jobID); err != nil {
				return 0, err
			}
			if _, err = tx.Exec(ctx, `SELECT append_progress_event($1,'task.failed','failed',jsonb_build_object('task_id',$2::text,'code','execution_attempts_exhausted'))`, item.jobID, item.taskID); err != nil {
				return 0, err
			}
			continue
		}
		tag, err := tx.Exec(ctx, `
			UPDATE tasks SET status='retry_wait', active_attempt_id=NULL, lease_expires_at=NULL,
				fencing_token=fencing_token+1, optimistic_version=optimistic_version+1, available_at=now()
			WHERE id=$1 AND status='running' AND active_attempt_id=$2`, item.taskID, item.attemptID)
		if err != nil {
			return 0, err
		}
		if tag.RowsAffected() != 1 {
			continue
		}
		if _, err = tx.Exec(ctx, `SELECT append_progress_event($1,'task.retry_scheduled','retry_wait',jsonb_build_object('task_id',$2::text,'code','lease_expired'))`, item.jobID, item.taskID); err != nil {
			return 0, err
		}
		if _, err = tx.Exec(ctx, `SELECT append_audit_event('system','system','task.retry','task',$1,NULL,jsonb_build_object('code','lease_expired','attempt',$2))`, item.taskID.String(), item.attempts); err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(expiredTasks), nil
}

func (s *Store) FailDeliveryExhausted(ctx context.Context, taskID uuid.UUID) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var jobID uuid.UUID
	var jobStatus string
	if err = tx.QueryRow(ctx, `SELECT j.id,j.status::text FROM jobs j JOIN tasks t ON t.job_id=j.id
		WHERE t.id=$1 FOR UPDATE OF j`, taskID).Scan(&jobID, &jobStatus); errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	var taskStatus string
	var taskStage string
	var activeAttempt *uuid.UUID
	var leaseExpiresAt *time.Time
	err = tx.QueryRow(ctx, `SELECT status::text,stage,active_attempt_id,lease_expires_at FROM tasks WHERE id=$1 FOR UPDATE`, taskID).
		Scan(&taskStatus, &taskStage, &activeAttempt, &leaseExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if taskStatus == "succeeded" || taskStatus == "failed" || taskStatus == "cancelled" || taskStatus == "split" || taskStatus == "quarantined" {
		return tx.Commit(ctx)
	}
	if taskStatus == "running" && leaseExpiresAt != nil && leaseExpiresAt.After(time.Now()) {
		// Delivery exhaustion must not fence a healthy execution whose
		// authoritative database lease is still being renewed. The advisory
		// consumer will retry until this attempt commits or its lease is reaped.
		return ErrConflict
	}
	if jobStatus == "cancel_requested" {
		if activeAttempt != nil {
			if _, err = tx.Exec(ctx, `UPDATE task_attempts SET status='cancelled',finished_at=now(),error_code='job_cancelled'
				WHERE id=$1 AND status IN ('claimed','running')`, *activeAttempt); err != nil {
				return err
			}
			if taskStage == "conformer" {
				if err = chargeGPUTimeTx(ctx, tx, jobID, *activeAttempt); err != nil {
					return err
				}
			}
		}
		if _, err = tx.Exec(ctx, `UPDATE tasks SET status='cancelled',active_attempt_id=NULL,lease_expires_at=NULL,
			fencing_token=fencing_token+1,optimistic_version=optimistic_version+1 WHERE id=$1`, taskID); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `UPDATE jobs SET status='cancelled',terminal_at=now(),expires_at=now()+interval '7 days'
			WHERE id=$1 AND status='cancel_requested'`, jobID); err != nil {
			return err
		}
		if err = stopSiblingTasksTx(ctx, tx, jobID, taskID); err != nil {
			return err
		}
		if err = retainJobStorageTx(ctx, tx, jobID); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `SELECT append_progress_event($1,'job.cancelled','cancelled','{}'::jsonb)`, jobID); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	if activeAttempt != nil {
		if _, err = tx.Exec(ctx, `UPDATE task_attempts SET status='failed',finished_at=now(),error_code='delivery_budget_exhausted'
			WHERE id=$1 AND status IN ('claimed','running')`, *activeAttempt); err != nil {
			return err
		}
		if taskStage == "conformer" {
			if err = chargeGPUTimeTx(ctx, tx, jobID, *activeAttempt); err != nil {
				return err
			}
		}
	}
	_, err = tx.Exec(ctx, `UPDATE tasks SET status='failed', active_attempt_id=NULL, lease_expires_at=NULL, fencing_token=fencing_token+1, optimistic_version=optimistic_version+1 WHERE id=$1 AND status NOT IN ('succeeded','failed','cancelled','split','quarantined')`, taskID)
	if err != nil {
		return err
	}
	_, _ = tx.Exec(ctx, `SELECT set_config('app.reason_code','delivery_budget_exhausted',true)`)
	_, err = tx.Exec(ctx, `UPDATE jobs SET status='failed', failure_code='delivery_budget_exhausted',
		terminal_at=now(),expires_at=now()+interval '7 days'
		WHERE id=$1 AND status IN ('queued','running','finalizing','cancel_requested')`, jobID)
	if err != nil {
		return err
	}
	if err = stopSiblingTasksTx(ctx, tx, jobID, taskID); err != nil {
		return err
	}
	if err = retainJobStorageTx(ctx, tx, jobID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `SELECT append_progress_event($1,'task.delivery_exhausted','failed',jsonb_build_object('code','delivery_budget_exhausted'))`, jobID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
