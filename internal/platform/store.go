package platform

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound            = errors.New("not found")
	ErrConflict            = errors.New("conflict")
	ErrLeaseLost           = errors.New("attempt lease or fencing token is stale")
	ErrCapacityUnavailable = errors.New("storage capacity reservation unavailable")
	ErrQuotaExceeded       = errors.New("account quota exceeded")
	ErrActiveJobLimit      = errors.New("account active job limit reached")
	ErrDailyGPUQuota       = errors.New("daily GPU quota exhausted")
)

type Store struct {
	pool                 *pgxpool.Pool
	globalStorageCeiling int64
}

func OpenStore(ctx context.Context, databaseURL string, globalStorageCeiling int64) (*Store, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}
	config.MaxConns = 30
	config.MinConns = 2
	config.MaxConnLifetime = 45 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute
	config.HealthCheckPeriod = 30 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	return &Store{pool: pool, globalStorageCeiling: globalStorageCeiling}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *Store) CreateJob(ctx context.Context, ownerSub, idempotencyKey, requestHash string, req CreateJobRequest) (Job, bool, error) {
	if req.RequestedConformers < 1 || req.RequestedConformers > 10 || req.Preset == "" || req.AlgorithmVersion == "" {
		return Job{}, false, ErrConflict
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Job{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var existingID *uuid.UUID
	var existingHash string
	err = tx.QueryRow(ctx, `
		SELECT resource_id, request_hash
		FROM idempotency_keys
		WHERE owner_sub=$1 AND operation='create_job' AND idempotency_key=$2 AND expires_at > now()
		FOR UPDATE`, ownerSub, idempotencyKey).Scan(&existingID, &existingHash)
	if err == nil {
		if existingHash != requestHash || existingID == nil {
			return Job{}, false, ErrConflict
		}
		job, getErr := scanJob(tx.QueryRow(ctx, jobSelect+` WHERE id=$1 AND owner_sub=$2`, *existingID, ownerSub))
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

	var uploadStatus, inputBucket, inputObjectKey, inputChecksum string
	var inputBytes int64
	var uploadExpiresAt time.Time
	err = tx.QueryRow(ctx, `
		SELECT status::text, bucket, object_key, expected_bytes, expected_checksum, expires_at
		FROM uploads WHERE id=$1 AND owner_sub=$2 FOR UPDATE`, req.InputUploadID, ownerSub).
		Scan(&uploadStatus, &inputBucket, &inputObjectKey, &inputBytes, &inputChecksum, &uploadExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, ErrNotFound
	}
	if err != nil {
		return Job{}, false, err
	}
	if uploadStatus != "completed" {
		return Job{}, false, fmt.Errorf("%w: input upload is %s", ErrConflict, uploadStatus)
	}
	if req.ParentJobID != nil {
		var parentUploadID uuid.UUID
		var parentStatus string
		if err = tx.QueryRow(ctx, `SELECT input_upload_id,status::text FROM jobs
			WHERE id=$1 AND owner_sub=$2 FOR UPDATE`, *req.ParentJobID, ownerSub).
			Scan(&parentUploadID, &parentStatus); errors.Is(err, pgx.ErrNoRows) {
			return Job{}, false, ErrNotFound
		} else if err != nil {
			return Job{}, false, err
		}
		if parentUploadID != req.InputUploadID || (parentStatus != "succeeded" && parentStatus != "failed") {
			return Job{}, false, fmt.Errorf("%w: parent job is not rerunnable", ErrConflict)
		}
	} else {
		if !uploadExpiresAt.After(time.Now()) {
			return Job{}, false, fmt.Errorf("%w: input upload has expired", ErrConflict)
		}
		var alreadyUsed bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM jobs WHERE input_upload_id=$1)`, req.InputUploadID).Scan(&alreadyUsed); err != nil {
			return Job{}, false, err
		}
		if alreadyUsed {
			return Job{}, false, fmt.Errorf("%w: input upload already belongs to a job", ErrConflict)
		}
	}
	// Admission estimates are server-owned. A browser-provided estimate must
	// never be able to under-reserve shared storage.
	req.Reservation = serverReservation(inputBytes, req.RequestedConformers)
	if req.Reservation.PeakBytes() <= 0 || req.Reservation.PeakBytes() > s.globalStorageCeiling {
		return Job{}, false, ErrCapacityUnavailable
	}

	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('global-storage-reservations'))`); err != nil {
		return Job{}, false, err
	}
	var activeGlobal int64
	if err = tx.QueryRow(ctx, `
		SELECT COALESCE(sum(reserved_bytes), 0)::bigint FROM storage_reservations
		WHERE scope='global' AND released_at IS NULL`).Scan(&activeGlobal); err != nil {
		return Job{}, false, err
	}
	if activeGlobal+req.Reservation.PeakBytes() > s.globalStorageCeiling {
		return Job{}, false, ErrCapacityUnavailable
	}
	var activeUser, userLimit int64
	if err = tx.QueryRow(ctx, `
		SELECT COALESCE(sum(r.reserved_bytes),0)::bigint, COALESCE(q.retained_bytes,107374182400)
		FROM (SELECT $1::text AS owner_sub) u
		LEFT JOIN storage_reservations r ON r.scope='user' AND r.scope_key=u.owner_sub
			AND r.released_at IS NULL
		LEFT JOIN account_quotas q ON q.owner_sub=u.owner_sub
		GROUP BY q.retained_bytes`, ownerSub).Scan(&activeUser, &userLimit); err != nil {
		return Job{}, false, err
	}
	if activeUser+req.Reservation.PeakBytes() > userLimit {
		return Job{}, false, ErrQuotaExceeded
	}
	var queuedJobs, queuedLimit int
	err = tx.QueryRow(ctx, `
		SELECT
			count(*) FILTER (WHERE j.status='queued'),
			COALESCE(q.queued_jobs, 1)
		FROM (SELECT $1::text AS owner_sub) u
		LEFT JOIN jobs j ON j.owner_sub=u.owner_sub
		LEFT JOIN account_quotas q ON q.owner_sub=u.owner_sub
		GROUP BY q.queued_jobs`, ownerSub).Scan(&queuedJobs, &queuedLimit)
	if err != nil {
		return Job{}, false, err
	}
	if queuedJobs >= queuedLimit {
		return Job{}, false, ErrQuotaExceeded
	}

	jobID, taskID, globalReservationID, userReservationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	if _, err = tx.Exec(ctx, `SELECT set_config('app.actor', $1, true)`, ownerSub); err != nil {
		return Job{}, false, err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO jobs(id, parent_job_id, owner_sub, status, preset, requested_conformers, algorithm_version, input_upload_id)
		VALUES ($1,$2,$3,'uploading',$4,$5,$6,$7)`,
		jobID, req.ParentJobID, ownerSub, req.Preset, req.RequestedConformers, req.AlgorithmVersion, req.InputUploadID)
	if err != nil {
		return Job{}, false, err
	}
	if _, err = tx.Exec(ctx, `UPDATE jobs SET status='validating' WHERE id=$1`, jobID); err != nil {
		return Job{}, false, err
	}
	taskSpec, _ := json.Marshal(map[string]any{
		"schema_version": "1", "job_id": jobID, "task_id": taskID, "stage": "profile",
		"input_upload_id": req.InputUploadID, "limits_profile": "alpha-v1",
		"preset": req.Preset, "requested_conformers": req.RequestedConformers,
		"enumerate_tautomers":     req.Preset == "enumerate",
		"input":                   map[string]any{"bucket": inputBucket, "object_key": inputObjectKey, "size_bytes": inputBytes, "checksum_sha256": inputChecksum},
		"predicted_scratch_bytes": req.Reservation.PredictedWorkingSetBytes,
		"predicted_output_bytes":  boundedStageBytes(req.Reservation.PredictedWorkingSetBytes),
	})
	_, err = tx.Exec(ctx, `
		INSERT INTO tasks(id, job_id, stage, shard_index, status, required_capability, task_spec)
		VALUES ($1,$2,'profile',0,'queued','cpu',$3)`, taskID, jobID, taskSpec)
	if err != nil {
		return Job{}, false, err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO storage_reservations(
			id, job_id, task_id, owner_sub, scope, scope_key,
			retained_input_bytes, working_set_bytes, final_output_bytes,
			finalization_margin_bytes, retry_margin_bytes, multipart_margin_bytes, expires_at)
		VALUES ($1,$2,$3,$4,'global','fleet',$5,$6,$7,$8,$9,$10,now()+interval '8 days')`,
		globalReservationID, jobID, taskID, ownerSub,
		req.Reservation.RetainedInputBytes, req.Reservation.PredictedWorkingSetBytes,
		req.Reservation.PredictedFinalBytes, req.Reservation.FinalizationMarginBytes,
		req.Reservation.RetryMarginBytes, req.Reservation.MultipartMarginBytes)
	if err != nil {
		return Job{}, false, err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO storage_reservations(
			id, job_id, task_id, owner_sub, scope, scope_key,
			retained_input_bytes, working_set_bytes, final_output_bytes,
			finalization_margin_bytes, retry_margin_bytes, multipart_margin_bytes, expires_at)
		VALUES ($1,$2,$3,$4,'user',$4,$5,$6,$7,$8,$9,$10,now()+interval '8 days')`,
		userReservationID, jobID, taskID, ownerSub,
		req.Reservation.RetainedInputBytes, req.Reservation.PredictedWorkingSetBytes,
		req.Reservation.PredictedFinalBytes, req.Reservation.FinalizationMarginBytes,
		req.Reservation.RetryMarginBytes, req.Reservation.MultipartMarginBytes)
	if err != nil {
		return Job{}, false, err
	}
	payload, _ := json.Marshal(map[string]any{"task_id": taskID, "job_id": jobID, "required_capability": "cpu"})
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events(aggregate_type, aggregate_id, event_type, subject, payload)
		VALUES ('task',$1,'task.ready','tasks.cpu',$2)`, taskID, payload)
	if err != nil {
		return Job{}, false, err
	}
	if _, err = tx.Exec(ctx, `SELECT append_progress_event($1,'job.queued','queued',jsonb_build_object('status','queued'))`, jobID); err != nil {
		return Job{}, false, err
	}
	auditAction := "job.create"
	auditDetail := map[string]any{"preset": req.Preset}
	if req.ParentJobID != nil {
		auditAction = "job.rerun"
		auditDetail["parent_job_id"] = req.ParentJobID
	}
	auditJSON, _ := json.Marshal(auditDetail)
	if _, err = tx.Exec(ctx, `SELECT append_audit_event($1,'user',$2,'job',$3,NULL,$4)`, ownerSub, auditAction, jobID.String(), auditJSON); err != nil {
		return Job{}, false, err
	}
	if _, err = tx.Exec(ctx, `UPDATE jobs SET status='queued' WHERE id=$1`, jobID); err != nil {
		return Job{}, false, err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO idempotency_keys(owner_sub, operation, idempotency_key, request_hash, resource_id, expires_at)
		VALUES ($1,'create_job',$2,$3,$4,now()+interval '24 hours')`, ownerSub, idempotencyKey, requestHash, jobID)
	if err != nil {
		return Job{}, false, err
	}
	job, err := scanJob(tx.QueryRow(ctx, jobSelect+` WHERE id=$1`, jobID))
	if err != nil {
		return Job{}, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Job{}, false, err
	}
	return job, false, nil
}

func boundedStageBytes(value int64) int64 {
	if value < 1<<30 {
		return 1 << 30
	}
	if value > 100<<30 {
		return 100 << 30
	}
	return value
}

func (s *Store) RerunJob(ctx context.Context, ownerSub string, parentID uuid.UUID, idempotencyKey, requestHash string) (Job, bool, error) {
	var req CreateJobRequest
	var status string
	if err := s.pool.QueryRow(ctx, `SELECT input_upload_id,preset,requested_conformers,algorithm_version,status::text
		FROM jobs WHERE id=$1 AND owner_sub=$2`, parentID, ownerSub).
		Scan(&req.InputUploadID, &req.Preset, &req.RequestedConformers, &req.AlgorithmVersion, &status); errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, ErrNotFound
	} else if err != nil {
		return Job{}, false, err
	}
	if status != "succeeded" && status != "failed" {
		return Job{}, false, fmt.Errorf("%w: parent job is not rerunnable", ErrConflict)
	}
	req.ParentJobID = &parentID
	return s.CreateJob(ctx, ownerSub, idempotencyKey, requestHash, req)
}

const jobSelect = `SELECT id, parent_job_id, owner_sub, status::text, preset, requested_conformers,
	algorithm_version, optimistic_version, cleanup_pending, failure_code, failure_detail, created_at, updated_at, expires_at FROM jobs`

type rowScanner interface{ Scan(dest ...any) error }

func scanJob(row rowScanner) (Job, error) {
	var job Job
	err := row.Scan(&job.ID, &job.ParentJobID, &job.OwnerSub, &job.Status, &job.Preset,
		&job.RequestedConformers, &job.AlgorithmVersion, &job.OptimisticVersion,
		&job.CleanupPending, &job.FailureCode, &job.FailureDetail, &job.CreatedAt, &job.UpdatedAt, &job.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	return job, err
}

func (s *Store) GetJob(ctx context.Context, ownerSub string, id uuid.UUID) (Job, error) {
	return scanJob(s.pool.QueryRow(ctx, jobSelect+` WHERE id=$1 AND owner_sub=$2`, id, ownerSub))
}

func (s *Store) ListJobs(ctx context.Context, ownerSub string, limit int) ([]Job, error) {
	if limit < 1 || limit > 100 {
		limit = 25
	}
	rows, err := s.pool.Query(ctx, jobSelect+` WHERE owner_sub=$1 ORDER BY created_at DESC LIMIT $2`, ownerSub, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := make([]Job, 0, limit)
	for rows.Next() {
		job, scanErr := scanJob(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s *Store) ClaimTask(ctx context.Context, taskID, workerID uuid.UUID, lease time.Duration) (AttemptClaim, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return AttemptClaim{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var claim AttemptClaim
	var status string
	var attemptCount int
	var ownerSub string
	var jobStatus string
	var predictedScratch, predictedOutput int64
	var retryAvoidWorker *uuid.UUID
	err = tx.QueryRow(ctx, `
		SELECT t.job_id, t.status::text, t.fencing_token, t.optimistic_version, t.execution_attempt_count,
			t.required_capability, t.task_spec, j.owner_sub, j.status::text,
			COALESCE((t.task_spec->>'predicted_scratch_bytes')::bigint,0),
			COALESCE((t.task_spec->>'predicted_output_bytes')::bigint,0), t.retry_avoid_worker_id
		FROM tasks t JOIN jobs j ON j.id=t.job_id WHERE t.id=$1 AND t.available_at<=now() FOR UPDATE OF t`, taskID).Scan(&claim.JobID, &status, &claim.FencingToken,
		&claim.TaskVersion, &attemptCount, &claim.RequiredGPUType, &claim.TaskSpec, &ownerSub, &jobStatus, &predictedScratch, &predictedOutput, &retryAvoidWorker)
	if errors.Is(err, pgx.ErrNoRows) {
		return AttemptClaim{}, ErrNotFound
	}
	if err != nil {
		return AttemptClaim{}, err
	}
	if status != "queued" && status != "retry_wait" && status != "pending" {
		return AttemptClaim{}, ErrConflict
	}
	if jobStatus != "queued" && jobStatus != "running" && jobStatus != "finalizing" {
		return AttemptClaim{}, ErrConflict
	}
	if jobStatus == "queued" {
		if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('active-job:'||$1))`, ownerSub); err != nil {
			return AttemptClaim{}, err
		}
		var activeJobs, activeLimit int
		if err = tx.QueryRow(ctx, `SELECT count(j.id),COALESCE(q.active_jobs,1)
			FROM (SELECT $1::text AS owner_sub) u
			LEFT JOIN jobs j ON j.owner_sub=u.owner_sub AND j.id<>$2
				AND j.status IN ('validating','running','finalizing','cancel_requested')
			LEFT JOIN account_quotas q ON q.owner_sub=u.owner_sub
			GROUP BY q.active_jobs`, ownerSub, claim.JobID).Scan(&activeJobs, &activeLimit); err != nil {
			return AttemptClaim{}, err
		}
		if activeJobs >= activeLimit {
			return AttemptClaim{}, ErrActiveJobLimit
		}
	}
	var workerFree int64
	var schedulingEnabled, chemistryEnabled bool
	var workerCapabilities []string
	if err = tx.QueryRow(ctx, `SELECT free_scratch_bytes,scheduling_enabled,chemistry_enabled,capabilities FROM workers WHERE id=$1 FOR UPDATE`, workerID).
		Scan(&workerFree, &schedulingEnabled, &chemistryEnabled, &workerCapabilities); errors.Is(err, pgx.ErrNoRows) {
		return AttemptClaim{}, ErrNotFound
	} else if err != nil {
		return AttemptClaim{}, err
	}
	if !schedulingEnabled || !chemistryEnabled {
		return AttemptClaim{}, ErrCapacityUnavailable
	}
	if retryAvoidWorker != nil && *retryAvoidWorker == workerID {
		return AttemptClaim{}, ErrCapacityUnavailable
	}
	capable := false
	for _, capability := range workerCapabilities {
		if capability == claim.RequiredGPUType {
			capable = true
			break
		}
	}
	if !capable {
		return AttemptClaim{}, ErrCapacityUnavailable
	}
	if claim.RequiredGPUType == "gpu" {
		if _, err = tx.Exec(ctx, `INSERT INTO quota_usage_daily(owner_sub,usage_date) VALUES ($1,CURRENT_DATE)
			ON CONFLICT (owner_sub,usage_date) DO NOTHING`, ownerSub); err != nil {
			return AttemptClaim{}, err
		}
		var usedSeconds, allowedSeconds int64
		if err = tx.QueryRow(ctx, `SELECT u.gpu_seconds,COALESCE(q.gpu_seconds_per_day,86400)
			FROM quota_usage_daily u LEFT JOIN account_quotas q ON q.owner_sub=u.owner_sub
			WHERE u.owner_sub=$1 AND u.usage_date=CURRENT_DATE FOR UPDATE OF u`, ownerSub).Scan(&usedSeconds, &allowedSeconds); err != nil {
			return AttemptClaim{}, err
		}
		if usedSeconds >= allowedSeconds {
			return AttemptClaim{}, ErrDailyGPUQuota
		}
	}
	const gib = int64(1024 * 1024 * 1024)
	requiredScratch := (predictedScratch*3+1)/2 + 25*gib + predictedOutput
	var hostReserved int64
	if err = tx.QueryRow(ctx, `
		SELECT COALESCE(sum(reserved_bytes),0)::bigint FROM storage_reservations
		WHERE scope='host' AND scope_key=$1 AND released_at IS NULL AND expires_at>now()`, workerID.String()).Scan(&hostReserved); err != nil {
		return AttemptClaim{}, err
	}
	if workerFree-hostReserved < requiredScratch {
		return AttemptClaim{}, ErrCapacityUnavailable
	}
	claim.TaskID = taskID
	claim.AttemptID = uuid.New()
	if err = tx.QueryRow(ctx, `SELECT COALESCE(max(attempt_number),0)+1 FROM task_attempts WHERE task_id=$1`, taskID).
		Scan(&claim.AttemptNumber); err != nil {
		return AttemptClaim{}, err
	}
	claim.ExecutionAttemptNumber = attemptCount + 1
	claim.FencingToken++
	claim.TaskVersion++
	claim.LeaseExpiresAt = time.Now().UTC().Add(lease)
	_, err = tx.Exec(ctx, `
		INSERT INTO task_attempts(id, task_id, attempt_number, fencing_token, worker_id, lease_expires_at)
		VALUES ($1,$2,$3,$4,$5,$6)`, claim.AttemptID, taskID, claim.AttemptNumber,
		claim.FencingToken, workerID, claim.LeaseExpiresAt)
	if err != nil {
		return AttemptClaim{}, err
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO storage_reservations(job_id,task_id,owner_sub,scope,scope_key,working_set_bytes,expires_at)
		VALUES ($1,$2,$3,'host',$4,$5,$6)`, claim.JobID, taskID, ownerSub, workerID.String(), requiredScratch, claim.LeaseExpiresAt); err != nil {
		return AttemptClaim{}, err
	}
	commandTag, err := tx.Exec(ctx, `
		UPDATE tasks SET status='running', active_attempt_id=$2, fencing_token=$3,
			lease_expires_at=$4, last_heartbeat_at=now(), execution_attempt_count=$5,
			optimistic_version=optimistic_version+1, retry_avoid_worker_id=NULL
		WHERE id=$1 AND optimistic_version=$6`, taskID, claim.AttemptID, claim.FencingToken,
		claim.LeaseExpiresAt, claim.ExecutionAttemptNumber, claim.TaskVersion-1)
	if err != nil {
		return AttemptClaim{}, err
	}
	if commandTag.RowsAffected() != 1 {
		return AttemptClaim{}, ErrConflict
	}
	if _, err = tx.Exec(ctx, `UPDATE jobs SET status='running' WHERE id=$1 AND status='queued'`, claim.JobID); err != nil {
		return AttemptClaim{}, err
	}
	if _, err = tx.Exec(ctx, `SELECT append_progress_event($1,'task.started','running',jsonb_build_object('task_id',$2::text,'attempt',$3))`, claim.JobID, taskID, claim.AttemptNumber); err != nil {
		return AttemptClaim{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return AttemptClaim{}, err
	}
	return claim, nil
}

func (s *Store) HeartbeatAttempt(ctx context.Context, taskID, attemptID, workerID uuid.UUID, token int64, lease time.Duration) (time.Time, error) {
	expires := time.Now().UTC().Add(lease)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return time.Time{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `
		UPDATE tasks SET lease_expires_at=$4, last_heartbeat_at=now()
		WHERE id=$1 AND active_attempt_id=$2 AND fencing_token=$3 AND status='running' AND lease_expires_at > now()
			AND EXISTS (SELECT 1 FROM task_attempts a WHERE a.id=$2 AND a.worker_id=$5 AND a.fencing_token=$3)
			AND EXISTS (SELECT 1 FROM jobs j WHERE j.id=tasks.job_id AND j.status IN ('running','finalizing'))`,
		taskID, attemptID, token, expires, workerID)
	if err != nil {
		return time.Time{}, err
	}
	if tag.RowsAffected() != 1 {
		return time.Time{}, ErrLeaseLost
	}
	_, err = tx.Exec(ctx, `
		UPDATE task_attempts SET lease_expires_at=$3, last_heartbeat_at=now(), status='running'
		WHERE id=$1 AND fencing_token=$2 AND worker_id=$4`, attemptID, token, expires, workerID)
	if err != nil {
		return time.Time{}, err
	}
	if _, err = tx.Exec(ctx, `UPDATE storage_reservations SET expires_at=$2 WHERE task_id=$1 AND scope='host' AND released_at IS NULL`, taskID, expires); err != nil {
		return time.Time{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return time.Time{}, err
	}
	return expires, nil
}

func (s *Store) FailAttempt(ctx context.Context, taskID, attemptID, workerID uuid.UUID, token int64, terminal bool, code, detail string) (string, error) {
	if len(detail) > 1024 {
		detail = detail[:1024]
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var jobID uuid.UUID
	var jobStatus string
	if err = tx.QueryRow(ctx, `SELECT j.id,j.status::text FROM jobs j JOIN tasks t ON t.job_id=j.id
		WHERE t.id=$1 FOR UPDATE OF j`, taskID).Scan(&jobID, &jobStatus); errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	} else if err != nil {
		return "", err
	}
	var currentAttempt *uuid.UUID
	var currentToken int64
	var taskStage string
	var attemptCount, maxAttempts int
	err = tx.QueryRow(ctx, `SELECT active_attempt_id,fencing_token,stage,execution_attempt_count,max_execution_attempts FROM tasks WHERE id=$1 FOR UPDATE`, taskID).
		Scan(&currentAttempt, &currentToken, &taskStage, &attemptCount, &maxAttempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}
	if currentAttempt == nil || *currentAttempt != attemptID || currentToken != token {
		return "", ErrLeaseLost
	}
	var attemptWorker uuid.UUID
	if err = tx.QueryRow(ctx, `SELECT worker_id FROM task_attempts WHERE id=$1 AND fencing_token=$2`, attemptID, token).Scan(&attemptWorker); err != nil {
		return "", err
	}
	if attemptWorker != workerID {
		return "", ErrLeaseLost
	}
	if jobStatus == "cancel_requested" {
		if _, err = tx.Exec(ctx, `UPDATE task_attempts SET status='cancelled',error_code='job_cancelled',finished_at=now()
			WHERE id=$1 AND fencing_token=$2`, attemptID, token); err != nil {
			return "", err
		}
		if taskStage == "conformer" {
			if err = chargeGPUTimeTx(ctx, tx, jobID, attemptID); err != nil {
				return "", err
			}
		}
		if _, err = tx.Exec(ctx, `UPDATE tasks SET status='cancelled',active_attempt_id=NULL,lease_expires_at=NULL,
			fencing_token=fencing_token+1,optimistic_version=optimistic_version+1 WHERE id=$1`, taskID); err != nil {
			return "", err
		}
		if _, err = tx.Exec(ctx, `UPDATE jobs SET status='cancelled',terminal_at=now(),expires_at=now()+interval '7 days'
			WHERE id=$1 AND status='cancel_requested'`, jobID); err != nil {
			return "", err
		}
		if err = stopSiblingTasksTx(ctx, tx, jobID, taskID); err != nil {
			return "", err
		}
		if err = retainJobStorageTx(ctx, tx, jobID); err != nil {
			return "", err
		}
		if _, err = tx.Exec(ctx, `SELECT append_progress_event($1,'job.cancelled','cancelled','{}'::jsonb)`, jobID); err != nil {
			return "", err
		}
		if err = tx.Commit(ctx); err != nil {
			return "", err
		}
		return "terminal", nil
	}
	if _, err = tx.Exec(ctx, `
		UPDATE task_attempts SET status='failed', error_code=$2, error_detail=$3, finished_at=now()
		WHERE id=$1 AND fencing_token=$4`, attemptID, code, detail, token); err != nil {
		return "", err
	}
	if taskStage == "conformer" {
		if err = chargeGPUTimeTx(ctx, tx, jobID, attemptID); err != nil {
			return "", err
		}
	}
	if !terminal && taskStage == "conformer" && attemptCount >= 2 && (code == "cuda_oom" || code == "sandbox_timeout") {
		handled, splitErr := s.splitOrQuarantineTaskTx(ctx, tx, jobID, taskID, code)
		if splitErr != nil {
			return "", splitErr
		}
		if handled {
			if _, err = tx.Exec(ctx, `UPDATE storage_reservations SET released_at=now() WHERE task_id=$1 AND scope='host' AND released_at IS NULL`, taskID); err != nil {
				return "", err
			}
			if err = tx.Commit(ctx); err != nil {
				return "", err
			}
			return "terminal", nil
		}
	}
	quarantined := false
	if !terminal && code == "output_checksum_mismatch" {
		if attemptCount >= 2 {
			terminal = true
			quarantined = taskStage == "conformer"
		} else if _, err = tx.Exec(ctx, `UPDATE tasks SET retry_avoid_worker_id=$2 WHERE id=$1`, taskID, attemptWorker); err != nil {
			return "", err
		}
	}
	if !terminal && attemptCount >= maxAttempts {
		terminal = true
	}
	status := "retry_wait"
	if terminal {
		status = "failed"
		if quarantined {
			status = "quarantined"
		}
	}
	if _, err = tx.Exec(ctx, `
		UPDATE tasks SET status=$2, active_attempt_id=NULL, lease_expires_at=NULL,
			fencing_token=fencing_token+1, optimistic_version=optimistic_version+1,
			available_at=CASE WHEN $2='retry_wait' THEN now()+interval '30 seconds' ELSE available_at END
		WHERE id=$1`, taskID, status); err != nil {
		return "", err
	}
	if _, err = tx.Exec(ctx, `UPDATE storage_reservations SET released_at=now() WHERE task_id=$1 AND scope='host' AND released_at IS NULL`, taskID); err != nil {
		return "", err
	}
	if terminal && !quarantined {
		_, _ = tx.Exec(ctx, `SELECT set_config('app.reason_code',$1,true)`, code)
		if _, err = tx.Exec(ctx, `
			UPDATE jobs SET status='failed', failure_code=$2, failure_detail=$3,
				terminal_at=now(), expires_at=now()+interval '7 days'
			WHERE id=$1 AND status IN ('queued','running','finalizing','cancel_requested')`, jobID, code, detail); err != nil {
			return "", err
		}
		if err = stopSiblingTasksTx(ctx, tx, jobID, taskID); err != nil {
			return "", err
		}
		if err = retainJobStorageTx(ctx, tx, jobID); err != nil {
			return "", err
		}
	}
	eventType := "task.retry_scheduled"
	stage := "retry_wait"
	if terminal {
		eventType, stage = "task.failed", "failed"
		if quarantined {
			eventType, stage = "task.quarantined", "quarantined"
		}
	}
	if _, err = tx.Exec(ctx, `SELECT append_progress_event($1,$2,$3,jsonb_build_object('task_id',$4::text,'code',$5))`, jobID, eventType, stage, taskID, code); err != nil {
		return "", err
	}
	if quarantined {
		var unfinished int
		if err = tx.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE job_id=$1 AND stage='conformer'
			AND status NOT IN ('succeeded','split','quarantined')`, jobID).Scan(&unfinished); err != nil {
			return "", err
		}
		if unfinished == 0 {
			if err = s.enqueueFinalizerTx(ctx, tx, jobID); err != nil {
				return "", err
			}
		}
	}
	if !terminal {
		if _, err = tx.Exec(ctx, `SELECT append_audit_event('system','system','task.retry','task',$1,NULL,jsonb_build_object('code',$2,'attempt',$3))`, taskID.String(), code, attemptCount); err != nil {
			return "", err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return "", err
	}
	if terminal {
		return "terminal", nil
	}
	return "retry", nil
}

func (s *Store) CommitAttempt(ctx context.Context, taskID, workerID uuid.UUID, req CommitAttemptRequest) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var jobID uuid.UUID
	var jobStatus string
	if err = tx.QueryRow(ctx, `
		SELECT j.id,j.status::text FROM jobs j JOIN tasks t ON t.job_id=j.id
		WHERE t.id=$1 FOR UPDATE OF j`, taskID).Scan(&jobID, &jobStatus); errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	var currentAttempt *uuid.UUID
	var currentToken, currentVersion int64
	var status, stage string
	err = tx.QueryRow(ctx, `
		SELECT active_attempt_id, fencing_token, optimistic_version, status::text, stage
		FROM tasks WHERE id=$1 FOR UPDATE`, taskID).Scan(&currentAttempt, &currentToken, &currentVersion, &status, &stage)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if status != "running" || currentAttempt == nil || *currentAttempt != req.AttemptID ||
		currentToken != req.FencingToken || currentVersion != req.TaskVersion {
		return ErrLeaseLost
	}
	var attemptWorker uuid.UUID
	if err = tx.QueryRow(ctx, `SELECT worker_id FROM task_attempts WHERE id=$1 AND fencing_token=$2 FOR UPDATE`,
		req.AttemptID, req.FencingToken).Scan(&attemptWorker); err != nil {
		return err
	}
	if attemptWorker != workerID {
		return ErrLeaseLost
	}
	var leaseValid bool
	if err = tx.QueryRow(ctx, `SELECT lease_expires_at > now() FROM tasks WHERE id=$1`, taskID).Scan(&leaseValid); err != nil {
		return err
	}
	if !leaseValid {
		return ErrLeaseLost
	}
	if jobStatus == "cancel_requested" {
		if _, err = tx.Exec(ctx, `UPDATE task_attempts SET status='cancelled',finished_at=now() WHERE id=$1 AND fencing_token=$2`, req.AttemptID, req.FencingToken); err != nil {
			return err
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
		_, err = tx.Exec(ctx, `SELECT append_progress_event($1,'job.cancelled','cancelled','{}'::jsonb)`, jobID)
		if err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	if jobStatus != "running" && jobStatus != "finalizing" {
		return ErrConflict
	}
	prefix := fmt.Sprintf("jobs/%s/tasks/%s/attempts/%s/", jobID, taskID, req.AttemptID)
	var alreadyCommitted, reserved int64
	if err = tx.QueryRow(ctx, `SELECT COALESCE(sum(size_bytes),0)::bigint FROM artifacts WHERE job_id=$1 AND status='committed'`, jobID).Scan(&alreadyCommitted); err != nil {
		return err
	}
	if err = tx.QueryRow(ctx, `SELECT COALESCE(max(reserved_bytes),0)::bigint FROM storage_reservations WHERE job_id=$1 AND scope='global' AND released_at IS NULL`, jobID).Scan(&reserved); err != nil {
		return err
	}
	newBytes := int64(0)
	for _, artifact := range req.Artifacts {
		if artifact.SizeBytes > s.globalStorageCeiling-newBytes {
			return ErrCapacityUnavailable
		}
		newBytes += artifact.SizeBytes
	}
	if reserved <= 0 || alreadyCommitted > reserved-newBytes {
		return ErrCapacityUnavailable
	}
	for _, artifact := range req.Artifacts {
		if artifact.Bucket != "library-artifacts" || !strings.HasPrefix(artifact.ObjectKey, prefix) ||
			!validSHA256Hex(artifact.ChecksumSHA256) || artifact.SizeBytes < 0 ||
			strings.TrimSpace(artifact.Kind) == "" || strings.TrimSpace(artifact.MediaType) == "" {
			return fmt.Errorf("%w: invalid attempt artifact", ErrConflict)
		}
		if len(artifact.Manifest) == 0 {
			artifact.Manifest = json.RawMessage(`{}`)
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO artifacts(job_id, task_id, attempt_id, kind, bucket, object_key, size_bytes,
				checksum_sha256, media_type, manifest, expires_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,now()+interval '7 days')`,
			jobID, taskID, req.AttemptID, artifact.Kind, artifact.Bucket, artifact.ObjectKey,
			artifact.SizeBytes, strings.ToLower(artifact.ChecksumSHA256), artifact.MediaType, artifact.Manifest)
		if err != nil {
			return err
		}
	}
	if len(req.Metrics) == 0 {
		req.Metrics = json.RawMessage(`{}`)
	}
	if _, err = tx.Exec(ctx, `
		UPDATE task_attempts SET status='succeeded', finished_at=now(), metrics=$2
		WHERE id=$1 AND fencing_token=$3`, req.AttemptID, req.Metrics, req.FencingToken); err != nil {
		return err
	}
	if stage == "conformer" {
		if err = chargeGPUTimeTx(ctx, tx, jobID, req.AttemptID); err != nil {
			return err
		}
	}
	tag, err := tx.Exec(ctx, `
		UPDATE tasks SET status='succeeded', active_attempt_id=NULL, lease_expires_at=NULL,
			last_heartbeat_at=now(), optimistic_version=optimistic_version+1
		WHERE id=$1 AND active_attempt_id=$2 AND fencing_token=$3 AND optimistic_version=$4 AND lease_expires_at > now()`,
		taskID, req.AttemptID, req.FencingToken, req.TaskVersion)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	if _, err = tx.Exec(ctx, `UPDATE storage_reservations SET released_at=now() WHERE task_id=$1 AND scope='host' AND released_at IS NULL`, taskID); err != nil {
		return err
	}
	payload, _ := json.Marshal(map[string]any{"job_id": jobID, "task_id": taskID, "attempt_id": req.AttemptID})
	if _, err = tx.Exec(ctx, `
		INSERT INTO outbox_events(aggregate_type, aggregate_id, event_type, subject, payload)
		VALUES ('task',$1,'task.succeeded','events.task.succeeded',$2)`, taskID, payload); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `SELECT append_progress_event($1,'task.succeeded','running',jsonb_build_object('task_id',$2::text,'attempt_id',$3::text))`, jobID, taskID, req.AttemptID); err != nil {
		return err
	}
	if err = s.advanceWorkflowTx(ctx, tx, jobID, taskID, stage); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func serverReservation(inputBytes int64, conformers int) ReservationEstimate {
	const gib = int64(1024 * 1024 * 1024)
	working := inputBytes * 3
	if working < 5*gib {
		working = 5 * gib
	}
	if working > 200*gib {
		working = 200 * gib
	}
	final := inputBytes * 6 * int64(conformers)
	if final < gib {
		final = gib
	}
	return ReservationEstimate{
		RetainedInputBytes: inputBytes, PredictedWorkingSetBytes: working,
		PredictedFinalBytes: final, FinalizationMarginBytes: final / 10,
		RetryMarginBytes: (working + final) / 4, MultipartMarginBytes: inputBytes / 10,
	}
}

func (s *Store) ProgressEvents(ctx context.Context, ownerSub string, jobID uuid.UUID, after int64, limit int) ([]ProgressEvent, error) {
	if limit < 1 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT e.job_id, e.sequence, e.event_type, e.payload, e.created_at
		FROM progress_events e JOIN jobs j ON j.id=e.job_id
		WHERE e.job_id=$1 AND j.owner_sub=$2 AND e.sequence>$3
		ORDER BY e.sequence LIMIT $4`, jobID, ownerSub, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []ProgressEvent
	for rows.Next() {
		var event ProgressEvent
		if err = rows.Scan(&event.JobID, &event.Sequence, &event.EventType, &event.Payload, &event.CreatedAt); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s *Store) ProgressSnapshot(ctx context.Context, ownerSub string, jobID uuid.UUID) (ProgressSnapshot, error) {
	var snapshot ProgressSnapshot
	err := s.pool.QueryRow(ctx, `
		SELECT p.job_id,p.sequence,p.stage,p.completed_units,p.total_units,p.approximate_percent,
			p.detail,p.updated_at
		FROM progress_snapshots p JOIN jobs j ON j.id=p.job_id
		WHERE p.job_id=$1 AND j.owner_sub=$2`, jobID, ownerSub).
		Scan(&snapshot.JobID, &snapshot.Sequence, &snapshot.Stage, &snapshot.CompletedUnits,
			&snapshot.TotalUnits, &snapshot.ApproximatePercent, &snapshot.Detail, &snapshot.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ProgressSnapshot{}, ErrNotFound
	}
	return snapshot, err
}

func RequestHash(body []byte) string {
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}

func validSHA256Hex(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}
