package platform

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type ExpiredArtifact struct {
	ID        uuid.UUID
	JobID     uuid.UUID
	Bucket    string
	ObjectKey string
}

type ExpiredUpload struct {
	ID               uuid.UUID
	Bucket           string
	ObjectKey        string
	ProviderUploadID *string
	Status           string
}

type UncommittedAttempt struct {
	AttemptID uuid.UUID
	JobID     uuid.UUID
	TaskID    uuid.UUID
}

func (s *Store) ExpireDueJobs(ctx context.Context, limit int) (int64, error) {
	if limit < 1 || limit > 1000 {
		limit = 500
	}
	tag, err := s.pool.Exec(ctx, `WITH due AS (
		SELECT id FROM jobs
		WHERE status IN ('succeeded','failed','cancelled') AND expires_at<=now()
		ORDER BY expires_at LIMIT $1 FOR UPDATE SKIP LOCKED
	)
	UPDATE jobs j SET status='expired', cleanup_pending=(
		EXISTS(SELECT 1 FROM artifacts a WHERE a.job_id=j.id AND a.status='committed')
		OR EXISTS(SELECT 1 FROM uploads u WHERE u.id=j.input_upload_id AND u.status<>'expired')
	) FROM due WHERE j.id=due.id`, limit)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *Store) ExpiredArtifacts(ctx context.Context, limit int) ([]ExpiredArtifact, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT a.id,a.job_id,a.bucket,a.object_key FROM artifacts a
		JOIN jobs j ON j.id=a.job_id
		WHERE j.status='expired' AND a.status='committed' AND a.expires_at<=now()
			AND NOT EXISTS (
				SELECT 1 FROM artifact_downloads d WHERE d.artifact_id=a.id
				AND d.expires_at>now() AND d.last_seen_at>now()-interval '30 minutes')
		ORDER BY a.expires_at LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var candidates []ExpiredArtifact
	for rows.Next() {
		var item ExpiredArtifact
		if err = rows.Scan(&item.ID, &item.JobID, &item.Bucket, &item.ObjectKey); err != nil {
			return nil, err
		}
		candidates = append(candidates, item)
	}
	return candidates, rows.Err()
}

func (s *Store) MarkArtifactDeleted(ctx context.Context, artifactID, jobID uuid.UUID) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `UPDATE artifacts SET status='deleted',deleted_at=now() WHERE id=$1 AND status='committed' AND expires_at<=now()`, artifactID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	var remaining int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM artifacts WHERE job_id=$1 AND status='committed'`, jobID).Scan(&remaining); err != nil {
		return err
	}
	if remaining == 0 {
		_, _ = tx.Exec(ctx, `SELECT set_config('app.reason_code','retention_expired',true)`)
		if _, err = tx.Exec(ctx, `WITH cleaned AS (
			UPDATE jobs j SET cleanup_pending=false
			WHERE j.id=$1 AND j.status='expired'
				AND NOT EXISTS(SELECT 1 FROM artifacts a WHERE a.job_id=j.id AND a.status='committed')
				AND NOT EXISTS(SELECT 1 FROM uploads u WHERE u.id=j.input_upload_id AND u.status<>'expired')
			RETURNING j.id
		)
		UPDATE storage_reservations r SET released_at=now()
		WHERE r.job_id IN (SELECT id FROM cleaned) AND r.released_at IS NULL`, jobID); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) ExpiredUploads(ctx context.Context, limit int) ([]ExpiredUpload, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT u.id,u.bucket,u.object_key,u.provider_upload_id,u.status::text FROM uploads u
		WHERE (u.status IN ('initiated','uploading') AND u.expires_at<=now())
			OR (u.status='completed' AND (
				(NOT EXISTS(SELECT 1 FROM jobs j WHERE j.input_upload_id=u.id) AND u.expires_at<=now())
				OR (EXISTS(SELECT 1 FROM jobs j WHERE j.input_upload_id=u.id)
					AND NOT EXISTS(SELECT 1 FROM jobs j WHERE j.input_upload_id=u.id AND j.status<>'expired'))
			))
		ORDER BY u.expires_at LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var candidates []ExpiredUpload
	for rows.Next() {
		var item ExpiredUpload
		if err = rows.Scan(&item.ID, &item.Bucket, &item.ObjectKey, &item.ProviderUploadID, &item.Status); err != nil {
			return nil, err
		}
		candidates = append(candidates, item)
	}
	return candidates, rows.Err()
}

func (s *Store) MarkUploadExpired(ctx context.Context, id uuid.UUID) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `UPDATE uploads u SET status='expired' WHERE u.id=$1 AND (
		(u.status IN ('initiated','uploading') AND u.expires_at<=now())
		OR (u.status='completed' AND (
			(NOT EXISTS(SELECT 1 FROM jobs j WHERE j.input_upload_id=u.id) AND u.expires_at<=now())
			OR (EXISTS(SELECT 1 FROM jobs j WHERE j.input_upload_id=u.id)
				AND NOT EXISTS(SELECT 1 FROM jobs j WHERE j.input_upload_id=u.id AND j.status<>'expired'))
		))
	)`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	if _, err = tx.Exec(ctx, `WITH cleaned AS (
		UPDATE jobs j SET cleanup_pending=false
		WHERE j.input_upload_id=$1 AND j.status='expired'
			AND NOT EXISTS(SELECT 1 FROM artifacts a WHERE a.job_id=j.id AND a.status='committed')
			AND NOT EXISTS(SELECT 1 FROM uploads u WHERE u.id=j.input_upload_id AND u.status<>'expired')
		RETURNING j.id
	)
	UPDATE storage_reservations r SET released_at=now()
	WHERE r.job_id IN (SELECT id FROM cleaned) AND r.released_at IS NULL`, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) UncommittedAttempts(ctx context.Context, limit int) ([]UncommittedAttempt, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT ta.id,t.job_id,ta.task_id
		FROM task_attempts ta JOIN tasks t ON t.id=ta.task_id
		WHERE ta.status IN ('failed','stale','cancelled','timed_out')
			AND ta.finished_at<now()-interval '24 hours' AND ta.gc_completed_at IS NULL
			AND NOT EXISTS (SELECT 1 FROM artifacts a WHERE a.attempt_id=ta.id AND a.status='committed')
		ORDER BY ta.finished_at LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var candidates []UncommittedAttempt
	for rows.Next() {
		var item UncommittedAttempt
		if err = rows.Scan(&item.AttemptID, &item.JobID, &item.TaskID); err != nil {
			return nil, err
		}
		candidates = append(candidates, item)
	}
	return candidates, rows.Err()
}

func (s *Store) MarkAttemptGCComplete(ctx context.Context, id uuid.UUID) error {
	tag, err := s.pool.Exec(ctx, `UPDATE task_attempts SET gc_completed_at=now() WHERE id=$1 AND gc_completed_at IS NULL`, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	return nil
}

func (s *Store) AttemptPrefixCollectible(ctx context.Context, attemptID uuid.UUID, cutoff time.Time) (bool, error) {
	var collectible bool
	err := s.pool.QueryRow(ctx, `SELECT
		NOT EXISTS (SELECT 1 FROM artifacts a WHERE a.attempt_id=$1 AND a.status='committed')
		AND (
			NOT EXISTS (SELECT 1 FROM task_attempts ta WHERE ta.id=$1)
			OR EXISTS (SELECT 1 FROM task_attempts ta WHERE ta.id=$1
				AND ta.status IN ('failed','stale','cancelled','timed_out','capacity_deferred')
				AND COALESCE(ta.finished_at,ta.started_at)<$2)
		)`, attemptID, cutoff).Scan(&collectible)
	return collectible, err
}
