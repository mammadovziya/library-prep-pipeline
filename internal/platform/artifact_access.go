package platform

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (s *Store) ListArtifacts(ctx context.Context, ownerSub string, jobID uuid.UUID) ([]Artifact, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT a.id,a.job_id,a.kind,a.size_bytes,a.checksum_sha256,a.media_type,a.manifest,a.created_at,a.expires_at
		FROM artifacts a JOIN jobs j ON j.id=a.job_id
		WHERE a.job_id=$1 AND j.owner_sub=$2 AND j.status='succeeded' AND a.status='committed'
		ORDER BY a.kind,a.object_key`, jobID, ownerSub)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var artifacts []Artifact
	for rows.Next() {
		var artifact Artifact
		if err = rows.Scan(&artifact.ID, &artifact.JobID, &artifact.Kind, &artifact.SizeBytes,
			&artifact.ChecksumSHA256, &artifact.MediaType, &artifact.Manifest, &artifact.CreatedAt, &artifact.ExpiresAt); err != nil {
			return nil, err
		}
		artifacts = append(artifacts, artifact)
	}
	return artifacts, rows.Err()
}

func (s *Store) AuthorizeArtifactDownload(ctx context.Context, ownerSub string, artifactID uuid.UUID, validity time.Duration) (string, string, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return "", "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var bucket, objectKey string
	var size int64
	err = tx.QueryRow(ctx, `
		SELECT a.bucket,a.object_key,a.size_bytes
		FROM artifacts a JOIN jobs j ON j.id=a.job_id
		WHERE a.id=$1 AND j.owner_sub=$2 AND j.status='succeeded' AND a.status='committed'
			AND a.expires_at>now() FOR UPDATE OF a`, artifactID, ownerSub).Scan(&bucket, &objectKey, &size)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", ErrNotFound
	}
	if err != nil {
		return "", "", err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO quota_usage_daily(owner_sub,usage_date) VALUES ($1,CURRENT_DATE)
		ON CONFLICT (owner_sub,usage_date) DO NOTHING`, ownerSub); err != nil {
		return "", "", err
	}
	var used, allowed int64
	if err = tx.QueryRow(ctx, `SELECT u.downloaded_bytes,COALESCE(q.download_bytes_per_day,214748364800)
		FROM quota_usage_daily u LEFT JOIN account_quotas q ON q.owner_sub=u.owner_sub
		WHERE u.owner_sub=$1 AND u.usage_date=CURRENT_DATE FOR UPDATE OF u`, ownerSub).Scan(&used, &allowed); err != nil {
		return "", "", err
	}
	if used+size > allowed {
		return "", "", ErrQuotaExceeded
	}
	if _, err = tx.Exec(ctx, `UPDATE quota_usage_daily SET downloaded_bytes=downloaded_bytes+$2
		WHERE owner_sub=$1 AND usage_date=CURRENT_DATE`, ownerSub, size); err != nil {
		return "", "", err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO artifact_downloads(artifact_id,owner_sub,expires_at,bytes_served)
		VALUES ($1,$2,now()+$3::interval,$4)`, artifactID, ownerSub, validity.String(), size); err != nil {
		return "", "", err
	}
	if _, err = tx.Exec(ctx, `SELECT append_audit_event($1,'user','artifact.download','artifact',$2,NULL,jsonb_build_object('bytes',$3))`, ownerSub, artifactID.String(), size); err != nil {
		return "", "", err
	}
	if err = tx.Commit(ctx); err != nil {
		return "", "", err
	}
	return bucket, objectKey, nil
}
