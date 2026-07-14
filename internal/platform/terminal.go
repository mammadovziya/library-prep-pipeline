package platform

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// stopSiblingTasksTx fences every unfinished sibling after a terminal job
// failure. A running sandbox then loses its next heartbeat and cannot commit.
func stopSiblingTasksTx(ctx context.Context, tx pgx.Tx, jobID, terminalTaskID uuid.UUID) error {
	if _, err := tx.Exec(ctx, `UPDATE task_attempts a SET status='cancelled',finished_at=now(),error_code='job_terminal'
		FROM tasks t WHERE t.active_attempt_id=a.id AND t.job_id=$1 AND t.id<>$2
			AND a.status IN ('claimed','running')`, jobID, terminalTaskID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE tasks SET status='cancelled',active_attempt_id=NULL,
		lease_expires_at=NULL,last_heartbeat_at=NULL,fencing_token=fencing_token+1,
		optimistic_version=optimistic_version+1
		WHERE job_id=$1 AND id<>$2 AND status IN ('pending','queued','running','retry_wait')`, jobID, terminalTaskID); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE storage_reservations SET released_at=now()
		WHERE job_id=$1 AND scope='host' AND released_at IS NULL`, jobID)
	return err
}

// retainJobStorageTx shrinks peak reservations to bytes that remain after a
// terminal transition. Global/user capacity stays held until object cleanup
// succeeds; an expiry timestamp never implicitly releases physical capacity.
func retainJobStorageTx(ctx context.Context, tx pgx.Tx, jobID uuid.UUID) error {
	_, err := tx.Exec(ctx, `UPDATE storage_reservations r SET
		retained_input_bytes=COALESCE((
			SELECT COALESCE(u.actual_bytes,u.expected_bytes) FROM jobs j
			JOIN uploads u ON u.id=j.input_upload_id WHERE j.id=$1
		),0),
		working_set_bytes=0,
		final_output_bytes=COALESCE((SELECT sum(a.size_bytes) FROM artifacts a
			WHERE a.job_id=$1 AND a.status='committed'),0),
		finalization_margin_bytes=0,
		retry_margin_bytes=0,
		multipart_margin_bytes=0,
		expires_at=COALESCE((SELECT expires_at FROM jobs WHERE id=$1),now()+interval '7 days')
		WHERE r.job_id=$1 AND r.scope IN ('global','user') AND r.released_at IS NULL`, jobID)
	return err
}
