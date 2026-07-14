package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type profileShard struct {
	Index           int     `json:"index"`
	StartRow        int64   `json:"start_row"`
	EndRow          int64   `json:"end_row"`
	AcceptedRecords int64   `json:"accepted_records"`
	EstimatedCost   float64 `json:"estimated_cost"`
}

type committedArtifact struct {
	ID       uuid.UUID
	Kind     string
	Bucket   string
	Key      string
	Size     int64
	Checksum string
	Media    string
	Manifest json.RawMessage
}

// advanceWorkflowTx is called only after the current attempt has won its fenced
// commit. Task fan-out and publication therefore remain atomic with that commit.
func (s *Store) advanceWorkflowTx(ctx context.Context, tx pgx.Tx, jobID, taskID uuid.UUID, stage string) error {
	switch stage {
	case "profile":
		return s.createConformerTasksTx(ctx, tx, jobID, taskID)
	case "conformer":
		var unfinished int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE job_id=$1 AND stage='conformer' AND status NOT IN ('succeeded','split','quarantined')`, jobID).Scan(&unfinished); err != nil {
			return err
		}
		if unfinished == 0 {
			return s.enqueueFinalizerTx(ctx, tx, jobID)
		}
	case "finalize":
		tag, err := tx.Exec(ctx, `UPDATE jobs SET status='succeeded',terminal_at=now(),expires_at=now()+interval '7 days'
			WHERE id=$1 AND status='finalizing'`, jobID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return ErrConflict
		}
		if _, err = tx.Exec(ctx, `UPDATE artifacts SET expires_at=now()+interval '7 days'
			WHERE job_id=$1 AND status='committed'`, jobID); err != nil {
			return err
		}
		if err = retainJobStorageTx(ctx, tx, jobID); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `SELECT append_progress_event($1,'job.succeeded','succeeded',jsonb_build_object('status','succeeded'))`, jobID)
		return err
	default:
		return fmt.Errorf("%w: unsupported workflow stage %q", ErrConflict, stage)
	}
	return nil
}

func (s *Store) createConformerTasksTx(ctx context.Context, tx pgx.Tx, jobID, profileTaskID uuid.UUID) error {
	var profile committedArtifact
	if err := tx.QueryRow(ctx, `
		SELECT id,kind,bucket,object_key,size_bytes,checksum_sha256,media_type,manifest
		FROM artifacts WHERE task_id=$1 AND kind='profile' AND status='committed'`, profileTaskID).
		Scan(&profile.ID, &profile.Kind, &profile.Bucket, &profile.Key, &profile.Size, &profile.Checksum, &profile.Media, &profile.Manifest); errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: profile artifact missing", ErrConflict)
	} else if err != nil {
		return err
	}
	var plan struct {
		Shards []profileShard `json:"shards"`
	}
	var rawPlan json.RawMessage
	if err := tx.QueryRow(ctx, `SELECT manifest FROM artifacts WHERE task_id=$1 AND kind='shard_plan' AND status='committed'`, profileTaskID).Scan(&rawPlan); errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: shard plan artifact missing", ErrConflict)
	} else if err != nil {
		return err
	}
	if err := json.Unmarshal(rawPlan, &plan); err != nil {
		return fmt.Errorf("%w: invalid shard plan manifest", ErrConflict)
	}
	var preset, algorithm string
	var conformers int
	if err := tx.QueryRow(ctx, `SELECT preset,requested_conformers,algorithm_version FROM jobs WHERE id=$1`, jobID).
		Scan(&preset, &conformers, &algorithm); err != nil {
		return err
	}
	var predictedScratch, predictedOutput int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(max(working_set_bytes),0)::bigint,COALESCE(max(final_output_bytes),0)::bigint
		FROM storage_reservations WHERE job_id=$1 AND scope='global'`, jobID).
		Scan(&predictedScratch, &predictedOutput); err != nil {
		return err
	}
	acceptedShards := 0
	for _, shard := range plan.Shards {
		if shard.EndRow <= shard.StartRow || shard.EndRow-shard.StartRow > 50_000 || shard.AcceptedRecords < 0 {
			return fmt.Errorf("%w: invalid shard boundary", ErrConflict)
		}
		if shard.AcceptedRecords > 0 {
			acceptedShards++
		}
	}
	if acceptedShards == 0 {
		return s.enqueueFinalizerTx(ctx, tx, jobID)
	}
	perShardScratch := predictedScratch / int64(acceptedShards)
	if perShardScratch < 1<<30 {
		perShardScratch = 1 << 30
	}
	perShardOutput := boundedStageBytes(predictedOutput / int64(acceptedShards))
	for _, shard := range plan.Shards {
		if shard.AcceptedRecords == 0 {
			continue
		}
		id := uuid.New()
		spec, _ := json.Marshal(map[string]any{
			"schema_version": "1", "stage": "conformer", "job_id": jobID, "task_id": id,
			"preset": preset, "algorithm_version": algorithm, "requested_conformers": conformers,
			"range":             map[string]any{"start_row": shard.StartRow, "end_row": shard.EndRow},
			"input":             map[string]any{"bucket": profile.Bucket, "object_key": profile.Key, "size_bytes": profile.Size, "checksum_sha256": profile.Checksum},
			"input_artifact_id": profile.ID, "predicted_scratch_bytes": perShardScratch,
			"predicted_output_bytes": perShardOutput,
		})
		if _, err := tx.Exec(ctx, `
			INSERT INTO tasks(id,job_id,stage,shard_index,status,required_capability,estimated_cost,input_artifact_id,task_spec)
			VALUES ($1,$2,'conformer',$3,'queued','gpu',$4,$5,$6)`, id, jobID, shard.Index, shard.EstimatedCost, profile.ID, spec); err != nil {
			return err
		}
		payload, _ := json.Marshal(map[string]any{"task_id": id, "job_id": jobID, "required_capability": "gpu"})
		if _, err := tx.Exec(ctx, `INSERT INTO outbox_events(aggregate_type,aggregate_id,event_type,subject,payload)
			VALUES ('task',$1,'task.ready','tasks.gpu',$2)`, id, payload); err != nil {
			return err
		}
	}
	_, err := tx.Exec(ctx, `SELECT append_progress_event($1,'job.sharded','running',jsonb_build_object('shards',$2))`, jobID, acceptedShards)
	return err
}

func (s *Store) enqueueFinalizerTx(ctx context.Context, tx pgx.Tx, jobID uuid.UUID) error {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM tasks WHERE job_id=$1 AND stage='finalize')`, jobID).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	rows, err := tx.Query(ctx, `SELECT id,kind,bucket,object_key,size_bytes,checksum_sha256,media_type,manifest
		FROM artifacts WHERE job_id=$1 AND status='committed' ORDER BY kind,object_key`, jobID)
	if err != nil {
		return err
	}
	var artifacts []map[string]any
	for rows.Next() {
		var item committedArtifact
		if err = rows.Scan(&item.ID, &item.Kind, &item.Bucket, &item.Key, &item.Size, &item.Checksum, &item.Media, &item.Manifest); err != nil {
			rows.Close()
			return err
		}
		artifacts = append(artifacts, map[string]any{
			"artifact_id": item.ID, "kind": item.Kind, "bucket": item.Bucket, "object_key": item.Key,
			"size_bytes": item.Size, "checksum_sha256": item.Checksum, "media_type": item.Media,
			"manifest": item.Manifest,
		})
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	var preset, algorithm string
	var conformers int
	var inputBucket, inputKey, inputChecksum string
	var inputBytes int64
	if err = tx.QueryRow(ctx, `SELECT j.preset,j.requested_conformers,j.algorithm_version,
		u.bucket,u.object_key,COALESCE(u.actual_bytes,u.expected_bytes),u.expected_checksum
		FROM jobs j JOIN uploads u ON u.id=j.input_upload_id WHERE j.id=$1`, jobID).
		Scan(&preset, &conformers, &algorithm, &inputBucket, &inputKey, &inputBytes, &inputChecksum); err != nil {
		return err
	}
	quarantinedRows, err := tx.Query(ctx, `SELECT id,task_spec->'range',
		COALESCE((SELECT error_code FROM task_attempts WHERE task_id=tasks.id ORDER BY attempt_number DESC LIMIT 1),'quarantined')
		FROM tasks WHERE job_id=$1 AND status='quarantined' ORDER BY stage,shard_index`, jobID)
	if err != nil {
		return err
	}
	var quarantined []map[string]any
	for quarantinedRows.Next() {
		var id uuid.UUID
		var rowRange json.RawMessage
		var code string
		if err = quarantinedRows.Scan(&id, &rowRange, &code); err != nil {
			quarantinedRows.Close()
			return err
		}
		quarantined = append(quarantined, map[string]any{"task_id": id, "range": rowRange, "code": code})
	}
	quarantinedRows.Close()
	if err = quarantinedRows.Err(); err != nil {
		return err
	}
	id := uuid.New()
	spec, _ := json.Marshal(map[string]any{
		"schema_version": "1", "stage": "finalize", "job_id": jobID, "task_id": id,
		"preset": preset, "algorithm_version": algorithm, "requested_conformers": conformers,
		"seed_derivation": "sha256(algorithm_version|shard_start_row|shard_end_row)",
		"input":           map[string]any{"bucket": inputBucket, "object_key": inputKey, "size_bytes": inputBytes, "checksum_sha256": inputChecksum},
		"artifacts":       artifacts, "quarantined_tasks": quarantined, "predicted_scratch_bytes": 1 << 30,
		"predicted_output_bytes": 1 << 30,
	})
	if _, err = tx.Exec(ctx, `INSERT INTO tasks(id,job_id,stage,shard_index,status,required_capability,task_spec)
		VALUES ($1,$2,'finalize',0,'queued','cpu',$3)`, id, jobID, spec); err != nil {
		return err
	}
	payload, _ := json.Marshal(map[string]any{"task_id": id, "job_id": jobID, "required_capability": "cpu"})
	if _, err = tx.Exec(ctx, `INSERT INTO outbox_events(aggregate_type,aggregate_id,event_type,subject,payload)
		VALUES ('task',$1,'task.ready','tasks.cpu',$2)`, id, payload); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE jobs SET status='finalizing' WHERE id=$1 AND status='running'`, jobID); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `SELECT append_progress_event($1,'job.finalizing','finalizing',jsonb_build_object('artifact_count',$2))`, jobID, len(artifacts))
	return err
}

func (s *Store) splitOrQuarantineTaskTx(ctx context.Context, tx pgx.Tx, jobID, taskID uuid.UUID, code string) (bool, error) {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, jobID.String()); err != nil {
		return false, err
	}
	var specJSON json.RawMessage
	var inputArtifactID *uuid.UUID
	var estimatedCost float64
	if err := tx.QueryRow(ctx, `SELECT task_spec,input_artifact_id,estimated_cost FROM tasks WHERE id=$1`, taskID).
		Scan(&specJSON, &inputArtifactID, &estimatedCost); err != nil {
		return false, err
	}
	var spec map[string]any
	if err := json.Unmarshal(specJSON, &spec); err != nil {
		return false, err
	}
	rowRange, ok := spec["range"].(map[string]any)
	if !ok {
		return false, nil
	}
	start, startOK := jsonNumberToInt64(rowRange["start_row"])
	end, endOK := jsonNumberToInt64(rowRange["end_row"])
	if !startOK || !endOK || end <= start {
		return false, nil
	}
	terminalStatus := "quarantined"
	if end-start > 1 {
		terminalStatus = "split"
	}
	if _, err := tx.Exec(ctx, `UPDATE tasks SET status=$2,active_attempt_id=NULL,lease_expires_at=NULL,
		fencing_token=fencing_token+1,optimistic_version=optimistic_version+1 WHERE id=$1`, taskID, terminalStatus); err != nil {
		return false, err
	}
	if end-start > 1 {
		mid := start + (end-start)/2
		var nextIndex int
		if err := tx.QueryRow(ctx, `SELECT COALESCE(max(shard_index),-1)+1 FROM tasks WHERE job_id=$1 AND stage='conformer'`, jobID).Scan(&nextIndex); err != nil {
			return false, err
		}
		for offset, bounds := range [][2]int64{{start, mid}, {mid, end}} {
			childID := uuid.New()
			childSpec := make(map[string]any, len(spec)+2)
			for key, value := range spec {
				childSpec[key] = value
			}
			childSpec["task_id"] = childID
			childSpec["parent_task_id"] = taskID
			childSpec["range"] = map[string]any{"start_row": bounds[0], "end_row": bounds[1]}
			if scratch, ok := jsonNumberToInt64(childSpec["predicted_scratch_bytes"]); ok {
				scratch /= 2
				if scratch < 1<<30 {
					scratch = 1 << 30
				}
				childSpec["predicted_scratch_bytes"] = scratch
			}
			if output, ok := jsonNumberToInt64(childSpec["predicted_output_bytes"]); ok {
				childSpec["predicted_output_bytes"] = boundedStageBytes(output / 2)
			}
			childJSON, _ := json.Marshal(childSpec)
			if _, err := tx.Exec(ctx, `INSERT INTO tasks(id,job_id,stage,shard_index,status,required_capability,estimated_cost,input_artifact_id,task_spec)
				VALUES ($1,$2,'conformer',$3,'queued','gpu',$4,$5,$6)`, childID, jobID, nextIndex+offset, estimatedCost/2, inputArtifactID, childJSON); err != nil {
				return false, err
			}
			payload, _ := json.Marshal(map[string]any{"task_id": childID, "job_id": jobID, "required_capability": "gpu"})
			if _, err := tx.Exec(ctx, `INSERT INTO outbox_events(aggregate_type,aggregate_id,event_type,subject,payload)
				VALUES ('task',$1,'task.ready','tasks.gpu',$2)`, childID, payload); err != nil {
				return false, err
			}
		}
		_, err := tx.Exec(ctx, `SELECT append_progress_event($1,'task.split','running',jsonb_build_object('task_id',$2::text,'code',$3))`, jobID, taskID, code)
		return true, err
	}
	var unfinished int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE job_id=$1 AND stage='conformer' AND status NOT IN ('succeeded','split','quarantined')`, jobID).Scan(&unfinished); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `SELECT append_progress_event($1,'record.quarantined','running',jsonb_build_object('task_id',$2::text,'code',$3))`, jobID, taskID, code); err != nil {
		return false, err
	}
	if unfinished == 0 {
		if err := s.enqueueFinalizerTx(ctx, tx, jobID); err != nil {
			return false, err
		}
	}
	return true, nil
}

func jsonNumberToInt64(value any) (int64, bool) {
	switch number := value.(type) {
	case float64:
		return int64(number), number >= 0 && number == float64(int64(number))
	case int64:
		return number, number >= 0
	case json.Number:
		parsed, err := number.Int64()
		return parsed, err == nil && parsed >= 0
	default:
		return 0, false
	}
}

func chargeGPUTimeTx(ctx context.Context, tx pgx.Tx, jobID, attemptID uuid.UUID) error {
	var ownerSub string
	var elapsed int64
	if err := tx.QueryRow(ctx, `SELECT j.owner_sub,GREATEST(1,ceil(extract(epoch FROM (now()-a.started_at)))::bigint)
		FROM jobs j JOIN task_attempts a ON a.id=$2 WHERE j.id=$1`, jobID, attemptID).Scan(&ownerSub, &elapsed); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO quota_usage_daily(owner_sub,usage_date,gpu_seconds) VALUES ($1,CURRENT_DATE,$2)
		ON CONFLICT (owner_sub,usage_date) DO UPDATE SET gpu_seconds=quota_usage_daily.gpu_seconds+EXCLUDED.gpu_seconds`, ownerSub, elapsed); err != nil {
		return err
	}
	return nil
}
