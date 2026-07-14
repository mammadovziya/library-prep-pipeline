BEGIN;
DROP TABLE IF EXISTS artifact_downloads, quota_usage_daily, account_quotas, idempotency_keys, audit_events,
    workers, progress_events, progress_snapshots, artifacts, storage_reservations,
    outbox_events, task_attempts, tasks, multipart_parts, jobs, uploads, job_transitions CASCADE;
DROP FUNCTION IF EXISTS forbid_audit_mutation();
DROP FUNCTION IF EXISTS append_progress_event(uuid,text,text,jsonb);
DROP FUNCTION IF EXISTS audit_account_quota_change();
DROP FUNCTION IF EXISTS append_audit_event(text,text,text,text,text,text,jsonb);
DROP FUNCTION IF EXISTS record_initial_job_transition();
DROP FUNCTION IF EXISTS enforce_job_transition();
DROP FUNCTION IF EXISTS touch_updated_at();
DROP TYPE IF EXISTS artifact_status, reservation_scope, outbox_status, attempt_status,
    task_status, upload_status, job_status;
COMMIT;
