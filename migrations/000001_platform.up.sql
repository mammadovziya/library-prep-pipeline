CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TYPE job_status AS ENUM (
    'uploading', 'validating', 'queued', 'running', 'finalizing',
    'cancel_requested', 'succeeded', 'failed', 'cancelled', 'expired'
);
CREATE TYPE upload_status AS ENUM ('initiated', 'uploading', 'completed', 'aborted', 'expired');
CREATE TYPE task_status AS ENUM (
    'pending', 'queued', 'running', 'retry_wait', 'succeeded',
    'failed', 'cancelled', 'split', 'quarantined'
);
CREATE TYPE attempt_status AS ENUM (
    'claimed', 'running', 'succeeded', 'failed', 'stale', 'cancelled', 'timed_out',
    'capacity_deferred'
);
CREATE TYPE outbox_status AS ENUM ('pending', 'publishing', 'delivered', 'failed');
CREATE TYPE reservation_scope AS ENUM ('global', 'user', 'host');
CREATE TYPE artifact_status AS ENUM ('committed', 'expired', 'deleted', 'quarantined');

CREATE TABLE jobs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    parent_job_id uuid REFERENCES jobs(id),
    owner_sub text NOT NULL CHECK (length(owner_sub) BETWEEN 1 AND 255),
    status job_status NOT NULL DEFAULT 'uploading',
    preset text NOT NULL,
    requested_conformers integer NOT NULL CHECK (requested_conformers BETWEEN 1 AND 10),
    algorithm_version text NOT NULL,
    input_upload_id uuid,
    optimistic_version bigint NOT NULL DEFAULT 1,
    cleanup_pending boolean NOT NULL DEFAULT false,
    failure_code text,
    failure_detail text CHECK (octet_length(failure_detail) <= 1024),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    terminal_at timestamptz,
    expires_at timestamptz,
    CHECK (failure_code IS NULL OR status IN ('failed', 'expired'))
);

CREATE TABLE job_transitions (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    from_status job_status,
    to_status job_status NOT NULL,
    actor text NOT NULL DEFAULT 'system',
    reason_code text,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX job_transitions_job_seq_idx ON job_transitions(job_id, id);

CREATE TABLE uploads (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_sub text NOT NULL,
    bucket text NOT NULL,
    object_key text NOT NULL UNIQUE,
    provider_upload_id text,
    status upload_status NOT NULL DEFAULT 'initiated',
    expected_bytes bigint NOT NULL CHECK (expected_bytes BETWEEN 1 AND 21474836480),
    actual_bytes bigint CHECK (actual_bytes BETWEEN 0 AND 21474836480),
    checksum_algorithm text NOT NULL DEFAULT 'SHA256' CHECK (checksum_algorithm = 'SHA256'),
    expected_checksum text NOT NULL CHECK (expected_checksum ~ '^[a-fA-F0-9]{64}$'),
    verified_checksum text CHECK (verified_checksum ~ '^[a-fA-F0-9]{64}$'),
    part_size_bytes bigint NOT NULL CHECK (part_size_bytes BETWEEN 5242880 AND 5368709120),
    max_parts integer NOT NULL CHECK (max_parts BETWEEN 1 AND 10000),
    expires_at timestamptz NOT NULL,
    completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE jobs ADD CONSTRAINT jobs_input_upload_fk
    FOREIGN KEY (input_upload_id) REFERENCES uploads(id);
CREATE INDEX uploads_owner_status_idx ON uploads(owner_sub, status, created_at DESC);

CREATE TABLE multipart_parts (
    upload_id uuid NOT NULL REFERENCES uploads(id) ON DELETE CASCADE,
    part_number integer NOT NULL CHECK (part_number BETWEEN 1 AND 10000),
    etag text,
    checksum_sha256 text CHECK (checksum_sha256 ~ '^[a-fA-F0-9]{64}$'),
    checksum_sha256_base64 text CHECK (checksum_sha256_base64 ~ '^[A-Za-z0-9+/]{43}=$'),
    size_bytes bigint NOT NULL CHECK (size_bytes > 0),
    signed_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    PRIMARY KEY (upload_id, part_number)
);

CREATE TABLE tasks (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    stage text NOT NULL,
    shard_index integer NOT NULL CHECK (shard_index >= 0),
    status task_status NOT NULL DEFAULT 'pending',
    required_capability text NOT NULL,
    active_attempt_id uuid,
    fencing_token bigint NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    lease_expires_at timestamptz,
    last_heartbeat_at timestamptz,
    optimistic_version bigint NOT NULL DEFAULT 1,
    execution_attempt_count integer NOT NULL DEFAULT 0 CHECK (execution_attempt_count >= 0),
    delivery_deferral_count integer NOT NULL DEFAULT 0 CHECK (delivery_deferral_count >= 0),
    max_execution_attempts integer NOT NULL DEFAULT 3 CHECK (max_execution_attempts BETWEEN 1 AND 10),
    retry_avoid_worker_id uuid,
    estimated_cost numeric(24,6) NOT NULL DEFAULT 0 CHECK (estimated_cost >= 0),
    input_artifact_id uuid,
    task_spec jsonb NOT NULL DEFAULT '{}'::jsonb,
    available_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (job_id, stage, shard_index)
);
CREATE INDEX tasks_runnable_idx ON tasks(required_capability, available_at, created_at)
    WHERE status IN ('pending', 'queued', 'retry_wait');
CREATE INDEX tasks_expired_lease_idx ON tasks(lease_expires_at)
    WHERE status = 'running';

CREATE TABLE task_attempts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id uuid NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    worker_id uuid NOT NULL,
    status attempt_status NOT NULL DEFAULT 'claimed',
    lease_expires_at timestamptz NOT NULL,
    last_heartbeat_at timestamptz NOT NULL DEFAULT now(),
    blocked_reason text,
    error_code text,
    error_detail text CHECK (octet_length(error_detail) <= 1024),
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
    gc_completed_at timestamptz,
    UNIQUE (task_id, attempt_number),
    UNIQUE (task_id, fencing_token)
);
ALTER TABLE tasks ADD CONSTRAINT tasks_active_attempt_fk
    FOREIGN KEY (active_attempt_id) REFERENCES task_attempts(id) DEFERRABLE INITIALLY DEFERRED;
CREATE INDEX task_attempts_worker_idx ON task_attempts(worker_id, started_at DESC);

CREATE TABLE outbox_events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type text NOT NULL,
    aggregate_id uuid NOT NULL,
    event_type text NOT NULL,
    subject text NOT NULL,
    payload jsonb NOT NULL,
    status outbox_status NOT NULL DEFAULT 'pending',
    publish_attempts integer NOT NULL DEFAULT 0 CHECK (publish_attempts >= 0),
    available_at timestamptz NOT NULL DEFAULT now(),
    locked_at timestamptz,
    delivered_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX outbox_pending_idx ON outbox_events(available_at, created_at)
    WHERE status IN ('pending', 'failed');

CREATE TABLE storage_reservations (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    task_id uuid REFERENCES tasks(id) ON DELETE CASCADE,
    owner_sub text NOT NULL,
    scope reservation_scope NOT NULL,
    scope_key text NOT NULL,
    retained_input_bytes bigint NOT NULL DEFAULT 0 CHECK (retained_input_bytes >= 0),
    working_set_bytes bigint NOT NULL DEFAULT 0 CHECK (working_set_bytes >= 0),
    final_output_bytes bigint NOT NULL DEFAULT 0 CHECK (final_output_bytes >= 0),
    finalization_margin_bytes bigint NOT NULL DEFAULT 0 CHECK (finalization_margin_bytes >= 0),
    retry_margin_bytes bigint NOT NULL DEFAULT 0 CHECK (retry_margin_bytes >= 0),
    multipart_margin_bytes bigint NOT NULL DEFAULT 0 CHECK (multipart_margin_bytes >= 0),
    reserved_bytes bigint GENERATED ALWAYS AS (
        retained_input_bytes + working_set_bytes + final_output_bytes +
        finalization_margin_bytes + retry_margin_bytes + multipart_margin_bytes
    ) STORED,
    expires_at timestamptz NOT NULL,
    released_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX storage_reservations_active_idx ON storage_reservations(scope, scope_key, expires_at)
    WHERE released_at IS NULL;

CREATE TABLE artifacts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    task_id uuid REFERENCES tasks(id) ON DELETE CASCADE,
    attempt_id uuid REFERENCES task_attempts(id) ON DELETE RESTRICT,
    parent_artifact_id uuid REFERENCES artifacts(id),
    kind text NOT NULL,
    bucket text NOT NULL,
    object_key text NOT NULL UNIQUE,
    size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
    checksum_sha256 text NOT NULL CHECK (checksum_sha256 ~ '^[a-fA-F0-9]{64}$'),
    media_type text NOT NULL,
    manifest jsonb NOT NULL DEFAULT '{}'::jsonb,
    status artifact_status NOT NULL DEFAULT 'committed',
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz,
    deleted_at timestamptz
);
ALTER TABLE tasks ADD CONSTRAINT tasks_input_artifact_fk
    FOREIGN KEY (input_artifact_id) REFERENCES artifacts(id);
CREATE INDEX artifacts_job_idx ON artifacts(job_id, status, created_at);
CREATE INDEX artifacts_attempt_idx ON artifacts(attempt_id);

CREATE TABLE progress_snapshots (
    job_id uuid PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
    sequence bigint NOT NULL DEFAULT 0,
    stage text NOT NULL,
    completed_units bigint NOT NULL DEFAULT 0 CHECK (completed_units >= 0),
    total_units bigint CHECK (total_units >= 0),
    approximate_percent numeric(5,2) CHECK (approximate_percent BETWEEN 0 AND 100),
    detail jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE progress_events (
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    sequence bigint NOT NULL CHECK (sequence > 0),
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, sequence)
);
CREATE INDEX progress_events_retention_idx ON progress_events(created_at);

CREATE TABLE workers (
    id uuid PRIMARY KEY,
    name text NOT NULL UNIQUE,
    identity text NOT NULL UNIQUE,
    gpu_uuid text NOT NULL UNIQUE,
    gpu_type text NOT NULL CHECK (gpu_type IN ('rtx4090', 'rtx5090')),
    capabilities text[] NOT NULL DEFAULT '{}',
    max_concurrency integer NOT NULL DEFAULT 1 CHECK (max_concurrency BETWEEN 0 AND 8),
    scheduling_enabled boolean NOT NULL DEFAULT true,
    chemistry_enabled boolean NOT NULL DEFAULT true,
    image_digest text NOT NULL,
    driver_version text,
    free_scratch_bytes bigint NOT NULL DEFAULT 0 CHECK (free_scratch_bytes >= 0),
    last_seen_at timestamptz,
    last_preflight jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE task_attempts ADD CONSTRAINT task_attempts_worker_fk
    FOREIGN KEY (worker_id) REFERENCES workers(id);

CREATE TABLE audit_events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    sequence bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    actor_sub text NOT NULL,
    actor_role text NOT NULL,
    action text NOT NULL,
    target_type text NOT NULL,
    target_id text NOT NULL,
    request_id text,
    source_ip inet,
    detail jsonb NOT NULL DEFAULT '{}'::jsonb,
    previous_hash bytea,
    event_hash bytea NOT NULL
);
CREATE INDEX audit_events_actor_idx ON audit_events(actor_sub, occurred_at DESC);
CREATE INDEX audit_events_target_idx ON audit_events(target_type, target_id, occurred_at DESC);

CREATE TABLE idempotency_keys (
    owner_sub text NOT NULL,
    operation text NOT NULL,
    idempotency_key text NOT NULL CHECK (length(idempotency_key) BETWEEN 8 AND 128),
    request_hash text NOT NULL,
    response_status integer,
    response_body jsonb,
    resource_id uuid,
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (owner_sub, operation, idempotency_key)
);

CREATE TABLE account_quotas (
    owner_sub text PRIMARY KEY,
    active_jobs integer NOT NULL DEFAULT 1 CHECK (active_jobs BETWEEN 0 AND 20),
    queued_jobs integer NOT NULL DEFAULT 1 CHECK (queued_jobs BETWEEN 0 AND 100),
    active_multipart_uploads integer NOT NULL DEFAULT 2 CHECK (active_multipart_uploads BETWEEN 0 AND 20),
    upload_bytes_per_day bigint NOT NULL DEFAULT 21474836480,
    retained_bytes bigint NOT NULL DEFAULT 107374182400,
    download_bytes_per_day bigint NOT NULL DEFAULT 214748364800,
    gpu_seconds_per_day bigint NOT NULL DEFAULT 86400,
    signed_parts_per_day integer NOT NULL DEFAULT 10000,
    suspended_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE quota_usage_daily (
    owner_sub text NOT NULL,
    usage_date date NOT NULL DEFAULT CURRENT_DATE,
    uploaded_bytes bigint NOT NULL DEFAULT 0 CHECK (uploaded_bytes >= 0),
    downloaded_bytes bigint NOT NULL DEFAULT 0 CHECK (downloaded_bytes >= 0),
    gpu_seconds bigint NOT NULL DEFAULT 0 CHECK (gpu_seconds >= 0),
    signed_part_requests integer NOT NULL DEFAULT 0 CHECK (signed_part_requests >= 0),
    PRIMARY KEY (owner_sub, usage_date)
);

CREATE TABLE artifact_downloads (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    artifact_id uuid NOT NULL REFERENCES artifacts(id) ON DELETE CASCADE,
    owner_sub text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    bytes_served bigint NOT NULL DEFAULT 0 CHECK (bytes_served >= 0)
);

CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END $$;

CREATE TRIGGER jobs_touch BEFORE UPDATE ON jobs
FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER uploads_touch BEFORE UPDATE ON uploads
FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER tasks_touch BEFORE UPDATE ON tasks
FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

CREATE OR REPLACE FUNCTION enforce_job_transition() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    allowed boolean := false;
    transition_actor text := COALESCE(NULLIF(current_setting('app.actor', true), ''), 'system');
    transition_reason text := NULLIF(current_setting('app.reason_code', true), '');
BEGIN
    IF NEW.status = OLD.status THEN
        RETURN NEW;
    END IF;

    allowed := CASE OLD.status
        WHEN 'uploading' THEN NEW.status IN ('validating', 'cancelled', 'failed')
        WHEN 'validating' THEN NEW.status IN ('queued', 'cancelled', 'failed')
        WHEN 'queued' THEN NEW.status IN ('running', 'cancelled', 'failed')
        WHEN 'running' THEN NEW.status IN ('finalizing', 'cancel_requested', 'failed')
        WHEN 'cancel_requested' THEN NEW.status IN ('cancelled', 'failed')
        WHEN 'finalizing' THEN NEW.status IN ('succeeded', 'cancelled', 'failed')
        WHEN 'succeeded' THEN NEW.status = 'expired'
        WHEN 'failed' THEN NEW.status = 'expired'
        WHEN 'cancelled' THEN NEW.status = 'expired'
        ELSE false
    END;
    IF NOT allowed THEN
        RAISE EXCEPTION 'invalid job transition % -> % for job %', OLD.status, NEW.status, NEW.id
            USING ERRCODE = 'check_violation';
    END IF;

    NEW.optimistic_version = OLD.optimistic_version + 1;
    INSERT INTO job_transitions(job_id, from_status, to_status, actor, reason_code)
    VALUES (NEW.id, OLD.status, NEW.status, transition_actor, transition_reason);
    RETURN NEW;
END $$;

CREATE TRIGGER jobs_enforce_transition
BEFORE UPDATE OF status ON jobs
FOR EACH ROW EXECUTE FUNCTION enforce_job_transition();

CREATE OR REPLACE FUNCTION record_initial_job_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO job_transitions(job_id, from_status, to_status, actor, reason_code)
    VALUES (
        NEW.id,
        NULL,
        NEW.status,
        COALESCE(NULLIF(current_setting('app.actor', true), ''), 'system'),
        NULLIF(current_setting('app.reason_code', true), '')
    );
    RETURN NEW;
END $$;
CREATE TRIGGER jobs_record_initial_transition
AFTER INSERT ON jobs
FOR EACH ROW EXECUTE FUNCTION record_initial_job_transition();

CREATE OR REPLACE FUNCTION forbid_audit_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'audit_events is append-only';
END $$;
CREATE TRIGGER audit_events_immutable
BEFORE UPDATE OR DELETE ON audit_events
FOR EACH ROW EXECUTE FUNCTION forbid_audit_mutation();

CREATE OR REPLACE FUNCTION append_audit_event(
    p_actor_sub text,
    p_actor_role text,
    p_action text,
    p_target_type text,
    p_target_id text,
    p_request_id text,
    p_detail jsonb
) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE
    event_id uuid := gen_random_uuid();
    prior_hash bytea;
    computed_hash bytea;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('audit-event-chain'));
    SELECT event_hash INTO prior_hash FROM audit_events ORDER BY sequence DESC LIMIT 1;
    computed_hash := digest(
        COALESCE(prior_hash, ''::bytea) ||
        convert_to(concat_ws('|', event_id::text, p_actor_sub, p_actor_role, p_action,
            p_target_type, p_target_id, COALESCE(p_request_id, ''), COALESCE(p_detail, '{}'::jsonb)::text), 'UTF8'),
        'sha256'
    );
    INSERT INTO audit_events(id, actor_sub, actor_role, action, target_type, target_id,
        request_id, detail, previous_hash, event_hash)
    VALUES (event_id, p_actor_sub, p_actor_role, p_action, p_target_type, p_target_id,
        p_request_id, COALESCE(p_detail, '{}'::jsonb), prior_hash, computed_hash);
    RETURN event_id;
END $$;

CREATE OR REPLACE FUNCTION audit_account_quota_change() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    audit_action text;
    actor text := COALESCE(NULLIF(current_setting('app.actor', true), ''), 'system');
    actor_role text := COALESCE(NULLIF(current_setting('app.actor_role', true), ''), 'system');
BEGIN
    IF TG_OP = 'INSERT' THEN
        audit_action := 'account.provision';
    ELSIF OLD.suspended_at IS DISTINCT FROM NEW.suspended_at THEN
        audit_action := CASE WHEN NEW.suspended_at IS NULL THEN 'account.resume' ELSE 'account.suspend' END;
    ELSE
        audit_action := 'account.quota_change';
    END IF;
    PERFORM append_audit_event(
        actor,
        actor_role,
        audit_action,
        'account',
        NEW.owner_sub,
        NULL,
        jsonb_build_object(
            'active_jobs', NEW.active_jobs,
            'queued_jobs', NEW.queued_jobs,
            'active_uploads', NEW.active_multipart_uploads,
            'upload_bytes_per_day', NEW.upload_bytes_per_day,
            'retained_bytes', NEW.retained_bytes,
            'download_bytes_per_day', NEW.download_bytes_per_day,
            'gpu_seconds_per_day', NEW.gpu_seconds_per_day,
            'signed_parts_per_day', NEW.signed_parts_per_day,
            'suspended', NEW.suspended_at IS NOT NULL
        )
    );
    RETURN NEW;
END $$;

CREATE TRIGGER account_quotas_audit
AFTER INSERT OR UPDATE OF active_jobs, queued_jobs, active_multipart_uploads,
    upload_bytes_per_day, retained_bytes, download_bytes_per_day,
    gpu_seconds_per_day, signed_parts_per_day, suspended_at
ON account_quotas
FOR EACH ROW EXECUTE FUNCTION audit_account_quota_change();

CREATE OR REPLACE FUNCTION append_progress_event(
    p_job_id uuid,
    p_event_type text,
    p_stage text,
    p_payload jsonb
) RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    next_sequence bigint;
BEGIN
    INSERT INTO progress_snapshots(job_id, sequence, stage, detail)
    VALUES (p_job_id, 1, p_stage, COALESCE(p_payload, '{}'::jsonb))
    ON CONFLICT (job_id) DO UPDATE SET
        sequence = progress_snapshots.sequence + 1,
        stage = EXCLUDED.stage,
        detail = EXCLUDED.detail,
        updated_at = now()
    RETURNING sequence INTO next_sequence;
    INSERT INTO progress_events(job_id, sequence, event_type, payload)
    VALUES (p_job_id, next_sequence, p_event_type, COALESCE(p_payload, '{}'::jsonb));
    RETURN next_sequence;
END $$;
