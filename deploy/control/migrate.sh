#!/usr/bin/env bash
set -euo pipefail

psql -v ON_ERROR_STOP=1 <<'SQL'
CREATE TABLE IF NOT EXISTS public.schema_migrations (
  version bigint PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT now()
);
SQL

if [[ "$(psql -Atqc 'SELECT count(*) FROM public.schema_migrations WHERE version=1')" == "0" ]]; then
  psql -v ON_ERROR_STOP=1 <<'SQL'
BEGIN;
\ir /migrations/000001_platform.up.sql
INSERT INTO public.schema_migrations(version) VALUES (1);
COMMIT;
SQL
fi
