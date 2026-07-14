#!/usr/bin/env bash
set -euo pipefail

psql -v ON_ERROR_STOP=1 \
  --set=authentik_password="$AUTHENTIK_DB_PASSWORD" \
  --set=filer_password="$SEAWEED_FILER_DB_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE authentik LOGIN PASSWORD %L', :'authentik_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname='authentik') \gexec
SELECT format('ALTER ROLE authentik PASSWORD %L', :'authentik_password') \gexec
SELECT 'CREATE DATABASE authentik OWNER authentik'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='authentik') \gexec

SELECT format('CREATE ROLE seaweed_filer LOGIN PASSWORD %L', :'filer_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname='seaweed_filer') \gexec
SELECT format('ALTER ROLE seaweed_filer PASSWORD %L', :'filer_password') \gexec
SELECT 'CREATE DATABASE seaweed_filer OWNER seaweed_filer'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='seaweed_filer') \gexec
SQL
