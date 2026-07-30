#!/usr/bin/env bash
set -euo pipefail

backup_type=diff
if [[ "$(date -u +%u)" == "7" ]]; then
  backup_type=full
fi

docker compose exec -T --user postgres postgres \
  pgbackrest --stanza=library-prep --repo=1 --type="$backup_type" backup
docker compose exec -T --user postgres postgres \
  pgbackrest --stanza=library-prep --repo=1 check
