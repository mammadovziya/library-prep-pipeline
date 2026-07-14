#!/usr/bin/env bash
set -euo pipefail

for repo in 1 2; do
  pgbackrest --stanza=library-prep --repo="$repo" stanza-create
  pgbackrest --stanza=library-prep --repo="$repo" check
done
