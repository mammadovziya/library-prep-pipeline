#!/usr/bin/env bash
set -euo pipefail

pgbackrest --stanza=library-prep --repo=1 stanza-create
pgbackrest --stanza=library-prep --repo=1 check
