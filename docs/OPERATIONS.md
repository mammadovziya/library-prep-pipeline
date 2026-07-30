# Operations and recovery

## Service expectations

- `mscoc6` is the only website job execution host.
- One worker consumes CPU and GPU tasks with maximum concurrency one.
- Loss of `mscoc6` stops the website and all execution.
- PostgreSQL recovery depends on the independently mounted backup filesystem.
- Local object data has one copy and no host-loss guarantee.
- Completed artifacts are visible for seven days.

## Routine checks

Check:

- `systemctl --failed`;
- free space under `/srv/library-prep`;
- external backup mount presence and writeability;
- PostgreSQL WAL archive freshness and last successful backup;
- JetStream disk usage, pending outbox age, and expired leases;
- local S3 latency and orphan counts;
- configured GPU UUID, health, temperature, driver, and unexpected compute PIDs;
- sandbox failures and scratch/output limit events.

Prometheus listens only on `127.0.0.1:9090`. Reach it through a restricted SSH
tunnel. Never put job, user, molecule, SMILES, or object identifiers in metric
labels.

## PostgreSQL backups

PostgreSQL uses data checksums, continuous WAL archiving, an archive timeout of
60 seconds, and encrypted pgBackRest backups under the external mount. The
systemd timer runs a differential backup daily and a full backup on Sunday.

Verify:

```bash
findmnt -n /mnt/library-prep-backup
systemctl list-timers library-prep-mscoc6-backup.timer
journalctl -u library-prep-mscoc6-backup.service --since today
```

Monthly restore rehearsal:

1. Restore to an isolated test PostgreSQL instance, never over production.
2. Choose a timestamp and verify uninterrupted WAL exists.
3. Restore the newest valid base backup to that timestamp.
4. Verify migrations, jobs, outbox, audit chain, and filer metadata.
5. Record achieved recovery point and recovery time.

A successful local backup is not sufficient. The repository must live on a
filesystem administered independently from `mscoc6`.

## Total JetStream loss

1. Stop the platform service.
2. Preserve the old NATS volume for investigation.
3. Start an empty JetStream store.
4. Start the scheduler once with `REBUILD_JETSTREAM_FROM_DB=true`.
5. Remove the flag and restart normally.
6. Verify terminal tasks were not re-enqueued and stale attempts cannot commit.

## Updating the worker

1. Set `workers.scheduling_enabled=false` for `mscoc6`.
2. Wait for the active attempt to finish.
3. Update only to an approved agent/chemistry digest and driver combination.
4. Restart sandboxd and the Compose project.
5. Run the real startup qualification.
6. Re-enable scheduling and observe one canary job.

There is no second worker to absorb traffic during an update, so queued work
will wait.

## Local object-storage loss

SeaweedFS stores one local copy. A corrupted or lost object volume can destroy
uploads and completed artifacts even when PostgreSQL is recoverable.

On loss:

1. Stop new job admission.
2. Preserve the failed volume for investigation.
3. Restore the platform database if required.
4. Mark missing jobs/artifacts explicitly failed or expired.
5. Notify affected users and do not imply that deleted data was recoverable.

Users must download completed outputs they need to retain.

## Garbage collection

The Go garbage collector runs hourly:

- seven-day artifact visibility;
- active-download grace;
- abandoned multipart cleanup;
- 24-hour failed-attempt cleanup;
- PostgreSQL-authoritative reservation release;
- daily orphan-prefix reconciliation.

Run a daily report comparing committed artifacts, upload objects, attempt
prefixes, reservations, leases, and pending outbox rows.

## Fault-injection checklist

- Kill the sole agent during download, chemistry, upload, commit, and ACK.
- Reboot `mscoc6`.
- Stop PostgreSQL, NATS, S3, API, and sandboxd independently.
- Destroy and reconstruct NATS.
- Race cancellation with final commit.
- Redeliver an old fencing token.
- Fill scratch/output immediately below and above limits.
- Unmount the backup filesystem and prove deployment/backup fails loudly.
- Leave multipart uploads and uncommitted prefixes, then verify cleanup.

Design review alone does not pass a release gate. Retain commands, timestamps,
metrics, logs, and restored checksums.
