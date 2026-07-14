# Operations and recovery

## Objectives

- Control-plane RTO: four hours.
- PostgreSQL RPO: five minutes.
- Object data: two copies on separate hosts, without site-loss guarantee.
- Completed artifacts: seven-day application retention measured from job completion; native storage lifecycle is a 30-day disaster-cleanup safety net so it cannot race a long-running job.

## Routine checks

Check WireGuard handshakes, clock offset, filesystem reserve, PostgreSQL archive freshness, both backup repositories, JetStream file usage, pending outbox age, expired leases, orphan counts, S3 gateway latency, object replica placement, GPU health, and sandbox failures. Alert on any WAL archive gap approaching five minutes.

Never use job, user, molecule, SMILES, or object identifiers as Prometheus labels. Put correlation IDs in bounded logs and PostgreSQL audit/progress records.

## Private monitoring

Prometheus retains 15 days of data and is published only on `127.0.0.1:9090` on the control host. Operators reach it through a restricted SSH tunnel; there is no public monitoring route. Node exporters bind only to WireGuard addresses and nftables permits scrapes only from `10.77.0.1`. Blackbox probes use the dedicated monitoring client certificate for API readiness and both S3 gateways.

The initial rules alert on fixed fleet target loss, less than 20 GiB filesystem reserve, and sustained host memory pressure. Connect Prometheus alerts to the organization's approved pager before alpha. Labels are limited to fixed host/service names—never users, jobs, uploads, molecule identifiers, or object keys.

## PostgreSQL backups and PITR

PostgreSQL runs with data checksums, `archive_mode=on`, a 60-second archive timeout, async pgBackRest spooling, and encrypted repositories on `storage-b` and `storage-c`. The systemd timer runs differential backups daily and full backups each Sunday.

Monthly restore rehearsal:

1. Record a target timestamp and verify uninterrupted WAL exists in both repositories.
2. Stop the isolated restore target; never rehearse over production.
3. Restore the latest valid base backup from repository 1 with a time target before the marker.
4. Start PostgreSQL, verify migration version, jobs, outbox, audit hash chain, and Seaweed filer tables.
5. Repeat from repository 2 at least quarterly.
6. Record achieved RPO/RTO and remediate any result beyond five minutes/four hours.

Follow the installed pgBackRest/PostgreSQL version syntax; the conceptual restore is:

```bash
pgbackrest --stanza=library-prep --repo=1 --type=time \
  --target='2026-07-14 10:15:00+00' --target-action=promote restore
```

PITR requires an uninterrupted WAL sequence from the chosen base backup. A successful base backup without archive continuity does not satisfy the RPO.

## Total JetStream loss

1. Stop scheduler and agents.
2. Preserve NATS files for investigation, then start an empty JetStream store.
3. Start scheduler once with `REBUILD_JETSTREAM_FROM_DB=true`.
4. It recreates streams/consumers and outbox entries for runnable PostgreSQL tasks.
5. Remove the flag, restart scheduler normally, then start agents.
6. Verify terminal tasks were not re-enqueued and no stale attempt can commit.

## Rolling worker update

1. Set `workers.scheduling_enabled=false` for one worker.
2. Wait until it has no running attempt; do not kill a healthy long shard merely to deploy.
3. Update the pinned agent/chemistry digests and host driver only to an approved release-matrix combination.
4. Restart sandboxd and the worker Compose project.
5. The agent waits for an idle GPU, runs the real chemistry qualification, and registers its new digest/driver.
6. Re-enable scheduling and observe one canary task before continuing.

The agent forwards SIGTERM and waits; sandboxd sends bounded termination then SIGKILL. Attempt leases and fencing cover forced host loss.

## Storage failure

One volume/gateway host loss should leave one object copy and the second S3 gateway. Disable chemistry on a degraded storage host by setting `workers.chemistry_enabled=false`. Replace/repair the node, verify cross-rack replica repair, then re-enable. Simultaneous loss of both copies is unrecoverable in alpha; mark affected jobs failed/expired and notify owners explicitly.

## Garbage collection

The Go GC runs hourly:

- Terminal jobs retain their actual input/artifact reservation until verified object deletion; timestamps alone never free capacity.
- Expired committed artifacts are deleted only after active-download grace.
- Incomplete uploads are aborted before their row is marked expired. A completed input shared by reruns is deleted only after every referencing job is expired.
- Failed/stale/cancelled attempt prefixes are removed after 24 hours only if unreferenced.
- Once per day, the GC lists attempt prefixes and removes prefixes older than 24 hours only when PostgreSQL has no committed artifact reference and no live attempt.
- Native S3 lifecycle removes one-day abandoned multipart data and serves as a 30-day backstop; PostgreSQL GC owns exact visibility and deletion times.

Run a daily reconciliation report comparing committed artifact rows, upload objects, attempt prefixes, active reservations, leases, and pending outbox rows. A database row is never marked cleaned when the storage operation failed.

## Fault-injection checklist

- Kill agent during download, sandbox execution, upload, CAS commit, and just before ACK.
- Reboot control, every storage node, and one worker.
- Partition worker↔API, worker↔NATS, worker↔S3, filer↔PostgreSQL, and storage peers.
- Destroy NATS and reconstruct it.
- Race cancellation against final commit.
- Redeliver an old fencing token after a newer attempt commits.
- Fill scratch/output to one byte below and above the hard cap.
- Leave multipart uploads and uncommitted prefixes, then verify cleanup.
- Run large range downloads across artifact expiration.

No release gate passes from design review alone; retain command output, timestamps, metrics, and restored checksums as evidence.
