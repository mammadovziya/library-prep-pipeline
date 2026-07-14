# Release gates

Status legend: implemented means source/configuration exists; qualified means evidence from the actual fleet has passed. This repository alone cannot mark an operational test qualified.

## Controlled alpha

| Gate | Implementation | Fleet evidence |
|---|---|---|
| WireGuard names, firewall, exact flows | Implemented in Ansible | Required |
| External browser multipart/checksum/range test | Implemented paths | Required |
| Transactional outbox and stable NATS message ID | Implemented | Fault test required |
| Leases, fencing, attempt prefixes, CAS commit | Implemented | Stale-attempt test required |
| Explicit DLQ advisory handling and reconciliation | Implemented | MaxDeliver test required |
| SeaweedFS 4.29, two copies, lifecycle, two gateways | Implemented | S3 suite/replica audit required |
| pgBackRest two-host WAL/base backup | Implemented | Bare-metal PITR required |
| NATS reconstruction from PostgreSQL | Implemented | Total-loss drill required |
| Offline runner and narrow sandboxd | Implemented | Kernel containment test required |
| Server-owned peak reservations and hard counters | Implemented | Boundary/fill tests required |
| Admin MFA and distinct service credentials | Configured design | Authentik/operator verification required |
| RTX 4090/5090 startup chemistry smoke | Implemented | Every host required |

Alpha remains blocked until every “required” item has retained evidence and the operator accepts four-hour RTO, five-minute PostgreSQL RPO, and non-confidential-input limitations.

## Public registration

- Threat model review and independent penetration test.
- Verified email, CAPTCHA, IP/account/global rate limits, suspension tooling.
- Compression, row/field, molecular expansion, output, and multipart bomb tests.
- Privacy, retention, object non-redundancy, and prohibited-data notices.
- Cross-user upload/job/artifact/presigned-URL authorization tests.
- Worker compromise containment and credential-exfiltration tests.
- Prolonged partitions and monitored restore schedule.
- Image/package advisory ownership and documented patch SLA.
- Caddy benchmark: at least 80% measured NIC line rate, API p95 under 500 ms during six object transfers, flat memory relative to object size, working ranges/resume.

## Five-million-record jobs

- Docking preset with one conformer finishes within 12 execution hours while all six GPUs are genuinely idle.
- Peak—not final—storage remains below the calibrated logical ceiling.
- Worst-case expansion corpus and cost-weighted sharding pass.
- RTX 4090/5090 exact-invariant and numerical-tolerance corpus passes.
- Per-molecule seed derivation/retry reproducibility is demonstrated.
- Storage/chemistry coexistence causes no OOM/swap, less than 20% throughput loss, and less than 2× p95 S3 latency; otherwise storage nodes become storage-only.
- Simultaneous transfers meet the network target.
- Fault injection leaves no stale commit, multipart upload, attempt prefix, reservation, lease, claim, or pending outbox orphan.

Until these pass, enforce smaller alpha limits even though the schema can represent larger jobs.
