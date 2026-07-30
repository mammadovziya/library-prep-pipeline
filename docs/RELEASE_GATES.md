# Release gates

Implementation means source/configuration exists. Qualification requires
retained evidence from the real `mscoc6` deployment.

## Controlled alpha

| Gate | Implementation | Host evidence |
|---|---|---|
| Sole worker identity is `mscoc6` | Configured | Required |
| Worker maximum concurrency is one | Configured | Queue test required |
| No cluster fan-out or other schedulable workers | Configured | DB/NATS audit required |
| Dedicated non-login UID/GID 65532 owns attempts | Implemented in Ansible | Permission audit required |
| Only TCP 80/443 public; internal services private | Implemented | External port scan required |
| Upload/checksum/range/resume flow | Implemented | Browser/S3 suite required |
| Leases, fencing, attempt prefixes, CAS commit | Implemented | Stale-attempt test required |
| Queue loss rebuild from PostgreSQL | Implemented | Total-loss drill required |
| Single-node S3 and seven-day retention | Implemented | Capacity/cleanup tests required |
| External encrypted PostgreSQL backup | Implemented | Point-in-time restore required |
| Offline sandbox and narrow sandboxd | Implemented | Kernel containment test required |
| Server-owned reservations and hard counters | Implemented | Boundary/fill tests required |
| Admin MFA and distinct service credentials | Designed | Authentik verification required |
| Actual mscoc6 GPU profile | Parameterized | Scientific qualification required |

Alpha remains blocked until every required item has evidence and the operator
accepts the single-host website/execution failure domain and single-copy object
storage.

## Public registration

- Independent threat-model review and penetration test.
- Verified email, CAPTCHA, per-IP/account limits, and suspension tooling.
- Compression, molecular expansion, output, and multipart bomb tests.
- Privacy, retention, single-copy storage, and prohibited-data notices.
- Cross-user job, upload, artifact, and presigned-URL authorization tests.
- Service-account and sandbox escape tests.
- Security-update ownership and patch SLA.
- Measured upload/download throughput without memory growth.

Do not enable self-registration during the controlled alpha.

## Large-job qualification

- Define the maximum supported compound count from measured `mscoc6`
  throughput, not from the superseded six-GPU estimate.
- Demonstrate the largest supported job finishes within the agreed queue and
  wall-clock limit on the sole GPU.
- Prove peak storage, scratch, and final output remain within the calibrated
  ceiling.
- Verify recursive shard splitting cannot create unbounded queue or disk use.
- Demonstrate retry reproducibility on the actual GPU and driver.
- Confirm a large job does not make the website, API, or storage unusable.
