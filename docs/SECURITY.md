# Security model

## Alpha trust boundary

Alpha users are approved manually and may submit only trusted, non-confidential inputs. This is still treated as hostile parser input: native RDKit, nvMolKit, CSV, Parquet, gzip, and CUDA code run only inside a fresh offline container.

The website and every submitted job run on `mscoc6`. No other computer is a
schedulable worker. The main threats are parser/native-code compromise,
credential theft, cross-user object access, stale workers, storage exhaustion,
expansion/compression bombs, public abuse, administrator compromise, and loss
of this single non-redundant host.

## Isolation

The networked Go agent owns worker credentials, verifies input/output checksums, and uploads artifacts. It does not parse chemistry. The offline runner receives no network namespace, credentials, unrelated environment, or other-attempt paths. Its root filesystem is read-only and it runs non-root with capabilities dropped, no-new-privileges, AppArmor/seccomp, explicit resource limits, and one allowlisted GPU UUID.

After the sandbox exits, the agent resolves every returned artifact path and accepts only a regular, non-symlink file beneath that attempt's output root. A compromised native runner therefore cannot use an output symlink to make the credential-bearing agent read and upload one of its mounted secrets.

Root-owned sandboxd is intentionally small. Its API accepts only a task UUID, attempt UUID, allowlisted image digest, allowlisted GPU UUID, and named resource profile. It constructs every image, command, mount, security option, and entrypoint itself. The agent cannot launch an arbitrary container and never mounts the Docker socket.

The supplied seccomp/AppArmor profiles are release artifacts, not paper controls: both must pass the real qualification and compromise-containment tests on the production kernel/driver before alpha.

## Input limits

- 20 GB compressed; 50 GB decompressed; maximum 100:1 ratio.
- No nested or recursive compression.
- 1 MiB line/row; 256 KiB field; 100 columns.
- 256 atoms, 512 bonds, 16 fragments, molecular weight 5,000.
- Four stereoisomers, five tautomers, four protonation states, ten conformers.
- Forty generated variants per source molecule; excess rejects the record.
- Rejection text is bounded to 1 KiB.
- Stage/shard wall clocks and streaming input/output/scratch counters are hard limits.

## Identity and authorization

Authentik is the OIDC authority. The API validates issuer, audience, signature, expiry, not-before, immutable subject, role, and current account status on every request. A disabled account loses API access even if an encrypted browser session remains.

The host has a dedicated, non-login `libraryprep` account with fixed UID/GID
65532. Attempt files and the networked agent run under that identity; they do
not use an individual researcher's account, home directory, SSH keys, or
credentials. Root is retained only for host administration and the small
root-owned sandbox launcher.

Web mutations require same-origin plus a double-bound CSRF token. The browser never receives internal service keys. Every object key is server generated, and upload completion independently verifies owner, job state, key, part set, size, and a streamed whole-object SHA-256.

Internal API calls require mTLS and bind certificate CN to worker UUID. The API
allowlists the sole worker name `mscoc6`; NATS maps only that worker certificate
to consumer permissions. The worker has an S3 identity limited to object reads
and attempt-prefix writes; delete, list, and administration are denied. Offline
sandboxes receive no identity. GC, storage-init, API, Caddy, and administration
credentials are separate.

Initial account quotas are one active/one queued job, two multipart uploads, 20 GB upload/day, 100 GB retained artifacts, 200 GB signed download/day, 24 GPU-hours/day, and 10,000 signed parts/day. Server-owned storage admission prevents browser under-reservation.

## Audit and data handling

Approval, suspension, quota changes, job creation/cancellation/retry, download signing, and artifact access belong in append-only audit events. Audit rows form a SHA-256 hash chain and database triggers reject mutation/deletion. Chemistry content, SMILES, presigned URLs, and user/job IDs must not appear in metrics labels or routine logs.

Object visibility is seven days. Expiration blocks new URLs, honors active-download grace, and then deletes. Cleanup failure keeps an internal pending condition; it must not silently report deletion.

## Public-registration blockers

Do not enable self-registration until verified email, CAPTCHA/bot defenses, per-IP limits, suspension tooling, privacy/non-redundancy notices, security-update ownership, compression/expansion bomb tests, cross-user authorization tests, prolonged partitions, worker compromise containment, and an independent penetration test have passed.

No claim should imply high availability, protection from loss of `mscoc6` and
its local object disk, or suitability for HIPAA, GxP, regulated, or
confidential proprietary chemistry.
