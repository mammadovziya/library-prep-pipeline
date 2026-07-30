# Deploying on mscoc6

This is the current deployment. The retired seven-host Compose projects are
reference material only.

## 1. Confirm host and network ownership

Before changing the host, obtain:

- the approved SSH administration account and source network;
- permission to publish the three HTTPS DNS names;
- the exact GPU UUID and model on `mscoc6`;
- a mounted departmental backup filesystem that is physically independent of
  the host;
- the approved container registry and external alert destination.

Only TCP 80/443 is public. SSH is restricted to `admin_network`. No cluster node
other than `mscoc6` is registered as a worker.

## 2. Prepare the host

Use a supported Ubuntu LTS release with synchronized time, AppArmor, Docker
Engine/Compose v2, the NVIDIA driver, and NVIDIA Container Toolkit.

Verify before applying Ansible:

```bash
nvidia-smi -L
nvidia-ctk --version
docker info
findmnt -n /mnt/library-prep-backup
```

The playbook fails if the GPU tooling or external backup mount is absent.

The playbook creates a non-login account named `libraryprep` with UID/GID
65532. Do not create jobs beneath a personal home directory.

## 3. Build and pin release artifacts

Build on a trusted Linux builder:

```bash
docker build -f docker/postgres.Dockerfile -t registry.internal/library-prep-postgres:18.4 .
for service in api scheduler gc storageinit; do
  docker build -f docker/go.Dockerfile --build-arg SERVICE=$service \
    -t registry.internal/library-prep-$service:candidate .
done
docker build -f docker/agent.Dockerfile -t registry.internal/library-prep-agent:candidate .
docker build -f docker/chemistry.Dockerfile -t registry.internal/library-prep-chemistry:candidate .
docker build -f docker/web.Dockerfile -t registry.internal/library-prep-web:candidate .
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath \
  -o build/linux-amd64/library-prep-sandboxd ./cmd/sandboxd
```

Scan and test every image, push it, and resolve it to
`name@sha256:...`. Mutable tags are not production inputs.

## 4. Create production inventory

Copy:

```bash
cp deploy/ansible/inventory/example.yml \
   deploy/ansible/inventory/production.yml
```

Replace the example host address, `admin_network`, backup mount, GPU UUID, and
qualified GPU profile. Leave `platform_start_enabled: false` for the first
Ansible pass.

If the card is not an RTX 4090 or RTX 5090, stop and add a real profile plus
scientific qualification. Do not disguise another card as a supported model.

## 5. Prepare Vault inputs

Use encrypted `group_vars/all/vault.yml`. The playbook expects
`vault_environment_files`, a mapping from filename to complete file content.

Required files:

| File | Purpose |
|---|---|
| `images.env` | Immutable image digests |
| `domains.env` | `APP_DOMAIN`, `AUTH_DOMAIN`, `OBJECTS_DOMAIN`, `ACME_EMAIL` |
| `compose.env` | Compose-time DB passwords, GPU UUID, backup mount, storage ceiling and volume count |
| `control.env` | PostgreSQL, NATS, OIDC, API S3 identity |
| `gc.env` | PostgreSQL and GC-only S3 identity |
| `storage-init.env` | Storage administrator S3 identity |
| `worker.env` | Sole `mscoc6` worker identity, GPU profile and worker S3 identity |
| `web.env` | Session secret, OIDC endpoints, API URL and web mTLS paths |
| `authentik.env` | Authentik database and bootstrap configuration |
| `sandboxd.env` | Allowlisted image, GPU UUID, paths and resource policy |

Key worker values:

```text
WORKER_NAME=mscoc6
WORKER_ID=<stable UUID>
GPU_UUID=<nvidia-smi UUID>
GPU_TYPE=<qualified profile>
WORKER_CAPABILITIES=cpu,gpu
WORKER_MAX_CONCURRENCY=1
S3_ARTIFACT_BUCKET=library-artifacts
CHEMISTRY_IMAGE_DIGEST=<immutable digest>
```

The Compose project fixes `ALLOWED_WORKER_NAME`, the agent name, and concurrency
to `mscoc6`, `mscoc6`, and one. An accidental Vault edit therefore cannot turn
another machine into a worker.

Key sandbox values:

```text
ATTEMPT_ROOT=/srv/library-prep/attempts
SANDBOXD_SOCKET=/run/library-prep/sandboxd.sock
SANDBOXD_GROUP=libraryprep
ALLOWED_GPU_UUIDS=<same exact UUID>
CHEMISTRY_IMAGE_DIGEST=<same immutable digest>
CHEMISTRY_SECCOMP_PROFILE=/etc/library-prep/chemistry-seccomp.json
CHEMISTRY_APPARMOR_PROFILE=library-prep-chemistry
```

Vault also supplies the filer password, pgBackRest cipher passphrase, and four
distinct S3 credential pairs: storage-init, API, GC, and worker.

## 6. Issue internal certificates

Use an internal CA and separate certificates for:

- API (`api.internal`);
- NATS (`nats.internal`, CN `nats`);
- scheduler (CN `scheduler`);
- storage (`storage.internal`);
- storage-init;
- garbage collector;
- website backend;
- Caddy storage client;
- monitoring;
- worker (CN exactly `mscoc6`).

Install them through `vault_pki_files`. Private keys are never committed or put
in a browser.

## 7. Install without starting the full platform

Run the first Ansible pass while `platform_start_enabled` is still `false`:

```bash
cd deploy/ansible
ansible-galaxy collection install -r requirements.yml
ansible -i inventory/production.yml platform_nodes -m ping
ansible-playbook -i inventory/production.yml --ask-vault-pass site.yml
```

This installs the dedicated account, files, firewall, backup configuration,
sandbox policy, Compose project, and systemd units. It intentionally does not
start the complete website yet.

## 8. Bootstrap authentication

Start only the database, Authentik, and Caddy first. Caddy does not depend on
the application being ready, so the authentication domain is available while
the API remains stopped:

```bash
cd /opt/library-prep/deploy/mscoc6
docker compose --env-file /etc/library-prep/images.env \
  --env-file /etc/library-prep/domains.env \
  --env-file /etc/library-prep/compose.env \
  up -d postgres authentik-server authentik-worker caddy
```

Then:

1. Create an OAuth2/OIDC provider using Authorization Code + PKCE.
2. Set callback `https://app.example.org/auth/callback`.
3. Configure immutable subject, verified email, account status, and roles
   `user`, `operator`, `admin`.
4. Require MFA for administrators and manual approval for every alpha account.
5. Put `AUTH_MODE=oidc` and the exact issuer/client ID in `control.env`.
6. Set `platform_start_enabled: true` in `inventory/production.yml`.

Do not use development authentication on the deployed host.

## 9. Start the complete platform

```bash
cd deploy/ansible
ansible-playbook -i inventory/production.yml --ask-vault-pass site.yml
```

This second pass installs the production OIDC settings, starts the complete
single-host platform, and enables the external PostgreSQL backup timer.

## 10. Inspect the result

On `mscoc6`:

```bash
systemctl --failed
systemctl status library-prep-sandboxd.service
systemctl status library-prep-mscoc6.service
systemctl status library-prep-mscoc6-backup.timer

cd /opt/library-prep/deploy/mscoc6
docker compose \
  --env-file /etc/library-prep/images.env \
  --env-file /etc/library-prep/domains.env \
  --env-file /etc/library-prep/compose.env \
  ps

ss -lntup
nft list ruleset
```

Confirm that only SSH from the approved network and public TCP 80/443 are
reachable. PostgreSQL, NATS, API, storage internals, and Prometheus must not be
public.

## 11. Qualify before users

Run a small end-to-end job first, then retain evidence for:

- one worker row named `mscoc6` and no other schedulable worker;
- `WORKER_MAX_CONCURRENCY=1`;
- queued behavior while the GPU is busy;
- GPU UUID/profile match and real chemistry smoke;
- upload, checksum, range, resume, download, and retention;
- sandbox containment and scratch/output limits;
- worker crash, lease expiry, and stale fencing;
- complete JetStream loss and database reconstruction;
- PostgreSQL restore from the external filesystem;
- host reboot and automatic service recovery.

The alpha stays closed until [RELEASE_GATES.md](RELEASE_GATES.md) has retained
evidence from the real host.
