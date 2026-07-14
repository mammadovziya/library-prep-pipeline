# Seven-host deployment

## 1. Host baseline

Use supported Ubuntu LTS hosts with synchronized clocks, Docker Engine/Compose v2, the NVIDIA container toolkit on compute nodes, AppArmor, WireGuard, nftables, and a restricted operations account. Assign the inventory in `deploy/ansible/inventory/example.yml` to exactly one control host, one RTX 5090 worker, and five RTX 4090 workers. The three largest compute volumes also receive `storage` roles.

Public DNS:

```text
app.example.org      -> public control IP
auth.example.org     -> public control IP
objects.example.org  -> public control IP
```

The Ansible host template maps `objects.example.org` privately to both gateway WireGuard IPs. Storage certificates must contain `storage.internal`, `storage-a.internal`, `storage-b.internal`, and the public object hostname as SANs.

## 2. Build and pin artifacts

Build on a trusted Linux builder and scan before promotion:

```bash
docker build -f docker/postgres.Dockerfile -t registry.internal/library-prep-postgres:18.4 .
for service in api scheduler gc storageinit; do
  docker build -f docker/go.Dockerfile --build-arg SERVICE=$service -t registry.internal/library-prep-$service:candidate .
done
docker build -f docker/agent.Dockerfile -t registry.internal/library-prep-agent:candidate .
docker build -f docker/chemistry.Dockerfile -t registry.internal/library-prep-chemistry:candidate .
docker build -f docker/web.Dockerfile -t registry.internal/library-prep-web:candidate .
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o build/linux-amd64/library-prep-sandboxd ./cmd/sandboxd
```

Resolve every base and final image to a registry digest. Populate `images.env` with `name@sha256:...`; mutable tags are rejected by release review even though `.env.example` shows the intended version family.

## 3. Vault inputs

Create encrypted `group_vars`/`host_vars`, never plaintext repository files. The common role consumes:

- `vault_wireguard_private_key` and every peer public key.
- `vault_environment_files`: a mapping of filename to complete file content.
- `vault_pki_files`: a per-host mapping of certificate/key filename to PEM content.
- Per-service S3 keys, PostgreSQL/Authenik secrets, OIDC secret, session secret, pgBackRest cipher passphrases, and backup SSH material.

Required environment files:

| Host | Files |
|---|---|
| Control | `images.env`, `domains.env`, `control-compose.env`, `control.env`, `gc.env`, `storage-init.env`, `web.env`, `authentik.env` |
| Worker | `images.env`, `worker.env`, `sandboxd.env` |
| Storage | `images.env`, `storage.env` |

Key worker values include unique `WORKER_ID`, certificate-matching `WORKER_NAME`, exact `GPU_UUID`, `GPU_TYPE`, `WORKER_CAPABILITIES=cpu,gpu`, `WORKER_MAX_CONCURRENCY=1`, `INTERNAL_API_URL=https://api.internal:8443`, `NATS_URL=tls://nats.internal:4222`, `S3_INTERNAL_URL=https://objects.example.org`, distinct S3 credentials, attempt root, and the qualified chemistry digest.

`control-compose.env` contains only the three secrets required while Compose renders the project: `POSTGRES_PASSWORD`, `AUTHENTIK_DB_PASSWORD`, and `SEAWEED_FILER_DB_PASSWORD`. The runtime service files remain separate. Every production environment file is supplied through Ansible Vault.

`sandboxd.env` contains only policy configuration: attempt root, exact chemistry digest, allowed GPU UUID, seccomp path, AppArmor profile, Unix socket, and `SANDBOXD_GROUP=libraryprep-agent`. It contains no network credentials. Ansible creates fixed GID 65532 for the agent socket and fixed GID 65200 for read-only access to per-service private keys; only containers that need PKI material receive the latter supplementary group.

## 4. Internal PKI

Issue a private CA and short-lived service certificates. Required identities include API, scheduler, GC, storage-init, web BFF, Caddy, NATS, both storage gateways, and every worker. Worker certificate CN must exactly equal `WORKER_NAME`; the API binds that CN to the worker UUID on every internal request. NATS maps certificate identities to subject permissions.

Do not mount the Docker socket into Authentik or an agent. Authentik runs server and worker containers without managed outposts.

## 5. Apply configuration

```bash
cd deploy/ansible
ansible-galaxy collection install -r requirements.yml
ansible-playbook -i inventory/production.yml --ask-vault-pass site.yml
```

The playbook installs full-mesh WireGuard `/32` peers, private names, nftables, Compose projects, secrets/PKI, migrations, systemd units, the host sandboxd binary, storage metadata, S3 credentials, private Prometheus/node/blackbox monitoring, and backup repositories.

Inspect after deployment:

```bash
systemctl --failed
wg show
nft list ruleset
docker compose -f /opt/library-prep/deploy/control/compose.yml ps
docker compose -f /opt/library-prep/deploy/worker/compose.yml ps
docker compose -f /opt/library-prep/deploy/storage/compose.yml ps
```

## 6. Authentik

Create an OIDC provider using Authorization Code + PKCE. Configure exact issuer/audience, callback `https://app.example.org/auth/callback`, and claims for immutable `sub`, email verification, account status, and roles `user`, `operator`, `admin`. Require MFA for admins and manual approval for every alpha account. Disable managed outposts.

## 7. Qualification before enabling users

1. Run S3 tests through both the public Caddy hostname and private split-horizon hostname: multipart, checksum headers, ETag CORS exposure, abort, range, presign, listing, lifecycle, and gateway failover. Verify each worker policy permits only `GetObject` on inputs/attempt artifacts and `PutObject` beneath attempt prefixes; explicitly prove that `DeleteObject`, listing, bucket administration, and writes outside the attempt hierarchy are denied.
2. Confirm Caddy streams a file larger than RAM without proportional RSS growth.
3. Reboot every host and verify the pinned systemd unit restores the intended Compose project.
4. Run the chemistry image startup qualification on each GPU generation with the production AppArmor/seccomp policies.
5. Perform the restore, NATS reconstruction, stale-fencing, network partition, and orphan-cleanup gates.

Do not change the web label from `CONTROLLED ALPHA` until every public-registration gate is signed off.
