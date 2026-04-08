# Data Lake Provisioner (Go)

## Purpose
Provision and manage Data Lake instances for Movincloud on top of **Ceph RGW**.

This service currently focuses on Ceph provisioning lifecycle (internal RGW user, quota, and evolving multi-bucket model) plus async operation tracking.

## Current status
Implemented:
- Go API service with Chi
- PostgreSQL persistence for lakes/operations
- Durable background operation runner backed by PostgreSQL
- Ceph **RGW Admin Ops API** adapter via `github.com/ceph/go-ceph/rgw/admin` (no CLI dependency)
- OpenAPI contract (`api/openapi.yaml`)

Operational runbook:
- `docs/lab-ceph-rgw-setup.md` (exact lab steps/config used on Proxmox + Ceph)
- `docs/movincloud-provisioning-flow.md` (full Movincloud -> Helm -> AKS provisioning flow)

Kubernetes manifests (k3s lab):
- `k8s/` (see `k8s/README.md`)

Helm chart (k3s lab):
- `charts/datalake-provisioner`

Not implemented yet:
- Kubernetes deployment automation for UI/backend

## Architecture principle: source of truth
- **PostgreSQL** is the source of truth for the **product model**:
  - lake existence
  - bucket existence as product resources
  - tenant mapping
  - lifecycle/status metadata
  - committed quota in the control-plane
- **Ceph RGW** is the source of truth for **physical storage facts**:
  - actual user/bucket presence
  - configured quota state
  - ownership
  - usage bytes/object counts
- If DB and RGW diverge, that is **drift to detect/reconcile**, not a reason to derive the product inventory from RGW alone.

## Project structure

```text
datalake-provisioner/
  cmd/server/main.go
  api/openapi.yaml
  migrations/
    0001_init.up.sql
    0001_init.down.sql
  internal/
    app/
    config/
    domain/
    http/
      handlers/
    service/
    store/postgres/
    ceph/
```

## Environment variables
- `HTTP_PORT` (default: `8081`)
- `READ_HEADER_TIMEOUT_SECONDS` (default: `5`)
- `READY_TIMEOUT_SECONDS` (default: `3`)
- `LOG_FORMAT` (`json|text`, default: `json`)
- `LOG_LEVEL` (`debug|info|warn|error`, default: `info`)
- `OTEL_ENABLED` (`true|false`, default: `false`)
- `OTEL_EXPORTER_OTLP_ENDPOINT` (OTLP gRPC endpoint, e.g. `otel-collector.observability.svc.cluster.local:4317`)
- `OTEL_EXPORTER_OTLP_INSECURE` (`true|false`, default: `true`)
- `DATABASE_URL` (default local postgres URL)
- `WORKER_ENABLED` (default: `true`)
- `WORKER_POLL_INTERVAL_SECONDS` (default: `2`)
- `WORKER_STALE_AFTER_SECONDS` (default: `120`)
- `WORKER_MAX_ATTEMPTS` (default: `3`)

### Ceph RGW Admin API
- `RGW_ENDPOINT` (internal/admin RGW endpoint used by provisioner, e.g. `http://rook-ceph-rgw...:8080`)
- `RGW_ADMIN_PATH` (default and currently required: `/admin`)
- `RGW_REGION` (default: `us-east-1`)
- `RGW_S3_ADVERTISED_ENDPOINT` (customer-facing S3 endpoint returned by customer S3 access API; defaults to `RGW_ENDPOINT` when unset)
- `RGW_ACCESS_KEY_ID` (admin user)
- `RGW_SECRET_ACCESS_KEY` (admin user)
- `RGW_INSECURE_SKIP_VERIFY` (`true|false`)

### Critical endpoint split (production)
- `RGW_ENDPOINT` is **internal** and may expose `/admin`.
- `RGW_S3_ADVERTISED_ENDPOINT` is **customer-facing** (for SDK/CLI) and must be a stable HTTPS host (e.g. `https://s3.leonardocloudeverywhere.com`).
- Do **not** return internal RGW node/Tailscale endpoints to customers.

## Request headers
- `X-Tenant`: tenant context header
- `Idempotency-Key`: optional header supported on create/resize/delete operations

## Security note
- The service currently has no built-in authn/authz layer.
- It should be exposed only through trusted private network paths / gateway infrastructure.
- Keep RGW Admin Ops (`/admin`) reachable only from trusted internal paths (provisioner/backend), not from the public customer S3 endpoint.

## Health and readiness
- `/health`: lightweight liveness endpoint
- `/ready`: dependency readiness endpoint (DB + RGW)

## Logging
- Service logs are emitted as structured JSON to stdout by default.
- The intended production model is for the platform log pipeline to ship those logs to OpenSearch.

## Metrics
- `/metrics`: Prometheus metrics endpoint
- Exposes HTTP, readiness, worker, operation, and Ceph adapter metrics.
- Kubernetes/Helm service manifests include Prometheus scrape annotations for `/metrics` on port `8081`.

## Tracing
- OTLP trace export is supported via OpenTelemetry.
- Current scope instruments HTTP, worker execution, service methods, and Ceph adapter calls.
- Async trace propagation across persisted operations is not implemented yet, so API-request traces and later worker traces are not stitched into a single end-to-end trace yet.

## Provision flow (implemented)
1. `POST /v1/lakes`
2. create lake row (`provisioning`) + operation row (`pending`)
3. background worker picks up the pending operation from PostgreSQL:
   - mark operation `running`
   - Ceph: get/create internal RGW user
   - Ceph: ensure internal S3 key
   - Ceph: set/enable user quota
   - mark lake `ready`
   - mark operation `success`
4. a newly provisioned lake is an **empty boundary** with quota and internal storage credentials; buckets will be explicit child resources
5. on errors: operation is retried up to `WORKER_MAX_ATTEMPTS`, then lake and operation are marked `failed`

## Customer S3 credential endpoint (implemented)
- `POST /v1/internal/lakes/{lakeId}/customer-s3-access`
- Requires `X-Tenant`.
- Lake must be `ready`.
- Returns customer-facing S3 material:
  - `lakeId`, `rgwUser`, `s3Endpoint`, `s3Region`, `accessKeyId`, `secretAccessKey`, `issuedAt`, `credentialStatus`.
- `s3Endpoint` value is always derived from `RGW_S3_ADVERTISED_ENDPOINT`.

### Important credential semantics
- Model remains: `1 lake = 1 RGW user`.
- Internal and customer credentials are tracked as separate product concerns.
- Current behavior can reuse an existing RGW user key when first materializing customer access if no active customer key metadata exists yet.
- If strict guarantee is required (`customer key` always distinct from historical internal key), enforce an explicit forced key creation/rotation policy.

## End-to-end workflow (lab validated)
1. User (or Movincloud) requests provisioning with `tenant` and `sizeGiB`.
2. Request reaches `datalake-provisioner` API (`POST /v1/lakes`) with:
   - `X-Tenant`
3. Provisioner stores operation/lake state in PostgreSQL.
4. Provisioner calls Ceph RGW Admin API (`RGW_ENDPOINT`, `RGW_ADMIN_PATH=/admin`) using admin credentials.
5. Ceph side actions performed:
   - RGW user create/reuse (lake-scoped uid)
   - internal S3 access keys ensure/create
   - `user_quota` set and enabled to requested size
6. Provisioner updates DB state:
   - operation `success`
   - lake `ready`
7. Client queries:
   - `GET /v1/operations/{operationId}`
   - `GET /v1/lakes/{lakeId}`
8. Ceph validation (manual):
   - `radosgw-admin user info --uid <rgwUser>`

## Manual infrastructure setup performed (lab)
### Proxmox/Ceph host
- Installed RGW package: `radosgw`
- Configured and started `ceph-radosgw@rgw.pve1`
- Added RGW config in `/etc/pve/ceph.conf`:
  - `rgw_frontends = beast port=7480`
  - keyring path under `/var/lib/ceph/radosgw/...`
- Created provisioner system user and caps:
  - `provisioner-admin`
  - caps: `users=*;buckets=*;metadata=*;usage=*`

### k3s VM
- Deployed manifests from `k8s/` (Postgres + migration + provisioner)
- Set Kubernetes secrets:
  - Postgres credentials
  - `DATABASE_URL`
  - RGW admin access key/secret
- Built/pushed amd64 image: `dev3at/datalake-provisioner:0.1.1`
- Set numeric security context (`runAsUser/runAsGroup=65532`) for distroless container compatibility
- Created `datalake` DB manually (existing PG volume), then reran migration job

## Customer-access workflow (recommended)
1. Platform/Helm hook provisions the lake (`POST /v1/lakes`).
2. Poll operation until lake is `ready`.
3. Call `POST /v1/internal/lakes/{lakeId}/customer-s3-access`.
4. Store returned S3 creds in tenant secret store (K8s Secret / platform vault).
5. Return to customer:
   - `s3Endpoint` (advertised endpoint),
   - `s3Region`,
   - `accessKeyId`,
   - `secretAccessKey`.

## Exposing RGW to the internet (domain + DNS)
Assuming domain `leonardocloudeverywhere.com` and subdomain `s3.leonardocloudeverywhere.com`:

1. Create a public entrypoint (LB/Ingress/Gateway) that can reach RGW data-plane.
2. Create DNS record:
   - `A` / `CNAME` for `s3.leonardocloudeverywhere.com` -> public entrypoint.
3. Configure TLS certificate for `s3.leonardocloudeverywhere.com`.
4. Proxy S3 traffic to RGW (`http://<rgw-internal-ip>:7480`) with minimal rewrites.
5. Block `/admin` on this public endpoint.
6. Set `RGW_S3_ADVERTISED_ENDPOINT=https://s3.leonardocloudeverywhere.com` in provisioner config.

### Kong / Ingress notes
- Prefer **host-based** route: `s3.leonardocloudeverywhere.com`.
- Avoid path-prefix style (`/s3`) for real S3 clients.
- Keep proxy behavior transparent (headers/query/body/signature-sensitive requests).
- Ensure large body / long timeout settings for multipart uploads.
- Explicitly deny or do not route `/admin` externally.

## Local run
1. Run PostgreSQL and create DB.
2. Apply migrations from `migrations/*.up.sql`.
3. Export env vars (or use `.env.example`).
4. Start API:

```bash
go run ./cmd/server
```

## Docker Compose (non-Kubernetes / target environment)
For simple testing outside Kubernetes, use the included Compose setup:

- `compose.yaml`
- `compose.env.example`

### 1) Prepare environment

```bash
cp compose.env.example .env.compose
```

Then edit at least:
- `RGW_ENDPOINT`
- `RGW_ACCESS_KEY_ID`
- `RGW_SECRET_ACCESS_KEY`
- optional Postgres / OTEL values if needed

### 2) Start the stack

```bash
docker compose --env-file .env.compose up --build
```

This starts:
- `postgres`
- `migrate` (one-shot SQL migration container)
- `provisioner`

### 3) Validate the service

```bash
curl http://localhost:8081/health
curl http://localhost:8081/ready
curl http://localhost:8081/metrics
```

### 4) Smoke-test the API

```bash
curl -X POST http://localhost:8081/v1/lakes \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant: tenant-a' \
  -d '{"sizeGiB":10}'
```

Poll operation:

```bash
curl -H 'X-Tenant: tenant-a' \
  http://localhost:8081/v1/operations/<operationId>
```

Get lake:

```bash
curl -H 'X-Tenant: tenant-a' \
  http://localhost:8081/v1/lakes/<lakeId>
```

Validate returned customer key against RGW/S3 endpoint:

```bash
# 1) Request customer S3 access
ACCESS_JSON=$(curl -sS -X POST \
  -H 'X-Tenant: tenant-a' \
  http://localhost:8081/v1/internal/lakes/<lakeId>/customer-s3-access)

# 2) Extract values
ENDPOINT=$(echo "$ACCESS_JSON" | jq -r .s3Endpoint)
REGION=$(echo "$ACCESS_JSON" | jq -r .s3Region)
AK=$(echo "$ACCESS_JSON" | jq -r .accessKeyId)
SK=$(echo "$ACCESS_JSON" | jq -r .secretAccessKey)

# 3) Use standard AWS CLI against Ceph RGW
AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" \
aws --region "$REGION" --endpoint-url "$ENDPOINT" s3 ls
```

### Notes for Compose usage
- The service still expects an external Ceph RGW endpoint; Compose only supplies Postgres + migrations.
- The migration container reapplies all `*.up.sql` files on startup; the current migrations are written to tolerate repeated application for lab/test usage.
- Helm charts were removed and will be rebuilt later; Compose is the current recommended path for non-Kubernetes testing.

## Container image (Docker)
Build locally:

```bash
docker build -t datalake-provisioner:0.1.1 .
```

Tag for Docker Hub:

```bash
docker tag datalake-provisioner:0.1.1 <dockerhub-username>/datalake-provisioner:0.1.1
```

Push:

```bash
docker login
docker push <dockerhub-username>/datalake-provisioner:0.1.1
```

