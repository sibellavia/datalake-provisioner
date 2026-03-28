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
- `DATABASE_URL` (default local postgres URL)
- `INTERNAL_TOKEN` (required in real deployments)
- `WORKER_ENABLED` (default: `true`)
- `WORKER_POLL_INTERVAL_SECONDS` (default: `2`)
- `WORKER_STALE_AFTER_SECONDS` (default: `120`)
- `WORKER_MAX_ATTEMPTS` (default: `3`)

### Ceph RGW Admin API
- `RGW_ENDPOINT` (e.g. `http://rook-ceph-rgw...:8080`)
- `RGW_ADMIN_PATH` (default and currently required: `/admin`)
- `RGW_REGION` (default: `us-east-1`)
- `RGW_ACCESS_KEY_ID` (admin user)
- `RGW_SECRET_ACCESS_KEY` (admin user)
- `RGW_INSECURE_SKIP_VERIFY` (`true|false`)

## API security (MVP)
- `X-Internal-Token`: shared internal token
- `X-Tenant`: tenant context header
- `Idempotency-Key`: optional header supported on create/resize/delete operations

## Health and readiness
- `/health`: lightweight liveness endpoint
- `/ready`: dependency readiness endpoint (DB + RGW)

## Logging
- Service logs are emitted as structured JSON to stdout by default.
- The intended production model is for the platform log pipeline to ship those logs to OpenSearch.

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

## End-to-end workflow (lab validated)
1. User (or Movincloud) requests provisioning with `tenant`, `userId`, `sizeGiB`.
2. Request reaches `datalake-provisioner` API (`POST /v1/lakes`) with:
   - `X-Internal-Token`
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
  - internal token
- Built/pushed amd64 image: `dev3at/datalake-provisioner:0.1.1`
- Set numeric security context (`runAsUser/runAsGroup=65532`) for distroless container compatibility
- Created `datalake` DB manually (existing PG volume), then reran migration job

## Local run
1. Run PostgreSQL and create DB.
2. Apply migrations.
3. Export env vars (or use `.env.example`).
4. Start API:

```bash
go run ./cmd/server
```

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

## Helm install (lab)
Install/upgrade with explicit values for RGW + token:

```bash
helm upgrade --install datalake-provisioner ./charts/datalake-provisioner \
  --namespace datalake --create-namespace \
  --set provisioner.internalToken=change-me \
  --set provisioner.rgwEndpoint=http://192.168.3.251:7480 \
  --set provisioner.rgwAccessKeyId=<RGW_ACCESS_KEY_ID> \
  --set provisioner.rgwSecretAccessKey=<RGW_SECRET_ACCESS_KEY>
```

Then port-forward:

```bash
kubectl -n datalake port-forward svc/datalake-provisioner-datalake-provisioner 8081:8081
```

## Quick test
```bash
curl -X POST http://localhost:8081/v1/lakes \
  -H 'Content-Type: application/json' \
  -H 'X-Internal-Token: change-me' \
  -H 'X-Tenant: tenant-a' \
  -d '{"userId":"user-1","sizeGiB":10}'
```

Poll operation:
```bash
curl -H 'X-Internal-Token: change-me' -H 'X-Tenant: tenant-a' \
  http://localhost:8081/v1/operations/<operationId>
```

Get lake:
```bash
curl -H 'X-Internal-Token: change-me' -H 'X-Tenant: tenant-a' \
  http://localhost:8081/v1/lakes/<lakeId>
```
