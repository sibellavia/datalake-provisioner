# Data Lake Provisioner (Go)

## Purpose
Provision and manage Data Lake instances for Movincloud on top of **Ceph RGW**.

This service currently focuses on Ceph provisioning lifecycle (user, bucket, quota) and async operation tracking.

## Current status
Implemented:
- Go API service with Chi
- PostgreSQL persistence for lakes/operations
- Async provisioning workflow
- Ceph **RGW Admin Ops API** adapter (no CLI dependency)
- OpenAPI contract (`api/openapi.yaml`)

Operational runbook:
- `docs/lab-ceph-rgw-setup.md` (exact lab steps/config used on Proxmox + Ceph)

Kubernetes manifests (k3s lab):
- `k8s/` (see `k8s/README.md`)

Helm chart (k3s lab):
- `charts/datalake-provisioner`

Not implemented yet:
- idempotency key handling
- Kubernetes deployment automation for UI/backend

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
- `DATABASE_URL` (default local postgres URL)
- `INTERNAL_TOKEN` (required in real deployments)

### Ceph RGW Admin API
- `RGW_ENDPOINT` (e.g. `http://rook-ceph-rgw...:8080`)
- `RGW_ADMIN_PATH` (default: `/admin`)
- `RGW_REGION` (default: `us-east-1`)
- `RGW_ACCESS_KEY_ID` (admin user)
- `RGW_SECRET_ACCESS_KEY` (admin user)
- `RGW_INSECURE_SKIP_VERIFY` (`true|false`)

## API security (MVP)
- `X-Internal-Token`: shared internal token
- `X-Tenant`: tenant context header

## Provision flow (implemented)
1. `POST /v1/lakes`
2. create lake row (`provisioning`) + operation row (`pending`)
3. async worker starts:
   - mark operation `running`
   - Ceph: get/create user
   - Ceph: ensure S3 key
   - Ceph: ensure bucket
   - Ceph: set/enable user quota
   - mark lake `ready`
   - mark operation `success`
4. on errors: mark lake `failed`, operation `failed`

## End-to-end workflow (lab validated)
1. User (or Movincloud) requests provisioning with `tenant`, `userId`, `sizeGiB`.
2. Request reaches `datalake-provisioner` API (`POST /v1/lakes`) with:
   - `X-Internal-Token`
   - `X-Tenant`
3. Provisioner stores operation/lake state in PostgreSQL.
4. Provisioner calls Ceph RGW Admin API (`RGW_ENDPOINT`, `RGW_ADMIN_PATH=/admin`) using admin credentials.
5. Ceph side actions performed:
   - RGW user create/reuse (lake-scoped uid)
   - S3 access keys ensure/create
   - bucket create/ensure
   - `user_quota` set and enabled to requested size
6. Provisioner updates DB state:
   - operation `success`
   - lake `ready`
7. Client queries:
   - `GET /v1/operations/{operationId}`
   - `GET /v1/lakes/{lakeId}`
8. Ceph validation (manual):
   - `radosgw-admin user info --uid <rgwUser>`
   - `radosgw-admin bucket list | grep <bucketName>`

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
