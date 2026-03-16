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
docker build -t datalake-provisioner:0.1.0 .
```

Tag for Docker Hub:

```bash
docker tag datalake-provisioner:0.1.0 <dockerhub-username>/datalake-provisioner:0.1.0
```

Push:

```bash
docker login
docker push <dockerhub-username>/datalake-provisioner:0.1.0
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
