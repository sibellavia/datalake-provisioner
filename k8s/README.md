# k3s manifests (lab)

These manifests deploy:
- PostgreSQL (single-instance lab setup)
- DB migration job (`0001_init.up.sql`)
- Data Lake Provisioner API (`dev3at/datalake-provisioner:0.1.1`)
- ClusterIP service + optional Ingress

## 1) Create secrets from templates

```bash
cp secret.example.yaml secret.yaml
cp postgres-secret.example.yaml postgres-secret.yaml
```

Edit:
- `secret.yaml`:
  - `RGW_ACCESS_KEY_ID`
  - `RGW_SECRET_ACCESS_KEY`
  - `DATABASE_URL` (if different)
- `postgres-secret.yaml`:
  - `POSTGRES_PASSWORD` (and user/db if desired)

Apply secrets:

```bash
kubectl apply -f namespace.yaml
kubectl apply -f postgres-secret.yaml
kubectl apply -f secret.yaml
```

## 2) Deploy all other resources

```bash
kubectl apply -k .
```

## 3) Check rollout

```bash
kubectl -n datalake get pods
kubectl -n datalake get svc
kubectl -n datalake get jobs
```

## 4) Access API

Port-forward:

```bash
kubectl -n datalake port-forward svc/datalake-provisioner 8081:8081
```

Then:

```bash
curl -X POST http://127.0.0.1:8081/v1/lakes \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant: tenant-a' \
  -d '{"userId":"user-1","sizeGiB":10}'
```

## Notes
- `ingress.yaml` uses host `datalake-provisioner.lks.local` and Traefik ingress class.
- For production, replace in-cluster Postgres and static secrets with managed services/secret manager.
