# Helm chart: datalake-provisioner

This chart deploys (non-optional for now):
- PostgreSQL
- Migration Job
- Data Lake Provisioner API

## Install

```bash
helm upgrade --install datalake-provisioner . \
  --namespace datalake --create-namespace \
  --set provisioner.internalToken=change-me \
  --set provisioner.rgwEndpoint=http://192.168.3.251:7480 \
  --set provisioner.rgwAccessKeyId=<RGW_ACCESS_KEY_ID> \
  --set provisioner.rgwSecretAccessKey=<RGW_SECRET_ACCESS_KEY>
```

## Verify

```bash
kubectl -n datalake get pods
kubectl -n datalake get jobs
kubectl -n datalake get svc
```

## Test

```bash
kubectl -n datalake port-forward svc/datalake-provisioner-datalake-provisioner 8081:8081
```

```bash
curl -X POST http://127.0.0.1:8081/v1/lakes \
  -H 'Content-Type: application/json' \
  -H 'X-Internal-Token: change-me' \
  -H 'X-Tenant: tenant-a' \
  -d '{"userId":"user-1","sizeGiB":10}'
```
