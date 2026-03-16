# Movincloud -> Data Lake Provisioning Flow (Documented)

This document describes the observed end-to-end flow when a user provisions the custom Helm service **Data Lake** from Movincloud.

It is based on:
- browser network calls
- Kubernetes routing configuration in Kong (`kong/kong-cfg`)
- runtime logs from CMP services (`cmpprovisioning`, `cmp-orchestrator`, `cmp-iac-engine`)

---

## 1) Actors / Services involved

- **Movincloud UI** (`www.movincloud.com`)
- **Kong API Gateway** (`kong` namespace)
- **Resource Manager Service Manager** (`cmp-rm-service-manager`, namespace `resource-manager`)
- **Orchestrator** (`cmp-orchestrator`, namespace `orchestrator`)
- **Provisioning Service** (`cmpprovisioning`, namespace `provisioning`)
- **IAC/Helm Executor** (`cmp-iac-engine`, namespace `provisioning`)
- **IAM backend** (`backend-iam-service`, namespace `iamnew`)
- **MinIO** (assets storage)
- **Kafka** (internal async messaging)
- **Target AKS cluster** where Helm release is installed

---

## 2) API routing (from Kong config)

From `ConfigMap kong-cfg` in namespace `kong`:

- `/api/rmservice` -> `cmp-rm-service-manager.resource-manager:9090`
- `/api/orchestrator` -> `cmp-orchestrator.orchestrator:8080`
- `/api/provisioning` -> `cmpprovisioning.provisioning:8081`
- `/api/iac` -> `cmp-iac-engine.provisioning:8081`
- `/api/iam` -> `backend-iam-service.iamnew:8086/iam`

---

## 3) Provisioning sequence (observed)

### Step A — Service definition already present in catalog
Catalog item id example:
- `69b82677183c95e294154af5`

(Previously created via `/api/rmservice/v2/services`.)

### Step B — Upload runtime asset for this provisioning
UI call:
- `PUT /api/orchestrator/provisioning/{serviceId}/upload`

Observed log (`cmp-orchestrator`):
- `Object assets/69b82677183c95e294154af5/blob uploaded`

### Step C — Submit provisioning request
UI call:
- `POST /api/provisioning/service/apply`

Payload includes:
- `systemUuid` (target system)
- `clusterId` (target cluster)
- `id` (catalog service id)
- Helm parameters (`release`, `namespace`)
- `minioExtraAssetsUri`

Response example (request UUID):
- `Z0Y_OavDRkWmtizDB2Q27g`

### Step D — Async handoff through Kafka
Observed log (`cmp-iac-engine`):
- `Received Provisioning Message ... resources_<systemUuid>_<requestUuid>`

### Step E — Helm deployment execution
Observed log (`cmp-iac-engine`):
- `Executing Helm login to registry: oci://leonardoengcr.azurecr.io`
- `Executing Helm command: helm upgrade --install datalake oci://leonardoengcr.azurecr.io/leonardo/datalake-provisioner --version 0.1.2 --namespace datalake --create-namespace --values values.yaml --kubeconfig /var/tmp/<requestUuid>/kubeconfig`

### Step F — Result publication
Observed log (`cmp-iac-engine`):
- `Provisioning Service: Sending result ... status: COMPLETE`

### Step G — Provisioning status update
Observed log (`cmpprovisioning`):
- `Received Message: key:resources_ND_CREATE ... message: Z0Y_OavDRkWmtizDB2Q27g`
- `Updating Provisioning result: uuid:Z0Y_OavDRkWmtizDB2Q27g; script type:HELM; status COMPLETE; is success? true`

### Step H — UI polling / dashboard refresh
Observed UI calls:
- `GET /api/provisioning/plans/stats`
- `GET /api/orchestrator/provisioning/running/stats`
- `GET /api/provisioning/service/pipelines/check`

These endpoints provide state/health counters and checks, not the initial trigger.

---

## 4) Concrete IDs from inspected execution

- Catalog service id: `69b82677183c95e294154af5`
- systemUuid: `6de2a278-36be-4fd8-9afd-cfccf1118b91`
- request/provisioning UUID: `Z0Y_OavDRkWmtizDB2Q27g`
- Helm release: `datalake`
- Helm chart: `oci://leonardoengcr.azurecr.io/leonardo/datalake-provisioner`
- Helm chart version used in observed run: `0.1.2`
- Namespace: `datalake`

---

## 5) Notes / caveats

- Some historical runs showed retries/failures before final completion (seen in logs with other UUIDs).
- `cmpprovisioning` logs also show Redis connection errors in the same timeframe; despite that, the inspected request completed successfully.
- Sensitive credentials/tokens should never be kept in plain text logs/documentation.

---

## 6) Sequence diagram (textual)

1. UI -> Orchestrator: upload asset blob (`/api/orchestrator/provisioning/{id}/upload`)
2. UI -> Provisioning: apply request (`/api/provisioning/service/apply`) -> returns request UUID
3. Provisioning -> Kafka: publish provisioning message
4. IAC Engine <- Kafka: consume provisioning message
5. IAC Engine -> ACR: helm registry login
6. IAC Engine -> AKS API: `helm upgrade --install ...`
7. IAC Engine -> Kafka: publish provisioning result (COMPLETE/FAILED)
8. Provisioning <- Kafka: consume result, update status
9. UI -> Provisioning/Orchestrator: poll/check stats and pipeline state
