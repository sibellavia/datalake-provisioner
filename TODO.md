# TODO / Roadmap

This file captures the recommended implementation order to evolve `datalake-provisioner` from the current MVP into a production-ready **Data Lake as a Service** control-plane.

## Current decisions

- Keep the current **direct Ceph RGW** architecture for now.
- Keep the Go service as the **product API / control-plane**.
- Treat **Kong** as the trusted gateway / single ingress point.
- Evolve the product model toward:
  - **1 lake = 1 top-level product object**
  - **1 lake = 1 RGW user/account**
  - **1 lake quota = 1 RGW user quota**
  - **N buckets per lake**
- The provider view must support both:
  - **per-lake usage**
  - **global usage across all lakes**
  - **committed quota across all lakes**
  - **cluster capacity from Ceph**

---

## Priority order

Multi-bucket is now promoted earlier in the roadmap because it is part of the product identity. However, we should still put it on top of the minimum safety rails first:

- durable async execution
- idempotency
- explicit operation/state rules

That gives us the best balance between **product evolution** and **production safety**.

### P0 — minimum safety rails before multi-bucket
These items should be completed before exposing multi-bucket APIs.

#### 1. Fix bucket ownership
**Why:** buckets must belong to the lake identity, not the admin identity.

- [x] Create buckets using the **lake user's S3 credentials**, not the admin S3 client
- [x] Verify bucket owner becomes the lake RGW user
- [x] Verify upload with lake credentials works
- [x] Verify quota / usage accounting align with the lake identity
- [x] Re-run the demo and capture expected evidence

**Validation completed**
- Live check confirmed that a newly created lake bucket is now owned by the lake RGW user, not `provisioner-admin`.
- Upload with lake credentials succeeded.
- Bucket stats and user stats now align with the lake identity.
- Example verified on Ceph host:
  - `rgwUser = lake-bf47c248599f46788f38`
  - `bucket = lake-bf47c248599f46788f38cdf1f36fa740`
  - `owner = lake-bf47c248599f46788f38`

**Done when**
- `radosgw-admin bucket stats --bucket <bucket>` shows `owner=<lake-user>`
- Upload works with the lake credentials
- Usage is attributable to the lake identity

---

#### 2. Replace in-process goroutines with a DB-backed operation queue
**Why:** current async operations are started with goroutines inside the API process, so restarts can strand work.

- [ ] Add durable operation execution model in Postgres
- [ ] Add worker process / worker loop to claim runnable operations
- [ ] Add leases / heartbeats / lease expiry recovery
- [ ] Add retry scheduling with backoff
- [ ] Ensure crashed or interrupted operations can be resumed or failed safely

**Done when**
- Pod restart during provisioning does not lose the operation
- Stuck `running` operations can be recovered automatically
- At-least-once execution is safe with idempotent handling

---

#### 3. Implement real idempotency
**Why:** OpenAPI and schema already mention idempotency, but code does not enforce it.

- [ ] Parse `Idempotency-Key` for create / resize / delete operations
- [ ] Persist idempotency key + request hash + operation link
- [ ] Return the existing operation for same key + same request
- [ ] Return `409` for same key + different request
- [ ] Add tests for retry behavior

**Done when**
- Safe client retries are supported
- Duplicate requests do not create duplicate lakes or operations

---

#### 4. Add real operation state rules
**Why:** the service needs explicit lifecycle guarantees before we expand the product model.

- [ ] Define allowed lake state transitions
- [ ] Define allowed operation transitions
- [ ] Enforce one active operation per lake
- [ ] Block invalid actions, e.g. resize on deleted lake, delete during conflicting operation
- [ ] Return proper `404` / `409` / `400` instead of generic `500`

**Done when**
- Invalid transitions are rejected deterministically
- Concurrent conflicting operations cannot corrupt state

---

### P1 — core product evolution: real multi-bucket lake model
Do this immediately after P0 is in place.

#### 5. Introduce lake + buckets domain model
**Target model**
- 1 lake = 1 RGW user/account
- 1 lake quota = RGW user quota
- N buckets under that lake

- [ ] Add `buckets` table and associated repository/service methods
- [ ] Remove single-bucket assumption from the `lakes` domain model
- [ ] Keep backward compatibility for existing single-bucket lakes
- [ ] Decide whether lake creation auto-creates a default bucket

**Done when**
- The service no longer models a lake as exactly one bucket

---

#### 6. Add bucket lifecycle APIs
- [ ] `POST /v1/lakes/{lakeId}/buckets`
- [ ] `GET /v1/lakes/{lakeId}/buckets`
- [ ] `GET /v1/lakes/{lakeId}/buckets/{bucketId}`
- [ ] `DELETE /v1/lakes/{lakeId}/buckets/{bucketId}`
- [ ] Idempotency + state handling for bucket operations

**Done when**
- A lake can contain multiple buckets managed through the control-plane

---

#### 7. Aggregate usage at lake level and expose fleet-wide totals
**Why:** once a lake can contain multiple buckets, usage must be visible both per lake and across all lakes.

- [ ] Sum bucket usage into lake usage
- [ ] Expose **per-lake total usage** across all buckets in that lake
- [ ] Expose **global total usage across all lakes**
- [ ] Keep bucket count and per-bucket usage available
- [ ] Keep **global committed quota across all lakes** as a first-class reporting requirement

**Done when**
- One lake exposes total usage across all of its buckets
- The service can show global total usage across all lakes
- Provider/dashboard consumers can distinguish lake totals from bucket totals

---

### P2 — production hardening around the multi-bucket model
These items should follow immediately after the core multi-bucket model exists.

#### 8. Improve retries / timeouts / error classification
- [ ] Add per-operation timeout budget
- [ ] Add Ceph / RGW request timeouts
- [ ] Classify transient vs permanent errors
- [ ] Retry only transient failures
- [ ] Add clearer error messages / codes for operators

**Done when**
- Temporary RGW/network issues are retried safely
- Permanent failures fail fast with useful diagnostics

---

#### 9. Add observability and readiness
- [ ] Add `/ready` endpoint that checks DB + RGW reachability
- [ ] Keep `/health` as lightweight liveness endpoint
- [ ] Switch to structured logging (`operationId`, `lakeId`, `bucketId`, `tenantId`, `siteId`, `attempt`)
- [ ] Add Prometheus metrics for operations, duration, retries, failures
- [ ] Add request correlation across API and worker paths

**Done when**
- Operators can answer: what failed, where, why, and for which tenant/lake/bucket
- Kubernetes readiness reflects real service dependencies

---

#### 10. Security hardening around Kong
**Why:** Kong is the trusted entry point, so the service should be hardened around that deployment model instead of assuming open direct access.

- [ ] Restrict service exposure so traffic comes from Kong / trusted network paths only
- [ ] Treat `X-Tenant` as trusted only when forwarded by Kong
- [ ] Keep or harden the internal token strategy between Kong and service
- [ ] Support `existingSecret` / external secret management in Helm
- [ ] Reject insecure defaults outside dev/lab mode
- [ ] Enforce TLS to RGW where possible
- [ ] Document trust boundaries clearly

**Done when**
- The service is not exposed as a weak standalone API
- Tenant context is only accepted from the trusted gateway path
- Secrets are not managed through plain prod Helm values

---

#### 11. Improve deployment and multi-DC configuration
- [ ] Add startup config validation
- [ ] Add `SITE_ID` / `DC_ID` to config, logs, and metadata
- [ ] Make external Postgres first-class in Helm
- [ ] Keep in-cluster Postgres as lab-only / optional
- [ ] Add per-DC values overlays / examples

**Done when**
- Each datacenter can deploy the service with minimal custom work
- Misconfiguration fails fast at startup

---

### P3 — provider reporting and reconciliation
These items add the provider view needed to operate the service at scale.

#### 12. Add usage / quota / capacity reporting
**Why:** the provider needs both per-lake and global visibility, plus capacity context from Ceph.

- [ ] Add periodic sync job that reads RGW user stats and bucket stats
- [ ] Store usage snapshots in Postgres
- [ ] Expose **per-lake total usage**
- [ ] Expose **global total usage across all lakes**
- [ ] Expose **global committed quota across all lakes**
- [ ] Expose Ceph cluster capacity snapshot(s)
- [ ] Calculate oversubscription ratio (`committed / usable capacity`)

**Done when**
- Operators can see:
  - usage for one lake
  - usage for all lakes combined
  - committed quota for all lakes combined
  - Ceph capacity and headroom

---

#### 13. Add reconciliation / drift detection
- [ ] Detect DB lakes missing in RGW
- [ ] Detect RGW users/buckets not tracked in DB
- [ ] Detect quota mismatches
- [ ] Detect orphaned resources after partial failures
- [ ] Add reconciliation reports / alerts

**Done when**
- The control-plane can detect and surface drift rather than silently diverging

---

### P4 — richer product features
These features make the service feel more like a complete managed object storage product.

#### 14. Credentials lifecycle
- [ ] Decide how credentials are returned to clients
- [ ] Add credential rotation flow
- [ ] Add revoke/regenerate support
- [ ] Consider multiple credentials / service accounts per lake later

---

#### 15. Bucket features
- [ ] Bucket versioning support
- [ ] Lifecycle policy support
- [ ] Tags / metadata
- [ ] Optional retention / object lock evaluation

---

#### 16. Deletion workflows
- [ ] Define conservative delete vs force delete behavior
- [ ] Handle non-empty buckets cleanly
- [ ] Add purge options where product allows it

---

#### 17. Access model evolution
- [ ] Evaluate bucket-scoped policies
- [ ] Evaluate read-only / read-write service accounts
- [ ] Keep lake-wide credentials as initial simple model

---

## Suggested PR sequence

1. [x] **PR-1**: Fix bucket ownership
2. [ ] **PR-2**: DB-backed operation queue + worker
3. [ ] **PR-3**: Idempotency + typed errors / API correctness
4. [ ] **PR-4**: State machine / concurrency guards
5. [ ] **PR-5**: Multi-bucket schema / domain model
6. [ ] **PR-6**: Bucket lifecycle APIs
7. [ ] **PR-7**: Lake aggregated usage + fleet-wide totals
8. [ ] **PR-8**: Retries / timeouts / error classification
9. [ ] **PR-9**: Observability / readiness / metrics
10. [ ] **PR-10**: Security hardening for Kong deployment model
11. [ ] **PR-11**: Deployment + multi-DC configuration
12. [ ] **PR-12**: Usage sync + capacity reporting
13. [ ] **PR-13**: Reconciliation / drift detection
14. [ ] **PR-14+**: Credentials, lifecycle, policies, richer product features

---

## Explicit non-priorities for now

- [ ] Do **not** switch to Rook/OBC until there is a dedicated quota/resize spike with clear pass/fail criteria
- [ ] Do **not** redesign around Proxmox-native storage objects (`pvesm`, CephFS, RBD) for this object-storage product
- [ ] Do **not** add a Proxmox plugin before the core control-plane is production-safe

---

## Success criteria for the next milestone

The next major milestone is complete when all of the following are true:

- [x] Bucket owner is the lake RGW user
- [ ] Operations survive API pod restart
- [ ] Idempotent retries are safe
- [ ] Conflicting operations are prevented
- [ ] One lake can contain multiple buckets
- [ ] Per-lake usage is aggregated across all buckets in the lake
- [ ] **Global total usage across all lakes** is available
- [ ] Basic DB / RGW readiness is visible
- [ ] Basic structured operation logs exist

