# TODO / Roadmap

This file captures the recommended implementation order to evolve `datalake-provisioner` from the current MVP into a production-ready **Data Lake as a Service** control-plane.

## Current decisions

- Keep the current **direct Ceph RGW** architecture for now.
- Keep the Go service as the **product API / control-plane**.
- Treat **Kong** as the trusted gateway / single ingress point.
- End users must **never** interact directly with **RGW** or **Ceph**.
- Evolve the product model toward:
  - **1 lake = 1 top-level product object**
  - **1 lake = 1 internal RGW user/account**
  - **1 lake quota = 1 RGW user quota**
  - **N buckets per lake**
- For the clean design, a new lake should be provisioned as an **empty boundary**:
  - quota + internal storage credentials
  - **zero buckets by default**
- No backward-compatibility constraints are required yet because the service is not in use.
- First multi-bucket version should keep **one internal lake storage credential pair per lake**.
- Those RGW credentials are **backend-only** and must not be exposed directly to end users.
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

#### 2. Replace in-process goroutines with a minimal durable operation runner
**Why:** current async operations are started with goroutines inside the API process, so restarts can strand work.

- [x] Add durable operation execution model in Postgres
- [x] Add worker process / worker loop to claim runnable operations
- [x] Store request payload on operations so work can be replayed from DB state
- [x] Add retry scheduling with backoff
- [x] Add single-worker leadership via Postgres advisory lock
- [x] Add stale `running` operation reset back to `pending`
- [ ] Ensure crashed or interrupted operations can be resumed or failed safely

**Validation completed so far**
- Live deployment on `lks` successfully ran the new durable runner version.
- Worker leadership was acquired via Postgres advisory lock.
- A newly created lake operation was claimed from PostgreSQL and completed successfully.
- The resulting lake was then validated end-to-end with direct object upload and Ceph-side usage/accounting checks.

**Validation still pending**
- Explicit restart-recovery test while an operation is in-flight.

**Done when**
- Pod restart during provisioning does not lose the operation
- Stale `running` operations are recovered automatically
- At-least-once execution is safe with idempotent handling

---

#### 3. Implement real idempotency
**Why:** OpenAPI and schema already mention idempotency, but code does not enforce it.

- [x] Parse `Idempotency-Key` for create / resize / delete operations
- [x] Persist idempotency key + request hash + operation link
- [x] Return the existing operation for same key + same request
- [x] Return `409` for same key + different request
- [x] Add live validation for retry behavior

**Validation completed**
- Live validation on `lks` confirmed correct idempotency behavior for provision, resize, and deprovision.
- Same key + same request returned the same operation.
- Same key + different request returned `409 Conflict`.

**Done when**
- Safe client retries are supported
- Duplicate requests do not create duplicate lakes or operations

---

#### 4. Add real operation state rules
**Why:** before multi-bucket, we still need a **minimal** state machine for the current lake lifecycle so that conflicting operations cannot corrupt state.

**Minimum scope before multi-bucket**
- [x] Define allowed lake state transitions for current operations (`provision`, `resize`, `deprovision`)
- [x] Define allowed operation transitions for the current lifecycle
- [x] Enforce one active operation per lake
- [x] Block invalid actions, e.g. resize on deleted/provisioning lake, delete during conflicting operation
- [x] Return proper `404` / `409` / `400` instead of generic `500`

**Validation completed**
- Live validation on `lks` confirmed:
  - first resize request is accepted
  - second concurrent resize is rejected with `409 Conflict`
  - delete during active resize is rejected with `409 Conflict`
  - delete is accepted once the lake is back in `ready`
  - resize after delete is rejected with `409 Conflict`

**Why we still needed it before multi-bucket**
- Multi-bucket will add more mutating operations, so the current single-lake lifecycle must already reject conflicting actions deterministically.
- Without this, adding bucket create/delete on top would compound concurrency and invalid-state problems.

**Done when**
- Invalid transitions are rejected deterministically
- Concurrent conflicting operations cannot corrupt state

---

### P1 — core product evolution: clean multi-bucket lake model
Do this immediately after P0 is in place.

**Architectural decisions for P1**
- We do **not** need backward compatibility or migration shims for legacy consumers.
- A newly created lake should start **empty**.
- `POST /v1/lakes` provisions the **lake boundary** only:
  - internal RGW user/account
  - lake-wide quota
  - internal lake storage credentials
  - zero buckets by default
- Buckets are explicit child resources created later.
- Keep **one internal lake storage credential pair** for the first version.
- Raw RGW credentials are **backend-only** and must not be exposed to end users.
- End users must not access RGW or Ceph directly; user-facing access will be issued by the control-plane later as a separate product-layer concern.
- Keep **lake quota = RGW user quota**.
- Keep **one active operation per lake** for now, even for bucket operations.
- Per-lake aggregate usage should come from **RGW user stats**.
- Per-bucket usage should come from **bucket stats**.
- Global total usage should be computed across all lakes.
- Lake deletion should remain conservative:
  - bucket delete only if bucket is empty
  - lake delete only when no active/non-deleted buckets remain

#### 5. Introduce a clean lake + bucket schema
**Target model**
- 1 lake = 1 internal RGW user/account
- 1 lake quota = 1 RGW user quota
- 1 lake = 0..N buckets

- [ ] Add a first-class `buckets` table
- [ ] Make `lakes` represent only the lake boundary, not a single bucket
- [ ] Remove the single-bucket assumption from domain models, services, and adapter logic
- [ ] Add bucket statuses (`creating`, `ready`, `deleting`, `failed`, `deleted`)
- [ ] Store both:
  - logical bucket name inside the lake (e.g. `raw`, `bronze`)
  - physical globally unique `bucket_name`
- [ ] Define the bucket naming strategy for physical S3 bucket names

**Done when**
- A lake is modeled as an empty boundary with quota and internal storage credentials
- Buckets are first-class child resources in the schema
- The service no longer assumes one bucket per lake anywhere in the core model

---

#### 6. Refactor the RGW adapter for explicit lake vs bucket operations
**Why:** the current adapter still assumes one bucket derived from `lakeId`.

- [ ] Split lake/account operations from bucket operations
- [ ] Add explicit methods for:
  - ensuring lake user/account
  - ensuring lake keys
  - setting lake quota
  - creating bucket for a lake user
  - deleting bucket if empty
  - querying user usage
  - querying bucket usage
- [ ] Remove implicit `buildBucketName(lakeID)` assumptions from the main provisioning path
- [ ] Keep AWS S3 SDK for S3 data-plane bucket operations

**Done when**
- The adapter can manage a lake with zero or many buckets
- Bucket operations are explicit and no longer derived from a one-bucket lake assumption

---

#### 7. Add bucket lifecycle APIs and operations
- [ ] Add `POST /v1/lakes/{lakeId}/buckets`
- [ ] Add `GET /v1/lakes/{lakeId}/buckets`
- [ ] Add `GET /v1/lakes/{lakeId}/buckets/{bucketId}`
- [ ] Add `DELETE /v1/lakes/{lakeId}/buckets/{bucketId}`
- [ ] Add worker operation types for bucket create/delete
- [ ] Apply idempotency and conflict/state rules to bucket mutations

**Done when**
- A lake can be provisioned empty
- Buckets can be created and deleted explicitly through the control-plane
- Bucket lifecycle runs through the same durable worker model

---

#### 8. Expose lake usage, bucket usage, and fleet-wide totals
**Why:** once lakes can contain multiple buckets, the product must expose usage at the right boundaries.

- [ ] Expose **per-lake total usage** from RGW user stats
- [ ] Expose **per-bucket usage** from bucket stats
- [ ] Expose **global total usage across all lakes**
- [ ] Expose **global committed quota across all lakes**
- [ ] Expose bucket count per lake
- [ ] Define lake delete semantics around existing buckets

**Done when**
- Lake totals, bucket totals, and fleet totals are all available and clearly separated
- The API models a lake as an empty boundary with explicit child buckets

---

### P2 — production hardening around the multi-bucket model
These items should follow immediately after the core multi-bucket model exists.

#### 9. Improve retries / timeouts / error classification
- [ ] Add per-operation timeout budget
- [ ] Add Ceph / RGW request timeouts
- [ ] Classify transient vs permanent errors
- [ ] Retry only transient failures
- [ ] Add clearer error messages / codes for operators

**Done when**
- Temporary RGW/network issues are retried safely
- Permanent failures fail fast with useful diagnostics

---

#### 10. Add observability and readiness
- [ ] Add `/ready` endpoint that checks DB + RGW reachability
- [ ] Keep `/health` as lightweight liveness endpoint
- [ ] Switch to structured logging (`operationId`, `lakeId`, `bucketId`, `tenantId`, `siteId`, `attempt`)
- [ ] Add Prometheus metrics for operations, duration, retries, failures
- [ ] Add request correlation across API and worker paths

**Done when**
- Operators can answer: what failed, where, why, and for which tenant/lake/bucket
- Kubernetes readiness reflects real service dependencies

---

#### 11. Security hardening around Kong
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

#### 12. Improve deployment and multi-DC configuration
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

#### 13. Add usage / quota / capacity reporting
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

#### 14. Add reconciliation / drift detection
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

#### 15. Credentials lifecycle
- [ ] Decide how **product-issued** credentials are returned to clients
- [ ] Keep internal RGW credentials backend-only and never expose them directly to end users
- [ ] Add product credential rotation flow
- [ ] Add revoke/regenerate support
- [ ] Consider multiple product credentials / service accounts per lake later

---

#### 16. Bucket features
- [ ] Bucket versioning support
- [ ] Lifecycle policy support
- [ ] Tags / metadata
- [ ] Optional retention / object lock evaluation

---

#### 17. Deletion workflows
- [ ] Define conservative delete vs force delete behavior
- [ ] Handle non-empty buckets cleanly
- [ ] Add purge options where product allows it

---

#### 18. Access model evolution
- [ ] Evaluate bucket-scoped policies
- [ ] Evaluate read-only / read-write service accounts
- [ ] Keep a **lake-scoped product access model** as the initial simple model
- [ ] Ensure all user-facing access is mediated by the product control-plane/gateway, not direct RGW exposure

---

#### 19. Evaluate `go-ceph/rgw/admin` to reduce custom Admin Ops code
**Why:** after the multi-bucket model is in place, we may be able to reduce some of our custom RGW Admin Ops implementation while keeping our internal adapter.

- [ ] Evaluate replacing custom RGW Admin Ops request/signing code with `github.com/ceph/go-ceph/rgw/admin`
- [ ] Keep AWS S3 SDK for bucket create/head/delete and other S3 data-plane operations
- [ ] Validate compatibility with our RGW endpoint behavior, especially:
  - fixed `/admin` path assumption in the library
  - signing/auth behavior against our Ceph RGW deployment
  - user/key/quota/bucket-info APIs we rely on
- [ ] Adopt only if it reduces maintenance without breaking working behavior

**Notes**
- This is an internal refactor candidate, not a product milestone.
- It should come **after** multi-bucket work, not before it.
- We should keep our own `ceph.Adapter` abstraction even if we replace some internals.

---

## Suggested PR sequence

1. [x] **PR-1**: Fix bucket ownership
2. [ ] **PR-2**: Minimal durable operation runner (implemented, restart-recovery validation pending)
3. [x] **PR-3**: Idempotency + typed errors / API correctness
4. [x] **PR-4**: State machine / concurrency guards
5. [ ] **PR-5**: Clean multi-bucket schema / domain model (empty lake by default)
6. [ ] **PR-6**: RGW adapter refactor for explicit lake vs bucket operations
7. [ ] **PR-7**: Bucket lifecycle APIs + worker operations
8. [ ] **PR-8**: Lake usage, bucket usage, and fleet-wide totals
9. [ ] **PR-9**: Retries / timeouts / error classification
10. [ ] **PR-10**: Observability / readiness / metrics
11. [ ] **PR-11**: Security hardening for Kong deployment model
12. [ ] **PR-12**: Deployment + multi-DC configuration
13. [ ] **PR-13**: Usage sync + capacity reporting
14. [ ] **PR-14**: Reconciliation / drift detection
15. [ ] **PR-15+**: Credentials, lifecycle, policies, richer product features

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
- [x] Idempotent retries are safe
- [x] Conflicting operations are prevented
- [ ] A lake is provisioned as an **empty boundary** (quota + internal storage credentials, zero buckets)
- [ ] One lake can contain multiple explicit buckets
- [ ] Per-lake usage is exposed from RGW user stats
- [ ] Per-bucket usage is exposed from bucket stats
- [ ] **Global total usage across all lakes** is available
- [ ] Basic DB / RGW readiness is visible
- [ ] Basic structured operation logs exist

