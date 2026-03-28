# Lab Runbook: Ceph RGW + Data Lake Provisioner (Proxmox)

This runbook captures the exact configuration/actions needed to make the Data Lake Provisioner work against Ceph RGW on a Proxmox-managed Ceph cluster.

---

## 1) Initial findings

- Ceph cluster was healthy (`ceph -s`), but:
  - no orchestrator (`ceph orch ...` not available in this setup)
  - no RGW daemon running/listening
- `ceph-radosgw` package name was not available.
- Correct package name in this repo set: `radosgw`.

Commands used:

```bash
ceph -s
ceph orch ps | grep rgw
ss -lntp | grep radosgw
apt-cache policy radosgw ceph-radosgw
apt-cache search radosgw
```

---

## 2) Install and start RGW on Proxmox node

### Install package

```bash
apt update
apt install -y radosgw
```

### Create RGW auth key

```bash
ceph auth get-or-create client.rgw.pve1 \
  mon 'allow rw' \
  osd 'allow rwx' \
  -o /var/lib/ceph/radosgw/ceph-rgw.pve1/keyring

chown -R ceph:ceph /var/lib/ceph/radosgw/ceph-rgw.pve1
chmod 600 /var/lib/ceph/radosgw/ceph-rgw.pve1/keyring
```

> Note: `chown` on `/etc/pve/priv/...` is not permitted by Proxmox FS, so local path under `/var/lib/ceph/radosgw/...` is used.

### Configure `/etc/pve/ceph.conf`

```ini
[client.rgw.pve1]
host = pve1
keyring = /var/lib/ceph/radosgw/ceph-rgw.pve1/keyring
rgw_frontends = beast port=7480
```

### Start service

```bash
systemctl enable --now ceph-radosgw@rgw.pve1
systemctl reset-failed ceph-radosgw@rgw.pve1
systemctl restart ceph-radosgw@rgw.pve1
systemctl status ceph-radosgw@rgw.pve1 --no-pager -l
```

### Verify RGW

```bash
ss -lntp | grep 7480
curl -i http://127.0.0.1:7480/
curl -i "http://127.0.0.1:7480/admin/user?uid=provisioner-admin"
```

Expected:
- `/` returns HTTP 200 (S3 XML)
- `/admin/...` returns HTTP 403 without signature (this is OK and proves Admin API path exists)

---

## 3) Create provisioner admin user/caps

```bash
radosgw-admin user create --uid=provisioner-admin --display-name="Provisioner Admin" --system
radosgw-admin caps add --uid=provisioner-admin --caps="users=*;buckets=*;metadata=*;usage=*"
radosgw-admin user info --uid=provisioner-admin
```

Verify output contains caps:
- `users=*`
- `buckets=*`
- `metadata=*`
- `usage=*`

---

## 4) Provisioner service configuration

Set environment variables for `datalake-provisioner`:

```bash
HTTP_PORT=8081
DATABASE_URL=postgres://postgres:postgres@localhost:5432/datalake?sslmode=disable

RGW_ENDPOINT=http://192.168.3.251:7480
RGW_ADMIN_PATH=/admin
RGW_REGION=us-east-1
RGW_ACCESS_KEY_ID=<provisioner-admin-access-key>
RGW_SECRET_ACCESS_KEY=<provisioner-admin-secret-key>
RGW_INSECURE_SKIP_VERIFY=false
```

---

## 5) API test sequence

### Provision

```bash
curl -X POST http://127.0.0.1:8081/v1/lakes \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant: tenant-a' \
  -d '{"userId":"user-1","sizeGiB":10}'
```

### Poll operation

```bash
curl -H 'X-Tenant: tenant-a' \
  http://127.0.0.1:8081/v1/operations/<operationId>
```

### Get lake

```bash
curl -H 'X-Tenant: tenant-a' \
  http://127.0.0.1:8081/v1/lakes/<lakeId>
```

### Validate Ceph-side resources

```bash
radosgw-admin user list | grep lake-
radosgw-admin user info --uid=<rgwUser>
radosgw-admin bucket list | grep <bucketName>
```

---

## 6) Known pitfalls hit during setup

1. Wrong package name (`ceph-radosgw` not available; use `radosgw`).
2. Proxmox `pveceph` command in this version does not support `rgw` subcommand.
3. Keyring path under `/etc/pve/priv` caused permission/ownership issues for RGW daemon user.
4. Typo in keyring path (`randosgw` vs `radosgw`) prevented startup.
5. `caps add` initially applied only one cap; semicolon-separated caps string fixed it.
6. k3s node is `linux/amd64`: image had to be pushed for amd64 (`buildx --platform linux/amd64`).
7. Kubernetes `runAsNonRoot` with distroless required numeric `runAsUser/runAsGroup` (65532).
8. Existing Postgres data dir can skip init DB creation; `datalake` DB was created manually, then migration job re-run.
9. Invalid `DATABASE_URL` in secret caused bootstrap failure (`sslmode` parse error).

---

## 7) Near-production hardening checklist (next)

- Move admin credentials to Kubernetes Secret / Vault.
- Rotate admin keys periodically.
- Restrict network access to RGW endpoint.
- Enable TLS for RGW endpoint.
- Add idempotency handling in provisioner.
- Add structured logging + request correlation IDs to every async operation step.
