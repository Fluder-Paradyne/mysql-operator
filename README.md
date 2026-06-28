# mysql-operator

A Kubernetes operator written in Go that deploys single-instance MySQL using a custom resource.

## What it does

When you create a `MySQL` custom resource, the operator reconciles:

| Resource | Purpose |
|---|---|
| **Secret** | Root password (`<name>-root`) unless you supply `rootPasswordSecretRef` |
| **Secret** | Replication password (`<name>-replication`) when `replicas > 1` |
| **ConfigMap** | Entrypoint enabling GTID + per-pod `server-id` |
| **Service** `<name>-headless` | Headless DNS for topology / replication |
| **Service** `<name>-primary` (and `<name>`) | Read/write — selects `mysql.asrk.dev/role=primary` (pod-0) |
| **Service** `<name>-reads` | Read traffic across all members |
| **StatefulSet** | `replicas` pods with PVC per member (`OrderedReady`) |

With **`spec.replicas >= 2`**, the operator configures **asynchronous GTID replication**:

1. Initial primary is pod `*-0` (stored in `status.primaryPod`)
2. Other pods are **replicas**
3. Replicas are bootstrapped with MySQL 8 **CLONE** from the primary, then `CHANGE REPLICATION SOURCE` + `START REPLICA`
4. Status tracks `mode=PrimaryReplica`, `replicating`, `primaryPod`, and conditions

### Automatic failover

When `replicas > 1`, **automatic failover is on by default** (`spec.failover.enabled`, default `true`):

1. If the current primary stays **Not Ready** for `spec.failover.unhealthySeconds` (default **30s**), the operator picks a **Ready** replica (highest ordinal) and **promotes** it (`STOP/RESET REPLICA`, `read_only=0`).
2. Updates `status.primaryPod` and pod role labels so **`*-primary` Service** points at the new writer.
3. Re-points remaining members (including a recovered old primary) at the new source with GTID auto-position.
4. Status: `phase=FailingOver` / `PrimaryDown`, `lastFailoverFrom` / `lastFailoverTo` / `lastFailoverTime`, condition `AutomaticFailover`.

Disable with:

```yaml
spec:
  failover:
    enabled: false
```

### HA example

```yaml
apiVersion: mysql.asrk.dev/v1alpha1
kind: MySQL
metadata:
  name: ha-mysql
spec:
  replicas: 3          # 1 primary + 2 replicas
  image: mysql:8.0
  storageSize: 5Gi
  database: app
```

```bash
kubectl apply -f config/samples/mysql_ha.yaml
kubectl get mysql ha-mysql
# writes:
kubectl port-forward svc/ha-mysql-primary 3306:3306
# reads (any member):
kubectl port-forward svc/ha-mysql-reads 3307:3306
```

## Project layout

```
api/v1alpha1/          # CRD Go types (MySQL)
cmd/main.go            # Operator entrypoint
internal/controller/   # Reconciler
config/crd/            # CRD manifest
config/rbac/           # RBAC for the operator
config/manager/        # Operator Deployment + Namespace
config/samples/        # Example MySQL CRs
```

## Prerequisites

- Go 1.22+ (Go 1.24+ recommended on recent macOS; this machine uses Go 1.26)
- A Kubernetes cluster (kind, minikube, Docker Desktop, etc.) for e2e
- `kubectl` configured for that cluster
- For fully local e2e without an existing cluster: Docker + [kind](https://kind.sigs.k8s.io/)

## Tests

Two layers prove the operator works locally:

| Target | What it verifies | Needs |
|---|---|---|
| `make test` / `make test-integration` | Reconciler creates Secret, Service, StatefulSet (envtest API server) | Downloads envtest binaries once |
| `make test-e2e` | Real MySQL: status `Running`, `mysqladmin ping`, `SELECT 1`, app DB, read/write | Existing kubeconfig + CRD (`make install`) |
| `make test-e2e-kind` | Same as e2e, but creates/reuses a local **kind** cluster first | Docker + kind + kubectl |

```bash
# Fast (no cluster): controller integration via envtest
make test-integration

# Proves mysqld is actually alive (uses current kube context)
make install && make test-e2e

# Fully local from zero (kind)
make test-e2e-kind
```

The e2e suite starts the operator **in-process** against the cluster, applies a `MySQL` CR, waits for `status.phase=Running` / ready pod, then `kubectl exec`s:

1. `mysqladmin ping` → `mysqld is alive`
2. `SELECT 1` + `SHOW DATABASES` for the configured database
3. `CREATE TABLE` / `INSERT` / `SELECT` on that database

## Quick start (local operator)

Run the controller on your machine against the cluster kubeconfig:

```bash
# Install the CRD
make install

# Run the operator (uses ~/.kube/config)
make run
```

In another terminal, create an instance:

```bash
make sample
# or
kubectl apply -f config/samples/mysql_v1alpha1_mysql.yaml
```

Watch status:

```bash
kubectl get mysql
kubectl get mysql example-mysql -o yaml
kubectl get sts,svc,secret,pvc -l app.kubernetes.io/instance=example-mysql
```

Connect (port-forward):

```bash
# Password (operator-generated secret)
kubectl get secret example-mysql-root -o jsonpath='{.data.password}' | base64 -d; echo

kubectl port-forward svc/example-mysql 3306:3306
mysql -h 127.0.0.1 -P 3306 -uroot -p
```

## Deploy operator in-cluster

```bash
# Build and load image into your cluster (example for kind)
make docker-build IMG=mysql-operator:dev
kind load docker-image mysql-operator:dev   # if using kind

# Point the Deployment at your image, then deploy
# Edit config/manager/manager.yaml image if needed
make deploy IMG=mysql-operator:dev
```

## MySQL CR example

```yaml
apiVersion: mysql.asrk.dev/v1alpha1
kind: MySQL
metadata:
  name: example-mysql
spec:
  replicas: 1          # only 1 supported in this starter
  image: mysql:8.0
  storageSize: 5Gi
  database: app        # optional MYSQL_DATABASE
  # rootPasswordSecretRef:
  #   name: my-mysql-root
  #   key: password
  resources:
    requests:
      cpu: 100m
      memory: 256Mi
```


## Backups (logical dump)

Create a **`MySQLBackup`** CR to run an on-demand **mysqldump** Job into a dedicated PVC.

```bash
# MySQL instance must already be Running
kubectl apply -f config/samples/mysql_backup.yaml
kubectl get mysqlbackup
# PHASE=Succeeded, PVC=<backup-name>-data, file /backup/dump.sql.gz

# Inspect / restore later (example):
kubectl run -it --rm restore-shell --image=mysql:8.0 --restart=Never \
  --overrides='{"spec":{"containers":[{"name":"shell","image":"mysql:8.0","command":["sleep","3600"],"volumeMounts":[{"name":"b","mountPath":"/backup"}]}],"volumes":[{"name":"b","persistentVolumeClaim":{"claimName":"ha-mysql-backup-1-data"}}]}}' \
  -- bash
# inside: gunzip -c /backup/dump.sql.gz | mysql -h ha-mysql-primary -uroot -p...
```

| Field | Meaning |
|-------|---------|
| `spec.mysqlName` | Target `MySQL` CR (same namespace) |
| `spec.storageSize` | PVC size for the dump (default `5Gi`) |
| `spec.databases` | Optional list; empty = `--all-databases` |
| `spec.image` | Job image (defaults to the MySQL CR image) |
| `status.pvcName` / `fileName` | Where the gzipped dump lives |

The Job connects to the instance **primary Service** using the root Secret.

### S3 / MinIO export

Set `spec.s3` to upload `dump.sql.gz` after the dump (init container = mysqldump, main = `amazon/aws-cli`):

```yaml
spec:
  mysqlName: ha-mysql
  s3:
    bucket: my-mysql-backups
    region: us-east-1
    credentialsSecretRef:
      name: aws-backup-creds   # keys: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
    # endpoint: https://minio:9000
    # forcePathStyle: true
    # skipPVC: true            # emptyDir only; object only in S3
```

On success, `status.s3URI` is set (e.g. `s3://my-mysql-backups/mysql-backups/ha-mysql/ha-mysql-backup-s3/dump.sql.gz`).
With default settings the dump is kept on the backup **PVC and** in S3.

 Dumps use `--single-transaction` and binlog coordinates (`--source-data` / `--master-data`) when supported.

## Point-in-time recovery (PITR)

1. Enable **`spec.pitr`** on `MySQL` (binlog CronJob → S3).
2. Take **`MySQLBackup`** base backups (prefer S3 upload).
3. Apply **`MySQLRestore`** with `restoreTo.time` (RFC3339) to replay binlogs after the dump.

Sample: `config/samples/mysql_pitr_restore_only.yaml` (edit names/time). Full enablement needs S3 creds like backup.

**Destructive** restore into the target instance. Logical PITR only (not continuous stream / physical).

## Limitations (current HA model)

- **Async primary/replica only** — not MySQL Group Replication / InnoDB Cluster (no quorum / fencing guarantees)
- Failover promotes the **highest-ordinal Ready** replica (not lag-aware GTID election); brief write unavailability during promotion
- No STONITH / guaranteed old-primary fencing beyond `read_only` on replicas — a split-brain window is possible if the old primary returns writable without operator demotion completing
- Replica bootstrap uses **CLONE** (MySQL 8+); first HA bring-up can take several minutes
- No automated backups, rolling major-version upgrades, or app user management beyond root / `repl`
- Storage size / class changes on existing PVCs are not resized by the operator

## Next steps you might want

1. Lag-aware candidate selection (compare GTID executed sets)
2. Semi-sync replication or Group Replication for stronger HA guarantees
3. Backup CronJob (mysqldump / XtraBackup)
4. Validating admission webhook for `spec`
5. Kubebuilder/Operator SDK codegen and Helm packaging

## Module

```
github.com/asrk/mysql-operator
```

Change the module path in `go.mod` and imports if you publish under a different org.
