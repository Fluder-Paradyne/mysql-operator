package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/asrk/mysql-operator/api/v1alpha1"
)

const restoreFinalizer = "mysql.asrk.dev/restore-finalizer"

// MySQLRestoreReconciler applies a base backup and optionally replays S3 binlogs to a point in time.
type MySQLRestoreReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Name   string
}

func (r *MySQLRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	restore := &mysqlv1alpha1.MySQLRestore{}
	if err := r.Get(ctx, req.NamespacedName, restore); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !restore.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(restore, restoreFinalizer) {
			controllerutil.RemoveFinalizer(restore, restoreFinalizer)
			_ = r.Update(ctx, restore)
		}
		return ctrl.Result{}, nil
	}
	if !controllerutil.ContainsFinalizer(restore, restoreFinalizer) {
		controllerutil.AddFinalizer(restore, restoreFinalizer)
		if err := r.Update(ctx, restore); err != nil {
			return ctrl.Result{}, err
		}
	}
	if restore.Status.Phase == "Succeeded" || restore.Status.Phase == "Failed" {
		return ctrl.Result{}, nil
	}

	mysql := &mysqlv1alpha1.MySQL{}
	if err := r.Get(ctx, types.NamespacedName{Name: restore.Spec.MySQLName, Namespace: restore.Namespace}, mysql); err != nil {
		_ = r.patchRestoreStatus(ctx, restore, "Failed", err.Error(), "")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	backupS3URI := restore.Spec.BackupS3URI
	var backupPVC string
	if restore.Spec.BackupName != "" {
		b := &mysqlv1alpha1.MySQLBackup{}
		if err := r.Get(ctx, types.NamespacedName{Name: restore.Spec.BackupName, Namespace: restore.Namespace}, b); err != nil {
			_ = r.patchRestoreStatus(ctx, restore, "Failed", fmt.Sprintf("backup %q: %v", restore.Spec.BackupName, err), "")
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
		if b.Status.S3URI != "" {
			backupS3URI = b.Status.S3URI
		}
		if b.Status.PVCName != "" {
			backupPVC = b.Status.PVCName
		}
		if backupS3URI == "" && backupPVC == "" {
			_ = r.patchRestoreStatus(ctx, restore, "Failed", "referenced backup has no S3URI or PVC", "")
			return ctrl.Result{}, nil
		}
	}
	if backupS3URI == "" && backupPVC == "" {
		_ = r.patchRestoreStatus(ctx, restore, "Failed", "set backupName or backupS3URI", "")
		return ctrl.Result{}, nil
	}

	binlogPrefix := restore.Spec.BinlogS3Prefix
	if binlogPrefix == "" {
		binlogPrefix = mysql.Status.BinlogArchivePrefix
	}
	if binlogPrefix == "" {
		binlogPrefix = fmt.Sprintf("mysql-binlogs/%s", mysql.Name)
	}
	binlogPrefix = strings.Trim(binlogPrefix, "/")

	backupOnly := restore.Spec.RestoreTo != nil && restore.Spec.RestoreTo.BackupOnly
	stopTime := ""
	if restore.Spec.RestoreTo != nil {
		stopTime = restore.Spec.RestoreTo.Time
	}
	if !backupOnly && stopTime == "" {
		_ = r.patchRestoreStatus(ctx, restore, "Failed", "restoreTo.time is required for PITR (or set restoreTo.backupOnly: true)", "")
		return ctrl.Result{}, nil
	}
	needS3 := backupS3URI != "" || !backupOnly
	var s3Spec *mysqlv1alpha1.BackupS3Spec
	if needS3 {
		if restore.Spec.S3 != nil {
			s3Spec = restore.Spec.S3
		} else if mysql.Spec.PITR != nil && mysql.Spec.PITR.BinlogArchive != nil {
			cp := mysql.Spec.PITR.BinlogArchive.S3
			s3Spec = &cp
		}
		if s3Spec == nil || s3Spec.CredentialsSecretRef.Name == "" {
			_ = r.patchRestoreStatus(ctx, restore, "Failed", "S3 credentials required (spec.s3 or mysql.spec.pitr.binlogArchive.s3)", "")
			return ctrl.Result{}, nil
		}
	}

	rootSecret := mysql.Status.RootSecretName
	if rootSecret == "" {
		rootSecret = mysql.Name + "-root"
	}
	rootKey := defaultSecretKey
	// Apply container needs mysql + mysqlbinlog; official mysql server image lacks mysqlbinlog.
	image := restore.Spec.Image
	if image == "" {
		image = defaultPITRToolsImage
	}
	awsImage := restore.Spec.AWSCLIImage
	if awsImage == "" {
		awsImage = defaultAWSCLIImage
	}
	primaryHost := fmt.Sprintf("%s.%s.svc", primaryServiceName(mysql), mysql.Namespace)

	jobName := restore.Name + "-job"
	if err := r.ensureRestoreJob(ctx, restore, jobName, primaryHost, rootSecret, rootKey, image, awsImage,
		backupS3URI, backupPVC, binlogPrefix, stopTime, backupOnly, s3Spec); err != nil {
		logger.Error(err, "ensure restore job")
		_ = r.patchRestoreStatus(ctx, restore, "Pending", err.Error(), jobName)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	job := &batchv1.Job{}
	if err := r.Get(ctx, types.NamespacedName{Name: jobName, Namespace: restore.Namespace}, job); err != nil {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, client.IgnoreNotFound(err)
	}
	phase, msg := "Running", "restore Job running"
	if job.Status.Succeeded > 0 {
		phase, msg = "Succeeded", "restore completed"
		if !backupOnly {
			msg = fmt.Sprintf("restore completed with PITR stop-datetime=%s", stopTime)
		}
	} else if job.Status.Failed > 0 {
		phase, msg = "Failed", "restore Job failed; inspect pod logs"
	}
	if err := r.patchRestoreStatus(ctx, restore, phase, msg, jobName); err != nil {
		return ctrl.Result{}, err
	}
	if phase == "Running" || phase == "Pending" {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

func (r *MySQLRestoreReconciler) ensureRestoreJob(
	ctx context.Context,
	restore *mysqlv1alpha1.MySQLRestore,
	jobName, primaryHost, rootSecret, rootKey, image, awsImage,
	backupS3URI, backupPVC, binlogPrefix, stopTime string,
	backupOnly bool,
	s3Spec *mysqlv1alpha1.BackupS3Spec,
) error {
	existing := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Name: jobName, Namespace: restore.Namespace}, existing)
	if err == nil {
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}

	volumes := []corev1.Volume{{
		Name: "work",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}}
	mounts := []corev1.VolumeMount{{Name: "work", MountPath: "/work"}}
	if backupPVC != "" && backupS3URI == "" {
		volumes = append(volumes, corev1.Volume{
			Name: "backup-pvc",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: backupPVC, ReadOnly: true},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{Name: "backup-pvc", MountPath: "/backup-src", ReadOnly: true})
	}

	var initContainers []corev1.Container
	var awsEnv []corev1.EnvVar
	if s3Spec != nil {
		awsEnv = s3CredEnv(s3Spec.CredentialsSecretRef.Name, s3Spec)
	}

	if backupS3URI != "" {
		fetchBackup := fmt.Sprintf(`set -euo pipefail
if [ -z "${AWS_ACCESS_KEY_ID:-}" ] && [ -n "${accessKeyId:-}" ]; then export AWS_ACCESS_KEY_ID="$accessKeyId"; fi
if [ -z "${AWS_SECRET_ACCESS_KEY:-}" ] && [ -n "${secretAccessKey:-}" ]; then export AWS_SECRET_ACCESS_KEY="$secretAccessKey"; fi
if [ -z "${AWS_SESSION_TOKEN:-}" ] && [ -n "${sessionToken:-}" ]; then export AWS_SESSION_TOKEN="$sessionToken"; fi
mkdir -p /work
EXTRA=()
if [ -n "${AWS_ENDPOINT_URL:-}" ]; then EXTRA+=(--endpoint-url "$AWS_ENDPOINT_URL"); fi
if [ "${AWS_S3_FORCE_PATH_STYLE:-}" = "true" ]; then aws configure set default.s3.addressing_style path; fi
aws s3 cp %q /work/dump.sql.gz "${EXTRA[@]}"
ls -la /work/dump.sql.gz
`, backupS3URI)
		initContainers = append(initContainers, corev1.Container{
			Name: "fetch-backup", Image: awsImage, Command: []string{"/bin/sh", "-c", fetchBackup},
			Env: awsEnv, VolumeMounts: mounts,
		})
	} else {
		fetchBackup := `set -euo pipefail
mkdir -p /work
cp /backup-src/dump.sql.gz /work/dump.sql.gz
ls -la /work/dump.sql.gz
`
		initContainers = append(initContainers, corev1.Container{
			Name: "fetch-backup", Image: image, Command: []string{"/bin/bash", "-c", fetchBackup},
			VolumeMounts: mounts,
		})
	}

	if !backupOnly && s3Spec != nil {
		fetchBinlogs := fmt.Sprintf(`set -euo pipefail
if [ -z "${AWS_ACCESS_KEY_ID:-}" ] && [ -n "${accessKeyId:-}" ]; then export AWS_ACCESS_KEY_ID="$accessKeyId"; fi
if [ -z "${AWS_SECRET_ACCESS_KEY:-}" ] && [ -n "${secretAccessKey:-}" ]; then export AWS_SECRET_ACCESS_KEY="$secretAccessKey"; fi
if [ -z "${AWS_SESSION_TOKEN:-}" ] && [ -n "${sessionToken:-}" ]; then export AWS_SESSION_TOKEN="$sessionToken"; fi
mkdir -p /work/binlogs
EXTRA=()
if [ -n "${AWS_ENDPOINT_URL:-}" ]; then EXTRA+=(--endpoint-url "$AWS_ENDPOINT_URL"); fi
if [ "${AWS_S3_FORCE_PATH_STYLE:-}" = "true" ]; then aws configure set default.s3.addressing_style path; fi
SRC=%q
aws s3 sync "$SRC" /work/binlogs/ "${EXTRA[@]}" || true
ls -la /work/binlogs || true
`, fmt.Sprintf("s3://%s/%s/", s3Spec.Bucket, binlogPrefix))
		initContainers = append(initContainers, corev1.Container{
			Name: "fetch-binlogs", Image: awsImage, Command: []string{"/bin/sh", "-c", fetchBinlogs},
			Env: awsEnv, VolumeMounts: mounts,
		})
	}

	applyScript := fmt.Sprintf(`set -euo pipefail
# Ensure mysql client + mysqlbinlog (debian tools image; server image lacks mysqlbinlog).
if ! command -v mysql >/dev/null 2>&1 || ! command -v mysqlbinlog >/dev/null 2>&1; then
  apt-get update -qq
  DEBIAN_FRONTEND=noninteractive apt-get install -y -qq default-mysql-client mariadb-client gzip ca-certificates >/dev/null
  if ! command -v mysqlbinlog >/dev/null 2>&1 && command -v mariadb-binlog >/dev/null 2>&1; then
    ln -sf "$(command -v mariadb-binlog)" /usr/local/bin/mysqlbinlog
  fi
fi
export MYSQL_PWD="$MYSQL_ROOT_PASSWORD"
HOST=%q
echo "waiting for MySQL at $HOST..."
for i in $(seq 1 60); do
  mysqladmin ping -h"$HOST" -uroot --silent && break
  sleep 2
done
echo "WARNING: destructive restore - applying base backup (mysqldump typically includes DROP/CREATE)"
# Clear local GTID history so dump / replay can apply on an in-place restore target.
mysql -h"$HOST" -uroot -e "RESET BINARY LOGS AND GTIDS" 2>/dev/null \
  || mysql -h"$HOST" -uroot -e "RESET MASTER" 2>/dev/null \
  || true
gunzip -c /work/dump.sql.gz | sed -E '/^SET @@GLOBAL.GTID_PURGED/d;/^SET @@SESSION.SQL_LOG_BIN/d' \
  | mysql -h"$HOST" -uroot
echo "base backup applied"
`, primaryHost)

	if !backupOnly && stopTime != "" {
		dt := stopTime
		dt = strings.ReplaceAll(dt, "T", " ")
		dt = strings.TrimSuffix(dt, "Z")
		if idx := strings.Index(dt, "+"); idx > 0 {
			dt = dt[:idx]
		}
		if len(dt) > 19 {
			dt = dt[:19]
		}
		// Replay archived binlogs up to stop-datetime. --force ignores duplicate-key noise from events
		// already represented in the logical dump; stop-datetime excludes later markers (PITR proof).
		applyScript += fmt.Sprintf(`
if ls /work/binlogs/mysql-bin.* >/dev/null 2>&1 || ls /work/binlogs/* >/dev/null 2>&1; then
  echo "replaying binlogs until %s"
  FILES=$(ls /work/binlogs/mysql-bin.* /work/binlogs/* 2>/dev/null | sort -u)
  if [ -n "$FILES" ]; then
    # GTID_MODE steps: ON -> ON_PERMISSIVE, then enforce can be OFF, then OFF_PERMISSIVE -> OFF.
    mysql -h"$HOST" -uroot -e "SET GLOBAL gtid_mode=ON_PERMISSIVE"
    mysql -h"$HOST" -uroot -e "SET GLOBAL enforce_gtid_consistency=OFF"
    mysql -h"$HOST" -uroot -e "SET GLOBAL gtid_mode=OFF_PERMISSIVE"
    mysql -h"$HOST" -uroot -e "SET GLOBAL gtid_mode=OFF"
    mysql -h"$HOST" -uroot -N -e "SELECT @@GLOBAL.gtid_mode"
    # shellcheck disable=SC2086
    if mysqlbinlog --help 2>&1 | grep -q -- '--skip-gtids'; then
      mysqlbinlog --skip-gtids --stop-datetime=%q $FILES | mysql --force -h"$HOST" -uroot
    else
      mysqlbinlog --stop-datetime=%q $FILES | mysql --force -h"$HOST" -uroot
    fi
    # Best-effort restore GTID for HA (step back up).
    mysql -h"$HOST" -uroot -e "SET GLOBAL gtid_mode=OFF_PERMISSIVE" || true
    mysql -h"$HOST" -uroot -e "SET GLOBAL gtid_mode=ON_PERMISSIVE" || true
    mysql -h"$HOST" -uroot -e "SET GLOBAL gtid_mode=ON" || true
    mysql -h"$HOST" -uroot -e "SET GLOBAL enforce_gtid_consistency=ON" || true
    echo "binlog replay finished"
  else
    echo "no binlog files found under /work/binlogs — PITR limited to base backup"
    exit 1
  fi
else
  echo "no binlogs downloaded — PITR limited to base backup"
  exit 1
fi
`, dt, dt, dt)
	}
	applyScript += "\necho restore complete\n"

	backoff := int32(0)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: restore.Namespace,
			Labels: map[string]string{
				labelAppKey: "mysql-restore", labelInstanceKey: restore.Name, labelManagedByKey: managedByValue,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy:  corev1.RestartPolicyNever,
					Volumes:        volumes,
					InitContainers: initContainers,
					Containers: []corev1.Container{{
						Name:    "restore",
						Image:   image,
						Command: []string{"/bin/bash", "-c", applyScript},
						Env: []corev1.EnvVar{{
							Name: "MYSQL_ROOT_PASSWORD",
							ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: rootSecret},
								Key:                  rootKey,
							}},
						}},
						VolumeMounts: mounts,
					}},
				},
			},
		},
	}
	if err := controllerutil.SetControllerReference(restore, job, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, job)
}

func (r *MySQLRestoreReconciler) patchRestoreStatus(ctx context.Context, restore *mysqlv1alpha1.MySQLRestore, phase, message, jobName string) error {
	cur := &mysqlv1alpha1.MySQLRestore{}
	if err := r.Get(ctx, types.NamespacedName{Name: restore.Name, Namespace: restore.Namespace}, cur); err != nil {
		return err
	}
	cur.Status.Phase = phase
	cur.Status.Message = message
	if jobName != "" {
		cur.Status.JobName = jobName
	}
	if cur.Status.StartTime == nil && (phase == "Running" || phase == "Pending") {
		t := metav1.Now()
		cur.Status.StartTime = &t
	}
	if phase == "Succeeded" || phase == "Failed" {
		t := metav1.Now()
		cur.Status.CompletionTime = &t
	}
	cond := metav1.Condition{
		Type: "Completed", Status: metav1.ConditionFalse, Reason: phase, Message: message,
		LastTransitionTime: metav1.Now(), ObservedGeneration: cur.Generation,
	}
	if phase == "Succeeded" {
		cond.Status = metav1.ConditionTrue
	}
	setCondition(&cur.Status.Conditions, cond)
	return r.Status().Update(ctx, cur)
}

func (r *MySQLRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	name := r.Name
	if name == "" {
		name = "mysqlrestore"
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		For(&mysqlv1alpha1.MySQLRestore{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}
