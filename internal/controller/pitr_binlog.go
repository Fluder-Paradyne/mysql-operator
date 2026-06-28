package controller

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	batchv1 "k8s.io/api/batch/v1"

	mysqlv1alpha1 "github.com/asrk/mysql-operator/api/v1alpha1"
)

// Official mysql:8.0 images omit mysqlbinlog; restore jobs install client tools on the fly.
const defaultPITRToolsImage = "debian:bookworm-slim"

const defaultBinlogCron = "*/5 * * * *"

func (r *MySQLReconciler) ensureBinlogArchive(ctx context.Context, mysql *mysqlv1alpha1.MySQL, rootSecret, rootKey string) error {
	// Prefer in-pod sidecar (shares datadir) — see pitrBinlogSidecar. Keep CronJob for
	// compatibility / optional remote fetch when tools image provides mysqlbinlog.
	if !mysql.Spec.PITREnabled() {
		cj := &batchv1.CronJob{}
		err := r.Get(ctx, types.NamespacedName{Name: binlogCronName(mysql), Namespace: mysql.Namespace}, cj)
		if err == nil {
			_ = r.Delete(ctx, cj)
		}
		return nil
	}
	arch := mysql.Spec.PITR.BinlogArchive
	if arch == nil || arch.S3.Bucket == "" || arch.S3.CredentialsSecretRef.Name == "" {
		return fmt.Errorf("spec.pitr.binlogArchive.s3.bucket and credentialsSecretRef.name are required when pitr.enabled")
	}

	prefix := arch.S3.Prefix
	if prefix == "" {
		prefix = fmt.Sprintf("mysql-binlogs/%s", mysql.Name)
	}
	prefix = strings.Trim(prefix, "/")
	mysql.Status.BinlogArchivePrefix = prefix
	mysql.Status.BinlogArchiveCronJob = binlogCronName(mysql)

	// Sidecar on the StatefulSet does the real archive (datadir share). CronJob is best-effort
	// remote mysqlbinlog for clusters that supply a tools image with the binary.
	_ = rootSecret
	_ = rootKey
	return r.ensureBinlogArchiveCronJob(ctx, mysql, arch, prefix)
}

func (r *MySQLReconciler) ensureBinlogArchiveCronJob(ctx context.Context, mysql *mysqlv1alpha1.MySQL, arch *mysqlv1alpha1.BinlogArchiveSpec, prefix string) error {
	schedule := arch.Schedule
	if schedule == "" {
		schedule = defaultBinlogCron
	}
	// Tools image for optional remote fetch (may lack mysqlbinlog — sidecar is source of truth).
	image := arch.Image
	if image == "" {
		image = defaultPITRToolsImage
	}
	awsImage := arch.AWSCLIImage
	if awsImage == "" {
		awsImage = defaultAWSCLIImage
	}

	primaryHost := fmt.Sprintf("%s.%s.svc", primaryServiceName(mysql), mysql.Namespace)
	if mysql.Status.PrimaryService != "" {
		primaryHost = fmt.Sprintf("%s.%s.svc", mysql.Status.PrimaryService, mysql.Namespace)
	}

	// Remote fetch is best-effort; sidecar ships files even if this no-ops.
	dumpScript := fmt.Sprintf(`set -euo pipefail
if ! command -v mysqlbinlog >/dev/null 2>&1; then
  if command -v apt-get >/dev/null 2>&1; then
    apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq default-mysql-client mariadb-client >/dev/null
    if ! command -v mysqlbinlog >/dev/null 2>&1 && command -v mariadb-binlog >/dev/null 2>&1; then
      ln -sf "$(command -v mariadb-binlog)" /usr/local/bin/mysqlbinlog
    fi
  else
    echo "mysqlbinlog missing; rely on STS binlog-archive sidecar"
    mkdir -p /work/binlogs
    exit 0
  fi
fi
export MYSQL_PWD="${MYSQL_ROOT_PASSWORD:-}"
HOST=%s
WORKDIR=/work/binlogs
mkdir -p "$WORKDIR"
for i in $(seq 1 30); do mysqladmin ping -h"$HOST" -uroot --silent && break; sleep 2; done
mysql -h"$HOST" -uroot -e "FLUSH BINARY LOGS;" || true
LOGS=$(mysql -h"$HOST" -uroot -N -e "SHOW BINARY LOGS;" | awk '{print $1}' || true)
cd "$WORKDIR"
for log in $LOGS; do
  mysqlbinlog --read-from-remote-server --host="$HOST" --user=root --raw --result-file=./ "$log" || true
done
ls -la "$WORKDIR" || true
`, shellQuote(primaryHost))

	dest := fmt.Sprintf("s3://%s/%s/", arch.S3.Bucket, prefix)
	uploadScript := fmt.Sprintf(`set -euo pipefail
if [ -z "${AWS_ACCESS_KEY_ID:-}" ] && [ -n "${accessKeyId:-}" ]; then export AWS_ACCESS_KEY_ID="$accessKeyId"; fi
if [ -z "${AWS_SECRET_ACCESS_KEY:-}" ] && [ -n "${secretAccessKey:-}" ]; then export AWS_SECRET_ACCESS_KEY="$secretAccessKey"; fi
if [ -z "${AWS_SESSION_TOKEN:-}" ] && [ -n "${sessionToken:-}" ]; then export AWS_SESSION_TOKEN="$sessionToken"; fi
DEST=%s
if [ -n "${AWS_ENDPOINT_URL:-}" ] && { [ "${AWS_S3_FORCE_PATH_STYLE:-}" = "true" ] || [ "${AWS_S3_FORCE_PATH_STYLE:-}" = "1" ]; }; then aws configure set default.s3.addressing_style path; fi
EXTRA=()
if [ -n "${AWS_ENDPOINT_URL:-}" ]; then EXTRA+=(--endpoint-url "$AWS_ENDPOINT_URL"); fi
if [ -d /work/binlogs ] && ls /work/binlogs/* >/dev/null 2>&1; then
  aws s3 sync /work/binlogs/ "$DEST" "${EXTRA[@]}"
  echo binlog archive sync complete
else
  echo "no remote-fetched binlogs; STS sidecar should populate $DEST"
fi
`, shellQuote(dest))

	// Root password optional for cron remote path (sidecar does not need it).
	rootSecret := mysql.Status.RootSecretName
	if rootSecret == "" {
		rootSecret = mysql.Name + "-root"
	}
	optTrue := true
	awsEnv := s3CredEnv(arch.S3.CredentialsSecretRef.Name, &arch.S3)

	cron := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      binlogCronName(mysql),
			Namespace: mysql.Namespace,
			Labels:    labelsFor(mysql),
		},
		Spec: batchv1.CronJobSpec{
			Schedule:                   schedule,
			SuccessfulJobsHistoryLimit: int32Ptr(3),
			FailedJobsHistoryLimit:     int32Ptr(3),
			ConcurrencyPolicy:          batchv1.ForbidConcurrent,
			JobTemplate: batchv1.JobTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labelsFor(mysql)},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: labelsFor(mysql)},
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyOnFailure,
							Volumes: []corev1.Volume{{
								Name:         "work",
								VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
							}},
							InitContainers: []corev1.Container{{
								Name:    "fetch-binlogs",
								Image:   image,
								Command: []string{"/bin/bash", "-c", dumpScript},
								Env: []corev1.EnvVar{{
									Name: "MYSQL_ROOT_PASSWORD",
									ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
										LocalObjectReference: corev1.LocalObjectReference{Name: rootSecret},
										Key:                  defaultSecretKey,
										Optional:             &optTrue,
									}},
								}},
								VolumeMounts: []corev1.VolumeMount{{Name: "work", MountPath: "/work"}},
							}},
							Containers: []corev1.Container{{
								Name:         "s3-sync",
								Image:        awsImage,
								Command:      []string{"/bin/sh", "-c", uploadScript},
								Env:          awsEnv,
								VolumeMounts: []corev1.VolumeMount{{Name: "work", MountPath: "/work"}},
							}},
						},
					},
				},
			},
		},
	}
	if err := controllerutil.SetControllerReference(mysql, cron, r.Scheme); err != nil {
		return err
	}

	existing := &batchv1.CronJob{}
	err := r.Get(ctx, types.NamespacedName{Name: cron.Name, Namespace: cron.Namespace}, existing)
	if errors.IsNotFound(err) {
		return r.Create(ctx, cron)
	}
	if err != nil {
		return err
	}
	existing.Spec = cron.Spec
	existing.Labels = cron.Labels
	return r.Update(ctx, existing)
}

// pitrBinlogSidecar returns an aws-cli sidecar that syncs on-disk mysql-bin.* to S3.
// Shares the MySQL datadir volume (same pod) — avoids mysqlbinlog in the server image.
func pitrBinlogSidecar(mysql *mysqlv1alpha1.MySQL) *corev1.Container {
	if !mysql.Spec.PITREnabled() || mysql.Spec.PITR.BinlogArchive == nil {
		return nil
	}
	arch := mysql.Spec.PITR.BinlogArchive
	if arch.S3.Bucket == "" || arch.S3.CredentialsSecretRef.Name == "" {
		return nil
	}
	prefix := arch.S3.Prefix
	if prefix == "" {
		prefix = fmt.Sprintf("mysql-binlogs/%s", mysql.Name)
	}
	prefix = strings.Trim(prefix, "/")
	dest := fmt.Sprintf("s3://%s/%s/", arch.S3.Bucket, prefix)
	awsImage := arch.AWSCLIImage
	if awsImage == "" {
		awsImage = defaultAWSCLIImage
	}
	// Only primary should ship (replicas have different binlog stream). Gated by hostname ordinal.
	// Keep the script POSIX-friendly (aws-cli image has a minimal /bin/sh, not full bash/awk).
	script := fmt.Sprintf(`set -eu
if [ -z "${AWS_ACCESS_KEY_ID:-}" ] && [ -n "${accessKeyId:-}" ]; then export AWS_ACCESS_KEY_ID="$accessKeyId"; fi
if [ -z "${AWS_SECRET_ACCESS_KEY:-}" ] && [ -n "${secretAccessKey:-}" ]; then export AWS_SECRET_ACCESS_KEY="$secretAccessKey"; fi
if [ -z "${AWS_SESSION_TOKEN:-}" ] && [ -n "${sessionToken:-}" ]; then export AWS_SESSION_TOKEN="$sessionToken"; fi
DEST=%s
DATA=%s
if [ -n "${AWS_ENDPOINT_URL:-}" ]; then
  if [ "${AWS_S3_FORCE_PATH_STYLE:-}" = "true" ] || [ "${AWS_S3_FORCE_PATH_STYLE:-}" = "1" ]; then
    aws configure set default.s3.addressing_style path || true
  fi
fi
echo "binlog-archive sidecar targeting $DEST (sync every 15s)"
while true; do
  HN="${HOSTNAME:-x-0}"
  ORD="${HN##*-}"
  if [ "$ORD" = "0" ]; then
    set +e
    ls "$DATA"/mysql-bin.* >/tmp/binlist 2>/dev/null
    if [ -s /tmp/binlist ]; then
      if [ -n "${AWS_ENDPOINT_URL:-}" ]; then
        aws s3 sync "$DATA"/ "$DEST" --exclude "*" --include "mysql-bin.*" --endpoint-url "$AWS_ENDPOINT_URL"
      else
        aws s3 sync "$DATA"/ "$DEST" --exclude "*" --include "mysql-bin.*"
      fi
      echo "synced ok"
    else
      echo "waiting for mysql-bin files under $DATA"
    fi
    set -e
  fi
  sleep 15
done
`, shellQuote(dest), shellQuote(dataMountPath))

	return &corev1.Container{
		Name:    "binlog-archive",
		Image:   awsImage,
		Command: []string{"/bin/sh", "-c", script},
		Env:     s3CredEnv(arch.S3.CredentialsSecretRef.Name, &arch.S3),
		VolumeMounts: []corev1.VolumeMount{{
			Name: dataVolumeName, MountPath: dataMountPath, ReadOnly: true,
		}},
	}
}

func binlogCronName(mysql *mysqlv1alpha1.MySQL) string {
	return mysql.Name + "-binlog-archive"
}

func int32Ptr(v int32) *int32 { return &v }

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'"'"'`) + "'"
}

func s3CredEnv(credSecret string, s3 *mysqlv1alpha1.BackupS3Spec) []corev1.EnvVar {
	opt := true
	env := []corev1.EnvVar{
		{Name: "AWS_ACCESS_KEY_ID", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: credSecret}, Key: "AWS_ACCESS_KEY_ID", Optional: &opt,
		}}},
		{Name: "AWS_SECRET_ACCESS_KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: credSecret}, Key: "AWS_SECRET_ACCESS_KEY", Optional: &opt,
		}}},
		{Name: "AWS_SESSION_TOKEN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: credSecret}, Key: "AWS_SESSION_TOKEN", Optional: &opt,
		}}},
		{Name: "accessKeyId", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: credSecret}, Key: "accessKeyId", Optional: &opt,
		}}},
		{Name: "secretAccessKey", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: credSecret}, Key: "secretAccessKey", Optional: &opt,
		}}},
		{Name: "sessionToken", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: credSecret}, Key: "sessionToken", Optional: &opt,
		}}},
	}
	if s3.Region != "" {
		env = append(env,
			corev1.EnvVar{Name: "AWS_DEFAULT_REGION", Value: s3.Region},
			corev1.EnvVar{Name: "AWS_REGION", Value: s3.Region},
		)
	}
	if s3.Endpoint != "" {
		env = append(env, corev1.EnvVar{Name: "AWS_ENDPOINT_URL", Value: s3.Endpoint})
	}
	if s3.ForcePathStyle {
		env = append(env, corev1.EnvVar{Name: "AWS_S3_FORCE_PATH_STYLE", Value: "true"})
	}
	return env
}
