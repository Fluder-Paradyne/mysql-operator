package controller

import (
	"context"
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	mysqlv1alpha1 "github.com/asrk/mysql-operator/api/v1alpha1"
)

const defaultBinlogCron = "*/5 * * * *"

func (r *MySQLReconciler) ensureBinlogArchive(ctx context.Context, mysql *mysqlv1alpha1.MySQL, rootSecret, rootKey string) error {
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

	schedule := arch.Schedule
	if schedule == "" {
		schedule = defaultBinlogCron
	}
	image := arch.Image
	if image == "" {
		image = mysql.Spec.Image
	}
	if image == "" {
		image = defaultImage
	}
	awsImage := arch.AWSCLIImage
	if awsImage == "" {
		awsImage = defaultAWSCLIImage
	}

	primaryHost := fmt.Sprintf("%s.%s.svc", primaryServiceName(mysql), mysql.Namespace)
	if mysql.Status.PrimaryService != "" {
		primaryHost = fmt.Sprintf("%s.%s.svc", mysql.Status.PrimaryService, mysql.Namespace)
	}

	dumpScript := fmt.Sprintf(`set -euo pipefail
HOST=%s
WORKDIR=/work/binlogs
mkdir -p "$WORKDIR"
for i in $(seq 1 30); do mysqladmin ping -h"$HOST" -uroot -p"$MYSQL_ROOT_PASSWORD" --silent && break; sleep 2; done
mysql -h"$HOST" -uroot -p"$MYSQL_ROOT_PASSWORD" -e "FLUSH BINARY LOGS;"
LOGS=$(mysql -h"$HOST" -uroot -p"$MYSQL_ROOT_PASSWORD" -N -e "SHOW BINARY LOGS;" | awk '{print $1}')
cd "$WORKDIR"
for log in $LOGS; do
  mysqlbinlog --read-from-remote-server --host="$HOST" --user=root --password="$MYSQL_ROOT_PASSWORD" --raw --result-file=./ "$log" || true
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
aws s3 sync /work/binlogs/ "$DEST" "${EXTRA[@]}"
echo binlog archive sync complete
`, shellQuote(dest))

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
										Key:                  rootKey,
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
