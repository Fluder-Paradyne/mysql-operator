package controller

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/asrk/mysql-operator/api/v1alpha1"
)

const (
	backupFinalizer    = "mysql.asrk.dev/backup-finalizer"
	backupMountPath    = "/backup"
	backupFileName     = "dump.sql.gz"
	defaultBackupSize  = "5Gi"
	defaultAWSCLIImage = "amazon/aws-cli:2.15.0"
)

// MySQLBackupReconciler reconciles MySQLBackup objects into PVC/emptyDir + Job (mysqldump, optional S3).
type MySQLBackupReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Name   string
}

// +kubebuilder:rbac:groups=mysql.asrk.dev,resources=mysqlbackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mysql.asrk.dev,resources=mysqlbackups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mysql.asrk.dev,resources=mysqlbackups/finalizers,verbs=update
// +kubebuilder:rbac:groups=mysql.asrk.dev,resources=mysqls,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch

func (r *MySQLBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	backup := &mysqlv1alpha1.MySQLBackup{}
	if err := r.Get(ctx, req.NamespacedName, backup); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !backup.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(backup, backupFinalizer) {
			controllerutil.RemoveFinalizer(backup, backupFinalizer)
			if err := r.Update(ctx, backup); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(backup, backupFinalizer) {
		controllerutil.AddFinalizer(backup, backupFinalizer)
		if err := r.Update(ctx, backup); err != nil {
			return ctrl.Result{}, err
		}
	}

	if backup.Status.Phase == "Succeeded" || backup.Status.Phase == "Failed" {
		return ctrl.Result{}, nil
	}

	mysql := &mysqlv1alpha1.MySQL{}
	if err := r.Get(ctx, types.NamespacedName{Name: backup.Spec.MySQLName, Namespace: backup.Namespace}, mysql); err != nil {
		msg := fmt.Sprintf("MySQL %q not found: %v", backup.Spec.MySQLName, err)
		_ = r.patchStatus(ctx, backup, "Failed", msg, "", "", "")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if backup.Spec.S3 != nil {
		if backup.Spec.S3.Bucket == "" || backup.Spec.S3.CredentialsSecretRef.Name == "" {
			_ = r.patchStatus(ctx, backup, "Failed", "spec.s3.bucket and credentialsSecretRef.name are required", "", "", "")
			return ctrl.Result{}, nil
		}
		sec := &corev1.Secret{}
		if err := r.Get(ctx, types.NamespacedName{Name: backup.Spec.S3.CredentialsSecretRef.Name, Namespace: backup.Namespace}, sec); err != nil {
			_ = r.patchStatus(ctx, backup, "Failed", fmt.Sprintf("S3 credentials secret: %v", err), "", "", "")
			return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		}
	}

	rootSecret := mysql.Status.RootSecretName
	if rootSecret == "" {
		if mysql.Spec.RootPasswordSecretRef != nil && mysql.Spec.RootPasswordSecretRef.Name != "" {
			rootSecret = mysql.Spec.RootPasswordSecretRef.Name
		} else {
			rootSecret = mysql.Name + "-root"
		}
	}
	rootKey := defaultSecretKey
	if mysql.Spec.RootPasswordSecretRef != nil && mysql.Spec.RootPasswordSecretRef.Key != "" {
		rootKey = mysql.Spec.RootPasswordSecretRef.Key
	}

	primaryHost := fmt.Sprintf("%s.%s.svc", primaryServiceName(mysql), mysql.Namespace)
	if mysql.Status.PrimaryService != "" {
		primaryHost = fmt.Sprintf("%s.%s.svc", mysql.Status.PrimaryService, mysql.Namespace)
	}

	image := backup.Spec.Image
	if image == "" {
		image = mysql.Spec.Image
	}
	if image == "" {
		image = defaultImage
	}

	usePVC := backup.Spec.S3 == nil || !backup.Spec.S3.SkipPVC
	pvcName := ""
	if usePVC {
		pvcName = backup.Name + "-data"
		if err := r.ensureBackupPVC(ctx, backup, pvcName); err != nil {
			logger.Error(err, "ensure backup PVC")
			_ = r.patchStatus(ctx, backup, "Pending", err.Error(), "", pvcName, "")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
	}

	jobName := backup.Name + "-job"
	s3URI := backupS3URI(backup)

	if err := r.ensureBackupJob(ctx, backup, mysql, jobName, pvcName, usePVC, primaryHost, rootSecret, rootKey, image, s3URI); err != nil {
		logger.Error(err, "ensure backup Job")
		_ = r.patchStatus(ctx, backup, "Pending", err.Error(), jobName, pvcName, "")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	job := &batchv1.Job{}
	if err := r.Get(ctx, types.NamespacedName{Name: jobName, Namespace: backup.Namespace}, job); err != nil {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, client.IgnoreNotFound(err)
	}

	phase := "Running"
	msg := "backup Job is running"
	statusS3 := ""
	if job.Status.Succeeded > 0 {
		phase = "Succeeded"
		if backup.Spec.S3 != nil {
			statusS3 = s3URI
			if usePVC {
				msg = fmt.Sprintf("backup on PVC %s (%s/%s) and uploaded to %s", pvcName, backupMountPath, backupFileName, s3URI)
			} else {
				msg = fmt.Sprintf("backup uploaded to %s", s3URI)
			}
		} else {
			msg = fmt.Sprintf("backup written to PVC %s path %s/%s", pvcName, backupMountPath, backupFileName)
		}
	} else if job.Status.Failed > 0 {
		phase = "Failed"
		msg = "backup Job failed; check job / init container logs"
	}

	if err := r.patchStatus(ctx, backup, phase, msg, jobName, pvcName, statusS3); err != nil {
		return ctrl.Result{}, err
	}

	if phase == "Running" || phase == "Pending" {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

func backupS3URI(backup *mysqlv1alpha1.MySQLBackup) string {
	if backup.Spec.S3 == nil || backup.Spec.S3.Bucket == "" {
		return ""
	}
	key := backupObjectKey(backup)
	return fmt.Sprintf("s3://%s/%s", backup.Spec.S3.Bucket, key)
}

func backupObjectKey(backup *mysqlv1alpha1.MySQLBackup) string {
	s3 := backup.Spec.S3
	if s3.ObjectKey != "" {
		return strings.TrimPrefix(s3.ObjectKey, "/")
	}
	prefix := s3.Prefix
	if prefix == "" {
		prefix = path.Join("mysql-backups", backup.Spec.MySQLName, backup.Name)
	}
	prefix = strings.Trim(prefix, "/")
	return prefix + "/" + backupFileName
}

func (r *MySQLBackupReconciler) ensureBackupPVC(ctx context.Context, backup *mysqlv1alpha1.MySQLBackup, pvcName string) error {
	size := backup.Spec.StorageSize
	if size == "" {
		size = defaultBackupSize
	}
	qty, err := resource.ParseQuantity(size)
	if err != nil {
		return fmt.Errorf("storageSize: %w", err)
	}

	pvc := &corev1.PersistentVolumeClaim{}
	err = r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: backup.Namespace}, pvc)
	if err == nil {
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}

	pvc = &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: backup.Namespace,
			Labels: map[string]string{
				labelAppKey:       "mysql-backup",
				labelInstanceKey:  backup.Name,
				labelManagedByKey: managedByValue,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: qty},
			},
		},
	}
	if backup.Spec.StorageClassName != nil {
		pvc.Spec.StorageClassName = backup.Spec.StorageClassName
	}
	if err := controllerutil.SetControllerReference(backup, pvc, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, pvc)
}

func (r *MySQLBackupReconciler) ensureBackupJob(
	ctx context.Context,
	backup *mysqlv1alpha1.MySQLBackup,
	mysql *mysqlv1alpha1.MySQL,
	jobName, pvcName string,
	usePVC bool,
	primaryHost, rootSecret, rootKey, image, s3URI string,
) error {
	existing := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Name: jobName, Namespace: backup.Namespace}, existing)
	if err == nil {
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}

	dumpArgs := "--all-databases"
	if len(backup.Spec.Databases) > 0 {
		dumpArgs = strings.Join(backup.Spec.Databases, " ")
	}

	dumpScript := fmt.Sprintf(`set -euo pipefail
HOST=%q
echo "waiting for MySQL at $HOST..."
for i in $(seq 1 60); do
  mysqladmin ping -h"$HOST" -uroot -p"$MYSQL_ROOT_PASSWORD" --silent && break
  sleep 2
done
mysqladmin ping -h"$HOST" -uroot -p"$MYSQL_ROOT_PASSWORD" --silent
OUT=%q
echo "dumping to $OUT"
if mysqldump --help 2>&1 | grep -q -- '--source-data'; then
  BINLOG_FLAG="--source-data=2"
else
  BINLOG_FLAG="--master-data=2"
fi
mysqldump -h"$HOST" -uroot -p"$MYSQL_ROOT_PASSWORD" \
  --single-transaction --routines --triggers --events $BINLOG_FLAG \
  %s | gzip -c > "$OUT"
ls -la "$OUT"
echo "dump complete"
`, primaryHost, backupMountPath+"/"+backupFileName, dumpArgs)

	volName := "backup"
	var volumes []corev1.Volume
	var mounts []corev1.VolumeMount
	mounts = []corev1.VolumeMount{{Name: volName, MountPath: backupMountPath}}
	if usePVC {
		volumes = []corev1.Volume{{
			Name: volName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName},
			},
		}}
	} else {
		volumes = []corev1.Volume{{
			Name: volName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		}}
	}

	rootEnv := corev1.EnvVar{
		Name: "MYSQL_ROOT_PASSWORD",
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: rootSecret},
				Key:                  rootKey,
			},
		},
	}

	backoff := int32(1)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: backup.Namespace,
			Labels: map[string]string{
				labelAppKey:       "mysql-backup",
				labelInstanceKey:  backup.Name,
				labelManagedByKey: managedByValue,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						labelAppKey:      "mysql-backup",
						labelInstanceKey: backup.Name,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Volumes:       volumes,
				},
			},
		},
	}

	if backup.Spec.S3 == nil {
		// Single container: dump only
		job.Spec.Template.Spec.Containers = []corev1.Container{{
			Name:         "backup",
			Image:        image,
			Command:      []string{"/bin/bash", "-c", dumpScript},
			Env:          []corev1.EnvVar{rootEnv},
			VolumeMounts: mounts,
		}}
	} else {
		// initContainer dumps; main container uploads with AWS CLI
		job.Spec.Template.Spec.InitContainers = []corev1.Container{{
			Name:         "mysqldump",
			Image:        image,
			Command:      []string{"/bin/bash", "-c", dumpScript},
			Env:          []corev1.EnvVar{rootEnv},
			VolumeMounts: mounts,
		}}

		awsImage := backup.Spec.AWSCLIImage
		if awsImage == "" {
			awsImage = defaultAWSCLIImage
		}
		credSecret := backup.Spec.S3.CredentialsSecretRef.Name
		objKey := backupObjectKey(backup)
		// Normalize credential env from either naming convention via a small shell prelude.
		uploadScript := fmt.Sprintf(`set -euo pipefail
# Map alternate secret key names if present
if [ -z "${AWS_ACCESS_KEY_ID:-}" ] && [ -n "${accessKeyId:-}" ]; then export AWS_ACCESS_KEY_ID="$accessKeyId"; fi
if [ -z "${AWS_SECRET_ACCESS_KEY:-}" ] && [ -n "${secretAccessKey:-}" ]; then export AWS_SECRET_ACCESS_KEY="$secretAccessKey"; fi
if [ -z "${AWS_SESSION_TOKEN:-}" ] && [ -n "${sessionToken:-}" ]; then export AWS_SESSION_TOKEN="$sessionToken"; fi
FILE=%q
test -s "$FILE"
DEST=%q
echo "uploading $FILE -> $DEST"
EXTRA=()
if [ -n "${AWS_ENDPOINT_URL:-}" ]; then
  EXTRA+=(--endpoint-url "$AWS_ENDPOINT_URL")
fi
# Path-style for MinIO etc.
if [ "${AWS_S3_FORCE_PATH_STYLE:-}" = "true" ] || [ "${AWS_S3_FORCE_PATH_STYLE:-}" = "1" ]; then
  # aws-cli v2 uses --endpoint-url; path style via config
  aws configure set default.s3.addressing_style path
fi
aws s3 cp "$FILE" "$DEST" "${EXTRA[@]}"
echo "upload complete: $DEST"
`, backupMountPath+"/"+backupFileName, fmt.Sprintf("s3://%s/%s", backup.Spec.S3.Bucket, objKey))

		awsEnv := []corev1.EnvVar{
			// Prefer standard AWS_* keys from the secret; optional alternates injected as plain env for mapping.
			{Name: "AWS_ACCESS_KEY_ID", ValueFrom: optionalSecretEnv(credSecret, "AWS_ACCESS_KEY_ID")},
			{Name: "AWS_SECRET_ACCESS_KEY", ValueFrom: optionalSecretEnv(credSecret, "AWS_SECRET_ACCESS_KEY")},
			{Name: "AWS_SESSION_TOKEN", ValueFrom: optionalSecretEnv(credSecret, "AWS_SESSION_TOKEN")},
			{Name: "accessKeyId", ValueFrom: optionalSecretEnv(credSecret, "accessKeyId")},
			{Name: "secretAccessKey", ValueFrom: optionalSecretEnv(credSecret, "secretAccessKey")},
			{Name: "sessionToken", ValueFrom: optionalSecretEnv(credSecret, "sessionToken")},
		}
		// Optional keys may be missing — Kubernetes rejects optional false missing keys.
		// Use optional: true on all secret key refs.
		for i := range awsEnv {
			if awsEnv[i].ValueFrom != nil && awsEnv[i].ValueFrom.SecretKeyRef != nil {
				t := true
				awsEnv[i].ValueFrom.SecretKeyRef.Optional = &t
			}
		}
		if backup.Spec.S3.Region != "" {
			awsEnv = append(awsEnv, corev1.EnvVar{Name: "AWS_DEFAULT_REGION", Value: backup.Spec.S3.Region})
			awsEnv = append(awsEnv, corev1.EnvVar{Name: "AWS_REGION", Value: backup.Spec.S3.Region})
		}
		if backup.Spec.S3.Endpoint != "" {
			awsEnv = append(awsEnv, corev1.EnvVar{Name: "AWS_ENDPOINT_URL", Value: backup.Spec.S3.Endpoint})
		}
		if backup.Spec.S3.ForcePathStyle {
			awsEnv = append(awsEnv, corev1.EnvVar{Name: "AWS_S3_FORCE_PATH_STYLE", Value: "true"})
		}

		job.Spec.Template.Spec.Containers = []corev1.Container{{
			Name:         "s3-upload",
			Image:        awsImage,
			// amazon/aws-cli entrypoint is "aws"; override to run shell.
			Command:      []string{"/bin/sh", "-c", uploadScript},
			Env:          awsEnv,
			VolumeMounts: mounts,
		}}
	}

	if backup.Spec.TTLSecondsAfterFinished != nil {
		job.Spec.TTLSecondsAfterFinished = backup.Spec.TTLSecondsAfterFinished
	}
	if err := controllerutil.SetControllerReference(backup, job, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, job)
}

func optionalSecretEnv(secretName, key string) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{
		SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
			Key:                  key,
		},
	}
}

func (r *MySQLBackupReconciler) patchStatus(ctx context.Context, backup *mysqlv1alpha1.MySQLBackup, phase, message, jobName, pvcName, s3URI string) error {
	cur := &mysqlv1alpha1.MySQLBackup{}
	if err := r.Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace}, cur); err != nil {
		return err
	}
	cur.Status.Phase = phase
	cur.Status.Message = message
	if jobName != "" {
		cur.Status.JobName = jobName
	}
	if pvcName != "" {
		cur.Status.PVCName = pvcName
		cur.Status.FileName = backupMountPath + "/" + backupFileName
	}
	if s3URI != "" {
		cur.Status.S3URI = s3URI
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
		Type:               "Completed",
		Status:             metav1.ConditionFalse,
		Reason:             phase,
		Message:            message,
		LastTransitionTime: metav1.Now(),
		ObservedGeneration: cur.Generation,
	}
	if phase == "Succeeded" {
		cond.Status = metav1.ConditionTrue
		cond.Reason = "Succeeded"
	}
	if phase == "Failed" {
		cond.Reason = "Failed"
	}
	setCondition(&cur.Status.Conditions, cond)
	return r.Status().Update(ctx, cur)
}

func (r *MySQLBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	name := r.Name
	if name == "" {
		name = "mysqlbackup"
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		For(&mysqlv1alpha1.MySQLBackup{}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Complete(r)
}
