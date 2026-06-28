package controller

import (
	"context"
	"fmt"
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
	backupFinalizer   = "mysql.asrk.dev/backup-finalizer"
	backupMountPath   = "/backup"
	backupFileName    = "dump.sql.gz"
	defaultBackupSize = "5Gi"
)

// MySQLBackupReconciler reconciles MySQLBackup objects into PVC + Job (mysqldump).
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
			// Keep PVC by default (backup data); only remove finalizer.
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

	// Terminal phases — do not recreate Jobs.
	if backup.Status.Phase == "Succeeded" || backup.Status.Phase == "Failed" {
		return ctrl.Result{}, nil
	}

	mysql := &mysqlv1alpha1.MySQL{}
	if err := r.Get(ctx, types.NamespacedName{Name: backup.Spec.MySQLName, Namespace: backup.Namespace}, mysql); err != nil {
		msg := fmt.Sprintf("MySQL %q not found: %v", backup.Spec.MySQLName, err)
		_ = r.patchStatus(ctx, backup, "Failed", msg, "", "")
		return ctrl.Result{}, client.IgnoreNotFound(err)
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
	// Prefer in-cluster primary service; falls back to CR status.
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

	pvcName := backup.Name + "-data"
	jobName := backup.Name + "-job"
	fileName := backupFileName

	if err := r.ensureBackupPVC(ctx, backup, pvcName); err != nil {
		logger.Error(err, "ensure backup PVC")
		_ = r.patchStatus(ctx, backup, "Pending", err.Error(), jobName, pvcName)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	if err := r.ensureBackupJob(ctx, backup, mysql, jobName, pvcName, primaryHost, rootSecret, rootKey, image, fileName); err != nil {
		logger.Error(err, "ensure backup Job")
		_ = r.patchStatus(ctx, backup, "Pending", err.Error(), jobName, pvcName)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	job := &batchv1.Job{}
	if err := r.Get(ctx, types.NamespacedName{Name: jobName, Namespace: backup.Namespace}, job); err != nil {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, client.IgnoreNotFound(err)
	}

	phase := "Running"
	msg := "backup Job is running"
	if job.Status.Succeeded > 0 {
		phase = "Succeeded"
		msg = fmt.Sprintf("backup written to PVC %s path %s/%s", pvcName, backupMountPath, fileName)
	} else if job.Status.Failed > 0 {
		phase = "Failed"
		msg = "backup Job failed; check job pods logs"
	}

	if err := r.patchStatus(ctx, backup, phase, msg, jobName, pvcName); err != nil {
		return ctrl.Result{}, err
	}

	if phase == "Running" || phase == "Pending" {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	return ctrl.Result{}, nil
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
	jobName, pvcName, primaryHost, rootSecret, rootKey, image, fileName string,
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

	// Shell script: wait for MySQL, run mysqldump, gzip to PVC.
	script := fmt.Sprintf(`set -euo pipefail
HOST=%q
echo "waiting for MySQL at $HOST..."
for i in $(seq 1 60); do
  mysqladmin ping -h"$HOST" -uroot -p"$MYSQL_ROOT_PASSWORD" --silent && break
  sleep 2
done
mysqladmin ping -h"$HOST" -uroot -p"$MYSQL_ROOT_PASSWORD" --silent
OUT=%q
echo "dumping to $OUT"
# Prefer --source-data (MySQL 8.0.26+); fall back to --master-data for older clients.
if mysqldump --help 2>&1 | grep -q -- '--source-data'; then
  BINLOG_FLAG="--source-data=2"
else
  BINLOG_FLAG="--master-data=2"
fi
mysqldump -h"$HOST" -uroot -p"$MYSQL_ROOT_PASSWORD" \
  --single-transaction --routines --triggers --events $BINLOG_FLAG \
  %s | gzip -c > "$OUT"
ls -la "$OUT"
echo "backup complete"
`, primaryHost, backupMountPath+"/"+fileName, dumpArgs)

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
					Containers: []corev1.Container{{
						Name:    "backup",
						Image:   image,
						Command: []string{"/bin/bash", "-c", script},
						Env: []corev1.EnvVar{{
							Name: "MYSQL_ROOT_PASSWORD",
							ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: rootSecret},
									Key:                  rootKey,
								},
							},
						}},
						VolumeMounts: []corev1.VolumeMount{{
							Name: "backup", MountPath: backupMountPath,
						}},
					}},
					Volumes: []corev1.Volume{{
						Name: "backup",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: pvcName,
							},
						},
					}},
				},
			},
		},
	}
	if backup.Spec.TTLSecondsAfterFinished != nil {
		job.Spec.TTLSecondsAfterFinished = backup.Spec.TTLSecondsAfterFinished
	}
	if err := controllerutil.SetControllerReference(backup, job, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, job)
}

func (r *MySQLBackupReconciler) patchStatus(ctx context.Context, backup *mysqlv1alpha1.MySQLBackup, phase, message, jobName, pvcName string) error {
	// Re-fetch for resourceVersion.
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
