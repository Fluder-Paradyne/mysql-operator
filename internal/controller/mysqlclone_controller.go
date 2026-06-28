package controller

import (
	"context"
	"fmt"
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

const cloneFinalizer = "mysql.asrk.dev/clone-finalizer"

// MySQLCloneReconciler copies live data from one MySQL CR into another.
type MySQLCloneReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Name   string
}

// +kubebuilder:rbac:groups=mysql.asrk.dev,resources=mysqlclones,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mysql.asrk.dev,resources=mysqlclones/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mysql.asrk.dev,resources=mysqlclones/finalizers,verbs=update
// +kubebuilder:rbac:groups=mysql.asrk.dev,resources=mysqls,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch

func (r *MySQLCloneReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	cl := &mysqlv1alpha1.MySQLClone{}
	if err := r.Get(ctx, req.NamespacedName, cl); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !cl.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(cl, cloneFinalizer) {
			controllerutil.RemoveFinalizer(cl, cloneFinalizer)
			_ = r.Update(ctx, cl)
		}
		return ctrl.Result{}, nil
	}
	if !controllerutil.ContainsFinalizer(cl, cloneFinalizer) {
		controllerutil.AddFinalizer(cl, cloneFinalizer)
		if err := r.Update(ctx, cl); err != nil {
			return ctrl.Result{}, err
		}
	}
	if cl.Status.Phase == "Succeeded" || cl.Status.Phase == "Failed" {
		return ctrl.Result{}, nil
	}

	if cl.Spec.SourceMySQLName == "" || cl.Spec.TargetMySQLName == "" {
		_ = r.patchStatus(ctx, cl, "Failed", "sourceMySQLName and targetMySQLName are required", "", "", "")
		return ctrl.Result{}, nil
	}
	if cl.Spec.SourceMySQLName == cl.Spec.TargetMySQLName {
		_ = r.patchStatus(ctx, cl, "Failed", "source and target must be different MySQL CRs", "", "", "")
		return ctrl.Result{}, nil
	}

	src := &mysqlv1alpha1.MySQL{}
	if err := r.Get(ctx, types.NamespacedName{Name: cl.Spec.SourceMySQLName, Namespace: cl.Namespace}, src); err != nil {
		_ = r.patchStatus(ctx, cl, "Failed", fmt.Sprintf("source MySQL: %v", err), "", "", "")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	dst := &mysqlv1alpha1.MySQL{}
	if err := r.Get(ctx, types.NamespacedName{Name: cl.Spec.TargetMySQLName, Namespace: cl.Namespace}, dst); err != nil {
		_ = r.patchStatus(ctx, cl, "Failed", fmt.Sprintf("target MySQL: %v", err), "", "", "")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	srcHost := serviceFQDN(src)
	dstHost := serviceFQDN(dst)
	srcSecret, srcKey := rootSecretRef(src)
	dstSecret, dstKey := rootSecretRef(dst)

	// Wait until both look ready enough (primary service exists; best-effort ready replicas).
	if src.Status.ReadyReplicas < 1 || dst.Status.ReadyReplicas < 1 {
		_ = r.patchStatus(ctx, cl, "Pending", "waiting for source and target to have ready replicas", "", srcHost, dstHost)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	method := cl.Spec.Method
	if method == "" {
		method = "ClonePlugin"
	}
	image := cl.Spec.Image
	if image == "" {
		image = src.Spec.Image
	}
	if image == "" {
		image = defaultImage
	}

	jobName := cl.Name + "-job"
	if err := r.ensureCloneJob(ctx, cl, jobName, method, image, srcHost, dstHost, srcSecret, srcKey, dstSecret, dstKey); err != nil {
		logger.Error(err, "ensure clone job")
		_ = r.patchStatus(ctx, cl, "Pending", err.Error(), jobName, srcHost, dstHost)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	job := &batchv1.Job{}
	if err := r.Get(ctx, types.NamespacedName{Name: jobName, Namespace: cl.Namespace}, job); err != nil {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, client.IgnoreNotFound(err)
	}
	phase, msg := "Running", fmt.Sprintf("clone Job running (%s)", method)
	if job.Status.Succeeded > 0 {
		phase, msg = "Succeeded", fmt.Sprintf("clone completed via %s; HA targets may re-sync replicas automatically", method)
	} else if job.Status.Failed > 0 {
		phase, msg = "Failed", "clone Job failed; check job pod logs"
	}
	if err := r.patchStatus(ctx, cl, phase, msg, jobName, srcHost, dstHost); err != nil {
		return ctrl.Result{}, err
	}
	if phase == "Running" || phase == "Pending" {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

func serviceFQDN(m *mysqlv1alpha1.MySQL) string {
	name := primaryServiceName(m)
	if m.Status.PrimaryService != "" {
		name = m.Status.PrimaryService
	}
	return fmt.Sprintf("%s.%s.svc", name, m.Namespace)
}

func rootSecretRef(m *mysqlv1alpha1.MySQL) (string, string) {
	name := m.Status.RootSecretName
	if name == "" {
		if m.Spec.RootPasswordSecretRef != nil && m.Spec.RootPasswordSecretRef.Name != "" {
			name = m.Spec.RootPasswordSecretRef.Name
		} else {
			name = m.Name + "-root"
		}
	}
	key := defaultSecretKey
	if m.Spec.RootPasswordSecretRef != nil && m.Spec.RootPasswordSecretRef.Key != "" {
		key = m.Spec.RootPasswordSecretRef.Key
	}
	return name, key
}

func (r *MySQLCloneReconciler) ensureCloneJob(
	ctx context.Context,
	cl *mysqlv1alpha1.MySQLClone,
	jobName, method, image, srcHost, dstHost, srcSecret, srcKey, dstSecret, dstKey string,
) error {
	existing := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Name: jobName, Namespace: cl.Namespace}, existing)
	if err == nil {
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}

	// Shared wait + logical stream (also used as fallback if CLONE INSTANCE is unavailable).
	logicalBody := `
export MYSQL_PWD_SRC="$SRC_ROOT_PASSWORD"
export MYSQL_PWD_DST="$DST_ROOT_PASSWORD"
echo "waiting for source $SRC and target $DST"
for i in $(seq 1 60); do
  MYSQL_PWD="$MYSQL_PWD_SRC" mysqladmin ping -h"$SRC" -uroot --silent && \
  MYSQL_PWD="$MYSQL_PWD_DST" mysqladmin ping -h"$DST" -uroot --silent && break
  sleep 2
done
MYSQL_PWD="$MYSQL_PWD_SRC" mysqladmin ping -h"$SRC" -uroot --silent
MYSQL_PWD="$MYSQL_PWD_DST" mysqladmin ping -h"$DST" -uroot --silent
echo "streaming mysqldump $SRC -> $DST (logical live clone)"
if mysqldump --help 2>&1 | grep -q -- '--source-data'; then BINLOG_FLAG="--source-data=2"; else BINLOG_FLAG="--master-data=2"; fi
MYSQL_PWD="$MYSQL_PWD_SRC" mysqldump -h"$SRC" -uroot --single-transaction --routines --triggers --events --all-databases $BINLOG_FLAG \
  | MYSQL_PWD="$MYSQL_PWD_DST" mysql -h"$DST" -uroot
echo "logical clone complete"
`

	var script string
	if method == "Logical" {
		script = fmt.Sprintf("set -euo pipefail\nSRC=%s\nDST=%s\n%s\n", shellQuote(srcHost), shellQuote(dstHost), logicalBody)
	} else {
		// ClonePlugin: try MySQL 8 CLONE INSTANCE; on failure fall back to logical stream so clones remain reliable.
		script = fmt.Sprintf(`set -euo pipefail
SRC=%s
DST=%s
PORT=3306
export MYSQL_PWD_SRC="$SRC_ROOT_PASSWORD"
export MYSQL_PWD_DST="$DST_ROOT_PASSWORD"
echo "waiting for source $SRC and target $DST"
for i in $(seq 1 60); do
  MYSQL_PWD="$MYSQL_PWD_SRC" mysqladmin ping -h"$SRC" -uroot --silent && \
  MYSQL_PWD="$MYSQL_PWD_DST" mysqladmin ping -h"$DST" -uroot --silent && break
  sleep 2
done
MYSQL_PWD="$MYSQL_PWD_SRC" mysqladmin ping -h"$SRC" -uroot --silent
MYSQL_PWD="$MYSQL_PWD_DST" mysqladmin ping -h"$DST" -uroot --silent

clone_plugin_try() {
  echo "attempting CLONE INSTANCE (MySQL 8 plugin)"
  MYSQL_PWD="$MYSQL_PWD_DST" mysql -h"$DST" -uroot -e "INSTALL PLUGIN IF NOT EXISTS clone SONAME 'mysql_clone.so';" 2>/tmp/clone_install.err || true
  cat /tmp/clone_install.err || true
  MYSQL_PWD="$MYSQL_PWD_DST" mysql -h"$DST" -uroot -e "SET GLOBAL clone_valid_donor_list='${SRC}:${PORT}';" || return 1
  CLONE_PASS=$(tr -dc 'a-zA-Z0-9' </dev/urandom | head -c 20)
  MYSQL_PWD="$MYSQL_PWD_SRC" mysql -h"$SRC" -uroot -e "CREATE USER IF NOT EXISTS 'op_clone'@'%%' IDENTIFIED BY '${CLONE_PASS}'; GRANT BACKUP_ADMIN, CLONE_ADMIN ON *.* TO 'op_clone'@'%%'; FLUSH PRIVILEGES;" || return 1
  set +e
  MYSQL_PWD="$MYSQL_PWD_DST" mysql -h"$DST" -uroot -e "CLONE INSTANCE FROM 'op_clone'@'${SRC}':${PORT} IDENTIFIED BY '${CLONE_PASS}';" 2>/tmp/clone_run.err
  RC=$?
  set -e
  echo "CLONE sql exit=$RC"
  cat /tmp/clone_run.err || true
  # Recipient restarts; poll with SOURCE password (data/auth now match donor) then original.
  for i in $(seq 1 90); do
    if MYSQL_PWD="$MYSQL_PWD_SRC" mysqladmin ping -h"$DST" -uroot --silent 2>/dev/null; then
      echo "CLONE appears successful (target up with donor credentials)"
      return 0
    fi
    if MYSQL_PWD="$MYSQL_PWD_DST" mysqladmin ping -h"$DST" -uroot --silent 2>/dev/null; then
      # Server up but may not have finished replace — check error log for success keywords
      if grep -qiE 'Clone|clone' /tmp/clone_run.err 2>/dev/null && [ "$RC" -eq 0 ]; then
        return 0
      fi
      # If CLONE returned 0 and target still on old creds, treat as OK only when RC=0
      if [ "$RC" -eq 0 ]; then return 0; fi
    fi
    sleep 2
  done
  return 1
}

if clone_plugin_try; then
  echo "ClonePlugin path succeeded"
  exit 0
fi

echo "CLONE INSTANCE failed or unavailable — falling back to logical mysqldump stream"
%s
`, shellQuote(srcHost), shellQuote(dstHost), logicalBody)
	}

	backoff := int32(0)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: cl.Namespace,
			Labels: map[string]string{
				labelAppKey: "mysql-clone", labelInstanceKey: cl.Name, labelManagedByKey: managedByValue,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:    "clone",
						Image:   image,
						Command: []string{"/bin/bash", "-c", script},
						Env: []corev1.EnvVar{
							{
								Name: "SRC_ROOT_PASSWORD",
								ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: srcSecret},
									Key:                  srcKey,
								}},
							},
							{
								Name: "DST_ROOT_PASSWORD",
								ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: dstSecret},
									Key:                  dstKey,
								}},
							},
						},
					}},
				},
			},
		},
	}
	if err := controllerutil.SetControllerReference(cl, job, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, job)
}

func (r *MySQLCloneReconciler) patchStatus(ctx context.Context, cl *mysqlv1alpha1.MySQLClone, phase, message, jobName, src, dst string) error {
	cur := &mysqlv1alpha1.MySQLClone{}
	if err := r.Get(ctx, types.NamespacedName{Name: cl.Name, Namespace: cl.Namespace}, cur); err != nil {
		return err
	}
	cur.Status.Phase = phase
	cur.Status.Message = message
	if jobName != "" {
		cur.Status.JobName = jobName
	}
	if src != "" {
		cur.Status.SourcePrimary = src
	}
	if dst != "" {
		cur.Status.TargetPrimary = dst
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

func (r *MySQLCloneReconciler) SetupWithManager(mgr ctrl.Manager) error {
	name := r.Name
	if name == "" {
		name = "mysqlclone"
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		For(&mysqlv1alpha1.MySQLClone{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}
