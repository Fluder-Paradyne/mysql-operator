package controller

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/asrk/mysql-operator/api/v1alpha1"
)

const (
	finalizerName     = "mysql.asrk.dev/finalizer"
	labelAppKey       = "app.kubernetes.io/name"
	labelInstanceKey  = "app.kubernetes.io/instance"
	labelManagedByKey = "app.kubernetes.io/managed-by"
	labelComponentKey = "app.kubernetes.io/component"
	labelRoleKey      = "mysql.asrk.dev/role"
	rolePrimary       = "primary"
	roleReplica       = "replica"
	managedByValue    = "mysql-operator"
	componentValue    = "database"
	dataVolumeName    = "data"
	dataMountPath     = "/var/lib/mysql"
	mysqlPortName     = "mysql"
	defaultImage      = "mysql:8.0"
	defaultStorage    = "10Gi"
	defaultPort       = int32(3306)
	defaultSecretKey  = "password"
	replUser          = "repl"
	scriptsVolume     = "operator-scripts"
	scriptsMount      = "/opt/operator"
	configVolume      = "operator-config"
	configMount       = "/etc/mysql/conf.d"
)

// MySQLReconciler reconciles a MySQL object.
type MySQLReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	Clientset  kubernetes.Interface
	RESTConfig *rest.Config
	// Name uniquely identifies this controller instance (required when multiple
	// managers/reconcilers are created in one process, e.g. sequential e2e tests).
	Name string
}

// +kubebuilder:rbac:groups=mysql.asrk.dev,resources=mysqls,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mysql.asrk.dev,resources=mysqls/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mysql.asrk.dev,resources=mysqls/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;patch;update
// +kubebuilder:rbac:groups=core,resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch

func (r *MySQLReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	mysql := &mysqlv1alpha1.MySQL{}
	if err := r.Get(ctx, req.NamespacedName, mysql); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !mysql.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(mysql, finalizerName) {
			controllerutil.RemoveFinalizer(mysql, finalizerName)
			if err := r.Update(ctx, mysql); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(mysql, finalizerName) {
		controllerutil.AddFinalizer(mysql, finalizerName)
		if err := r.Update(ctx, mysql); err != nil {
			return ctrl.Result{}, err
		}
	}

	rootSecret, rootKey, err := r.ensureRootSecret(ctx, mysql)
	if err != nil {
		logger.Error(err, "failed to ensure root secret")
		return ctrl.Result{}, err
	}

	var replSecret, replKey string
	if mysql.Spec.DesiredReplicas() > 1 {
		replSecret, replKey, err = r.ensureReplicationSecret(ctx, mysql)
		if err != nil {
			logger.Error(err, "failed to ensure replication secret")
			return ctrl.Result{}, err
		}
	}

	if err := r.ensureConfigMap(ctx, mysql); err != nil {
		logger.Error(err, "failed to ensure configmap")
		return ctrl.Result{}, err
	}

	if err := r.ensureServices(ctx, mysql); err != nil {
		logger.Error(err, "failed to ensure services")
		return ctrl.Result{}, err
	}

	sts, err := r.ensureStatefulSet(ctx, mysql, rootSecret, rootKey)
	if err != nil {
		logger.Error(err, "failed to ensure statefulset")
		return ctrl.Result{}, err
	}

	if err := r.ensurePodRoles(ctx, mysql); err != nil {
		logger.Error(err, "failed to label pod roles")
		return ctrl.Result{}, err
	}

	requeue, replicating, err := r.ensureReplication(ctx, mysql, rootSecret, rootKey, replSecret, replKey)
	if err != nil {
		logger.Error(err, "failed to configure replication")
		// Keep reconciling; pods may still be starting.
		_ = r.updateStatus(ctx, mysql, sts, rootSecret, replSecret, replicating)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	if err := r.updateStatus(ctx, mysql, sts, rootSecret, replSecret, replicating); err != nil {
		logger.Error(err, "failed to update status")
		return ctrl.Result{}, err
	}

	if requeue {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}
	// Periodically re-check replication health when HA is enabled.
	if mysql.Spec.DesiredReplicas() > 1 {
		return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

func (r *MySQLReconciler) ensureRootSecret(ctx context.Context, mysql *mysqlv1alpha1.MySQL) (string, string, error) {
	if mysql.Spec.RootPasswordSecretRef != nil && mysql.Spec.RootPasswordSecretRef.Name != "" {
		key := mysql.Spec.RootPasswordSecretRef.Key
		if key == "" {
			key = defaultSecretKey
		}
		sec := &corev1.Secret{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      mysql.Spec.RootPasswordSecretRef.Name,
			Namespace: mysql.Namespace,
		}, sec); err != nil {
			return "", "", fmt.Errorf("root password secret %q: %w", mysql.Spec.RootPasswordSecretRef.Name, err)
		}
		if _, ok := sec.Data[key]; !ok {
			return "", "", fmt.Errorf("root password secret %q missing key %q", mysql.Spec.RootPasswordSecretRef.Name, key)
		}
		return mysql.Spec.RootPasswordSecretRef.Name, key, nil
	}

	name := mysql.Name + "-root"
	existing := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: mysql.Namespace}, existing)
	if err == nil {
		return name, defaultSecretKey, nil
	}
	if !errors.IsNotFound(err) {
		return "", "", err
	}

	password, err := generatePassword(24)
	if err != nil {
		return "", "", err
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: mysql.Namespace,
			Labels:    labelsFor(mysql),
		},
		Type: corev1.SecretTypeOpaque,
		StringData: map[string]string{
			defaultSecretKey: password,
		},
	}
	if err := controllerutil.SetControllerReference(mysql, secret, r.Scheme); err != nil {
		return "", "", err
	}
	if err := r.Create(ctx, secret); err != nil {
		return "", "", err
	}
	return name, defaultSecretKey, nil
}

func (r *MySQLReconciler) ensureReplicationSecret(ctx context.Context, mysql *mysqlv1alpha1.MySQL) (string, string, error) {
	if mysql.Spec.ReplicationPasswordSecretRef != nil && mysql.Spec.ReplicationPasswordSecretRef.Name != "" {
		key := mysql.Spec.ReplicationPasswordSecretRef.Key
		if key == "" {
			key = defaultSecretKey
		}
		sec := &corev1.Secret{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      mysql.Spec.ReplicationPasswordSecretRef.Name,
			Namespace: mysql.Namespace,
		}, sec); err != nil {
			return "", "", fmt.Errorf("replication password secret %q: %w", mysql.Spec.ReplicationPasswordSecretRef.Name, err)
		}
		if _, ok := sec.Data[key]; !ok {
			return "", "", fmt.Errorf("replication password secret %q missing key %q", mysql.Spec.ReplicationPasswordSecretRef.Name, key)
		}
		return mysql.Spec.ReplicationPasswordSecretRef.Name, key, nil
	}

	name := mysql.Name + "-replication"
	existing := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: mysql.Namespace}, existing)
	if err == nil {
		return name, defaultSecretKey, nil
	}
	if !errors.IsNotFound(err) {
		return "", "", err
	}

	password, err := generatePassword(24)
	if err != nil {
		return "", "", err
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: mysql.Namespace,
			Labels:    labelsFor(mysql),
		},
		Type: corev1.SecretTypeOpaque,
		StringData: map[string]string{
			defaultSecretKey: password,
		},
	}
	if err := controllerutil.SetControllerReference(mysql, secret, r.Scheme); err != nil {
		return "", "", err
	}
	if err := r.Create(ctx, secret); err != nil {
		return "", "", err
	}
	return name, defaultSecretKey, nil
}

func (r *MySQLReconciler) ensureConfigMap(ctx context.Context, mysql *mysqlv1alpha1.MySQL) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName(mysql),
			Namespace: mysql.Namespace,
			Labels:    labelsFor(mysql),
		},
		Data: map[string]string{
			"entrypoint.sh": entrypointScript,
		},
	}
	if err := controllerutil.SetControllerReference(mysql, cm, r.Scheme); err != nil {
		return err
	}
	existing := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Name: cm.Name, Namespace: cm.Namespace}, existing)
	if errors.IsNotFound(err) {
		return r.Create(ctx, cm)
	}
	if err != nil {
		return err
	}
	existing.Data = cm.Data
	existing.Labels = cm.Labels
	return r.Update(ctx, existing)
}

func (r *MySQLReconciler) ensureServices(ctx context.Context, mysql *mysqlv1alpha1.MySQL) error {
	port := effectivePort(mysql)

	// Headless — stable DNS for replication (mysql-0.mysql-headless.ns.svc).
	if err := r.upsertService(ctx, mysql, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      headlessServiceName(mysql),
			Namespace: mysql.Namespace,
			Labels:    labelsFor(mysql),
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector: map[string]string{
				labelAppKey:      "mysql",
				labelInstanceKey: mysql.Name,
			},
			Ports: []corev1.ServicePort{{
				Name: mysqlPortName, Port: port, TargetPort: intstr.FromString(mysqlPortName), Protocol: corev1.ProtocolTCP,
			}},
		},
	}); err != nil {
		return err
	}

	// Primary (read/write) — selects role=primary.
	if err := r.upsertService(ctx, mysql, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      primaryServiceName(mysql),
			Namespace: mysql.Namespace,
			Labels:    labelsFor(mysql),
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Selector: map[string]string{
				labelAppKey:      "mysql",
				labelInstanceKey: mysql.Name,
				labelRoleKey:     rolePrimary,
			},
			Ports: []corev1.ServicePort{{
				Name: mysqlPortName, Port: port, TargetPort: intstr.FromString(mysqlPortName), Protocol: corev1.ProtocolTCP,
			}},
		},
	}); err != nil {
		return err
	}

	// Reads — all members (primary + replicas). Clients can load-balance reads.
	if err := r.upsertService(ctx, mysql, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      readsServiceName(mysql),
			Namespace: mysql.Namespace,
			Labels:    labelsFor(mysql),
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Selector: map[string]string{
				labelAppKey:      "mysql",
				labelInstanceKey: mysql.Name,
			},
			Ports: []corev1.ServicePort{{
				Name: mysqlPortName, Port: port, TargetPort: intstr.FromString(mysqlPortName), Protocol: corev1.ProtocolTCP,
			}},
		},
	}); err != nil {
		return err
	}

	// Legacy name `<mysql.Name>` keeps pointing at the primary for backwards compatibility.
	return r.upsertService(ctx, mysql, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName(mysql),
			Namespace: mysql.Namespace,
			Labels:    labelsFor(mysql),
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Selector: map[string]string{
				labelAppKey:      "mysql",
				labelInstanceKey: mysql.Name,
				labelRoleKey:     rolePrimary,
			},
			Ports: []corev1.ServicePort{{
				Name: mysqlPortName, Port: port, TargetPort: intstr.FromString(mysqlPortName), Protocol: corev1.ProtocolTCP,
			}},
		},
	})
}

func (r *MySQLReconciler) upsertService(ctx context.Context, mysql *mysqlv1alpha1.MySQL, desired *corev1.Service) error {
	if err := controllerutil.SetControllerReference(mysql, desired, r.Scheme); err != nil {
		return err
	}
	existing := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, existing)
	if errors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	existing.Spec.Selector = desired.Spec.Selector
	existing.Spec.Ports = desired.Spec.Ports
	existing.Spec.PublishNotReadyAddresses = desired.Spec.PublishNotReadyAddresses
	// ClusterIP / ClusterIPNone is immutable — only set on create.
	existing.Labels = desired.Labels
	return r.Update(ctx, existing)
}

func (r *MySQLReconciler) ensureStatefulSet(ctx context.Context, mysql *mysqlv1alpha1.MySQL, secretName, secretKey string) (*appsv1.StatefulSet, error) {
	image := mysql.Spec.Image
	if image == "" {
		image = defaultImage
	}
	replicas := mysql.Spec.DesiredReplicas()
	port := effectivePort(mysql)
	storageSize := mysql.Spec.StorageSize
	if storageSize == "" {
		storageSize = defaultStorage
	}
	qty, err := resource.ParseQuantity(storageSize)
	if err != nil {
		return nil, fmt.Errorf("invalid storageSize %q: %w", storageSize, err)
	}

	labels := labelsFor(mysql)
	stsName := statefulSetName(mysql)

	env := []corev1.EnvVar{
		{
			Name: "MYSQL_ROOT_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
					Key:                  secretKey,
				},
			},
		},
		{Name: "MYSQL_NAME", Value: mysql.Name},
		{Name: "HEADLESS_SERVICE", Value: headlessServiceName(mysql)},
		{
			Name: "POD_NAMESPACE",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
			},
		},
	}
	// Only apply MYSQL_DATABASE on first init; all pods get it but only primary should create app DB.
	// Replicas cloning from primary will receive the database via replication.
	if mysql.Spec.Database != "" {
		env = append(env, corev1.EnvVar{Name: "MYSQL_DATABASE", Value: mysql.Spec.Database})
	}

	pvcSpec := corev1.PersistentVolumeClaimSpec{
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		Resources: corev1.VolumeResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceStorage: qty},
		},
	}
	if mysql.Spec.StorageClassName != nil {
		pvcSpec.StorageClassName = mysql.Spec.StorageClassName
	}

	scriptMode := int32(0755)
	desired := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      stsName,
			Namespace: mysql.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName:         headlessServiceName(mysql),
			Replicas:            &replicas,
			PodManagementPolicy: appsv1.OrderedReadyPodManagement,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					labelAppKey:      "mysql",
					labelInstanceKey: mysql.Name,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					// Role labels are set by the controller after pods exist; start without role so
					// primary service does not select unready pods incorrectly for too long.
					Containers: []corev1.Container{
						{
							Name:    "mysql",
							Image:   image,
							Command: []string{"/bin/bash", scriptsMount + "/entrypoint.sh"},
							Ports: []corev1.ContainerPort{{
								Name: mysqlPortName, ContainerPort: port, Protocol: corev1.ProtocolTCP,
							}},
							Env:       env,
							Resources: mysql.Spec.Resources,
							VolumeMounts: []corev1.VolumeMount{
								{Name: dataVolumeName, MountPath: dataMountPath},
								{Name: scriptsVolume, MountPath: scriptsMount, ReadOnly: true},
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString(mysqlPortName)},
								},
								InitialDelaySeconds: 20,
								PeriodSeconds:       5,
								TimeoutSeconds:      3,
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString(mysqlPortName)},
								},
								InitialDelaySeconds: 60,
								PeriodSeconds:       10,
								TimeoutSeconds:      3,
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: scriptsVolume,
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: configMapName(mysql)},
									DefaultMode:          &scriptMode,
								},
							},
						},
					},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: dataVolumeName, Labels: labels},
				Spec:       pvcSpec,
			}},
		},
	}
	if err := controllerutil.SetControllerReference(mysql, desired, r.Scheme); err != nil {
		return nil, err
	}

	existing := &appsv1.StatefulSet{}
	err = r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, existing)
	if errors.IsNotFound(err) {
		if err := r.Create(ctx, desired); err != nil {
			return nil, err
		}
		return desired, nil
	}
	if err != nil {
		return nil, err
	}

	existing.Spec.Replicas = desired.Spec.Replicas
	existing.Spec.Template = desired.Spec.Template
	existing.Spec.ServiceName = desired.Spec.ServiceName
	existing.Spec.PodManagementPolicy = desired.Spec.PodManagementPolicy
	existing.Labels = desired.Labels
	if err := r.Update(ctx, existing); err != nil {
		return nil, err
	}
	return existing, nil
}

func (r *MySQLReconciler) ensurePodRoles(ctx context.Context, mysql *mysqlv1alpha1.MySQL) error {
	desired := mysql.Spec.DesiredReplicas()
	for i := int32(0); i < desired; i++ {
		podName := fmt.Sprintf("%s-%d", mysql.Name, i)
		pod := &corev1.Pod{}
		if err := r.Get(ctx, types.NamespacedName{Name: podName, Namespace: mysql.Namespace}, pod); err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			return err
		}
		role := roleReplica
		if i == 0 {
			role = rolePrimary
		}
		if pod.Labels == nil {
			pod.Labels = map[string]string{}
		}
		if pod.Labels[labelRoleKey] == role &&
			pod.Labels[labelAppKey] == "mysql" &&
			pod.Labels[labelInstanceKey] == mysql.Name {
			continue
		}
		pod.Labels[labelRoleKey] = role
		pod.Labels[labelAppKey] = "mysql"
		pod.Labels[labelInstanceKey] = mysql.Name
		pod.Labels[labelManagedByKey] = managedByValue
		pod.Labels[labelComponentKey] = componentValue
		if err := r.Update(ctx, pod); err != nil {
			return err
		}
	}
	return nil
}

// ensureReplication configures the replication user on the primary and GTID replicas on followers.
// Returns (requeue, replicatingCount, err).
func (r *MySQLReconciler) ensureReplication(ctx context.Context, mysql *mysqlv1alpha1.MySQL, rootSecret, rootKey, replSecret, replKey string) (bool, int32, error) {
	desired := mysql.Spec.DesiredReplicas()
	if desired <= 1 {
		return false, 0, nil
	}
	if r.Clientset == nil || r.RESTConfig == nil {
		return true, 0, fmt.Errorf("kubernetes clientset not configured for pod exec")
	}

	rootPass, err := r.secretValue(ctx, mysql.Namespace, rootSecret, rootKey)
	if err != nil {
		return true, 0, err
	}
	replPass, err := r.secretValue(ctx, mysql.Namespace, replSecret, replKey)
	if err != nil {
		return true, 0, err
	}

	primaryPod := primaryPodName(mysql)
	if !r.podReady(ctx, mysql.Namespace, primaryPod) {
		return true, 0, nil
	}

	// Create replication user on primary (idempotent).
	createReplSQL := fmt.Sprintf(`
CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY '%s';
ALTER USER '%s'@'%%' IDENTIFIED BY '%s';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, BACKUP_ADMIN, CLONE_ADMIN ON *.* TO '%s'@'%%';
FLUSH PRIVILEGES;
`, replUser, escapeSQL(replPass), replUser, escapeSQL(replPass), replUser)
	if _, err := r.execSQL(ctx, mysql.Namespace, primaryPod, rootPass, createReplSQL); err != nil {
		return true, 0, fmt.Errorf("primary replication user: %w", err)
	}

	primaryHost := fmt.Sprintf("%s.%s.%s.svc.cluster.local", primaryPod, headlessServiceName(mysql), mysql.Namespace)
	var replicating int32
	requeue := false

	for i := int32(1); i < desired; i++ {
		podName := fmt.Sprintf("%s-%d", mysql.Name, i)
		if !r.podReady(ctx, mysql.Namespace, podName) {
			requeue = true
			continue
		}

		statusOut, err := r.execSQL(ctx, mysql.Namespace, podName, rootPass,
			`SELECT COALESCE(SERVICE_STATE,'') FROM performance_schema.replication_connection_status LIMIT 1;`)
		if err != nil {
			// performance_schema might not have a row yet.
			statusOut = ""
		}
		if strings.Contains(statusOut, "ON") {
			// Also verify SQL thread.
			sqlOut, _ := r.execSQL(ctx, mysql.Namespace, podName, rootPass,
				`SELECT COALESCE(SERVICE_STATE,'') FROM performance_schema.replication_applier_status LIMIT 1;`)
			if strings.Contains(sqlOut, "ON") {
				replicating++
				continue
			}
		}

		// Bootstrap replica via CLONE (MySQL 8), then configure GTID replication.
		// CLONE restarts the server; we tolerate errors and requeue.
		cloneSQL := fmt.Sprintf(`
INSTALL PLUGIN IF NOT EXISTS clone SONAME 'mysql_clone.so';
SET GLOBAL clone_valid_donor_list = '%s:%d';
`, primaryHost, effectivePort(mysql))
		if _, err := r.execSQL(ctx, mysql.Namespace, podName, rootPass, cloneSQL); err != nil {
			// Plugin may already exist under a different path; continue to CLONE.
			log.FromContext(ctx).Info("clone plugin setup", "pod", podName, "err", err.Error())
		}

		// Only clone when not yet replicating — expensive but required for consistent data.
		markerOut, _ := r.execInPod(ctx, mysql.Namespace, podName, "mysql", []string{
			"bash", "-c", "test -f /var/lib/mysql/.operator_cloned && echo yes || echo no",
		})
		if !strings.Contains(markerOut, "yes") {
			cloneInstance := fmt.Sprintf(
				`CLONE INSTANCE FROM '%s'@'%s':%d IDENTIFIED BY '%s';`,
				replUser, primaryHost, effectivePort(mysql), escapeSQL(replPass),
			)
			_, cloneErr := r.execSQL(ctx, mysql.Namespace, podName, rootPass, cloneInstance)
			// CLONE restarts mysqld; connection errors are expected.
			if cloneErr != nil {
				log.FromContext(ctx).Info("clone instance requested", "pod", podName, "err", cloneErr.Error())
			}
			// Wait for mysqld to come back, then mark and configure replication.
			requeue = true
			// Best-effort marker after a short wait is done on next reconcile when pod is ready again.
			// Write marker via a follow-up once SQL works.
			if r.podReady(ctx, mysql.Namespace, podName) {
				_, _ = r.execInPod(ctx, mysql.Namespace, podName, "mysql", []string{
					"bash", "-c", "touch /var/lib/mysql/.operator_cloned",
				})
			}
			continue
		}

		changeSQL := fmt.Sprintf(`
STOP REPLICA;
CHANGE REPLICATION SOURCE TO
  SOURCE_HOST='%s',
  SOURCE_PORT=%d,
  SOURCE_USER='%s',
  SOURCE_PASSWORD='%s',
  SOURCE_AUTO_POSITION=1,
  GET_SOURCE_PUBLIC_KEY=1;
START REPLICA;
`, primaryHost, effectivePort(mysql), replUser, escapeSQL(replPass))
		if _, err := r.execSQL(ctx, mysql.Namespace, podName, rootPass, changeSQL); err != nil {
			requeue = true
			log.FromContext(ctx).Info("configure replica", "pod", podName, "err", err.Error())
			continue
		}
		replicating++
	}

	if replicating < desired-1 {
		requeue = true
	}
	return requeue, replicating, nil
}

func (r *MySQLReconciler) secretValue(ctx context.Context, namespace, name, key string) (string, error) {
	sec := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, sec); err != nil {
		return "", err
	}
	b, ok := sec.Data[key]
	if !ok {
		return "", fmt.Errorf("secret %s missing key %s", name, key)
	}
	return string(b), nil
}

func (r *MySQLReconciler) podReady(ctx context.Context, namespace, name string) bool {
	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, pod); err != nil {
		return false
	}
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (r *MySQLReconciler) execSQL(ctx context.Context, namespace, pod, rootPassword, sql string) (string, error) {
	// Use mysql client with password via env to reduce shell escaping issues for simple cases;
	// password still appears in process list — acceptable for operator bootstrap.
	cmd := []string{
		"mysql", "--protocol=TCP", "-h127.0.0.1", "-uroot", "-p" + rootPassword,
		"-N", "-e", sql,
	}
	return r.execInPod(ctx, namespace, pod, "mysql", cmd)
}

func (r *MySQLReconciler) execInPod(ctx context.Context, namespace, pod, container string, command []string) (string, error) {
	req := r.Clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(pod).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   command,
			Stdout:    true,
			Stderr:    true,
		}, clientgoscheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(r.RESTConfig, "POST", req.URL())
	if err != nil {
		return "", err
	}
	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr})
	out := stdout.String() + stderr.String()
	return out, err
}

func (r *MySQLReconciler) updateStatus(ctx context.Context, mysql *mysqlv1alpha1.MySQL, sts *appsv1.StatefulSet, rootSecret, replSecret string, replicating int32) error {
	desired := mysql.Spec.DesiredReplicas()
	phase := "Pending"
	if sts.Status.ReadyReplicas > 0 {
		phase = "Running"
	}
	if sts.Status.Replicas > 0 && sts.Status.ReadyReplicas == 0 {
		phase = "Starting"
	}
	if desired > 1 && sts.Status.ReadyReplicas >= desired && replicating < desired-1 {
		phase = "Replicating"
	}
	if desired > 1 && replicating >= desired-1 && sts.Status.ReadyReplicas >= desired {
		phase = "Ready"
	}

	mode := "Standalone"
	if desired > 1 {
		mode = "PrimaryReplica"
	}

	mysql.Status.Phase = phase
	mysql.Status.ReadyReplicas = sts.Status.ReadyReplicas
	mysql.Status.DesiredReplicas = desired
	mysql.Status.Mode = mode
	mysql.Status.PrimaryPod = primaryPodName(mysql)
	mysql.Status.PrimaryService = primaryServiceName(mysql)
	mysql.Status.ReadsService = readsServiceName(mysql)
	mysql.Status.HeadlessService = headlessServiceName(mysql)
	mysql.Status.Replicating = replicating
	mysql.Status.RootSecretName = rootSecret
	mysql.Status.ReplicationSecretName = replSecret

	ready := sts.Status.ReadyReplicas >= desired && (desired == 1 || replicating >= desired-1)
	cond := metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		Reason:             "NotReady",
		Message:            "MySQL cluster is not fully ready",
		LastTransitionTime: metav1.Now(),
		ObservedGeneration: mysql.Generation,
	}
	if ready {
		cond.Status = metav1.ConditionTrue
		cond.Reason = "Ready"
		cond.Message = "MySQL primary is ready"
		if desired > 1 {
			cond.Message = fmt.Sprintf("MySQL primary ready with %d/%d replicas replicating", replicating, desired-1)
		}
	}
	setCondition(&mysql.Status.Conditions, cond)

	replCond := metav1.Condition{
		Type:               "ReplicationHealthy",
		Status:             metav1.ConditionTrue,
		Reason:             "Standalone",
		Message:            "Single primary; replication not required",
		LastTransitionTime: metav1.Now(),
		ObservedGeneration: mysql.Generation,
	}
	if desired > 1 {
		if replicating >= desired-1 {
			replCond.Reason = "Healthy"
			replCond.Message = fmt.Sprintf("%d replica(s) replicating from primary", replicating)
			replCond.Status = metav1.ConditionTrue
		} else {
			replCond.Status = metav1.ConditionFalse
			replCond.Reason = "Degraded"
			replCond.Message = fmt.Sprintf("%d/%d replicas replicating", replicating, desired-1)
		}
	}
	setCondition(&mysql.Status.Conditions, replCond)

	return r.Status().Update(ctx, mysql)
}

func (r *MySQLReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Build clientset for pod exec from manager config.
	cfg := mgr.GetConfig()
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	r.Clientset = cs
	r.RESTConfig = cfg

	name := r.Name
	if name == "" {
		name = "mysql"
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		For(&mysqlv1alpha1.MySQL{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.Secret{}).
		Owns(&corev1.ConfigMap{}).
		Complete(r)
}

func labelsFor(mysql *mysqlv1alpha1.MySQL) map[string]string {
	return map[string]string{
		labelAppKey:       "mysql",
		labelInstanceKey:  mysql.Name,
		labelManagedByKey: managedByValue,
		labelComponentKey: componentValue,
	}
}

func serviceName(mysql *mysqlv1alpha1.MySQL) string       { return mysql.Name }
func primaryServiceName(mysql *mysqlv1alpha1.MySQL) string { return mysql.Name + "-primary" }
func readsServiceName(mysql *mysqlv1alpha1.MySQL) string   { return mysql.Name + "-reads" }
func headlessServiceName(mysql *mysqlv1alpha1.MySQL) string {
	return mysql.Name + "-headless"
}
func configMapName(mysql *mysqlv1alpha1.MySQL) string  { return mysql.Name + "-operator" }
func statefulSetName(mysql *mysqlv1alpha1.MySQL) string { return mysql.Name }
func primaryPodName(mysql *mysqlv1alpha1.MySQL) string  { return mysql.Name + "-0" }

func effectivePort(mysql *mysqlv1alpha1.MySQL) int32 {
	if mysql.Spec.Port == 0 {
		return defaultPort
	}
	return mysql.Spec.Port
}

func generatePassword(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	// URL-safe password without quotes/backslashes that break SQL/shell.
	return base64.RawURLEncoding.EncodeToString(b)[:n], nil
}

func escapeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func setCondition(conditions *[]metav1.Condition, newCond metav1.Condition) {
	for i := range *conditions {
		if (*conditions)[i].Type == newCond.Type {
			old := (*conditions)[i]
			if old.Status == newCond.Status && old.Reason == newCond.Reason && old.Message == newCond.Message && old.ObservedGeneration == newCond.ObservedGeneration {
				return
			}
			if old.Status == newCond.Status {
				newCond.LastTransitionTime = old.LastTransitionTime
			}
			(*conditions)[i] = newCond
			return
		}
	}
	*conditions = append(*conditions, newCond)
}

// entrypointScript writes GTID/server-id config then hands off to the official image entrypoint.
const entrypointScript = `#!/bin/bash
set -euo pipefail
ORDINAL="${HOSTNAME##*-}"
if ! [[ "$ORDINAL" =~ ^[0-9]+$ ]]; then
  ORDINAL=0
fi
SERVER_ID=$((1000 + ORDINAL))
mkdir -p /etc/mysql/conf.d
cat > /etc/mysql/conf.d/zz-operator.cnf <<EOF
[mysqld]
server-id=${SERVER_ID}
gtid_mode=ON
enforce_gtid_consistency=ON
log_bin=mysql-bin
binlog_format=ROW
binlog_expire_logs_seconds=604800
relay_log=relay-bin
relay_log_recovery=ON
skip_replica_start=ON
# Allow CLONE / operator connections during bootstrap
mysqlx=0
EOF
# Replicas should not create the optional app database during local init; primary owns schema.
if [[ "$ORDINAL" != "0" ]]; then
  unset MYSQL_DATABASE || true
fi
exec docker-entrypoint.sh mysqld
`

