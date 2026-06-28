//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	mysqlv1alpha1 "github.com/asrk/mysql-operator/api/v1alpha1"
	"github.com/asrk/mysql-operator/internal/controller"
)

const (
	minioNS       = "minio"
	minioAccess   = "minioadmin"
	minioSecret   = "minioadmin"
	backupBucket  = "mysql-backups-e2e"
	minioEndpoint = "http://minio.minio.svc.cluster.local:9000"
)

// TestMySQLBackupS3MinIO proves S3 export against an in-cluster MinIO:
// deploy MinIO → create bucket → HA/single MySQL → MySQLBackup with spec.s3 →
// status.Succeeded + status.s3URI set → object exists (aws s3 ls via Job).
func TestMySQLBackupS3MinIO(t *testing.T) {
	if os.Getenv("E2E_SKIP") == "1" {
		t.Skip("E2E_SKIP=1")
	}

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	cfg, err := loadRESTConfig()
	if err != nil {
		t.Skipf("no kubeconfig: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	sch := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(sch)
	_ = mysqlv1alpha1.AddToScheme(sch)
	_ = appsv1.AddToScheme(sch)
	_ = batchv1.AddToScheme(sch)

	c, err := client.New(cfg, client.Options{Scheme: sch})
	if err != nil {
		t.Fatal(err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ensureCRDInstalled(ctx, c); err != nil {
		t.Fatalf("CRD missing (make install): %v", err)
	}
	// MySQLBackup CRD
	bl := &mysqlv1alpha1.MySQLBackupList{}
	if err := c.List(ctx, bl); err != nil {
		t.Fatalf("MySQLBackup CRD missing: %v", err)
	}

	// --- MinIO ---
	if err := ensureMinIO(ctx, t, cs, cfg); err != nil {
		t.Fatalf("minio: %v", err)
	}
	if err := ensureMinIOBucket(ctx, t, cs, cfg, backupBucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	nsName := fmt.Sprintf("mysql-operator-e2e-s3-%d", time.Now().UnixNano()%1_000_000_000)
	if _, err := cs.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nsName}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		prop := metav1.DeletePropagationForeground
		_ = cs.CoreV1().Namespaces().Delete(cctx, nsName, metav1.DeleteOptions{PropagationPolicy: &prop})
	})

	// AWS-style secret for the operator backup Job (same values as MinIO root)
	credName := "aws-backup-creds"
	_, err = cs.CoreV1().Secrets(nsName).Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: credName, Namespace: nsName},
		StringData: map[string]string{
			"AWS_ACCESS_KEY_ID":     minioAccess,
			"AWS_SECRET_ACCESS_KEY": minioSecret,
		},
	}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 sch,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := (&controller.MySQLReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Name: "mysql-s3-e2e"}).SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}
	if err := (&controller.MySQLBackupReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Name: "mysqlbackup-s3-e2e"}).SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}
	mgrCtx, mgrCancel := context.WithCancel(ctx)
	defer mgrCancel()
	go func() { _ = mgr.Start(mgrCtx) }()
	if !mgr.GetCache().WaitForCacheSync(mgrCtx) {
		t.Fatal("cache sync")
	}

	mysqlName := "s3-mysql"
	var replicas int32 = 1
	mysql := &mysqlv1alpha1.MySQL{
		ObjectMeta: metav1.ObjectMeta{Name: mysqlName, Namespace: nsName},
		Spec: mysqlv1alpha1.MySQLSpec{
			Replicas:    &replicas,
			Image:       envOr("MYSQL_E2E_IMAGE", "mysql:8.0"),
			StorageSize: "1Gi",
			Database:    "app",
		},
	}
	if err := c.Create(ctx, mysql); err != nil {
		t.Fatal(err)
	}

	// Wait MySQL ready
	err = wait.PollUntilContextTimeout(ctx, 3*time.Second, 6*time.Minute, true, func(ctx context.Context) (bool, error) {
		cur := &mysqlv1alpha1.MySQL{}
		if err := c.Get(ctx, types.NamespacedName{Name: mysqlName, Namespace: nsName}, cur); err != nil {
			return false, nil
		}
		t.Logf("[mysql] phase=%s ready=%d", cur.Status.Phase, cur.Status.ReadyReplicas)
		return cur.Status.ReadyReplicas >= 1, nil
	})
	if err != nil {
		t.Fatalf("mysql not ready: %v", err)
	}

	// Seed a row so dump is non-trivial
	passSec := &corev1.Secret{}
	_ = c.Get(ctx, types.NamespacedName{Name: mysqlName + "-root", Namespace: nsName}, passSec)
	password := string(passSec.Data["password"])
	_, _ = execInPod(ctx, cfg, cs, nsName, mysqlName+"-0", "mysql", []string{
		"mysql", "-h127.0.0.1", "-uroot", "-p" + password, "-e",
		"CREATE DATABASE IF NOT EXISTS app; USE app; CREATE TABLE IF NOT EXISTS t(id INT PRIMARY KEY); INSERT INTO t VALUES (42) ON DUPLICATE KEY UPDATE id=id;",
	})

	backupName := "s3-backup-1"
	backup := &mysqlv1alpha1.MySQLBackup{
		ObjectMeta: metav1.ObjectMeta{Name: backupName, Namespace: nsName},
		Spec: mysqlv1alpha1.MySQLBackupSpec{
			MySQLName:   mysqlName,
			StorageSize: "1Gi",
			S3: &mysqlv1alpha1.BackupS3Spec{
				Bucket:         backupBucket,
				Region:         "us-east-1",
				Endpoint:       minioEndpoint,
				ForcePathStyle: true,
				Prefix:         fmt.Sprintf("e2e/%s/%s", nsName, backupName),
				CredentialsSecretRef: mysqlv1alpha1.SecretNameRef{Name: credName},
			},
		},
	}
	if err := c.Create(ctx, backup); err != nil {
		t.Fatal(err)
	}

	var s3URI string
	err = wait.PollUntilContextTimeout(ctx, 3*time.Second, 8*time.Minute, true, func(ctx context.Context) (bool, error) {
		cur := &mysqlv1alpha1.MySQLBackup{}
		if err := c.Get(ctx, types.NamespacedName{Name: backupName, Namespace: nsName}, cur); err != nil {
			return false, nil
		}
		t.Logf("[backup] phase=%s s3=%s msg=%s", cur.Status.Phase, cur.Status.S3URI, cur.Status.Message)
		if cur.Status.Phase == "Failed" {
			return false, fmt.Errorf("backup failed: %s", cur.Status.Message)
		}
		if cur.Status.Phase == "Succeeded" && cur.Status.S3URI != "" {
			s3URI = cur.Status.S3URI
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		// dump job logs
		pods, _ := cs.CoreV1().Pods(nsName).List(ctx, metav1.ListOptions{LabelSelector: "app.kubernetes.io/name=mysql-backup"})
		for _, p := range pods.Items {
			logs, _ := cs.CoreV1().Pods(nsName).GetLogs(p.Name, &corev1.PodLogOptions{Container: "s3-upload", TailLines: int64Ptr(50)}).Do(ctx).Raw()
			t.Logf("pod %s s3-upload logs:\n%s", p.Name, string(logs))
			logs2, _ := cs.CoreV1().Pods(nsName).GetLogs(p.Name, &corev1.PodLogOptions{Container: "mysqldump", TailLines: int64Ptr(30)}).Do(ctx).Raw()
			t.Logf("pod %s mysqldump logs:\n%s", p.Name, string(logs2))
		}
		t.Fatalf("backup did not succeed with s3URI: %v", err)
	}

	if !strings.HasPrefix(s3URI, "s3://"+backupBucket+"/") {
		t.Fatalf("unexpected s3URI %q", s3URI)
	}
	if !strings.HasSuffix(s3URI, "dump.sql.gz") {
		t.Fatalf("s3URI should end with dump.sql.gz: %q", s3URI)
	}

	// Prove object exists in MinIO via a one-shot aws s3 ls Job
	if err := assertS3ObjectExists(ctx, t, cs, cfg, nsName, credName, s3URI); err != nil {
		t.Fatalf("object not in MinIO: %v", err)
	}

	// PVC also retained by default
	cur := &mysqlv1alpha1.MySQLBackup{}
	_ = c.Get(ctx, types.NamespacedName{Name: backupName, Namespace: nsName}, cur)
	if cur.Status.PVCName == "" {
		t.Fatal("expected PVCName when skipPVC is false")
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, types.NamespacedName{Name: cur.Status.PVCName, Namespace: nsName}, pvc); err != nil {
		t.Fatalf("backup PVC: %v", err)
	}

	t.Logf("S3 BACKUP E2E OK: uri=%s pvc=%s", s3URI, cur.Status.PVCName)
}

func ensureMinIO(ctx context.Context, t *testing.T, cs kubernetes.Interface, _ interface{}) error {
	t.Helper()
	// Apply manifests by creating resources directly (no kubectl dependency in-process).
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: minioNS}}
	if _, err := cs.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "minio-creds", Namespace: minioNS},
		StringData: map[string]string{
			"MINIO_ROOT_USER":     minioAccess,
			"MINIO_ROOT_PASSWORD": minioSecret,
		},
	}
	if _, err := cs.CoreV1().Secrets(minioNS).Create(ctx, sec, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "minio", Namespace: minioNS},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "minio"},
			Ports:    []corev1.ServicePort{{Name: "api", Port: 9000, TargetPort: intstrFromInt(9000)}},
		},
	}
	if _, err := cs.CoreV1().Services(minioNS).Create(ctx, svc, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	var replicas int32 = 1
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "minio", Namespace: minioNS},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "minio"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "minio"}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "minio",
						Image: "quay.io/minio/minio:RELEASE.2024-01-16T16-07-38Z",
						Args:  []string{"server", "/data", "--console-address", ":9001"},
						Env: []corev1.EnvVar{
							{Name: "MINIO_ROOT_USER", Value: minioAccess},
							{Name: "MINIO_ROOT_PASSWORD", Value: minioSecret},
						},
						Ports: []corev1.ContainerPort{{ContainerPort: 9000}},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{Path: "/minio/health/ready", Port: intstrFromInt(9000)},
							},
							InitialDelaySeconds: 5,
							PeriodSeconds:       5,
						},
					}},
				},
			},
		},
	}
	if _, err := cs.AppsV1().Deployments(minioNS).Create(ctx, dep, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return wait.PollUntilContextTimeout(ctx, 2*time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
		d, err := cs.AppsV1().Deployments(minioNS).Get(ctx, "minio", metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		return d.Status.ReadyReplicas >= 1, nil
	})
}

func ensureMinIOBucket(ctx context.Context, t *testing.T, cs kubernetes.Interface, _ interface{}, bucket string) error {
	t.Helper()
	// Job using aws-cli against MinIO path-style
	name := "minio-mb-" + fmt.Sprintf("%d", time.Now().Unix()%100000)
	script := fmt.Sprintf(`set -e
export AWS_ACCESS_KEY_ID=%s AWS_SECRET_ACCESS_KEY=%s AWS_DEFAULT_REGION=us-east-1
aws configure set default.s3.addressing_style path
aws --endpoint-url %s s3 mb s3://%s 2>/dev/null || true
aws --endpoint-url %s s3 ls s3://%s
`, minioAccess, minioSecret, minioEndpoint, bucket, minioEndpoint, bucket)
	backoff := int32(1)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: minioNS},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:    "aws",
						Image:   "amazon/aws-cli:2.15.0",
						Command: []string{"/bin/sh", "-c", script},
					}},
				},
			},
		},
	}
	if _, err := cs.BatchV1().Jobs(minioNS).Create(ctx, job, metav1.CreateOptions{}); err != nil {
		return err
	}
	return wait.PollUntilContextTimeout(ctx, 2*time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		j, err := cs.BatchV1().Jobs(minioNS).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		if j.Status.Failed > 0 {
			return false, fmt.Errorf("bucket job failed")
		}
		return j.Status.Succeeded > 0, nil
	})
}

func assertS3ObjectExists(ctx context.Context, t *testing.T, cs kubernetes.Interface, _ interface{}, ns, credName, s3URI string) error {
	t.Helper()
	name := "s3-head-" + fmt.Sprintf("%d", time.Now().Unix()%100000)
	script := fmt.Sprintf(`set -e
export AWS_ACCESS_KEY_ID=$(cat /creds/AWS_ACCESS_KEY_ID) AWS_SECRET_ACCESS_KEY=$(cat /creds/AWS_SECRET_ACCESS_KEY) AWS_DEFAULT_REGION=us-east-1
aws configure set default.s3.addressing_style path
aws --endpoint-url %s s3 ls %q
# also show size
aws --endpoint-url %s s3 cp %q - | wc -c | awk '{if($1<100) exit 1}'
`, minioEndpoint, s3URI, minioEndpoint, s3URI)
	// mount secret as files — use env from secret instead
	script = fmt.Sprintf(`set -e
export AWS_ACCESS_KEY_ID=%s AWS_SECRET_ACCESS_KEY=%s AWS_DEFAULT_REGION=us-east-1
aws configure set default.s3.addressing_style path
aws --endpoint-url %s s3 ls %q
BYTES=$(aws --endpoint-url %s s3 cp %q - | wc -c)
echo bytes=$BYTES
test "$BYTES" -gt 100
`, minioAccess, minioSecret, minioEndpoint, s3URI, minioEndpoint, s3URI)
	backoff := int32(1)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name: "aws", Image: "amazon/aws-cli:2.15.0",
						Command: []string{"/bin/sh", "-c", script},
					}},
				},
			},
		},
	}
	if _, err := cs.BatchV1().Jobs(ns).Create(ctx, job, metav1.CreateOptions{}); err != nil {
		return err
	}
	return wait.PollUntilContextTimeout(ctx, 2*time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
		j, err := cs.BatchV1().Jobs(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		if j.Status.Failed > 0 {
			return false, fmt.Errorf("s3 ls job failed")
		}
		return j.Status.Succeeded > 0, nil
	})
}

func intstrFromInt(i int) intstr.IntOrString {
	return intstr.FromInt(i)
}
