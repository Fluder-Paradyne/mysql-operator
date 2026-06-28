//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/util/homedir"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	mysqlv1alpha1 "github.com/asrk/mysql-operator/api/v1alpha1"
	"github.com/asrk/mysql-operator/internal/controller"
)

const (
	testNamespace  = "mysql-operator-e2e"
	mysqlName      = "e2e-mysql"
	haMySQLName    = "e2e-ha-mysql"
	databaseName   = "e2e_app"
	overallTimeout = 10 * time.Minute
	pollInterval   = 3 * time.Second
)

func TestMySQLRunningProperly(t *testing.T) {
	runE2E(t, mysqlName, 1, false)
}

func TestMySQLHAReplication(t *testing.T) {
	runE2E(t, haMySQLName, 3, true)
}

func runE2E(t *testing.T, name string, replicas int32, checkReplication bool) {
	t.Helper()
	if os.Getenv("E2E_SKIP") == "1" {
		t.Skip("E2E_SKIP=1")
	}

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	cfg, err := loadRESTConfig()
	if err != nil {
		t.Skipf("no usable kubeconfig for e2e (set KUBECONFIG or run make test-e2e-kind): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), overallTimeout)
	defer cancel()

	sch := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(sch); err != nil {
		t.Fatal(err)
	}
	if err := mysqlv1alpha1.AddToScheme(sch); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(sch); err != nil {
		t.Fatal(err)
	}

	c, err := client.New(cfg, client.Options{Scheme: sch})
	if err != nil {
		t.Fatal(err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if err := ensureCRDInstalled(ctx, c); err != nil {
		t.Fatalf("CRD mysqls.mysql.asrk.dev not installed; run `make install` first: %v", err)
	}

	// Unique namespace per run avoids races with Terminating namespaces from prior tests.
	nsName := fmt.Sprintf("%s-%s-%d", testNamespace, strings.ReplaceAll(name, "_", "-"), time.Now().UnixNano()%1_000_000_000)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nsName}}
	if _, err := cs.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: sch,
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
		HealthProbeBindAddress: "0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := (&controller.MySQLReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Name:   "mysql-" + name,
	}).SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}

	mgrCtx, mgrCancel := context.WithCancel(ctx)
	defer mgrCancel()
	go func() {
		if err := mgr.Start(mgrCtx); err != nil {
			t.Logf("manager stopped: %v", err)
		}
	}()
	if !mgr.GetCache().WaitForCacheSync(mgrCtx) {
		t.Fatal("cache sync failed")
	}

	mysql := &mysqlv1alpha1.MySQL{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: nsName,
		},
		Spec: mysqlv1alpha1.MySQLSpec{
			Replicas:    &replicas,
			Image:       envOr("MYSQL_E2E_IMAGE", "mysql:8.0"),
			StorageSize: "1Gi",
			Database:    databaseName,
		},
	}
	if err := c.Create(ctx, mysql); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		_ = c.Delete(cleanupCtx, &mysqlv1alpha1.MySQL{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: nsName}})
		propagation := metav1.DeletePropagationForeground
		_ = cs.CoreV1().Namespaces().Delete(cleanupCtx, nsName, metav1.DeleteOptions{PropagationPolicy: &propagation})
	})

	var password string
	err = wait.PollUntilContextTimeout(ctx, pollInterval, overallTimeout-30*time.Second, true, func(ctx context.Context) (bool, error) {
		cur := &mysqlv1alpha1.MySQL{}
		if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: nsName}, cur); err != nil {
			return false, nil
		}
		t.Logf("[%s] phase=%s ready=%d/%d replicating=%d mode=%s",
			name, cur.Status.Phase, cur.Status.ReadyReplicas, cur.Status.DesiredReplicas, cur.Status.Replicating, cur.Status.Mode)
		if cur.Status.ReadyReplicas < replicas {
			return false, nil
		}
		if checkReplication && cur.Status.Replicating < replicas-1 {
			return false, nil
		}
		if cur.Status.RootSecretName == "" {
			return false, nil
		}
		sec := &corev1.Secret{}
		if err := c.Get(ctx, types.NamespacedName{Name: cur.Status.RootSecretName, Namespace: nsName}, sec); err != nil {
			return false, nil
		}
		password = string(sec.Data["password"])
		return password != "", nil
	})
	if err != nil {
		dumpDebug(ctx, t, cs, c, nsName, name)
		t.Fatalf("timed out waiting for MySQL cluster: %v", err)
	}

	podName := name + "-0"
	out, err := execInPod(ctx, cfg, cs, nsName, podName, "mysql", []string{
		"mysqladmin", "ping", "-h", "127.0.0.1", "-uroot", "-p" + password,
	})
	if err != nil {
		t.Fatalf("mysqladmin ping failed: %v\noutput: %s", err, out)
	}
	if !strings.Contains(strings.ToLower(out), "alive") {
		t.Fatalf("expected mysqladmin ping to report alive, got: %q", out)
	}

	marker := fmt.Sprintf("ha-probe-%d", time.Now().UnixNano())
	writeSQL := fmt.Sprintf(
		"CREATE DATABASE IF NOT EXISTS %s; USE %s; CREATE TABLE IF NOT EXISTS e2e_probe (id INT PRIMARY KEY, msg VARCHAR(64)); "+
			"INSERT INTO e2e_probe (id, msg) VALUES (1, '%s') ON DUPLICATE KEY UPDATE msg=VALUES(msg); SELECT msg FROM e2e_probe WHERE id=1;",
		databaseName, databaseName, marker,
	)
	rowOut, err := execInPod(ctx, cfg, cs, nsName, podName, "mysql", []string{
		"mysql", "-h", "127.0.0.1", "-uroot", "-p" + password, "-N", "-e", writeSQL,
	})
	if err != nil {
		t.Fatalf("write on primary failed: %v\noutput: %s", err, rowOut)
	}
	if !strings.Contains(rowOut, marker) {
		t.Fatalf("expected %q on primary, got: %q", marker, rowOut)
	}

	if checkReplication {
		// Wait for replica to apply the row
		replicaPod := name + "-1"
		err = wait.PollUntilContextTimeout(ctx, pollInterval, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
			out, err := execInPod(ctx, cfg, cs, nsName, replicaPod, "mysql", []string{
				"mysql", "-h", "127.0.0.1", "-uroot", "-p" + password, "-N", "-e",
				fmt.Sprintf("SELECT msg FROM %s.e2e_probe WHERE id=1;", databaseName),
			})
			if err != nil {
				t.Logf("replica query: %v (%s)", err, out)
				return false, nil
			}
			return strings.Contains(out, marker), nil
		})
		if err != nil {
			dumpDebug(ctx, t, cs, c, nsName, name)
			t.Fatalf("replica did not receive replicated row: %v", err)
		}

		// Confirm replication threads
		status, err := execInPod(ctx, cfg, cs, nsName, replicaPod, "mysql", []string{
			"mysql", "-h", "127.0.0.1", "-uroot", "-p" + password, "-N", "-e",
			`SELECT SERVICE_STATE FROM performance_schema.replication_connection_status LIMIT 1;`,
		})
		if err != nil || !strings.Contains(status, "ON") {
			t.Fatalf("replica IO thread not ON: err=%v out=%q", err, status)
		}
		t.Logf("HA ok: primary write %q visible on replica; replication IO=ON", marker)
	}

	t.Logf("MySQL %s OK (replicas=%d, replication=%v)", name, replicas, checkReplication)
}

func loadRESTConfig() (*rest.Config, error) {
	if kubeconfig := os.Getenv("KUBECONFIG"); kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	if home := homedir.HomeDir(); home != "" {
		path := filepath.Join(home, ".kube", "config")
		if _, err := os.Stat(path); err == nil {
			return clientcmd.BuildConfigFromFlags("", path)
		}
	}
	return rest.InClusterConfig()
}

func ensureCRDInstalled(ctx context.Context, c client.Client) error {
	list := &mysqlv1alpha1.MySQLList{}
	return c.List(ctx, list)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func execInPod(ctx context.Context, cfg *rest.Config, cs kubernetes.Interface, namespace, pod, container string, command []string) (string, error) {
	req := cs.CoreV1().RESTClient().Post().
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

	exec, err := remotecommand.NewSPDYExecutor(cfg, "POST", req.URL())
	if err != nil {
		return "", err
	}
	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	return stdout.String() + stderr.String(), err
}

func dumpDebug(ctx context.Context, t *testing.T, cs kubernetes.Interface, c client.Client, ns, name string) {
	t.Helper()
	pods, err := cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Logf("list pods: %v", err)
		return
	}
	for _, p := range pods.Items {
		t.Logf("pod %s phase=%s labels=%v", p.Name, p.Status.Phase, p.Labels)
		logs, err := cs.CoreV1().Pods(ns).GetLogs(p.Name, &corev1.PodLogOptions{Container: "mysql", TailLines: int64Ptr(40)}).Do(ctx).Raw()
		if err != nil {
			t.Logf("logs %s: %v", p.Name, err)
			continue
		}
		t.Logf("logs %s:\n%s", p.Name, string(logs))
	}
	cur := &mysqlv1alpha1.MySQL{}
	if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, cur); err == nil {
		t.Logf("CR status: %+v", cur.Status)
	}
}

func int64Ptr(v int64) *int64 { return &v }
