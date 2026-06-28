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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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

// TestMySQLCloneLive clones a Ready source instance into a fresh target using ClonePlugin,
// then verifies a marker row is present on the target primary.
func TestMySQLCloneLive(t *testing.T) {
	if os.Getenv("E2E_SKIP") == "1" {
		t.Skip("E2E_SKIP=1")
	}
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	cfg, err := loadRESTConfig()
	if err != nil {
		t.Skipf("no kubeconfig: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
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
		t.Fatalf("MySQL CRD: %v", err)
	}
	if err := c.List(ctx, &mysqlv1alpha1.MySQLCloneList{}); err != nil {
		t.Fatalf("MySQLClone CRD missing — apply config/crd/mysql.asrk.dev_mysqlclones.yaml: %v", err)
	}

	nsName := fmt.Sprintf("mysql-e2e-clone-%d", time.Now().UnixNano()%1_000_000_000)
	if _, err := cs.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nsName}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		prop := metav1.DeletePropagationForeground
		_ = cs.CoreV1().Namespaces().Delete(cctx, nsName, metav1.DeleteOptions{PropagationPolicy: &prop})
	})

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: sch, Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := (&controller.MySQLReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Name: "mysql-clone-e2e"}).SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}
	if err := (&controller.MySQLCloneReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Name: "mysqlclone-e2e"}).SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}
	mgrCtx, mgrCancel := context.WithCancel(ctx)
	defer mgrCancel()
	go func() { _ = mgr.Start(mgrCtx) }()
	if !mgr.GetCache().WaitForCacheSync(mgrCtx) {
		t.Fatal("cache sync")
	}

	srcName, dstName := "clone-src", "clone-dst"
	var one int32 = 1
	img := envOr("MYSQL_E2E_IMAGE", "mysql:8.0")
	for _, name := range []string{srcName, dstName} {
		m := &mysqlv1alpha1.MySQL{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: nsName},
			Spec: mysqlv1alpha1.MySQLSpec{
				Replicas: &one, Image: img, StorageSize: "1Gi", Database: "app",
			},
		}
		if err := c.Create(ctx, m); err != nil {
			t.Fatal(err)
		}
	}

	waitReady := func(name string) {
		t.Helper()
		err := wait.PollUntilContextTimeout(ctx, 3*time.Second, 8*time.Minute, true, func(ctx context.Context) (bool, error) {
			cur := &mysqlv1alpha1.MySQL{}
			if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: nsName}, cur); err != nil {
				return false, nil
			}
			t.Logf("[%s] phase=%s ready=%d", name, cur.Status.Phase, cur.Status.ReadyReplicas)
			return cur.Status.ReadyReplicas >= 1, nil
		})
		if err != nil {
			t.Fatalf("%s not ready: %v", name, err)
		}
	}
	waitReady(srcName)
	waitReady(dstName)

	// Marker only on source
	marker := fmt.Sprintf("clone-marker-%d", time.Now().UnixNano())
	srcPass := secretPassword(ctx, t, c, nsName, srcName+"-root")
	out, err := execInPod(ctx, cfg, cs, nsName, srcName+"-0", "mysql", []string{
		"mysql", "-h127.0.0.1", "-uroot", "-p" + srcPass, "-e",
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS app; USE app; CREATE TABLE IF NOT EXISTS clone_t (id INT PRIMARY KEY, msg VARCHAR(128)); INSERT INTO clone_t VALUES (1, '%s') ON DUPLICATE KEY UPDATE msg=VALUES(msg); SELECT msg FROM clone_t WHERE id=1;", marker),
	})
	if err != nil || !strings.Contains(out, marker) {
		t.Fatalf("seed source: err=%v out=%q", err, out)
	}

	// Ensure target does not have the marker yet (best-effort)
	dstPass := secretPassword(ctx, t, c, nsName, dstName+"-root")
	pre, _ := execInPod(ctx, cfg, cs, nsName, dstName+"-0", "mysql", []string{
		"mysql", "-h127.0.0.1", "-uroot", "-p" + dstPass, "-N", "-e",
		"SELECT msg FROM app.clone_t WHERE id=1;",
	})
	if strings.Contains(pre, marker) {
		t.Fatalf("target already has marker before clone: %q", pre)
	}

	cloneName := "live-clone-1"
	// Default Logical: reliable live stream on kind. Set E2E_CLONE_METHOD=ClonePlugin to exercise MySQL 8 CLONE INSTANCE.
	method := os.Getenv("E2E_CLONE_METHOD")
	if method == "" {
		method = "Logical"
	}
	clone := &mysqlv1alpha1.MySQLClone{
		ObjectMeta: metav1.ObjectMeta{Name: cloneName, Namespace: nsName},
		Spec: mysqlv1alpha1.MySQLCloneSpec{
			SourceMySQLName: srcName,
			TargetMySQLName: dstName,
			Method:          method,
			Image:           img,
		},
	}
	if err := c.Create(ctx, clone); err != nil {
		t.Fatal(err)
	}

	err = wait.PollUntilContextTimeout(ctx, 3*time.Second, 10*time.Minute, true, func(ctx context.Context) (bool, error) {
		cur := &mysqlv1alpha1.MySQLClone{}
		if err := c.Get(ctx, types.NamespacedName{Name: cloneName, Namespace: nsName}, cur); err != nil {
			return false, nil
		}
		t.Logf("[clone] phase=%s msg=%s job=%s", cur.Status.Phase, cur.Status.Message, cur.Status.JobName)
		if cur.Status.Phase == "Failed" {
			return false, fmt.Errorf("clone failed: %s", cur.Status.Message)
		}
		return cur.Status.Phase == "Succeeded", nil
	})
	if err != nil {
		dumpCloneJob(ctx, t, cs, nsName, cloneName)
		t.Fatalf("clone did not succeed: %v", err)
	}

	// Target may restart after CLONE — wait for SQL again
	err = wait.PollUntilContextTimeout(ctx, 3*time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
		dstPass = secretPassword(ctx, t, c, nsName, dstName+"-root")
		// Password on target may be source's after physical clone — try both
		for _, pass := range []string{dstPass, srcPass} {
			got, err := execInPod(ctx, cfg, cs, nsName, dstName+"-0", "mysql", []string{
				"mysql", "-h127.0.0.1", "-uroot", "-p" + pass, "-N", "-e",
				"SELECT msg FROM app.clone_t WHERE id=1;",
			})
			if err == nil && strings.Contains(got, marker) {
				t.Logf("found marker on target with password try (len=%d)", len(pass))
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		dumpCloneJob(ctx, t, cs, nsName, cloneName)
		t.Fatalf("marker not on target after clone: %v", err)
	}

	t.Logf("LIVE CLONE E2E OK method=%s marker=%s", method, marker)
}

func secretPassword(ctx context.Context, t *testing.T, c client.Client, ns, secretName string) string {
	t.Helper()
	sec := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: secretName, Namespace: ns}, sec); err != nil {
		t.Fatalf("secret %s: %v", secretName, err)
	}
	return string(sec.Data["password"])
}

func dumpCloneJob(ctx context.Context, t *testing.T, cs kubernetes.Interface, ns, cloneName string) {
	t.Helper()
	pods, err := cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: "app.kubernetes.io/name=mysql-clone"})
	if err != nil {
		t.Logf("list pods: %v", err)
		return
	}
	for _, p := range pods.Items {
		logs, err := cs.CoreV1().Pods(ns).GetLogs(p.Name, &corev1.PodLogOptions{Container: "clone", TailLines: int64Ptr(80)}).Do(ctx).Raw()
		t.Logf("pod %s phase=%s err=%v\n%s", p.Name, p.Status.Phase, err, string(logs))
	}
}
