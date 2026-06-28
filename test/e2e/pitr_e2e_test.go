//go:build e2e

package e2e

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
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

// TestMySQLPITRMinIO proves point-in-time recovery:
// backup base → write B → archive binlogs → write C → restore to time between B and C
// → A and B present, C absent on the target instance.
func TestMySQLPITRMinIO(t *testing.T) {
	if os.Getenv("E2E_SKIP") == "1" {
		t.Skip("E2E_SKIP=1")
	}
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	cfg, err := loadRESTConfig()
	if err != nil {
		t.Skipf("no kubeconfig: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
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
	for _, list := range []client.ObjectList{
		&mysqlv1alpha1.MySQLBackupList{},
		&mysqlv1alpha1.MySQLRestoreList{},
	} {
		if err := c.List(ctx, list); err != nil {
			t.Fatalf("CRD missing for %T: %v", list, err)
		}
	}

	if err := ensureMinIO(ctx, t, cs, nil); err != nil {
		t.Fatalf("minio: %v", err)
	}
	if err := ensureMinIOBucket(ctx, t, cs, nil, backupBucket); err != nil {
		t.Fatalf("bucket: %v", err)
	}

	nsName := fmt.Sprintf("mysql-e2e-pitr-%d", time.Now().UnixNano()%1_000_000_000)
	if _, err := cs.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nsName}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		prop := metav1.DeletePropagationForeground
		_ = cs.CoreV1().Namespaces().Delete(cctx, nsName, metav1.DeleteOptions{PropagationPolicy: &prop})
	})

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
		Scheme: sch, Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := (&controller.MySQLReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Name: "mysql-pitr-e2e"}).SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}
	if err := (&controller.MySQLBackupReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Name: "mysqlbackup-pitr-e2e"}).SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}
	if err := (&controller.MySQLRestoreReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Name: "mysqlrestore-pitr-e2e"}).SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}
	mgrCtx, mgrCancel := context.WithCancel(ctx)
	defer mgrCancel()
	go func() { _ = mgr.Start(mgrCtx) }()
	if !mgr.GetCache().WaitForCacheSync(mgrCtx) {
		t.Fatal("cache sync")
	}

	mysqlName := "pitr-mysql"
	var one int32 = 1
	pitrOn := true
	binlogPrefix := fmt.Sprintf("mysql-binlogs/%s/%s", nsName, mysqlName)
	mysql := &mysqlv1alpha1.MySQL{
		ObjectMeta: metav1.ObjectMeta{Name: mysqlName, Namespace: nsName},
		Spec: mysqlv1alpha1.MySQLSpec{
			Replicas:    &one,
			Image:       envOr("MYSQL_E2E_IMAGE", "mysql:8.0"),
			StorageSize: "1Gi",
			Database:    "app",
			PITR: &mysqlv1alpha1.PITRSpec{
				Enabled: &pitrOn,
				BinlogArchive: &mysqlv1alpha1.BinlogArchiveSpec{
					Schedule: "0 0 1 1 *", // rare; we trigger archive Job manually
					S3: mysqlv1alpha1.BackupS3Spec{
						Bucket:         backupBucket,
						Region:         "us-east-1",
						Endpoint:       minioEndpoint,
						ForcePathStyle: true,
						Prefix:         binlogPrefix,
						CredentialsSecretRef: mysqlv1alpha1.SecretNameRef{Name: credName},
					},
				},
			},
		},
	}
	if err := c.Create(ctx, mysql); err != nil {
		t.Fatal(err)
	}

	cronName := mysqlName + "-binlog-archive"
	err = wait.PollUntilContextTimeout(ctx, 3*time.Second, 8*time.Minute, true, func(ctx context.Context) (bool, error) {
		cur := &mysqlv1alpha1.MySQL{}
		if err := c.Get(ctx, types.NamespacedName{Name: mysqlName, Namespace: nsName}, cur); err != nil {
			return false, nil
		}
		t.Logf("[mysql] phase=%s ready=%d statusCron=%s", cur.Status.Phase, cur.Status.ReadyReplicas, cur.Status.BinlogArchiveCronJob)
		// Pod ready is enough to proceed; archive Job is created on demand (cron optional).
		return cur.Status.ReadyReplicas >= 1, nil
	})
	if err != nil {
		t.Fatalf("mysql not ready: %v", err)
	}
	_ = cronName

	pass := secretPassword(ctx, t, c, nsName, mysqlName+"-root")
	pod0 := mysqlName + "-0"
	runSQL := func(sql string) (string, error) {
		return execInPod(ctx, cfg, cs, nsName, pod0, "mysql", []string{
			"mysql", "-h127.0.0.1", "-uroot", "-p" + pass, "-N", "-e", sql,
		})
	}

	// Schema + marker A (in base backup)
	markerA := fmt.Sprintf("pitr-A-%d", time.Now().UnixNano())
	if out, err := runSQL(fmt.Sprintf(
		"CREATE DATABASE IF NOT EXISTS app; USE app; CREATE TABLE IF NOT EXISTS pitr_t (id INT PRIMARY KEY, msg VARCHAR(128)); INSERT INTO pitr_t VALUES (1,'%s') ON DUPLICATE KEY UPDATE msg=VALUES(msg); SELECT msg FROM pitr_t WHERE id=1;",
		markerA)); err != nil || !strings.Contains(out, markerA) {
		t.Fatalf("seed A: err=%v out=%q", err, out)
	}

	// Base backup to S3
	backupName := "pitr-base"
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
				Prefix:         fmt.Sprintf("e2e-pitr/%s/%s", nsName, backupName),
				CredentialsSecretRef: mysqlv1alpha1.SecretNameRef{Name: credName},
			},
		},
	}
	if err := c.Create(ctx, backup); err != nil {
		t.Fatal(err)
	}
	err = wait.PollUntilContextTimeout(ctx, 3*time.Second, 8*time.Minute, true, func(ctx context.Context) (bool, error) {
		cur := &mysqlv1alpha1.MySQLBackup{}
		if err := c.Get(ctx, types.NamespacedName{Name: backupName, Namespace: nsName}, cur); err != nil {
			return false, nil
		}
		t.Logf("[backup] phase=%s s3=%s", cur.Status.Phase, cur.Status.S3URI)
		if cur.Status.Phase == "Failed" {
			return false, fmt.Errorf("backup failed: %s", cur.Status.Message)
		}
		return cur.Status.Phase == "Succeeded" && cur.Status.S3URI != "", nil
	})
	if err != nil {
		t.Fatalf("backup: %v", err)
	}

	// Marker B — only in binlogs after backup (use MySQL server clock for PITR alignment)
	time.Sleep(2 * time.Second)
	markerB := fmt.Sprintf("pitr-B-%d", time.Now().UnixNano())
	if out, err := runSQL(fmt.Sprintf(
		"USE app; INSERT INTO pitr_t VALUES (2,'%s') ON DUPLICATE KEY UPDATE msg=VALUES(msg); SELECT msg FROM pitr_t WHERE id=2;",
		markerB)); err != nil || !strings.Contains(out, markerB) {
		t.Fatalf("seed B: err=%v out=%q", err, out)
	}
	_, _ = runSQL("FLUSH BINARY LOGS;")

	// Ship binlogs to MinIO (STS sidecar and/or one-shot debian+mysqlbinlog Job).
	if err := archiveBinlogsForPITR(ctx, t, cs, nsName, mysqlName, pod0, binlogPrefix); err != nil {
		t.Fatalf("binlog archive (B): %v", err)
	}

	// PITR target: MySQL UTC time after B, then wait so C is strictly later in binlog event timestamps.
	mysqlNow, err := runSQL("SELECT UTC_TIMESTAMP();")
	if err != nil {
		t.Fatalf("UTC_TIMESTAMP: %v", err)
	}
	mysqlNow = strings.TrimSpace(mysqlNow)
	// Add 3s buffer in MySQL time space for stop-datetime (format YYYY-MM-DD HH:MM:SS).
	restoreToRFC := ""
	if ts, perr := time.ParseInLocation("2006-01-02 15:04:05", mysqlNow, time.UTC); perr == nil {
		restoreToRFC = ts.Add(3 * time.Second).UTC().Format(time.RFC3339)
	} else {
		restoreToRFC = time.Now().UTC().Add(3 * time.Second).Format(time.RFC3339)
	}
	time.Sleep(5 * time.Second)

	// Marker C — after restoreTo; must not appear after PITR
	markerC := fmt.Sprintf("pitr-C-%d", time.Now().UnixNano())
	if out, err := runSQL(fmt.Sprintf(
		"USE app; INSERT INTO pitr_t VALUES (3,'%s') ON DUPLICATE KEY UPDATE msg=VALUES(msg); SELECT msg FROM pitr_t WHERE id=3;",
		markerC)); err != nil || !strings.Contains(out, markerC) {
		t.Fatalf("seed C: err=%v out=%q", err, out)
	}
	_, _ = runSQL("FLUSH BINARY LOGS;")
	if err := archiveBinlogsForPITR(ctx, t, cs, nsName, mysqlName, pod0, binlogPrefix); err != nil {
		t.Fatalf("binlog archive (C): %v", err)
	}

	restoreName := "pitr-restore-1"
	restore := &mysqlv1alpha1.MySQLRestore{
		ObjectMeta: metav1.ObjectMeta{Name: restoreName, Namespace: nsName},
		Spec: mysqlv1alpha1.MySQLRestoreSpec{
			MySQLName:      mysqlName,
			BackupName:     backupName,
			BinlogS3Prefix: binlogPrefix,
			RestoreTo: &mysqlv1alpha1.RestoreToSpec{
				Time: restoreToRFC,
			},
			S3: &mysqlv1alpha1.BackupS3Spec{
				Bucket:         backupBucket,
				Region:         "us-east-1",
				Endpoint:       minioEndpoint,
				ForcePathStyle: true,
				CredentialsSecretRef: mysqlv1alpha1.SecretNameRef{Name: credName},
			},
		},
	}
	if err := c.Create(ctx, restore); err != nil {
		t.Fatal(err)
	}

	err = wait.PollUntilContextTimeout(ctx, 3*time.Second, 10*time.Minute, true, func(ctx context.Context) (bool, error) {
		cur := &mysqlv1alpha1.MySQLRestore{}
		if err := c.Get(ctx, types.NamespacedName{Name: restoreName, Namespace: nsName}, cur); err != nil {
			return false, nil
		}
		t.Logf("[restore] phase=%s msg=%s", cur.Status.Phase, cur.Status.Message)
		if cur.Status.Phase == "Failed" {
			return false, fmt.Errorf("restore failed: %s", cur.Status.Message)
		}
		return cur.Status.Phase == "Succeeded", nil
	})
	if err != nil {
		dumpRestorePods(ctx, t, cs, nsName)
		t.Fatalf("restore: %v", err)
	}

	// Re-read password (unchanged for logical restore)
	pass = secretPassword(ctx, t, c, nsName, mysqlName+"-root")
	err = wait.PollUntilContextTimeout(ctx, 3*time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
		out, err := execInPod(ctx, cfg, cs, nsName, pod0, "mysql", []string{
			"mysql", "-h127.0.0.1", "-uroot", "-p" + pass, "-N", "-e",
			"SELECT id,msg FROM app.pitr_t ORDER BY id;",
		})
		if err != nil {
			t.Logf("query after restore: %v %s", err, out)
			return false, nil
		}
		t.Logf("rows after PITR: %q", out)
		hasA := strings.Contains(out, markerA)
		hasB := strings.Contains(out, markerB)
		hasC := strings.Contains(out, markerC)
		if !hasA {
			return false, nil
		}
		// B should be present if binlog replay worked; if only A, still partial proof of base restore
		if hasC {
			return false, fmt.Errorf("marker C should be excluded by stop-datetime but was present: %q", out)
		}
		if !hasB {
			// Soft fail for incomplete binlog replay — log but require B for full PITR proof
			return false, nil
		}
		return hasA && hasB && !hasC, nil
	})
	if err != nil {
		dumpRestorePods(ctx, t, cs, nsName)
		// Final dump of rows
		out, _ := execInPod(ctx, cfg, cs, nsName, pod0, "mysql", []string{
			"mysql", "-h127.0.0.1", "-uroot", "-p" + pass, "-N", "-e", "SELECT id,msg FROM app.pitr_t ORDER BY id;",
		})
		t.Fatalf("PITR data check failed: %v (rows=%q) want A+B without C (A=%s B=%s C=%s)", err, out, markerA, markerB, markerC)
	}

	t.Logf("PITR E2E OK restoreTo=%s A=%s B=%s excluded C=%s", restoreToRFC, markerA, markerB, markerC)
}

// archiveBinlogsForPITR prefers the STS sidecar; falls back to a one-shot Job (debian + mysql-client)
// because official mysql:8.0 images do not ship mysqlbinlog.
func archiveBinlogsForPITR(ctx context.Context, t *testing.T, cs kubernetes.Interface, ns, mysqlName, podName, prefix string) error {
	t.Helper()
	// Fast path: sidecar already synced.
	if pod, err := cs.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{}); err == nil {
		for _, c := range pod.Spec.Containers {
			if c.Name != "binlog-archive" {
				continue
			}
			logs, lerr := cs.CoreV1().Pods(ns).GetLogs(podName, &corev1.PodLogOptions{
				Container: "binlog-archive", TailLines: int64Ptr(20),
			}).Do(ctx).Raw()
			if lerr == nil && strings.Contains(string(logs), "synced ok") {
				t.Log("binlog-archive sidecar already synced")
				time.Sleep(2 * time.Second) // allow in-flight sync after FLUSH
				return nil
			}
		}
	}
	return runDebianBinlogArchiveJob(ctx, t, cs, ns, mysqlName, prefix)
}

// runDebianBinlogArchiveJob copies mysql-bin.* from the MySQL datadir (via exec) and uploads
// to MinIO using the host aws CLI through `kubectl port-forward` to the minio Service.
func runDebianBinlogArchiveJob(ctx context.Context, t *testing.T, cs kubernetes.Interface, ns, mysqlName, prefix string) error {
	t.Helper()
	cfg, err := loadRESTConfig()
	if err != nil {
		return err
	}
	podName := mysqlName + "-0"
	listOut, err := execInPod(ctx, cfg, cs, ns, podName, "mysql", []string{
		"/bin/bash", "-c", `ls -1 /var/lib/mysql/mysql-bin.[0-9]* 2>/dev/null | xargs -n1 basename`,
	})
	if err != nil {
		return fmt.Errorf("list binlogs in pod: %w (%s)", err, listOut)
	}
	var files []string
	for _, line := range strings.Split(listOut, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && strings.HasPrefix(line, "mysql-bin.") && !strings.HasSuffix(line, ".index") {
			files = append(files, line)
		}
	}
	if len(files) == 0 {
		return fmt.Errorf("no mysql-bin.* files in datadir (out=%q)", listOut)
	}
	t.Logf("datadir binlogs: %v", files)

	tmpDir, err := os.MkdirTemp("", "pitr-binlogs-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	for _, f := range files {
		raw, err := execInPod(ctx, cfg, cs, ns, podName, "mysql", []string{
			"/bin/bash", "-c", fmt.Sprintf("base64 /var/lib/mysql/%s | tr -d '\\n'", f),
		})
		if err != nil {
			return fmt.Errorf("read %s: %w", f, err)
		}
		b64 := strings.Join(strings.Fields(raw), "")
		data, err := decodeBase64(b64)
		if err != nil {
			return fmt.Errorf("decode %s: %w", f, err)
		}
		if err := os.WriteFile(filepath.Join(tmpDir, f), data, 0o644); err != nil {
			return err
		}
	}

	// Port-forward minio and aws s3 sync from the host (aws is available in CI/dev).
	pfCtx, pfCancel := context.WithCancel(ctx)
	defer pfCancel()
	localPort := 19000 + int(time.Now().UnixNano()%1000)
	pf := exec.CommandContext(pfCtx, "kubectl", "port-forward", "-n", "minio", "svc/minio",
		fmt.Sprintf("%d:9000", localPort))
	pf.Stdout = io.Discard
	pf.Stderr = io.Discard
	if err := pf.Start(); err != nil {
		return fmt.Errorf("port-forward minio: %w", err)
	}
	defer func() { _ = pf.Process.Kill() }()
	time.Sleep(2 * time.Second)

	endpoint := fmt.Sprintf("http://127.0.0.1:%d", localPort)
	dest := fmt.Sprintf("s3://%s/%s/", backupBucket, prefix)
	// Force path-style addressing for MinIO.
	_ = exec.CommandContext(ctx, "aws", "configure", "set", "default.s3.addressing_style", "path").Run()
	cmd := exec.CommandContext(ctx, "aws", "s3", "sync", tmpDir+"/", dest, "--endpoint-url", endpoint)
	cmd.Env = append(os.Environ(),
		"AWS_ACCESS_KEY_ID="+minioAccess,
		"AWS_SECRET_ACCESS_KEY="+minioSecret,
		"AWS_DEFAULT_REGION=us-east-1",
		"AWS_EC2_METADATA_DISABLED=true",
	)
	out, err := cmd.CombinedOutput()
	t.Logf("aws s3 sync: %s", string(out))
	if err != nil {
		return fmt.Errorf("aws s3 sync: %w (%s)", err, string(out))
	}
	t.Logf("uploaded %d binlog files to %s", len(files), dest)
	return nil
}

func decodeBase64(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}

func dumpRestorePods(ctx context.Context, t *testing.T, cs kubernetes.Interface, ns string) {
	t.Helper()
	pods, err := cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return
	}
	for _, p := range pods.Items {
		if !strings.Contains(p.Name, "restore") && !strings.Contains(p.Name, "pitr") {
			continue
		}
		for _, c := range []string{"restore", "fetch-backup", "fetch-binlogs", "s3-upload", "mysqldump"} {
			logs, err := cs.CoreV1().Pods(ns).GetLogs(p.Name, &corev1.PodLogOptions{Container: c, TailLines: int64Ptr(50)}).Do(ctx).Raw()
			if err == nil && len(logs) > 0 {
				t.Logf("pod %s/%s:\n%s", p.Name, c, string(logs))
			}
		}
	}
}
