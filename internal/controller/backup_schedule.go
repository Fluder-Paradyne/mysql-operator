package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/asrk/mysql-operator/api/v1alpha1"
)

const (
	labelScheduledBackup = "mysql.asrk.dev/scheduled-backup"
	labelMySQLInstance   = "mysql.asrk.dev/instance"
	// Keep a small skew so reconcile timing doesn't miss a cron tick.
	scheduleSkew = 2 * time.Minute
)

// reconcileScheduledBackups creates MySQLBackup CRs on the cron schedule and deletes ones
// older than retentionDays (default 30).
func (r *MySQLReconciler) reconcileScheduledBackups(ctx context.Context, mysql *mysqlv1alpha1.MySQL) (time.Duration, error) {
	logger := log.FromContext(ctx)
	if !mysql.Spec.BackupScheduleEnabled() {
		// Still allow GC if schedule was turned off but old labeled backups remain.
		_ = r.gcExpiredScheduledBackups(ctx, mysql)
		return 0, nil
	}

	retention := mysql.Spec.BackupRetentionDays()
	mysql.Status.BackupRetentionDays = retention

	if err := r.gcExpiredScheduledBackups(ctx, mysql); err != nil {
		logger.Error(err, "gc scheduled backups")
	}

	requeueAfter := 5 * time.Minute
	if mysql.Spec.BackupSuspended() {
		return requeueAfter, nil
	}

	// Only schedule while the instance is usable.
	if mysql.Status.ReadyReplicas < 1 && mysql.Status.Phase != "Running" {
		return 1 * time.Minute, nil
	}

	sched := mysql.Spec.BackupCronSchedule()
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	cronSched, err := parser.Parse(sched)
	if err != nil {
		return requeueAfter, fmt.Errorf("invalid backup.schedule %q: %w", sched, err)
	}

	now := time.Now().UTC()
	last := mysql.Status.LastScheduledBackupTime
	var lastTime time.Time
	if last != nil && !last.IsZero() {
		lastTime = last.Time.UTC()
	} else {
		// First enable: run once soon (treat as due if never ran).
		lastTime = now.Add(-24 * time.Hour)
	}

	// Next fire after last scheduled time; if that is in the past (within skew), create a backup.
	next := cronSched.Next(lastTime)
	if next.After(now.Add(scheduleSkew)) {
		// Not due yet — requeue around the next fire time (cap at 1h for responsiveness).
		d := time.Until(next)
		if d < 30*time.Second {
			d = 30 * time.Second
		}
		if d > time.Hour {
			d = time.Hour
		}
		return d, nil
	}

	// Avoid duplicate if we already created one very recently (controller restart).
	if last != nil && now.Sub(last.Time) < 2*time.Minute {
		return 2 * time.Minute, nil
	}

	name := fmt.Sprintf("%s-auto-%s", mysql.Name, now.Format("20060102-150405"))
	if len(name) > 63 {
		name = name[:63]
	}
	backup := &mysqlv1alpha1.MySQLBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: mysql.Namespace,
			Labels: map[string]string{
				labelAppKey:          "mysql",
				labelInstanceKey:     mysql.Name,
				labelMySQLInstance:   mysql.Name,
				labelScheduledBackup: "true",
			},
		},
		Spec: mysqlv1alpha1.MySQLBackupSpec{
			MySQLName:   mysql.Name,
			StorageSize: "5Gi",
		},
	}
	if mysql.Spec.Backup != nil {
		if mysql.Spec.Backup.StorageSize != "" {
			backup.Spec.StorageSize = mysql.Spec.Backup.StorageSize
		}
		if mysql.Spec.Backup.StorageClassName != nil {
			backup.Spec.StorageClassName = mysql.Spec.Backup.StorageClassName
		}
		if mysql.Spec.Backup.S3 != nil {
			s3 := *mysql.Spec.Backup.S3
			// Unique prefix per run so objects don't overwrite.
			if s3.Prefix == "" {
				s3.Prefix = fmt.Sprintf("mysql-backups/%s/scheduled/%s", mysql.Name, now.Format("20060102-150405"))
			} else {
				s3.Prefix = fmt.Sprintf("%s/%s", strings.Trim(s3.Prefix, "/"), now.Format("20060102-150405"))
			}
			// Prefer S3-only for scheduled to limit PVC pile-up; still allow PVC if skipPVC false.
			backup.Spec.S3 = &s3
		}
	}
	if err := controllerutil.SetControllerReference(mysql, backup, r.Scheme); err != nil {
		return requeueAfter, err
	}
	if err := r.Create(ctx, backup); err != nil {
		return 1 * time.Minute, fmt.Errorf("create scheduled MySQLBackup: %w", err)
	}

	mysql.Status.LastScheduledBackup = name
	t := metav1.NewTime(now)
	mysql.Status.LastScheduledBackupTime = &t
	logger.Info("created scheduled MySQLBackup", "name", name, "retentionDays", retention)
	return 2 * time.Minute, nil
}


func (r *MySQLReconciler) gcExpiredScheduledBackups(ctx context.Context, mysql *mysqlv1alpha1.MySQL) error {
	retention := mysql.Spec.BackupRetentionDays()
	if mysql.Spec.Backup == nil && retention == 30 {
		// Default retention only applies when backup section exists OR we still want GC of labeled backups.
		// Always GC labeled scheduled backups with effective retention (30 if only partial config).
	}
	cutoff := time.Now().UTC().Add(-time.Duration(retention) * 24 * time.Hour)

	list := &mysqlv1alpha1.MySQLBackupList{}
	if err := r.List(ctx, list, &client.ListOptions{
		Namespace: mysql.Namespace,
		LabelSelector: labels.SelectorFromSet(map[string]string{
			labelMySQLInstance:   mysql.Name,
			labelScheduledBackup: "true",
		}),
	}); err != nil {
		return err
	}

	var firstErr error
	for i := range list.Items {
		b := &list.Items[i]
		if b.CreationTimestamp.Time.After(cutoff) {
			continue
		}
		// Only GC finished backups (or very old pending stuck ones > retention).
		if b.Status.Phase != "Succeeded" && b.Status.Phase != "Failed" && b.Status.Phase != "" {
			if time.Since(b.CreationTimestamp.Time) < time.Duration(retention)*24*time.Hour {
				continue
			}
		}
		if err := r.Delete(ctx, b); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		log.FromContext(ctx).Info("deleted expired scheduled backup", "backup", b.Name, "age", time.Since(b.CreationTimestamp.Time).String())
	}
	return firstErr
}
