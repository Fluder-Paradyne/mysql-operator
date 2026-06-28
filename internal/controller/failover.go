package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/asrk/mysql-operator/api/v1alpha1"
)

// reconcileFailover promotes a healthy replica when the current primary stays NotReady
// longer than FailoverUnhealthySeconds. Returns whether the caller should requeue soon.
func (r *MySQLReconciler) reconcileFailover(ctx context.Context, mysql *mysqlv1alpha1.MySQL, rootSecret, rootKey, replSecret, replKey string) (bool, error) {
	logger := log.FromContext(ctx)

	if !mysql.Spec.FailoverEnabled() {
		if mysql.Status.PrimaryUnhealthySince != nil || mysql.Status.FailoverInProgress {
			mysql.Status.PrimaryUnhealthySince = nil
			mysql.Status.FailoverInProgress = false
			_ = r.Status().Update(ctx, mysql)
		}
		return false, nil
	}

	primary := mysql.EffectivePrimaryPod()
	if r.podReady(ctx, mysql.Namespace, primary) {
		changed := false
		if mysql.Status.PrimaryUnhealthySince != nil {
			mysql.Status.PrimaryUnhealthySince = nil
			changed = true
		}
		if mysql.Status.FailoverInProgress {
			// Promotion finished and new primary is accepting connections.
			mysql.Status.FailoverInProgress = false
			changed = true
		}
		if changed {
			if err := r.Status().Update(ctx, mysql); err != nil {
				return true, err
			}
		}
		return false, nil
	}

	// Primary not ready — start or continue the unhealthy timer.
	now := metav1.Now()
	if mysql.Status.PrimaryUnhealthySince == nil {
		mysql.Status.PrimaryUnhealthySince = &now
		if err := r.Status().Update(ctx, mysql); err != nil {
			return true, err
		}
		logger.Info("primary observed unhealthy; starting failover grace period",
			"primary", primary, "graceSeconds", mysql.Spec.FailoverUnhealthySeconds())
		return true, nil
	}

	elapsed := time.Since(mysql.Status.PrimaryUnhealthySince.Time)
	grace := time.Duration(mysql.Spec.FailoverUnhealthySeconds()) * time.Second
	if elapsed < grace {
		logger.Info("waiting for primary to recover before failover",
			"primary", primary, "elapsed", elapsed.Round(time.Second), "grace", grace)
		return true, nil
	}

	candidate := r.pickFailoverCandidate(ctx, mysql, primary)
	if candidate == "" {
		logger.Info("no ready replica available to promote", "failedPrimary", primary)
		return true, nil
	}

	if r.Clientset == nil || r.RESTConfig == nil {
		return true, fmt.Errorf("clientset required for failover promote")
	}
	rootPass, err := r.secretValue(ctx, mysql.Namespace, rootSecret, rootKey)
	if err != nil {
		return true, err
	}

	logger.Info("promoting replica to primary", "from", primary, "to", candidate)
	mysql.Status.FailoverInProgress = true
	if err := r.Status().Update(ctx, mysql); err != nil {
		return true, err
	}

	if err := r.promoteReplica(ctx, mysql, candidate, rootPass); err != nil {
		logger.Error(err, "promote failed", "candidate", candidate)
		return true, err
	}

	// Commit new primary in status; roles/replication loops will retarget followers.
	mysql.Status.PrimaryPod = candidate
	mysql.Status.PrimaryUnhealthySince = nil
	mysql.Status.LastFailoverFrom = primary
	mysql.Status.LastFailoverTo = candidate
	t := metav1.Now()
	mysql.Status.LastFailoverTime = &t
	// Leave FailoverInProgress=true until new primary is Ready (cleared above).
	if err := r.Status().Update(ctx, mysql); err != nil {
		return true, err
	}

	// Best-effort immediate role labels so primary Service switches ASAP.
	_ = r.ensurePodRoles(ctx, mysql)

	logger.Info("failover primary updated", "primary", candidate, "previous", primary)
	return true, nil
}

// pickFailoverCandidate chooses a Ready pod that is not the failed primary.
// Prefer the highest ordinal among ready candidates (often caught up if OrderedReady was used),
// falling back to any ready member.
func (r *MySQLReconciler) pickFailoverCandidate(ctx context.Context, mysql *mysqlv1alpha1.MySQL, failedPrimary string) string {
	desired := mysql.Spec.DesiredReplicas()
	var candidate string
	var bestOrdinal int32 = -1
	for i := int32(0); i < desired; i++ {
		podName := fmt.Sprintf("%s-%d", mysql.Name, i)
		if podName == failedPrimary {
			continue
		}
		if !r.podReady(ctx, mysql.Namespace, podName) {
			continue
		}
		if i >= bestOrdinal {
			bestOrdinal = i
			candidate = podName
		}
	}
	return candidate
}

// promoteReplica stops replication on the candidate and opens it for writes.
func (r *MySQLReconciler) promoteReplica(ctx context.Context, mysql *mysqlv1alpha1.MySQL, pod, rootPass string) error {
	// STOP/RESET replica, disable read_only so clients can write through primary Service.
	sql := `
STOP REPLICA;
RESET REPLICA ALL;
SET GLOBAL super_read_only=0;
SET GLOBAL read_only=0;
`
	out, err := r.execSQL(ctx, mysql.Namespace, pod, rootPass, sql)
	if err != nil {
		// STOP REPLICA may error if never configured; still try to clear read_only.
		if !strings.Contains(out, "read_only") {
			_, err2 := r.execSQL(ctx, mysql.Namespace, pod, rootPass, `SET GLOBAL super_read_only=0; SET GLOBAL read_only=0;`)
			if err2 != nil {
				return fmt.Errorf("promote %s: %w (also: %v) out=%s", pod, err, err2, out)
			}
		}
	}
	return nil
}
