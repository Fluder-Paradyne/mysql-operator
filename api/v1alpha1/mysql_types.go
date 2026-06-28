package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MySQLSpec defines the desired state of MySQL.
type MySQLSpec struct {
	// Replicas is the number of MySQL pods in the StatefulSet.
	// 1 = standalone primary. 2+ = primary plus asynchronous GTID replicas for HA / read scaling.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=5
	// +kubebuilder:default=1
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// Image is the MySQL container image (8.0+ required for clone-based replica bootstrap).
	// +kubebuilder:default="mysql:8.0"
	// +optional
	Image string `json:"image,omitempty"`

	// StorageSize is the size of the persistent volume claim for MySQL data (per pod).
	// +kubebuilder:default="10Gi"
	// +optional
	StorageSize string `json:"storageSize,omitempty"`

	// StorageClassName is the storage class for the PVC. Empty uses the cluster default.
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`

	// RootPasswordSecretRef references a Secret key that holds the MySQL root password.
	// If omitted, the operator creates a Secret named <mysql-name>-root with a generated password.
	// +optional
	RootPasswordSecretRef *SecretKeySelector `json:"rootPasswordSecretRef,omitempty"`

	// ReplicationPasswordSecretRef references a Secret key for the replication user password.
	// If omitted and replicas > 1, the operator creates <mysql-name>-replication.
	// +optional
	ReplicationPasswordSecretRef *SecretKeySelector `json:"replicationPasswordSecretRef,omitempty"`

	// Database is an optional database created on first boot on the primary (MYSQL_DATABASE).
	// +optional
	Database string `json:"database,omitempty"`

	// Resources are compute resource requirements for the MySQL container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Port is the MySQL service port.
	// +kubebuilder:default=3306
	// +optional
	Port int32 `json:"port,omitempty"`

	// Failover controls automatic primary promotion when the current primary is unhealthy.
	// Only applies when replicas > 1.
	// +optional
	Failover *FailoverSpec `json:"failover,omitempty"`

	// PITR enables binlog archiving for point-in-time recovery (used with MySQLBackup + MySQLRestore).
	// +optional
	PITR *PITRSpec `json:"pitr,omitempty"`
}

// FailoverSpec configures automatic failover behaviour.
type FailoverSpec struct {
	// Enabled turns automatic failover on or off. Defaults to true when the field is omitted
	// and replicas > 1 (see FailoverEnabled()).
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// UnhealthySeconds is how long the primary must stay not-Ready before promotion.
	// +kubebuilder:default=30
	// +kubebuilder:validation:Minimum=5
	// +kubebuilder:validation:Maximum=600
	// +optional
	UnhealthySeconds *int32 `json:"unhealthySeconds,omitempty"`
}

// SecretKeySelector selects a key of a Secret.
type SecretKeySelector struct {
	// Name of the Secret in the same namespace.
	Name string `json:"name"`
	// Key within the Secret. Defaults to "password".
	// +kubebuilder:default="password"
	// +optional
	Key string `json:"key,omitempty"`
}

// MySQLStatus defines the observed state of MySQL.
type MySQLStatus struct {
	// Phase is a high-level status of the MySQL instance.
	// +optional
	Phase string `json:"phase,omitempty"`

	// ReadyReplicas is the number of ready MySQL pods.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// DesiredReplicas is the configured replica count.
	// +optional
	DesiredReplicas int32 `json:"desiredReplicas,omitempty"`

	// Mode is "Standalone" or "PrimaryReplica".
	// +optional
	Mode string `json:"mode,omitempty"`

	// PrimaryPod is the StatefulSet pod acting as the writable primary.
	// Updated by the operator during automatic failover.
	// +optional
	PrimaryPod string `json:"primaryPod,omitempty"`

	// PrimaryService is the ClusterIP service for read/write traffic (points at the primary).
	// +optional
	PrimaryService string `json:"primaryService,omitempty"`

	// ReadsService is the ClusterIP service for read-only traffic (replicas, or primary when standalone).
	// +optional
	ReadsService string `json:"readsService,omitempty"`

	// HeadlessService is the headless service used for pod DNS / replication topology.
	// +optional
	HeadlessService string `json:"headlessService,omitempty"`

	// Replicating is the number of replicas that report IO+SQL threads running.
	// +optional
	Replicating int32 `json:"replicating,omitempty"`

	// FailoverInProgress is true while a promotion is underway.
	// +optional
	FailoverInProgress bool `json:"failoverInProgress,omitempty"`

	// PrimaryUnhealthySince is set when the current primary is first observed not Ready.
	// Cleared when the primary becomes Ready again or after a successful failover.
	// +optional
	PrimaryUnhealthySince *metav1.Time `json:"primaryUnhealthySince,omitempty"`

	// LastFailoverTime is when the last successful automatic failover completed.
	// +optional
	LastFailoverTime *metav1.Time `json:"lastFailoverTime,omitempty"`

	// LastFailoverFrom is the pod name that was primary before the last failover.
	// +optional
	LastFailoverFrom string `json:"lastFailoverFrom,omitempty"`

	// LastFailoverTo is the pod name promoted in the last failover.
	// +optional
	LastFailoverTo string `json:"lastFailoverTo,omitempty"`

	// RootSecretName is the Secret containing the root password.
	// +optional
	RootSecretName string `json:"rootSecretName,omitempty"`

	// ReplicationSecretName is the Secret containing the replication user password (HA only).
	// +optional
	ReplicationSecretName string `json:"replicationSecretName,omitempty"`

	// BinlogArchivePrefix is the S3 key prefix where binary logs are archived when PITR is enabled.
	// +optional
	BinlogArchivePrefix string `json:"binlogArchivePrefix,omitempty"`

	// BinlogArchiveCronJob is the CronJob name that ships binlogs.
	// +optional
	BinlogArchiveCronJob string `json:"binlogArchiveCronJob,omitempty"`

	// Conditions represent the latest available observations of the MySQL state.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Primary",type=string,JSONPath=`.status.primaryPod`
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.status.mode`
// +kubebuilder:printcolumn:name="Replicating",type=integer,JSONPath=`.status.replicating`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:resource:shortName=my

// MySQL is the Schema for the mysqls API.
type MySQL struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MySQLSpec   `json:"spec,omitempty"`
	Status MySQLStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MySQLList contains a list of MySQL.
type MySQLList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MySQL `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MySQL{}, &MySQLList{})
}

// DesiredReplicas returns the effective replica count.
func (s *MySQLSpec) DesiredReplicas() int32 {
	if s.Replicas == nil {
		return 1
	}
	return *s.Replicas
}

// FailoverEnabled reports whether automatic failover should run (HA only).
// Default is enabled when Failover is nil or Enabled is nil.
func (s *MySQLSpec) FailoverEnabled() bool {
	if s.DesiredReplicas() <= 1 {
		return false
	}
	if s.Failover == nil || s.Failover.Enabled == nil {
		return true
	}
	return *s.Failover.Enabled
}

// FailoverUnhealthySeconds returns the primary unhealthy grace period.
func (s *MySQLSpec) FailoverUnhealthySeconds() int32 {
	if s.Failover != nil && s.Failover.UnhealthySeconds != nil && *s.Failover.UnhealthySeconds > 0 {
		return *s.Failover.UnhealthySeconds
	}
	return 30
}


// PITRSpec configures point-in-time recovery prerequisites (binlog archiving to S3).
type PITRSpec struct {
	// Enabled turns binlog archiving on. Default false.
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// BinlogArchive is required when enabled; archives binary logs to S3 for PITR replay.
	// +optional
	BinlogArchive *BinlogArchiveSpec `json:"binlogArchive,omitempty"`
}

// BinlogArchiveSpec ships MySQL binary logs to S3 on a schedule.
type BinlogArchiveSpec struct {
	// Schedule is a Cron expression for the archive Job (default "*/5 * * * *" = every 5 minutes).
	// +optional
	Schedule string `json:"schedule,omitempty"`

	// S3 destination for archived binlogs (same shape as backup S3).
	S3 BackupS3Spec `json:"s3"`

	// AWSCLIImage for the uploader container (default amazon/aws-cli:2.15.0).
	// +optional
	AWSCLIImage string `json:"awsCLIImage,omitempty"`

	// Image for mysql client / mysqlbinlog (defaults to the MySQL CR image).
	// +optional
	Image string `json:"image,omitempty"`
}

// PITREnabled reports whether binlog archiving should run.
func (s *MySQLSpec) PITREnabled() bool {
	if s.PITR == nil || s.PITR.Enabled == nil {
		return false
	}
	return *s.PITR.Enabled
}

// EffectivePrimaryPod returns the pod that should act as primary.
func (m *MySQL) EffectivePrimaryPod() string {
	if m.Status.PrimaryPod != "" {
		return m.Status.PrimaryPod
	}
	return m.Name + "-0"
}
