package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MySQLRestoreSpec restores a logical backup and optionally replays binlogs to a point in time.
// WARNING: restore is destructive to the target MySQL instance data.
type MySQLRestoreSpec struct {
	// MySQLName is the target MySQL CR (same namespace) to restore into.
	// +kubebuilder:validation:MinLength=1
	MySQLName string `json:"mysqlName"`

	// BackupName references a MySQLBackup CR whose dump (PVC and/or S3) is the base backup.
	// Exactly one of BackupName or BackupS3URI should be set (BackupName preferred).
	// +optional
	BackupName string `json:"backupName,omitempty"`

	// BackupS3URI is an explicit s3://bucket/key to dump.sql.gz when not using BackupName.
	// +optional
	BackupS3URI string `json:"backupS3URI,omitempty"`

	// BinlogS3Prefix is the S3 prefix containing archived binlogs (no trailing slash).
	// Defaults to status.binlogArchivePrefix on the target MySQL, or
	// mysql-binlogs/<mysqlName> when PITR archiving used defaults.
	// +optional
	BinlogS3Prefix string `json:"binlogS3Prefix,omitempty"`

	// RestoreTo selects how far to replay binlogs after applying the base backup.
	// +optional
	RestoreTo *RestoreToSpec `json:"restoreTo,omitempty"`

	// S3 credentials / endpoint for downloading backup and binlogs (required if either is on S3).
	// +optional
	S3 *BackupS3Spec `json:"s3,omitempty"`

	// Image for mysql client tools (defaults to target MySQL image).
	// +optional
	Image string `json:"image,omitempty"`

	// AWSCLIImage for S3 downloads (default amazon/aws-cli:2.15.0).
	// +optional
	AWSCLIImage string `json:"awsCLIImage,omitempty"`
}

// RestoreToSpec defines the PITR target.
type RestoreToSpec struct {
	// Time is an RFC3339 timestamp (UTC recommended). Binlogs are replayed with
	// mysqlbinlog --stop-datetime up to this instant (inclusive of events before it).
	// Example: "2026-06-28T12:00:00Z"
	// +optional
	Time string `json:"time,omitempty"`

	// GTID stops replay at this GTID set end (advanced; requires GTID backups).
	// When set, takes precedence over Time for the stop condition if both are set
	// only Time is used in v1 (GTID reserved for future).
	// +optional
	GTID string `json:"gtid,omitempty"`

	// BackupOnly skips binlog replay (restore dump only — not PITR).
	// +optional
	BackupOnly bool `json:"backupOnly,omitempty"`
}

// MySQLRestoreStatus is the observed state of a restore.
type MySQLRestoreStatus struct {
	// Phase is Pending, Running, Succeeded, or Failed.
	// +optional
	Phase string `json:"phase,omitempty"`

	// JobName running the restore.
	// +optional
	JobName string `json:"jobName,omitempty"`

	// Message is a human-readable status detail.
	// +optional
	Message string `json:"message,omitempty"`

	// StartTime of the restore Job.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime when finished.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Conditions of the restore.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="MySQL",type=string,JSONPath=`.spec.mysqlName`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:resource:shortName=myr

// MySQLRestore restores a backup with optional point-in-time binlog replay.
type MySQLRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MySQLRestoreSpec   `json:"spec,omitempty"`
	Status MySQLRestoreStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MySQLRestoreList contains a list of MySQLRestore.
type MySQLRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MySQLRestore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MySQLRestore{}, &MySQLRestoreList{})
}
