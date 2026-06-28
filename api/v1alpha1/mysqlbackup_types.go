package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MySQLBackupSpec defines an on-demand logical backup of a MySQL instance.
type MySQLBackupSpec struct {
	// MySQLName is the name of the MySQL CR in the same namespace to back up.
	// +kubebuilder:validation:MinLength=1
	MySQLName string `json:"mysqlName"`

	// StorageSize is the size of the PVC that stores the dump file.
	// +kubebuilder:default="5Gi"
	// +optional
	StorageSize string `json:"storageSize,omitempty"`

	// StorageClassName for the backup PVC. Empty uses the cluster default.
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`

	// Image is the container image used for the backup Job (must include mysql client + gzip).
	// Defaults to the target MySQL CR image, or mysql:8.0.
	// +optional
	Image string `json:"image,omitempty"`

	// Databases restricts the dump to specific databases. Empty dumps all databases
	// (mysqldump --all-databases).
	// +optional
	Databases []string `json:"databases,omitempty"`

	// TTLSecondsAfterFinished, if set, is applied on the backup Job for automatic cleanup.
	// +optional
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`
}

// MySQLBackupStatus is the observed state of a backup.
type MySQLBackupStatus struct {
	// Phase is Pending, Running, Succeeded, or Failed.
	// +optional
	Phase string `json:"phase,omitempty"`

	// JobName is the Job running (or that ran) the dump.
	// +optional
	JobName string `json:"jobName,omitempty"`

	// PVCName holds the backup data (dump.sql.gz).
	// +optional
	PVCName string `json:"pvcName,omitempty"`

	// FileName is the dump file path inside the PVC (mounted at /backup).
	// +optional
	FileName string `json:"fileName,omitempty"`

	// Message is a human-readable status detail.
	// +optional
	Message string `json:"message,omitempty"`

	// StartTime when the backup Job was created.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime when the Job succeeded or failed.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Conditions of the backup.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="MySQL",type=string,JSONPath=`.spec.mysqlName`
// +kubebuilder:printcolumn:name="PVC",type=string,JSONPath=`.status.pvcName`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:resource:shortName=myb

// MySQLBackup is the Schema for logical MySQL backups.
type MySQLBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MySQLBackupSpec   `json:"spec,omitempty"`
	Status MySQLBackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MySQLBackupList contains a list of MySQLBackup.
type MySQLBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MySQLBackup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MySQLBackup{}, &MySQLBackupList{})
}
