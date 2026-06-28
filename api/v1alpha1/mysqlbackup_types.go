package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MySQLBackupSpec defines an on-demand logical backup of a MySQL instance.
type MySQLBackupSpec struct {
	// MySQLName is the name of the MySQL CR in the same namespace to back up.
	// +kubebuilder:validation:MinLength=1
	MySQLName string `json:"mysqlName"`

	// StorageSize is the size of the PVC that stores the dump file (also used as
	// a staging volume when S3 export is enabled). Ignored when s3.skipPVC is true.
	// +kubebuilder:default="5Gi"
	// +optional
	StorageSize string `json:"storageSize,omitempty"`

	// StorageClassName for the backup PVC. Empty uses the cluster default.
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`

	// Image is the container image used for mysqldump (must include mysql client + gzip).
	// Defaults to the target MySQL CR image, or mysql:8.0.
	// +optional
	Image string `json:"image,omitempty"`

	// AWSCLIImage is used for the S3 upload container when spec.s3 is set.
	// +kubebuilder:default="amazon/aws-cli:2.15.0"
	// +optional
	AWSCLIImage string `json:"awsCLIImage,omitempty"`

	// Databases restricts the dump to specific databases. Empty dumps all databases
	// (mysqldump --all-databases).
	// +optional
	Databases []string `json:"databases,omitempty"`

	// TTLSecondsAfterFinished, if set, is applied on the backup Job for automatic cleanup.
	// +optional
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`

	// S3, when set, uploads the gzipped dump to an S3 (or S3-compatible) bucket after the dump.
	// +optional
	S3 *BackupS3Spec `json:"s3,omitempty"`
}

// BackupS3Spec configures export of the dump object to S3 / MinIO / etc.
type BackupS3Spec struct {
	// Bucket is the target bucket name (required when s3 is set).
	// +kubebuilder:validation:MinLength=1
	Bucket string `json:"bucket"`

	// Prefix is the key prefix inside the bucket (no leading slash).
	// Default: mysql-backups/<mysqlName>/<backupName>
	// Final object key is <prefix>/dump.sql.gz unless ObjectKey is set.
	// +optional
	Prefix string `json:"prefix,omitempty"`

	// ObjectKey overrides the full object key (relative to the bucket).
	// When empty, uses <prefix>/dump.sql.gz.
	// +optional
	ObjectKey string `json:"objectKey,omitempty"`

	// Region is the AWS region (also used as default signing region for custom endpoints).
	// +optional
	Region string `json:"region,omitempty"`

	// Endpoint is an optional custom S3 API endpoint (e.g. https://minio.example.com).
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// ForcePathStyle uses path-style addressing (often required for MinIO).
	// +optional
	ForcePathStyle bool `json:"forcePathStyle,omitempty"`

	// CredentialsSecretRef names a Secret with AWS credentials.
	// Expected keys (any of the pairs work):
	//   - AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY
	//   - accessKeyId + secretAccessKey
	// Optional: AWS_SESSION_TOKEN or sessionToken
	// +kubebuilder:validation:Required
	CredentialsSecretRef SecretNameRef `json:"credentialsSecretRef"`

	// SkipPVC when true uses an emptyDir staging volume and does not create a backup PVC.
	// The dump only persists in S3 (plus Job logs). Default false (PVC + S3).
	// +optional
	SkipPVC bool `json:"skipPVC,omitempty"`
}

// SecretNameRef references a Secret by name in the same namespace.
type SecretNameRef struct {
	// Name of the Secret.
	Name string `json:"name"`
}

// MySQLBackupStatus is the observed state of a backup.
type MySQLBackupStatus struct {
	// Phase is Pending, Running, Succeeded, or Failed.
	// +optional
	Phase string `json:"phase,omitempty"`

	// JobName is the Job running (or that ran) the dump.
	// +optional
	JobName string `json:"jobName,omitempty"`

	// PVCName holds the backup data (dump.sql.gz) when a PVC is used.
	// +optional
	PVCName string `json:"pvcName,omitempty"`

	// FileName is the dump file path inside the volume (mounted at /backup).
	// +optional
	FileName string `json:"fileName,omitempty"`

	// S3URI is the s3://bucket/key location when S3 export succeeded.
	// +optional
	S3URI string `json:"s3URI,omitempty"`

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
// +kubebuilder:printcolumn:name="S3",type=string,JSONPath=`.status.s3URI`
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
