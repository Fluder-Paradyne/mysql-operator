package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MySQLSpec defines the desired state of MySQL.
type MySQLSpec struct {
	// Replicas is the number of MySQL pods in the StatefulSet.
	// 1 = standalone primary. 2+ = primary (pod-0) plus asynchronous GTID replicas for HA / read scaling.
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

	// PrimaryPod is the StatefulSet pod acting as the writable primary (usually <name>-0).
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

	// RootSecretName is the Secret containing the root password.
	// +optional
	RootSecretName string `json:"rootSecretName,omitempty"`

	// ReplicationSecretName is the Secret containing the replication user password (HA only).
	// +optional
	ReplicationSecretName string `json:"replicationSecretName,omitempty"`

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
