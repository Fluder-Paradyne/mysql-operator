package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MySQLCloneSpec clones live data from a source MySQL instance into a target instance.
// Uses MySQL 8 CLONE INSTANCE (preferred) or optional logical dump/restore.
// WARNING: the target instance is overwritten (destructive).
type MySQLCloneSpec struct {
	// SourceMySQLName is the MySQL CR to clone FROM (same namespace).
	// +kubebuilder:validation:MinLength=1
	SourceMySQLName string `json:"sourceMySQLName"`

	// TargetMySQLName is the MySQL CR to clone INTO (same namespace).
	// Should typically be a different instance; for HA targets, only the primary is cloned
	// and replicas are expected to be rebuilt by the operator replication loop afterward.
	// +kubebuilder:validation:MinLength=1
	TargetMySQLName string `json:"targetMySQLName"`

	// Method is "ClonePlugin" (default, MySQL 8 CLONE INSTANCE) or "Logical"
	// (mysqldump from source | mysql into target).
	// +kubebuilder:validation:Enum=ClonePlugin;Logical
	// +kubebuilder:default=ClonePlugin
	// +optional
	Method string `json:"method,omitempty"`

	// Image for the Job (mysql client). Defaults to the source MySQL image.
	// +optional
	Image string `json:"image,omitempty"`
}

// MySQLCloneStatus is the observed state of a clone operation.
type MySQLCloneStatus struct {
	// Phase is Pending, Running, Succeeded, or Failed.
	// +optional
	Phase string `json:"phase,omitempty"`

	// JobName executing the clone.
	// +optional
	JobName string `json:"jobName,omitempty"`

	// Message human detail.
	// +optional
	Message string `json:"message,omitempty"`

	// SourcePrimary is the donor host used.
	// +optional
	SourcePrimary string `json:"sourcePrimary,omitempty"`

	// TargetPrimary is the recipient host used.
	// +optional
	TargetPrimary string `json:"targetPrimary,omitempty"`

	// StartTime of the Job.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime when finished.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Conditions of the clone.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Source",type=string,JSONPath=`.spec.sourceMySQLName`
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.spec.targetMySQLName`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:resource:shortName=myc

// MySQLClone clones one operator-managed MySQL instance into another.
type MySQLClone struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MySQLCloneSpec   `json:"spec,omitempty"`
	Status MySQLCloneStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MySQLCloneList contains a list of MySQLClone.
type MySQLCloneList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MySQLClone `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MySQLClone{}, &MySQLCloneList{})
}
