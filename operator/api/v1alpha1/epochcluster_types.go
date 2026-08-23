package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EpochClusterSpec declares one fixed-voter regional Epoch deployment.
type EpochClusterSpec struct {
	NodeImage           string                      `json:"nodeImage"`
	ControlImage        string                      `json:"controlImage"`
	Region              string                      `json:"region,omitempty"`
	NodeClass           string                      `json:"nodeClass,omitempty"`
	Replicas            int32                       `json:"replicas"`
	Storage             apiresource.Quantity        `json:"storage"`
	StorageClassName    *string                     `json:"storageClassName,omitempty"`
	AuthPolicyConfigMap string                      `json:"authPolicyConfigMap"`
	CredentialSecret    string                      `json:"credentialSecret"`
	ServiceType         corev1.ServiceType          `json:"serviceType,omitempty"`
	AllowedOrigins      []string                    `json:"allowedOrigins,omitempty"`
	NodeResources       corev1.ResourceRequirements `json:"nodeResources,omitempty"`
	ControlResources    corev1.ResourceRequirements `json:"controlResources,omitempty"`
	NodeSelector        map[string]string           `json:"nodeSelector,omitempty"`
	Tolerations         []corev1.Toleration         `json:"tolerations,omitempty"`
	Affinity            *corev1.Affinity            `json:"affinity,omitempty"`
}

// EpochClusterStatus contains only observed Kubernetes state.
type EpochClusterStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	ReadyNodes         int32              `json:"readyNodes,omitempty"`
	ControlReady       bool               `json:"controlReady,omitempty"`
	Endpoint           string             `json:"endpoint,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=epoch
// +kubebuilder:printcolumn:name="Ready",type="integer",JSONPath=".status.readyNodes"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type EpochCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EpochClusterSpec   `json:"spec,omitempty"`
	Status EpochClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type EpochClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EpochCluster `json:"items"`
}
