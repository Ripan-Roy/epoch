package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TransportSecuritySpec references role-scoped identities issued by the same
// regional trust domain. Each Secret must contain ca.crt, tls.crt, and tls.key.
type TransportSecuritySpec struct {
	DataPlaneSecret    string `json:"dataPlaneSecret"`
	ControlPlaneSecret string `json:"controlPlaneSecret"`
	RegionalServerName string `json:"regionalServerName"`
}

// BackupSpec schedules application-layer encrypted semantic backups into a
// ReadWriteMany PVC. Encryption is mandatory even when the storage class also
// provides volume encryption.
type BackupSpec struct {
	Schedule         string `json:"schedule"`
	DestinationPVC   string `json:"destinationPVC"`
	EncryptionSecret string `json:"encryptionSecret"`
	KeyID            string `json:"keyID"`
	RetentionCount   int32  `json:"retentionCount"`
}

// RestoreSpec selects one encrypted semantic artifact during initial cluster
// creation. The reference is immutable after workloads are initialized.
type RestoreSpec struct {
	ObjectName       string `json:"objectName"`
	EncryptionSecret string `json:"encryptionSecret"`
}

// UpgradeSpec defines the fail-closed gates for a data-plane image rollout.
// A new retry token explicitly authorizes retrying a previously failed target.
type UpgradeSpec struct {
	BackupMaxAgeSeconds int64  `json:"backupMaxAgeSeconds,omitempty"`
	StepDeadlineSeconds int64  `json:"stepDeadlineSeconds,omitempty"`
	RollbackOnFailure   *bool  `json:"rollbackOnFailure,omitempty"`
	RetryToken          string `json:"retryToken,omitempty"`
}

// EpochClusterSpec declares one regional Epoch deployment.
type EpochClusterSpec struct {
	NodeImage    string `json:"nodeImage"`
	ControlImage string `json:"controlImage"`
	Region       string `json:"region,omitempty"`
	NodeClass    string `json:"nodeClass,omitempty"`
	// Replicas is physical data-node capacity. Each catalog or tablet Raft
	// group independently selects exactly three or five members from it.
	Replicas            int32                       `json:"replicas"`
	CatalogReplicas     int32                       `json:"catalogReplicas,omitempty"`
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
	TransportSecurity   TransportSecuritySpec       `json:"transportSecurity"`
	Backup              BackupSpec                  `json:"backup"`
	Restore             *RestoreSpec                `json:"restore,omitempty"`
	Upgrade             UpgradeSpec                 `json:"upgrade,omitempty"`
}

type BackupStatus struct {
	ObservedSchedule     string       `json:"observedSchedule,omitempty"`
	LastSuccessfulTime   *metav1.Time `json:"lastSuccessfulTime,omitempty"`
	LastSuccessfulObject string       `json:"lastSuccessfulObject,omitempty"`
	LastManifestSHA256   string       `json:"lastManifestSHA256,omitempty"`
	LastKeyID            string       `json:"lastKeyID,omitempty"`
	LastFailureTime      *metav1.Time `json:"lastFailureTime,omitempty"`
	LastFailureMessage   string       `json:"lastFailureMessage,omitempty"`
	RetainedObjects      int32        `json:"retainedObjects,omitempty"`
}

type UpgradePhase string

const (
	UpgradePhaseStable            UpgradePhase = "Stable"
	UpgradePhaseWaitingForBackup  UpgradePhase = "WaitingForBackup"
	UpgradePhasePreflight         UpgradePhase = "Preflight"
	UpgradePhaseDraining          UpgradePhase = "Draining"
	UpgradePhaseUpdating          UpgradePhase = "Updating"
	UpgradePhaseVerifying         UpgradePhase = "Verifying"
	UpgradePhaseRollbackPreflight UpgradePhase = "RollbackPreflight"
	UpgradePhaseRollbackDraining  UpgradePhase = "RollbackDraining"
	UpgradePhaseRollbackUpdating  UpgradePhase = "RollbackUpdating"
	UpgradePhaseRollbackVerifying UpgradePhase = "RollbackVerifying"
	UpgradePhaseFailed            UpgradePhase = "Failed"
)

// UpgradeStatus is the durable rollout plan. Kubernetes reconciliation may be
// restarted at any point without widening the one-node mutation boundary.
type UpgradeStatus struct {
	CurrentNodeImage     string       `json:"currentNodeImage,omitempty"`
	TargetNodeImage      string       `json:"targetNodeImage,omitempty"`
	RequestID            string       `json:"requestID,omitempty"`
	Phase                UpgradePhase `json:"phase,omitempty"`
	TargetOrdinal        *int32       `json:"targetOrdinal,omitempty"`
	StartedAt            *metav1.Time `json:"startedAt,omitempty"`
	StepStartedAt        *metav1.Time `json:"stepStartedAt,omitempty"`
	FailureMessage       string       `json:"failureMessage,omitempty"`
	Message              string       `json:"message,omitempty"`
	UpdatedNodes         int32        `json:"updatedNodes,omitempty"`
	TargetOrdinalUpdated bool         `json:"targetOrdinalUpdated,omitempty"`
}

// EpochClusterStatus contains only observed Kubernetes state.
type EpochClusterStatus struct {
	ObservedGeneration      int64              `json:"observedGeneration,omitempty"`
	ReadyNodes              int32              `json:"readyNodes,omitempty"`
	ControlReady            bool               `json:"controlReady,omitempty"`
	Endpoint                string             `json:"endpoint,omitempty"`
	Initialized             bool               `json:"initialized,omitempty"`
	RestoreObject           string             `json:"restoreObject,omitempty"`
	RestoreEncryptionSecret string             `json:"restoreEncryptionSecret,omitempty"`
	Backup                  BackupStatus       `json:"backup,omitempty"`
	Upgrade                 UpgradeStatus      `json:"upgrade,omitempty"`
	Conditions              []metav1.Condition `json:"conditions,omitempty"`
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
