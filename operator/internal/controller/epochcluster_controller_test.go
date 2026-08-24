package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	epochv1alpha1 "epoch.local/epoch/operator/api/v1alpha1"
)

func TestReconcileCreatesRunnableBoundedVoterTopology(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	cluster := validCluster()
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(cluster).
		WithObjects(cluster, policyConfigMap(cluster), credentialSecret(cluster), dataPlaneTLSSecret(cluster), controlPlaneTLSSecret(cluster), backupEncryptionSecret(cluster), backupPVC(cluster)).
		Build()
	reconciler := &EpochClusterReconciler{Client: client, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}}

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}
	// Reconciliation must be idempotent before any controller-managed status is available.
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}

	nodes := &appsv1.StatefulSet{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: nodeName(cluster)}, nodes); err != nil {
		t.Fatalf("data StatefulSet was not created: %v", err)
	}
	if nodes.Spec.Replicas == nil || *nodes.Spec.Replicas != 3 {
		t.Fatalf("unexpected replicas: %#v", nodes.Spec.Replicas)
	}
	if nodes.Spec.UpdateStrategy.RollingUpdate == nil ||
		nodes.Spec.UpdateStrategy.RollingUpdate.Partition == nil ||
		*nodes.Spec.UpdateStrategy.RollingUpdate.Partition != 0 {
		t.Fatalf("stable data StatefulSet must have an explicit zero partition: %#v", nodes.Spec.UpdateStrategy)
	}
	container := nodes.Spec.Template.Spec.Containers[0]
	if !containsEnvironment(container.Env, "EPOCH_CONSENSUS_PEERS", "1=https://orders-node-0.orders-peer:7701,2=https://orders-node-1.orders-peer:7701,3=https://orders-node-2.orders-peer:7701") {
		t.Fatalf("secure voter endpoints are absent: %#v", container.Env)
	}
	if !containsEnvironment(container.Env, "EPOCH_CONSENSUS_INITIAL_VOTERS", "1,2,3") {
		t.Fatalf("bounded catalog voter set is absent: %#v", container.Env)
	}
	if !containsEnvironment(container.Env, "EPOCH_TLS_REQUIRED", "true") ||
		!containsEnvironment(container.Env, "EPOCH_PEER_TLS_CA_PATH", "/etc/epoch/tls/ca.crt") ||
		!containsEnvironment(container.Env, "EPOCH_HTTP_TLS_CLIENT_CA_PATH", "/etc/epoch/tls/ca.crt") {
		t.Fatalf("data-plane mTLS configuration is absent: %#v", container.Env)
	}
	if container.ReadinessProbe == nil || container.ReadinessProbe.TCPSocket == nil || container.ReadinessProbe.HTTPGet != nil {
		t.Fatal("mTLS workloads require a transport probe that does not bypass client authentication")
	}
	if !containsEnvironment(container.Env, "EPOCH_REGIONAL_REGION", "ap-south") ||
		!containsEnvironment(container.Env, "EPOCH_REGIONAL_NODE_CLASS", "general-purpose") {
		t.Fatalf("explicit placement identity is absent: %#v", container.Env)
	}
	if container.SecurityContext == nil || container.SecurityContext.ReadOnlyRootFilesystem == nil || !*container.SecurityContext.ReadOnlyRootFilesystem {
		t.Fatal("data container must use a read-only root filesystem")
	}
	if nodes.Spec.Template.Spec.Affinity == nil || nodes.Spec.Template.Spec.Affinity.PodAntiAffinity == nil || len(nodes.Spec.Template.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 1 {
		t.Fatal("data replicas must require distinct Kubernetes nodes")
	}
	if got := nodes.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests.Storage().String(); got != "10Gi" {
		t.Fatalf("unexpected data volume request: %s", got)
	}

	control := &appsv1.StatefulSet{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: controlName(cluster)}, control); err != nil {
		t.Fatalf("control StatefulSet was not created: %v", err)
	}
	controlContainer := control.Spec.Template.Spec.Containers[0]
	if !containsSecretEnvironment(controlContainer.Env, "EPOCH_CONTROL_REGIONAL_TOKEN", cluster.Spec.CredentialSecret, credentialKey) {
		t.Fatal("control-to-data credential must come from the referenced Secret")
	}
	if !containsEnvironment(controlContainer.Env, "EPOCH_CONTROL_TLS_REQUIRED", "true") ||
		!containsEnvironment(controlContainer.Env, "EPOCH_CONTROL_REGIONAL_TLS_SERVER_NAME", "orders-peer.epoch-system.svc") {
		t.Fatalf("control-plane mTLS configuration is absent: %#v", controlContainer.Env)
	}
	if controlContainer.ReadinessProbe == nil || controlContainer.ReadinessProbe.TCPSocket == nil || controlContainer.ReadinessProbe.HTTPGet != nil {
		t.Fatal("control-plane mTLS must not depend on an unauthenticated HTTP probe")
	}
	for _, serviceName := range []string{peerName(cluster), publicName(cluster), controlName(cluster)} {
		service := &corev1.Service{}
		if err := client.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: serviceName}, service); err != nil {
			t.Fatalf("Service %s was not created: %v", serviceName, err)
		}
	}
	backup := &batchv1.CronJob{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: backupName(cluster)}, backup); err != nil {
		t.Fatalf("encrypted backup CronJob was not created: %v", err)
	}
	if backup.Spec.Schedule != "*/15 * * * *" || backup.Spec.ConcurrencyPolicy != batchv1.ForbidConcurrent {
		t.Fatalf("unexpected backup schedule: %#v", backup.Spec)
	}
	backupContainer := backup.Spec.JobTemplate.Spec.Template.Spec.Containers[0]
	if !containsEnvironment(backupContainer.Env, "EPOCH_BACKUP_RETENTION_COUNT", "7") ||
		!containsEnvironment(backupContainer.Env, "EPOCH_BACKUP_STATUS_PATH", "/dev/termination-log") {
		t.Fatalf("backup retention or durable status output is absent: %#v", backupContainer.Env)
	}

	observed := &epochv1alpha1.EpochCluster{}
	if err := client.Get(context.Background(), request.NamespacedName, observed); err != nil {
		t.Fatal(err)
	}
	if observed.Status.ObservedGeneration != cluster.Generation || observed.Status.Endpoint != "https://orders-control.epoch-system.svc:8080" {
		t.Fatalf("unexpected observed status: %#v", observed.Status)
	}
}

func TestReconcileFailsClosedWithoutCredentials(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	cluster := validCluster()
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(cluster).
		WithObjects(cluster, policyConfigMap(cluster)).
		Build()
	reconciler := &EpochClusterReconciler{Client: client, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}}

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("missing configuration should be represented in status: %v", err)
	}
	nodes := &appsv1.StatefulSet{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: nodeName(cluster)}, nodes); err == nil {
		t.Fatal("operator must not create workloads before credentials exist")
	}
	observed := &epochv1alpha1.EpochCluster{}
	if err := client.Get(context.Background(), request.NamespacedName, observed); err != nil {
		t.Fatal(err)
	}
	condition := conditionByType(observed.Status.Conditions, conditionAvailable)
	if condition == nil || condition.Reason != "ConfigurationMissing" || condition.Status != metav1.ConditionFalse {
		t.Fatalf("missing configuration condition was not published: %#v", observed.Status.Conditions)
	}
}

func TestReconcileFailsClosedWithoutTransportIdentity(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	cluster := validCluster()
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(cluster).
		WithObjects(cluster, policyConfigMap(cluster), credentialSecret(cluster)).
		Build()
	reconciler := &EpochClusterReconciler{Client: client, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}}

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("missing TLS identity should be represented in status: %v", err)
	}
	nodes := &appsv1.StatefulSet{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: nodeName(cluster)}, nodes); err == nil {
		t.Fatal("operator must not create plaintext workloads before TLS identities exist")
	}
}

func TestScheduledBackupStatusRecoversAfterFailure(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	cluster := validCluster()
	failedAt := metav1.NewTime(time.Unix(100, 0))
	completedAt := metav1.NewTime(time.Unix(200, 0))
	failed := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "orders-backup-failed", Namespace: cluster.Namespace, Labels: map[string]string{backupOwnerLabel: cluster.Name}},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
			Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: failedAt, Message: "temporary destination outage",
		}}},
	}
	succeeded := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "orders-backup-success", Namespace: cluster.Namespace, Labels: map[string]string{backupOwnerLabel: cluster.Name}},
		Status:     batchv1.JobStatus{Succeeded: 1, CompletionTime: &completedAt},
	}
	termination, err := json.Marshal(backupTerminationStatus{
		State: "succeeded", ObjectName: "200-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.epoch-backup.enc",
		CapturedAtMS: 200, ManifestSHA256: strings.Repeat("a", 64), PlaintextSHA256: strings.Repeat("b", 64),
		KeyID: "backup-key-2026-08", RetainedObjects: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "orders-backup-success-pod", Namespace: cluster.Namespace, Labels: map[string]string{"job-name": succeeded.Name}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name: "epoch-backup", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Message: string(termination)}},
		}}},
	}
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(cluster).
		WithObjects(cluster, policyConfigMap(cluster), credentialSecret(cluster), dataPlaneTLSSecret(cluster), controlPlaneTLSSecret(cluster), backupEncryptionSecret(cluster), backupPVC(cluster), failed, succeeded, pod).
		Build()
	reconciler := &EpochClusterReconciler{Client: client, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	observed := &epochv1alpha1.EpochCluster{}
	if err := client.Get(context.Background(), request.NamespacedName, observed); err != nil {
		t.Fatal(err)
	}
	if observed.Status.Backup.LastSuccessfulObject == "" ||
		observed.Status.Backup.LastKeyID != "backup-key-2026-08" ||
		observed.Status.Backup.RetainedObjects != 7 {
		t.Fatalf("successful encrypted object was not recorded: %#v", observed.Status.Backup)
	}
	condition := conditionByType(observed.Status.Conditions, conditionBackup)
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "BackupSucceeded" {
		t.Fatalf("newer success must recover the backup condition: %#v", condition)
	}
}

func TestBackupEncryptionSecretSupportsBoundedRotationKeyring(t *testing.T) {
	t.Parallel()
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "backup-keys"},
		Data: map[string][]byte{
			backupKey:                         bytes.Repeat([]byte{7}, 32),
			backupPreviousKey + "key-2026-07": bytes.Repeat([]byte{8}, 32),
		},
	}
	if message := validateBackupEncryptionSecret(secret, "key-2026-08"); message != "" {
		t.Fatalf("valid rotation keyring was rejected: %s", message)
	}
	secret.Data["unexpected"] = bytes.Repeat([]byte{9}, 32)
	if message := validateBackupEncryptionSecret(secret, "key-2026-08"); message == "" {
		t.Fatal("unknown backup Secret data key must fail closed")
	}
	delete(secret.Data, "unexpected")
	secret.Data[backupPreviousKey+"key-2026-06"] = bytes.Repeat([]byte{7}, 32)
	if message := validateBackupEncryptionSecret(secret, "key-2026-08"); message == "" {
		t.Fatal("duplicate backup key material must fail closed")
	}
}

func TestRestoreReferenceRendersFreshClusterInitAndBecomesImmutable(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	cluster.Spec.Restore = &epochv1alpha1.RestoreSpec{ObjectName: "100-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.epoch-backup.enc", EncryptionSecret: "old-backup-key"}
	nodes := nodeStatefulSet(cluster)
	if len(nodes.Spec.Template.Spec.InitContainers) != 1 || nodes.Spec.Template.Spec.InitContainers[0].Name != "epoch-restore" {
		t.Fatalf("restore init container is absent: %#v", nodes.Spec.Template.Spec.InitContainers)
	}
	if !strings.Contains(nodes.Spec.Template.Spec.Containers[0].Args[0], "EPOCH_REGIONAL_RESTORE_PATH") {
		t.Fatal("node startup does not consume the validated semantic restore artifact")
	}
	cluster.Status.Initialized = true
	cluster.Status.RestoreObject = cluster.Spec.Restore.ObjectName
	cluster.Status.RestoreEncryptionSecret = cluster.Spec.Restore.EncryptionSecret
	if err := validateRestoreImmutability(cluster); err != nil {
		t.Fatalf("idempotent restore reconciliation must remain valid: %v", err)
	}
	cluster.Spec.Restore.ObjectName = "101-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.epoch-backup.enc"
	if err := validateRestoreImmutability(cluster); err == nil {
		t.Fatal("restore object change after initialization must fail closed")
	}
}

func TestValidateSpecSeparatesPhysicalNodesFromBoundedCatalogVoters(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	cluster.Spec.Replicas = 7
	cluster.Spec.CatalogReplicas = 5
	if err := validateSpec(&cluster.Spec); err != nil {
		t.Fatalf("seven physical nodes with five catalog voters must be accepted: %v", err)
	}
	nodes := nodeStatefulSet(cluster)
	if nodes.Spec.Replicas == nil || *nodes.Spec.Replicas != 7 {
		t.Fatalf("unexpected physical-node StatefulSet: %#v", nodes.Spec.Replicas)
	}
	wantPeers := "1=https://orders-node-0.orders-peer:7701,2=https://orders-node-1.orders-peer:7701,3=https://orders-node-2.orders-peer:7701,4=https://orders-node-3.orders-peer:7701,5=https://orders-node-4.orders-peer:7701,6=https://orders-node-5.orders-peer:7701,7=https://orders-node-6.orders-peer:7701"
	if !containsEnvironment(nodes.Spec.Template.Spec.Containers[0].Env, "EPOCH_CONSENSUS_PEERS", wantPeers) {
		t.Fatalf("physical member directory is absent: %#v", nodes.Spec.Template.Spec.Containers[0].Env)
	}
	if !containsEnvironment(nodes.Spec.Template.Spec.Containers[0].Env, "EPOCH_CONSENSUS_INITIAL_VOTERS", "1,2,3,4,5") {
		t.Fatalf("five-voter catalog bootstrap is absent: %#v", nodes.Spec.Template.Spec.Containers[0].Env)
	}
	cluster.Spec.Replicas = 4
	cluster.Spec.CatalogReplicas = 3
	if err := validateSpec(&cluster.Spec); err != nil {
		t.Fatalf("four physical nodes with three catalog voters must be accepted: %v", err)
	}
	cluster.Spec.CatalogReplicas = 5
	if err := validateSpec(&cluster.Spec); err == nil {
		t.Fatal("catalog voters cannot exceed physical nodes")
	}
	cluster.Spec.Replicas = 2
	cluster.Spec.CatalogReplicas = 0
	if err := validateSpec(&cluster.Spec); err == nil {
		t.Fatal("fewer than three physical nodes must be rejected")
	}
	cluster.Spec.Replicas = 3
	cluster.Spec.Storage = apiresource.Quantity{}
	if err := validateSpec(&cluster.Spec); err == nil {
		t.Fatal("zero durable storage must be rejected")
	}
}

func TestObjectMatchesDesiredIgnoresApiDefaultsButDetectsOwnedDrift(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	desiredService := publicService(cluster)
	observedService := desiredService.DeepCopy()
	observedService.Spec.Ports[0].Protocol = corev1.ProtocolTCP
	observedService.Spec.SessionAffinity = corev1.ServiceAffinityNone
	if !objectMatchesDesired(desiredService, observedService) {
		t.Fatal("API-server defaults must not force an update on every reconcile")
	}
	observedService.Spec.Ports[0].Port = 9999
	if objectMatchesDesired(desiredService, observedService) {
		t.Fatal("drift in an operator-owned service port must be repaired")
	}

	desiredNodes := nodeStatefulSet(cluster)
	observedNodes := desiredNodes.DeepCopy()
	observedNodes.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyAlways
	if !objectMatchesDesired(desiredNodes, observedNodes) {
		t.Fatal("StatefulSet API defaults must not force an update")
	}
	two := int32(2)
	observedNodes.Spec.Replicas = &two
	if objectMatchesDesired(desiredNodes, observedNodes) {
		t.Fatal("drift in the configured voter count must be repaired")
	}
}

func TestUpgradeWaitsForPostRequestEncryptedBackup(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	cluster := validCluster()
	cluster.Status.Upgrade.CurrentNodeImage = cluster.Spec.NodeImage
	cluster.Spec.NodeImage = "ghcr.io/ripan-roy/epoch-node:v0.2.0-beta.2"
	startUpgradeRequest(cluster, upgradeRequestID(cluster.Spec.NodeImage, ""))
	started := cluster.Status.Upgrade.StartedAt.DeepCopy()
	oldBackup := metav1.NewTime(started.Add(-time.Minute))
	cluster.Status.Backup.LastSuccessfulTime = &oldBackup
	cluster.Status.Backup.LastKeyID = cluster.Spec.Backup.KeyID
	nodes := nodeStatefulSetShell(cluster)
	nodes.Status.ReadyReplicas = cluster.Spec.Replicas
	nodes.Status.CurrentReplicas = cluster.Spec.Replicas
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodes).Build()
	reconciler := &EpochClusterReconciler{Client: client, Scheme: scheme}

	if err := reconciler.startUpgradeAfterBackup(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Status.Upgrade.Phase != epochv1alpha1.UpgradePhaseWaitingForBackup {
		t.Fatalf("stale backup must hold rollout: %#v", cluster.Status.Upgrade)
	}
	waiting := nodeStatefulSet(cluster)
	if got := *waiting.Spec.UpdateStrategy.RollingUpdate.Partition; got != cluster.Spec.Replicas {
		t.Fatalf("waiting rollout must block every ordinal, got partition %d", got)
	}
	if got := waiting.Spec.Template.Spec.Containers[0].Image; got != cluster.Status.Upgrade.CurrentNodeImage {
		t.Fatalf("waiting rollout rendered unapproved target image %q", got)
	}

	fresh := metav1.NewTime(started.Add(time.Second))
	cluster.Status.Backup.LastSuccessfulTime = &fresh
	if err := reconciler.startUpgradeAfterBackup(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Status.Upgrade.Phase != epochv1alpha1.UpgradePhasePreflight ||
		cluster.Status.Upgrade.TargetOrdinal == nil || *cluster.Status.Upgrade.TargetOrdinal != 2 {
		t.Fatalf("fresh backup did not open last-ordinal preflight: %#v", cluster.Status.Upgrade)
	}
}

func TestUpgradeAdvancesExactlyOneOrdinalAfterDrainReadinessAndVerification(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	cluster := validCluster()
	oldImage := cluster.Spec.NodeImage
	cluster.Status.Upgrade.CurrentNodeImage = oldImage
	cluster.Spec.NodeImage = "ghcr.io/ripan-roy/epoch-node:v0.2.0-beta.2"
	cluster.Status.Upgrade.TargetNodeImage = cluster.Spec.NodeImage
	cluster.Status.Upgrade.RequestID = upgradeRequestID(cluster.Spec.NodeImage, "")
	cluster.Status.Upgrade.Phase = epochv1alpha1.UpgradePhasePreflight
	ordinal := int32(2)
	cluster.Status.Upgrade.TargetOrdinal = &ordinal
	now := metav1.Now()
	cluster.Status.Upgrade.StartedAt = &now
	cluster.Status.Upgrade.StepStartedAt = &now

	preflightJob, preflightPod := successfulMaintenanceObjects(cluster, "preflight", "verify", ordinal, oldImage, 0)
	drainJob, drainPod := successfulMaintenanceObjects(cluster, "drain", "drain", ordinal, oldImage, 1)
	postflightJob, postflightPod := successfulMaintenanceObjects(cluster, "postflight", "verify", ordinal, cluster.Spec.NodeImage, 0)
	dataPod := readyDataPod(cluster, ordinal, cluster.Spec.NodeImage)
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(preflightJob, preflightPod, drainJob, drainPod, postflightJob, postflightPod, dataPod).
		Build()
	reconciler := &EpochClusterReconciler{Client: client, Scheme: scheme}

	if err := reconciler.reconcileUpgrade(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Status.Upgrade.Phase != epochv1alpha1.UpgradePhaseDraining {
		t.Fatalf("preflight did not advance to drain: %#v", cluster.Status.Upgrade)
	}
	if got := *nodeStatefulSet(cluster).Spec.UpdateStrategy.RollingUpdate.Partition; got != 3 {
		t.Fatalf("drain must happen before releasing an ordinal; partition=%d", got)
	}

	if err := reconciler.reconcileUpgrade(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Status.Upgrade.Phase != epochv1alpha1.UpgradePhaseUpdating || !cluster.Status.Upgrade.TargetOrdinalUpdated {
		t.Fatalf("drain did not release exactly one ordinal: %#v", cluster.Status.Upgrade)
	}
	updating := nodeStatefulSet(cluster)
	if got := *updating.Spec.UpdateStrategy.RollingUpdate.Partition; got != 2 {
		t.Fatalf("only ordinal 2 may update, got partition %d", got)
	}
	if got := updating.Spec.Template.Spec.Containers[0].Image; got != cluster.Spec.NodeImage {
		t.Fatalf("target image was not rendered after drain: %q", got)
	}

	if err := reconciler.reconcileUpgrade(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Status.Upgrade.Phase != epochv1alpha1.UpgradePhaseVerifying {
		t.Fatalf("ready target did not advance to postflight: %#v", cluster.Status.Upgrade)
	}
	if err := reconciler.reconcileUpgrade(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Status.Upgrade.Phase != epochv1alpha1.UpgradePhasePreflight ||
		cluster.Status.Upgrade.TargetOrdinal == nil || *cluster.Status.Upgrade.TargetOrdinal != 1 ||
		cluster.Status.Upgrade.UpdatedNodes != 1 {
		t.Fatalf("verified ordinal did not advance serially: %#v", cluster.Status.Upgrade)
	}
	next := nodeStatefulSet(cluster)
	if got := *next.Spec.UpdateStrategy.RollingUpdate.Partition; got != 2 {
		t.Fatalf("ordinal 1 must remain blocked before its own checks, got partition %d", got)
	}
}

func TestUpgradeFailureStartsGuardedRollbackWithoutReleasingAnotherNode(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	cluster := validCluster()
	oldImage := cluster.Spec.NodeImage
	cluster.Status.Upgrade.CurrentNodeImage = oldImage
	cluster.Spec.NodeImage = "ghcr.io/ripan-roy/epoch-node:v0.2.0-beta.2"
	cluster.Status.Upgrade.TargetNodeImage = cluster.Spec.NodeImage
	cluster.Status.Upgrade.RequestID = upgradeRequestID(cluster.Spec.NodeImage, "")
	cluster.Status.Upgrade.Phase = epochv1alpha1.UpgradePhaseVerifying
	ordinal := int32(2)
	cluster.Status.Upgrade.TargetOrdinal = &ordinal
	cluster.Status.Upgrade.TargetOrdinalUpdated = true
	now := metav1.Now()
	cluster.Status.Upgrade.StartedAt = &now
	cluster.Status.Upgrade.StepStartedAt = &now
	failed := maintenanceJob(cluster, "postflight", "verify", ordinal, cluster.Spec.NodeImage)
	failed.Status.Conditions = []batchv1.JobCondition{{
		Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Message: "invariant breach",
	}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(failed).Build()
	reconciler := &EpochClusterReconciler{Client: client, Scheme: scheme}

	if err := reconciler.reconcileUpgrade(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Status.Upgrade.Phase != epochv1alpha1.UpgradePhaseRollbackPreflight ||
		cluster.Status.Upgrade.TargetOrdinal == nil || *cluster.Status.Upgrade.TargetOrdinal != 2 {
		t.Fatalf("failed postflight did not begin rollback: %#v", cluster.Status.Upgrade)
	}
	rollback := nodeStatefulSet(cluster)
	if got := rollback.Spec.Template.Spec.Containers[0].Image; got != oldImage {
		t.Fatalf("rollback did not restore stable template image: %q", got)
	}
	if got := *rollback.Spec.UpdateStrategy.RollingUpdate.Partition; got != 3 {
		t.Fatalf("rollback preflight must freeze every ordinal, got partition %d", got)
	}
}

func successfulMaintenanceObjects(
	cluster *epochv1alpha1.EpochCluster,
	stage string,
	operation string,
	ordinal int32,
	image string,
	transfers int,
) (*batchv1.Job, *corev1.Pod) {
	job := maintenanceJob(cluster, stage, operation, ordinal, image)
	completed := metav1.Now()
	job.Status.Succeeded = 1
	job.Status.CompletionTime = &completed
	state := "verified"
	if operation == "drain" {
		state = "drained"
	}
	receipt, _ := json.Marshal(maintenanceTerminationStatus{
		State: state, Operation: operation, TargetNodeID: uint64(ordinal + 1),
		ObservedNodeIDs: []uint64{1, 2, 3}, GroupsChecked: 2, LeadershipTransfers: transfers,
	})
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      job.Name + "-pod",
			Namespace: job.Namespace,
			Labels:    map[string]string{"job-name": job.Name},
		},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name: "epoch-maintenance",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: 0,
				Message:  string(receipt),
			}},
		}}},
	}
	return job, pod
}

func readyDataPod(cluster *epochv1alpha1.EpochCluster, ordinal int32, image string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      nodeName(cluster) + "-" + strconv.Itoa(int(ordinal)),
			Namespace: cluster.Namespace,
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "epoch-node", Image: image}}},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
			Type: corev1.PodReady, Status: corev1.ConditionTrue,
		}}},
	}
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := epochv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func validCluster() *epochv1alpha1.EpochCluster {
	return &epochv1alpha1.EpochCluster{
		TypeMeta:   metav1.TypeMeta{APIVersion: epochv1alpha1.GroupVersion.String(), Kind: "EpochCluster"},
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "epoch-system", Generation: 7},
		Spec: epochv1alpha1.EpochClusterSpec{
			NodeImage:           "ghcr.io/ripan-roy/epoch-node:v0.1.0-alpha.10",
			ControlImage:        "ghcr.io/ripan-roy/epoch-control:v0.1.0-alpha.10",
			Region:              "ap-south",
			NodeClass:           "general-purpose",
			Replicas:            3,
			Storage:             apiresource.MustParse("10Gi"),
			AuthPolicyConfigMap: "epoch-auth-policy",
			CredentialSecret:    "epoch-control-credentials",
			ServiceType:         corev1.ServiceTypeClusterIP,
			TransportSecurity: epochv1alpha1.TransportSecuritySpec{
				DataPlaneSecret:    "epoch-data-plane-tls",
				ControlPlaneSecret: "epoch-control-plane-tls",
				RegionalServerName: "orders-peer.epoch-system.svc",
			},
			Backup: epochv1alpha1.BackupSpec{
				Schedule:         "*/15 * * * *",
				DestinationPVC:   "epoch-backups",
				EncryptionSecret: "epoch-backup-key",
				KeyID:            "backup-key-2026-08",
				RetentionCount:   7,
			},
		},
	}
}

func policyConfigMap(cluster *epochv1alpha1.EpochCluster) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: cluster.Spec.AuthPolicyConfigMap, Namespace: cluster.Namespace},
		Data:       map[string]string{policyKey: `{"format_version":1}`},
	}
}

func credentialSecret(cluster *epochv1alpha1.EpochCluster) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: cluster.Spec.CredentialSecret, Namespace: cluster.Namespace},
		Data:       map[string][]byte{credentialKey: []byte("not-logged")},
	}
}

func dataPlaneTLSSecret(cluster *epochv1alpha1.EpochCluster) *corev1.Secret {
	return tlsSecret(cluster, cluster.Spec.TransportSecurity.DataPlaneSecret)
}

func controlPlaneTLSSecret(cluster *epochv1alpha1.EpochCluster) *corev1.Secret {
	return tlsSecret(cluster, cluster.Spec.TransportSecurity.ControlPlaneSecret)
}

func backupEncryptionSecret(cluster *epochv1alpha1.EpochCluster) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: cluster.Spec.Backup.EncryptionSecret, Namespace: cluster.Namespace},
		Data:       map[string][]byte{backupKey: make([]byte, 32)},
	}
}

func backupPVC(cluster *epochv1alpha1.EpochCluster) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: cluster.Spec.Backup.DestinationPVC, Namespace: cluster.Namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
}

func tlsSecret(cluster *epochv1alpha1.EpochCluster, name string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace},
		Data: map[string][]byte{
			"ca.crt":  []byte("test-ca"),
			"tls.crt": []byte("test-certificate"),
			"tls.key": []byte("test-private-key"),
		},
	}
}

func containsEnvironment(environment []corev1.EnvVar, name, value string) bool {
	for _, variable := range environment {
		if variable.Name == name && variable.Value == value {
			return true
		}
	}
	return false
}

func containsSecretEnvironment(environment []corev1.EnvVar, name, secret, key string) bool {
	for _, variable := range environment {
		if variable.Name == name && variable.ValueFrom != nil && variable.ValueFrom.SecretKeyRef != nil && variable.ValueFrom.SecretKeyRef.Name == secret && variable.ValueFrom.SecretKeyRef.Key == key {
			return true
		}
	}
	return false
}

func conditionByType(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	for index := range conditions {
		if conditions[index].Type == conditionType {
			return &conditions[index]
		}
	}
	return nil
}
