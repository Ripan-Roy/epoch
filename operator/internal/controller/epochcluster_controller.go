// Package controller reconciles EpochCluster resources into a regional data
// plane and its single-owner control-plane service.
package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	epochv1alpha1 "epoch.local/epoch/operator/api/v1alpha1"
)

const (
	conditionAvailable = "Available"
	conditionProgress  = "Progressing"
	conditionBackup    = "BackupReady"
	policyKey          = "bootstrap-policy.json"
	credentialKey      = "regional-token"
	tlsCAKey           = "ca.crt"
	tlsCertificateKey  = "tls.crt"
	tlsPrivateKeyKey   = "tls.key"
	backupKey          = "encryption.key"
	backupPreviousKey  = "previous."
	backupOwnerLabel   = "platform.epoch.dev/backup-owner"
	requeueAfter       = 5 * time.Second
)

type EpochClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

func (reconciler *EpochClusterReconciler) Reconcile(
	ctx context.Context,
	request ctrl.Request,
) (ctrl.Result, error) {
	cluster := &epochv1alpha1.EpochCluster{}
	if err := reconciler.Get(ctx, request.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if err := validateSpec(&cluster.Spec); err != nil {
		return ctrl.Result{}, reconciler.recordCondition(
			ctx,
			cluster,
			metav1.ConditionFalse,
			"InvalidSpec",
			err.Error(),
		)
	}
	if err := validateRestoreImmutability(cluster); err != nil {
		return ctrl.Result{}, reconciler.recordCondition(
			ctx,
			cluster,
			metav1.ConditionFalse,
			"InvalidRestoreChange",
			err.Error(),
		)
	}
	if missing, err := reconciler.requiredConfiguration(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	} else if missing != "" {
		if err := reconciler.recordCondition(
			ctx,
			cluster,
			metav1.ConditionFalse,
			"ConfigurationMissing",
			missing,
		); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}

	for _, object := range []client.Object{
		peerService(cluster),
		publicService(cluster),
		controlService(cluster),
		controlStatefulSet(cluster),
		backupCronJob(cluster),
	} {
		if err := reconciler.reconcileObject(ctx, cluster, object); err != nil {
			return ctrl.Result{}, err
		}
	}
	if err := reconciler.refreshBackupStatus(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}
	if err := reconciler.reconcileUpgrade(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}
	if err := reconciler.reconcileObject(ctx, cluster, nodeStatefulSet(cluster)); err != nil {
		return ctrl.Result{}, err
	}
	if err := reconciler.refreshStatus(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

func (reconciler *EpochClusterReconciler) SetupWithManager(manager ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(manager).
		For(&epochv1alpha1.EpochCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&batchv1.CronJob{}).
		Owns(&batchv1.Job{}).
		Complete(reconciler)
}

func validateSpec(spec *epochv1alpha1.EpochClusterSpec) error {
	if spec.Replicas < 3 || spec.Replicas > 1024 {
		return fmt.Errorf("replicas must contain between 3 and 1024 physical data nodes")
	}
	catalogReplicas := effectiveCatalogReplicas(spec)
	if (catalogReplicas != 3 && catalogReplicas != 5) || catalogReplicas > spec.Replicas {
		return fmt.Errorf("catalogReplicas must be 3 or 5 and cannot exceed physical replicas")
	}
	for field, value := range map[string]string{
		"nodeImage":                            spec.NodeImage,
		"controlImage":                         spec.ControlImage,
		"authPolicyConfigMap":                  spec.AuthPolicyConfigMap,
		"credentialSecret":                     spec.CredentialSecret,
		"transportSecurity.dataPlaneSecret":    spec.TransportSecurity.DataPlaneSecret,
		"transportSecurity.controlPlaneSecret": spec.TransportSecurity.ControlPlaneSecret,
		"transportSecurity.regionalServerName": spec.TransportSecurity.RegionalServerName,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", field)
		}
	}
	if spec.Storage.IsZero() || spec.Storage.Sign() < 0 {
		return fmt.Errorf("storage must be a positive quantity")
	}
	if spec.ServiceType != "" &&
		spec.ServiceType != corev1.ServiceTypeClusterIP &&
		spec.ServiceType != corev1.ServiceTypeLoadBalancer &&
		spec.ServiceType != corev1.ServiceTypeNodePort {
		return fmt.Errorf("serviceType must be ClusterIP, NodePort, or LoadBalancer")
	}
	if _, err := cron.ParseStandard(spec.Backup.Schedule); err != nil {
		return fmt.Errorf("backup.schedule must be a valid five-field cron schedule: %w", err)
	}
	for field, value := range map[string]string{
		"backup.destinationPVC":   spec.Backup.DestinationPVC,
		"backup.encryptionSecret": spec.Backup.EncryptionSecret,
		"backup.keyID":            spec.Backup.KeyID,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", field)
		}
	}
	if !safeIdentifier(spec.Backup.KeyID, 128) {
		return fmt.Errorf("backup.keyID must be a 1-128 byte safe identifier")
	}
	if spec.Backup.RetentionCount < 1 || spec.Backup.RetentionCount > 10000 {
		return fmt.Errorf("backup.retentionCount must be between 1 and 10000")
	}
	if spec.Upgrade.BackupMaxAgeSeconds != 0 &&
		(spec.Upgrade.BackupMaxAgeSeconds < 60 || spec.Upgrade.BackupMaxAgeSeconds > 604800) {
		return fmt.Errorf("upgrade.backupMaxAgeSeconds must be zero or between 60 and 604800")
	}
	if spec.Upgrade.StepDeadlineSeconds != 0 &&
		(spec.Upgrade.StepDeadlineSeconds < 30 || spec.Upgrade.StepDeadlineSeconds > 86400) {
		return fmt.Errorf("upgrade.stepDeadlineSeconds must be zero or between 30 and 86400")
	}
	if spec.Upgrade.RetryToken != "" && !safeIdentifier(spec.Upgrade.RetryToken, 128) {
		return fmt.Errorf("upgrade.retryToken must be empty or a 1-128 byte safe identifier")
	}
	if spec.Restore != nil {
		if !safeObjectName(spec.Restore.ObjectName) || !strings.HasSuffix(spec.Restore.ObjectName, ".epoch-backup.enc") {
			return fmt.Errorf("restore.objectName must be a safe encrypted backup object name")
		}
		if strings.TrimSpace(spec.Restore.EncryptionSecret) == "" {
			return fmt.Errorf("restore.encryptionSecret is required")
		}
	}
	return nil
}

func validateRestoreImmutability(cluster *epochv1alpha1.EpochCluster) error {
	desiredObject := ""
	desiredSecret := ""
	if cluster.Spec.Restore != nil {
		desiredObject = cluster.Spec.Restore.ObjectName
		desiredSecret = cluster.Spec.Restore.EncryptionSecret
	}
	if cluster.Status.Initialized && (desiredObject != cluster.Status.RestoreObject || desiredSecret != cluster.Status.RestoreEncryptionSecret) {
		return fmt.Errorf("restore reference is immutable after cluster initialization")
	}
	return nil
}

func (reconciler *EpochClusterReconciler) requiredConfiguration(
	ctx context.Context,
	cluster *epochv1alpha1.EpochCluster,
) (string, error) {
	configMap := &corev1.ConfigMap{}
	if err := reconciler.Get(ctx, types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      cluster.Spec.AuthPolicyConfigMap,
	}, configMap); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Sprintf("ConfigMap %s is missing", cluster.Spec.AuthPolicyConfigMap), nil
		}
		return "", err
	}
	if strings.TrimSpace(configMap.Data[policyKey]) == "" {
		return fmt.Sprintf("ConfigMap %s must contain %s", configMap.Name, policyKey), nil
	}
	secret := &corev1.Secret{}
	if err := reconciler.Get(ctx, types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      cluster.Spec.CredentialSecret,
	}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Sprintf("Secret %s is missing", cluster.Spec.CredentialSecret), nil
		}
		return "", err
	}
	if len(secret.Data[credentialKey]) == 0 {
		return fmt.Sprintf("Secret %s must contain %s", secret.Name, credentialKey), nil
	}
	for _, name := range []string{
		cluster.Spec.TransportSecurity.DataPlaneSecret,
		cluster.Spec.TransportSecurity.ControlPlaneSecret,
	} {
		message, err := reconciler.validateTLSSecret(ctx, cluster.Namespace, name)
		if err != nil || message != "" {
			return message, err
		}
	}
	backupSecret := &corev1.Secret{}
	if err := reconciler.Get(ctx, types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      cluster.Spec.Backup.EncryptionSecret,
	}, backupSecret); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Sprintf("backup encryption Secret %s is missing", cluster.Spec.Backup.EncryptionSecret), nil
		}
		return "", err
	}
	if message := validateBackupEncryptionSecret(backupSecret, cluster.Spec.Backup.KeyID); message != "" {
		return message, nil
	}
	backupPVC := &corev1.PersistentVolumeClaim{}
	if err := reconciler.Get(ctx, types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      cluster.Spec.Backup.DestinationPVC,
	}, backupPVC); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Sprintf("backup destination PVC %s is missing", cluster.Spec.Backup.DestinationPVC), nil
		}
		return "", err
	}
	if !containsAccessMode(backupPVC.Spec.AccessModes, corev1.ReadWriteMany) {
		return fmt.Sprintf("backup destination PVC %s must support ReadWriteMany", backupPVC.Name), nil
	}
	if backupPVC.Status.Phase != corev1.ClaimBound {
		return fmt.Sprintf("backup destination PVC %s is not Bound", backupPVC.Name), nil
	}
	if cluster.Spec.Restore != nil && cluster.Spec.Restore.EncryptionSecret != cluster.Spec.Backup.EncryptionSecret {
		restoreSecret := &corev1.Secret{}
		if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Spec.Restore.EncryptionSecret}, restoreSecret); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Sprintf("restore encryption Secret %s is missing", cluster.Spec.Restore.EncryptionSecret), nil
			}
			return "", err
		}
		if len(restoreSecret.Data[backupKey]) != 32 {
			return fmt.Sprintf("restore encryption Secret %s must contain a 32-byte %s", restoreSecret.Name, backupKey), nil
		}
	}
	return "", nil
}

func validateBackupEncryptionSecret(secret *corev1.Secret, activeKeyID string) string {
	active := secret.Data[backupKey]
	if len(active) != 32 {
		return fmt.Sprintf("backup encryption Secret %s must contain a 32-byte %s", secret.Name, backupKey)
	}
	seen := [][]byte{active}
	keys := make([]string, 0, len(secret.Data))
	for key := range secret.Data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if key == backupKey {
			continue
		}
		keyID, isPrevious := strings.CutPrefix(key, backupPreviousKey)
		if !isPrevious || !safeIdentifier(keyID, 128) || keyID == activeKeyID {
			return fmt.Sprintf(
				"backup encryption Secret %s data key %s must be previous.<non-active-safe-key-id>",
				secret.Name,
				key,
			)
		}
		material := secret.Data[key]
		if len(material) != 32 {
			return fmt.Sprintf("backup encryption Secret %s data key %s must contain 32 bytes", secret.Name, key)
		}
		for _, existing := range seen {
			if bytes.Equal(existing, material) {
				return fmt.Sprintf("backup encryption Secret %s contains duplicate key material", secret.Name)
			}
		}
		seen = append(seen, material)
	}
	return ""
}

func (reconciler *EpochClusterReconciler) validateTLSSecret(
	ctx context.Context,
	namespace string,
	name string,
) (string, error) {
	secret := &corev1.Secret{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Sprintf("TLS Secret %s is missing", name), nil
		}
		return "", err
	}
	for _, key := range []string{tlsCAKey, tlsCertificateKey, tlsPrivateKeyKey} {
		if len(secret.Data[key]) == 0 {
			return fmt.Sprintf("TLS Secret %s must contain %s", name, key), nil
		}
	}
	return "", nil
}

func (reconciler *EpochClusterReconciler) reconcileObject(
	ctx context.Context,
	cluster *epochv1alpha1.EpochCluster,
	desired client.Object,
) error {
	if err := controllerutil.SetControllerReference(cluster, desired, reconciler.Scheme); err != nil {
		return err
	}
	current := desired.DeepCopyObject().(client.Object)
	key := client.ObjectKeyFromObject(desired)
	err := reconciler.Get(ctx, key, current)
	if apierrors.IsNotFound(err) {
		return reconciler.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	if service, ok := desired.(*corev1.Service); ok {
		currentService := current.(*corev1.Service)
		service.Spec.ClusterIP = currentService.Spec.ClusterIP
		service.Spec.ClusterIPs = append([]string(nil), currentService.Spec.ClusterIPs...)
		service.Spec.IPFamilies = append([]corev1.IPFamily(nil), currentService.Spec.IPFamilies...)
		service.Spec.IPFamilyPolicy = currentService.Spec.IPFamilyPolicy
		service.Spec.HealthCheckNodePort = currentService.Spec.HealthCheckNodePort
		for desiredIndex := range service.Spec.Ports {
			for _, currentPort := range currentService.Spec.Ports {
				if currentPort.Name == service.Spec.Ports[desiredIndex].Name {
					service.Spec.Ports[desiredIndex].NodePort = currentPort.NodePort
				}
			}
		}
	}
	if objectMatchesDesired(desired, current) {
		return nil
	}
	desired.SetResourceVersion(current.GetResourceVersion())
	return reconciler.Update(ctx, desired)
}

func objectMatchesDesired(desired, current client.Object) bool {
	if !apiequality.Semantic.DeepEqual(desired.GetLabels(), current.GetLabels()) ||
		!apiequality.Semantic.DeepEqual(desired.GetAnnotations(), current.GetAnnotations()) ||
		!apiequality.Semantic.DeepEqual(desired.GetOwnerReferences(), current.GetOwnerReferences()) {
		return false
	}
	switch wanted := desired.(type) {
	case *corev1.Service:
		observed, ok := current.(*corev1.Service)
		return ok && apiequality.Semantic.DeepDerivative(wanted.Spec, observed.Spec)
	case *appsv1.StatefulSet:
		observed, ok := current.(*appsv1.StatefulSet)
		return ok && apiequality.Semantic.DeepDerivative(wanted.Spec, observed.Spec)
	case *batchv1.CronJob:
		observed, ok := current.(*batchv1.CronJob)
		return ok && apiequality.Semantic.DeepDerivative(wanted.Spec, observed.Spec)
	default:
		return false
	}
}

func (reconciler *EpochClusterReconciler) refreshStatus(
	ctx context.Context,
	cluster *epochv1alpha1.EpochCluster,
) error {
	nodes := &appsv1.StatefulSet{}
	control := &appsv1.StatefulSet{}
	_ = reconciler.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: nodeName(cluster)}, nodes)
	_ = reconciler.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: controlName(cluster)}, control)
	cluster.Status.ObservedGeneration = cluster.Generation
	cluster.Status.ReadyNodes = nodes.Status.ReadyReplicas
	cluster.Status.ControlReady = control.Status.ReadyReplicas == 1
	cluster.Status.Endpoint = fmt.Sprintf("https://%s.%s.svc:8080", controlName(cluster), cluster.Namespace)
	cluster.Status.Initialized = true
	cluster.Status.RestoreObject = ""
	cluster.Status.RestoreEncryptionSecret = ""
	if cluster.Spec.Restore != nil {
		cluster.Status.RestoreObject = cluster.Spec.Restore.ObjectName
		cluster.Status.RestoreEncryptionSecret = cluster.Spec.Restore.EncryptionSecret
	}
	if err := reconciler.refreshBackupStatus(ctx, cluster); err != nil {
		return err
	}
	available := nodes.Status.ReadyReplicas == cluster.Spec.Replicas && cluster.Status.ControlReady
	status := metav1.ConditionFalse
	reason := "ComponentsPending"
	message := fmt.Sprintf("%d/%d data nodes and %d/1 control replicas are ready", nodes.Status.ReadyReplicas, cluster.Spec.Replicas, control.Status.ReadyReplicas)
	if available {
		status = metav1.ConditionTrue
		reason = "ComponentsReady"
		message = "bounded-voter data plane and control plane are ready"
	}
	apimeta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               conditionAvailable,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: cluster.Generation,
	})
	apimeta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               conditionProgress,
		Status:             boolCondition(!available || cluster.Status.Upgrade.Phase != epochv1alpha1.UpgradePhaseStable),
		Reason:             reason,
		Message:            message,
		ObservedGeneration: cluster.Generation,
	})
	upgradeCondition := metav1.Condition{
		Type:               conditionUpgrade,
		Status:             metav1.ConditionFalse,
		Reason:             string(cluster.Status.Upgrade.Phase),
		Message:            cluster.Status.Upgrade.Message,
		ObservedGeneration: cluster.Generation,
	}
	if cluster.Status.Upgrade.Phase == epochv1alpha1.UpgradePhaseStable {
		upgradeCondition.Status = metav1.ConditionTrue
		upgradeCondition.Reason = "Stable"
	}
	if upgradeCondition.Reason == "" {
		upgradeCondition.Reason = "Initializing"
	}
	if upgradeCondition.Message == "" {
		upgradeCondition.Message = "upgrade state is initializing"
	}
	apimeta.SetStatusCondition(&cluster.Status.Conditions, upgradeCondition)
	backupCondition := metav1.Condition{
		Type:               conditionBackup,
		Status:             metav1.ConditionFalse,
		Reason:             "BackupPending",
		Message:            "no scheduled backup has completed successfully",
		ObservedGeneration: cluster.Generation,
	}
	if cluster.Status.Backup.LastSuccessfulTime != nil {
		backupCondition.Status = metav1.ConditionTrue
		backupCondition.Reason = "BackupSucceeded"
		backupCondition.Message = fmt.Sprintf("encrypted backup %s completed successfully", cluster.Status.Backup.LastSuccessfulObject)
	}
	if cluster.Status.Backup.LastFailureTime != nil &&
		(cluster.Status.Backup.LastSuccessfulTime == nil || cluster.Status.Backup.LastFailureTime.After(cluster.Status.Backup.LastSuccessfulTime.Time)) {
		backupCondition.Status = metav1.ConditionFalse
		backupCondition.Reason = "BackupFailed"
		backupCondition.Message = cluster.Status.Backup.LastFailureMessage
	}
	apimeta.SetStatusCondition(&cluster.Status.Conditions, backupCondition)
	return reconciler.Status().Update(ctx, cluster)
}

type backupTerminationStatus struct {
	State           string `json:"state"`
	ObjectName      string `json:"object_name"`
	CapturedAtMS    uint64 `json:"captured_at_ms"`
	ManifestSHA256  string `json:"manifest_sha256"`
	PlaintextSHA256 string `json:"plaintext_sha256"`
	KeyID           string `json:"key_id"`
	RetainedObjects int32  `json:"retained_objects"`
}

func (reconciler *EpochClusterReconciler) refreshBackupStatus(
	ctx context.Context,
	cluster *epochv1alpha1.EpochCluster,
) error {
	cluster.Status.Backup.ObservedSchedule = cluster.Spec.Backup.Schedule
	jobs := &batchv1.JobList{}
	if err := reconciler.List(
		ctx,
		jobs,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{backupOwnerLabel: cluster.Name},
	); err != nil {
		return err
	}
	for index := range jobs.Items {
		job := &jobs.Items[index]
		if job.Status.Succeeded > 0 && job.Status.CompletionTime != nil &&
			(cluster.Status.Backup.LastSuccessfulTime == nil || job.Status.CompletionTime.After(cluster.Status.Backup.LastSuccessfulTime.Time)) {
			status, err := reconciler.backupTerminationStatus(ctx, job)
			if err != nil {
				return err
			}
			if status != nil {
				completed := job.Status.CompletionTime.DeepCopy()
				cluster.Status.Backup.LastSuccessfulTime = completed
				cluster.Status.Backup.LastSuccessfulObject = status.ObjectName
				cluster.Status.Backup.LastManifestSHA256 = status.ManifestSHA256
				cluster.Status.Backup.LastKeyID = status.KeyID
				cluster.Status.Backup.RetainedObjects = status.RetainedObjects
			}
		}
		for _, condition := range job.Status.Conditions {
			if condition.Type != batchv1.JobFailed || condition.Status != corev1.ConditionTrue {
				continue
			}
			failedAt := condition.LastTransitionTime
			if cluster.Status.Backup.LastFailureTime == nil || failedAt.After(cluster.Status.Backup.LastFailureTime.Time) {
				cluster.Status.Backup.LastFailureTime = failedAt.DeepCopy()
				cluster.Status.Backup.LastFailureMessage = boundedStatusMessage(condition.Message)
				if cluster.Status.Backup.LastFailureMessage == "" {
					cluster.Status.Backup.LastFailureMessage = "scheduled encrypted backup job failed"
				}
			}
		}
	}
	return nil
}

func (reconciler *EpochClusterReconciler) backupTerminationStatus(
	ctx context.Context,
	job *batchv1.Job,
) (*backupTerminationStatus, error) {
	pods := &corev1.PodList{}
	if err := reconciler.List(
		ctx,
		pods,
		client.InNamespace(job.Namespace),
		client.MatchingLabels{"job-name": job.Name},
	); err != nil {
		return nil, err
	}
	for _, pod := range pods.Items {
		for _, container := range pod.Status.ContainerStatuses {
			terminated := container.State.Terminated
			if container.Name != "epoch-backup" || terminated == nil || terminated.ExitCode != 0 {
				continue
			}
			if len(terminated.Message) == 0 || len(terminated.Message) > 4096 {
				continue
			}
			status := &backupTerminationStatus{}
			decoder := json.NewDecoder(strings.NewReader(terminated.Message))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(status); err != nil || status.State != "succeeded" ||
				!safeObjectName(status.ObjectName) || !sha256Hex(status.ManifestSHA256) ||
				!sha256Hex(status.PlaintextSHA256) || !safeIdentifier(status.KeyID, 128) ||
				status.CapturedAtMS == 0 || status.RetainedObjects < 1 || status.RetainedObjects > 10_000 {
				continue
			}
			return status, nil
		}
	}
	return nil, nil
}

func (reconciler *EpochClusterReconciler) recordCondition(
	ctx context.Context,
	cluster *epochv1alpha1.EpochCluster,
	status metav1.ConditionStatus,
	reason string,
	message string,
) error {
	cluster.Status.ObservedGeneration = cluster.Generation
	apimeta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               conditionAvailable,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: cluster.Generation,
	})
	return reconciler.Status().Update(ctx, cluster)
}

func boolCondition(value bool) metav1.ConditionStatus {
	if value {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}

func labels(cluster *epochv1alpha1.EpochCluster, component string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "epoch",
		"app.kubernetes.io/instance":   cluster.Name,
		"app.kubernetes.io/component":  component,
		"app.kubernetes.io/managed-by": "epoch-operator",
	}
}

func nodeName(cluster *epochv1alpha1.EpochCluster) string    { return cluster.Name + "-node" }
func peerName(cluster *epochv1alpha1.EpochCluster) string    { return cluster.Name + "-peer" }
func publicName(cluster *epochv1alpha1.EpochCluster) string  { return cluster.Name }
func controlName(cluster *epochv1alpha1.EpochCluster) string { return cluster.Name + "-control" }
func backupName(cluster *epochv1alpha1.EpochCluster) string  { return cluster.Name + "-backup" }

func backupCronJob(cluster *epochv1alpha1.EpochCluster) *batchv1.CronJob {
	endpoints := make([]string, cluster.Spec.Replicas)
	for index := range endpoints {
		endpoints[index] = fmt.Sprintf("https://%s-%d.%s:7601/", nodeName(cluster), index, peerName(cluster))
	}
	forbid := batchv1.ForbidConcurrent
	startingDeadline := int64(300)
	activeDeadline := int64(600)
	backoffLimit := int32(2)
	successHistory := int32(3)
	failureHistory := int32(3)
	jobLabels := labels(cluster, "backup")
	jobLabels[backupOwnerLabel] = cluster.Name
	return &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backupName(cluster),
			Namespace: cluster.Namespace,
			Labels:    jobLabels,
		},
		Spec: batchv1.CronJobSpec{
			Schedule:                   cluster.Spec.Backup.Schedule,
			ConcurrencyPolicy:          forbid,
			StartingDeadlineSeconds:    &startingDeadline,
			SuccessfulJobsHistoryLimit: &successHistory,
			FailedJobsHistoryLimit:     &failureHistory,
			JobTemplate: batchv1.JobTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: jobLabels},
				Spec: batchv1.JobSpec{
					BackoffLimit:          &backoffLimit,
					ActiveDeadlineSeconds: &activeDeadline,
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: jobLabels},
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							SecurityContext: &corev1.PodSecurityContext{
								RunAsNonRoot: boolPointer(true),
								FSGroup:      int64Pointer(10001),
							},
							Containers: []corev1.Container{{
								Name:            "epoch-backup",
								Image:           operationalNodeImage(cluster),
								ImagePullPolicy: corev1.PullIfNotPresent,
								Command:         []string{"/usr/local/bin/epoch-backup", "capture"},
								Env: []corev1.EnvVar{
									{Name: "EPOCH_BACKUP_ENDPOINTS", Value: strings.Join(endpoints, ",")},
									{Name: "EPOCH_BACKUP_TOKEN_PATH", Value: "/etc/epoch/credential/" + credentialKey},
									{Name: "EPOCH_BACKUP_TLS_CA_PATH", Value: "/etc/epoch/tls/" + tlsCAKey},
									{Name: "EPOCH_BACKUP_TLS_CERT_PATH", Value: "/etc/epoch/tls/" + tlsCertificateKey},
									{Name: "EPOCH_BACKUP_TLS_KEY_PATH", Value: "/etc/epoch/tls/" + tlsPrivateKeyKey},
									{Name: "EPOCH_BACKUP_ENCRYPTION_KEY_PATH", Value: "/etc/epoch/backup-key/" + backupKey},
									{Name: "EPOCH_BACKUP_KEY_ID", Value: cluster.Spec.Backup.KeyID},
									{Name: "EPOCH_BACKUP_OUTPUT_DIR", Value: "/var/lib/epoch-backups"},
									{Name: "EPOCH_BACKUP_RETENTION_COUNT", Value: strconv.Itoa(int(cluster.Spec.Backup.RetentionCount))},
									{Name: "EPOCH_BACKUP_STATUS_PATH", Value: "/dev/termination-log"},
								},
								Resources: cluster.Spec.ControlResources,
								SecurityContext: &corev1.SecurityContext{
									AllowPrivilegeEscalation: boolPointer(false),
									ReadOnlyRootFilesystem:   boolPointer(true),
									Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
								},
								VolumeMounts: []corev1.VolumeMount{
									{Name: "backups", MountPath: "/var/lib/epoch-backups"},
									{Name: "credential", MountPath: "/etc/epoch/credential", ReadOnly: true},
									{Name: "transport-tls", MountPath: "/etc/epoch/tls", ReadOnly: true},
									{Name: "backup-key", MountPath: "/etc/epoch/backup-key", ReadOnly: true},
								},
							}},
							Volumes: []corev1.Volume{
								{Name: "backups", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: cluster.Spec.Backup.DestinationPVC}}},
								{Name: "credential", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: cluster.Spec.CredentialSecret}}},
								{Name: "transport-tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: cluster.Spec.TransportSecurity.ControlPlaneSecret}}},
								{Name: "backup-key", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: cluster.Spec.Backup.EncryptionSecret}}},
							},
						},
					},
				},
			},
		},
	}
}

func peerService(cluster *epochv1alpha1.EpochCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: peerName(cluster), Namespace: cluster.Namespace, Labels: labels(cluster, "data-plane")},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  labels(cluster, "data-plane"),
			Ports: []corev1.ServicePort{
				{Name: "https", Port: 7601, TargetPort: intstr.FromString("https")},
				{Name: "peer-tls", Port: 7701, TargetPort: intstr.FromString("peer-tls")},
			},
		},
	}
}

func publicService(cluster *epochv1alpha1.EpochCluster) *corev1.Service {
	serviceType := cluster.Spec.ServiceType
	if serviceType == "" {
		serviceType = corev1.ServiceTypeClusterIP
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: publicName(cluster), Namespace: cluster.Namespace, Labels: labels(cluster, "data-plane")},
		Spec: corev1.ServiceSpec{
			Type:     serviceType,
			Selector: labels(cluster, "data-plane"),
			Ports:    []corev1.ServicePort{{Name: "https", Port: 7601, TargetPort: intstr.FromString("https")}},
		},
	}
}

func controlService(cluster *epochv1alpha1.EpochCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: controlName(cluster), Namespace: cluster.Namespace, Labels: labels(cluster, "control-plane")},
		Spec: corev1.ServiceSpec{
			Selector: labels(cluster, "control-plane"),
			Ports: []corev1.ServicePort{
				{Name: "https", Port: 8080, TargetPort: intstr.FromString("https")},
				{Name: "grpcs", Port: 8081, TargetPort: intstr.FromString("grpcs")},
			},
		},
	}
}

func nodeStatefulSet(cluster *epochv1alpha1.EpochCluster) *appsv1.StatefulSet {
	name := nodeName(cluster)
	nodeImage := nodeRolloutImage(cluster)
	partition := nodeRolloutPartition(cluster)
	peers := make([]string, cluster.Spec.Replicas)
	for index := range peers {
		peers[index] = fmt.Sprintf("%d=https://%s-%d.%s:7701", index+1, name, index, peerName(cluster))
	}
	selector := labels(cluster, "data-plane")
	initialVoters := make([]string, effectiveCatalogReplicas(&cluster.Spec))
	for index := range initialVoters {
		initialVoters[index] = strconv.Itoa(index + 1)
	}
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace, Labels: selector},
		Spec: appsv1.StatefulSetSpec{
			ServiceName:         peerName(cluster),
			Replicas:            &cluster.Spec.Replicas,
			PodManagementPolicy: appsv1.ParallelPodManagement,
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
				RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
					Partition: &partition,
				},
			},
			Selector: &metav1.LabelSelector{MatchLabels: selector},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: selector},
				Spec: corev1.PodSpec{
					NodeSelector: cluster.Spec.NodeSelector,
					Tolerations:  cluster.Spec.Tolerations,
					Affinity:     effectiveAffinity(cluster),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: boolPointer(true),
						FSGroup:      int64Pointer(10001),
					},
					Containers: []corev1.Container{{
						Name:            "epoch-node",
						Image:           nodeImage,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Command:         []string{"/bin/sh", "-ec"},
						Args:            []string{`ordinal="${HOSTNAME##*-}"; export EPOCH_CONSENSUS_NODE_ID="$((ordinal + 1))"; exec /usr/local/bin/epoch-node`},
						Env: []corev1.EnvVar{
							{Name: "EPOCH_HTTP_LISTEN", Value: "0.0.0.0:7601"},
							{Name: "EPOCH_DATA_DIR", Value: "/var/lib/epoch"},
							{Name: "EPOCH_REGIONAL_RUNTIME_ENABLED", Value: "true"},
							{Name: "EPOCH_AUTH_POLICY_PATH", Value: "/etc/epoch/auth/bootstrap-policy.json"},
							{Name: "EPOCH_CONSENSUS_LISTEN", Value: "0.0.0.0:7701"},
							{Name: "EPOCH_CONSENSUS_PEERS", Value: strings.Join(peers, ",")},
							{Name: "EPOCH_CONSENSUS_INITIAL_VOTERS", Value: strings.Join(initialVoters, ",")},
							{Name: "EPOCH_REGIONAL_REGION", Value: effectiveRegion(cluster)},
							{Name: "EPOCH_REGIONAL_ZONE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"}}},
							{Name: "EPOCH_REGIONAL_NODE_CLASS", Value: effectiveNodeClass(cluster)},
							{Name: "EPOCH_TLS_REQUIRED", Value: "true"},
							{Name: "EPOCH_HTTP_TLS_CERT_PATH", Value: "/etc/epoch/tls/tls.crt"},
							{Name: "EPOCH_HTTP_TLS_KEY_PATH", Value: "/etc/epoch/tls/tls.key"},
							{Name: "EPOCH_HTTP_TLS_CLIENT_CA_PATH", Value: "/etc/epoch/tls/ca.crt"},
							{Name: "EPOCH_PEER_TLS_CA_PATH", Value: "/etc/epoch/tls/ca.crt"},
							{Name: "EPOCH_PEER_TLS_CERT_PATH", Value: "/etc/epoch/tls/tls.crt"},
							{Name: "EPOCH_PEER_TLS_KEY_PATH", Value: "/etc/epoch/tls/tls.key"},
						},
						Ports:          []corev1.ContainerPort{{Name: "https", ContainerPort: 7601}, {Name: "peer-tls", ContainerPort: 7701}},
						Resources:      cluster.Spec.NodeResources,
						ReadinessProbe: tlsSocketProbe("https"),
						LivenessProbe:  tlsSocketProbe("https"),
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: boolPointer(false),
							ReadOnlyRootFilesystem:   boolPointer(true),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/var/lib/epoch"}, {Name: "auth-policy", MountPath: "/etc/epoch/auth", ReadOnly: true}, {Name: "transport-tls", MountPath: "/etc/epoch/tls", ReadOnly: true}},
					}},
					Volumes: []corev1.Volume{
						{Name: "auth-policy", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: cluster.Spec.AuthPolicyConfigMap}}}},
						{Name: "transport-tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: cluster.Spec.TransportSecurity.DataPlaneSecret}}},
					},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "data"},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					StorageClassName: cluster.Spec.StorageClassName,
					Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: cluster.Spec.Storage}},
				},
			}},
		},
	}
	configureRestore(statefulSet, cluster)
	return statefulSet
}

func configureRestore(statefulSet *appsv1.StatefulSet, cluster *epochv1alpha1.EpochCluster) {
	if cluster.Spec.Restore == nil {
		return
	}
	pod := &statefulSet.Spec.Template.Spec
	node := &pod.Containers[0]
	node.Args = []string{`ordinal="${HOSTNAME##*-}"; export EPOCH_CONSENSUS_NODE_ID="$((ordinal + 1))"; if [ -f /var/lib/epoch-restore/backup.json ]; then export EPOCH_REGIONAL_RESTORE_PATH=/var/lib/epoch-restore/backup.json; fi; exec /usr/local/bin/epoch-node`}
	node.VolumeMounts = append(node.VolumeMounts,
		corev1.VolumeMount{Name: "restore", MountPath: "/var/lib/epoch-restore", ReadOnly: true},
	)
	pod.InitContainers = []corev1.Container{{
		Name:            "epoch-restore",
		Image:           nodeRolloutImage(cluster),
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"/bin/sh", "-ec"},
		Args:            []string{`rm -f /restore/backup.json; if [ ! -f /var/lib/epoch/.epoch-regional-restore-complete ]; then rm -rf /var/lib/epoch/consensus; exec /usr/local/bin/epoch-backup decrypt --input "/backups/${EPOCH_RESTORE_OBJECT}" --encryption-key /etc/epoch/backup-key/encryption.key --output /restore/backup.json; fi`},
		Env:             []corev1.EnvVar{{Name: "EPOCH_RESTORE_OBJECT", Value: cluster.Spec.Restore.ObjectName}},
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: boolPointer(false),
			ReadOnlyRootFilesystem:   boolPointer(true),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "data", MountPath: "/var/lib/epoch"},
			{Name: "backups", MountPath: "/backups", ReadOnly: true},
			{Name: "backup-key", MountPath: "/etc/epoch/backup-key", ReadOnly: true},
			{Name: "restore", MountPath: "/restore"},
		},
	}}
	pod.Volumes = append(pod.Volumes,
		corev1.Volume{Name: "backups", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: cluster.Spec.Backup.DestinationPVC, ReadOnly: true}}},
		corev1.Volume{Name: "backup-key", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: cluster.Spec.Restore.EncryptionSecret}}},
		corev1.Volume{Name: "restore", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	)
}

func controlStatefulSet(cluster *epochv1alpha1.EpochCluster) *appsv1.StatefulSet {
	one := int32(1)
	selector := labels(cluster, "control-plane")
	endpoints := make([]string, cluster.Spec.Replicas)
	for index := range endpoints {
		endpoints[index] = fmt.Sprintf("https://%s-%d.%s:7601", nodeName(cluster), index, peerName(cluster))
	}
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: controlName(cluster), Namespace: cluster.Namespace, Labels: selector},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: controlName(cluster),
			Replicas:    &one,
			Selector:    &metav1.LabelSelector{MatchLabels: selector},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: selector},
				Spec: corev1.PodSpec{
					SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: boolPointer(true), FSGroup: int64Pointer(10001)},
					Containers: []corev1.Container{{
						Name:            "epoch-control",
						Image:           cluster.Spec.ControlImage,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Env: []corev1.EnvVar{
							{Name: "EPOCH_CONTROL_ADDR", Value: ":8080"},
							{Name: "EPOCH_CONTROL_GRPC_ADDR", Value: ":8081"},
							{Name: "EPOCH_CONTROL_STATE_PATH", Value: "/var/lib/epoch-control/registry.db"},
							{Name: "EPOCH_CONTROL_REGIONAL_ENDPOINTS", Value: strings.Join(endpoints, ",")},
							{Name: "EPOCH_AUTH_POLICY_PATH", Value: "/etc/epoch/auth/bootstrap-policy.json"},
							{Name: "EPOCH_CONTROL_REGIONAL_TOKEN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: cluster.Spec.CredentialSecret}, Key: credentialKey}}},
							{Name: "EPOCH_CONTROL_ALLOWED_ORIGINS", Value: strings.Join(cluster.Spec.AllowedOrigins, ",")},
							{Name: "EPOCH_CONTROL_TLS_REQUIRED", Value: "true"},
							{Name: "EPOCH_CONTROL_TLS_CERT_PATH", Value: "/etc/epoch/tls/tls.crt"},
							{Name: "EPOCH_CONTROL_TLS_KEY_PATH", Value: "/etc/epoch/tls/tls.key"},
							{Name: "EPOCH_CONTROL_TLS_CLIENT_CA_PATH", Value: "/etc/epoch/tls/ca.crt"},
							{Name: "EPOCH_CONTROL_REGIONAL_TLS_CA_PATH", Value: "/etc/epoch/tls/ca.crt"},
							{Name: "EPOCH_CONTROL_REGIONAL_TLS_CERT_PATH", Value: "/etc/epoch/tls/tls.crt"},
							{Name: "EPOCH_CONTROL_REGIONAL_TLS_KEY_PATH", Value: "/etc/epoch/tls/tls.key"},
							{Name: "EPOCH_CONTROL_REGIONAL_TLS_SERVER_NAME", Value: cluster.Spec.TransportSecurity.RegionalServerName},
						},
						Ports:           []corev1.ContainerPort{{Name: "https", ContainerPort: 8080}, {Name: "grpcs", ContainerPort: 8081}},
						Resources:       cluster.Spec.ControlResources,
						ReadinessProbe:  tlsSocketProbe("https"),
						LivenessProbe:   tlsSocketProbe("https"),
						SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: boolPointer(false), ReadOnlyRootFilesystem: boolPointer(true), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
						VolumeMounts:    []corev1.VolumeMount{{Name: "data", MountPath: "/var/lib/epoch-control"}, {Name: "auth-policy", MountPath: "/etc/epoch/auth", ReadOnly: true}, {Name: "transport-tls", MountPath: "/etc/epoch/tls", ReadOnly: true}},
					}},
					Volumes: []corev1.Volume{
						{Name: "auth-policy", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: cluster.Spec.AuthPolicyConfigMap}}}},
						{Name: "transport-tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: cluster.Spec.TransportSecurity.ControlPlaneSecret}}},
					},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "data"},
				Spec:       corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: apiresource.MustParse("1Gi")}}},
			}},
		},
	}
}

func effectiveAffinity(cluster *epochv1alpha1.EpochCluster) *corev1.Affinity {
	if cluster.Spec.Affinity != nil {
		return cluster.Spec.Affinity.DeepCopy()
	}
	return &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
		LabelSelector: &metav1.LabelSelector{MatchLabels: labels(cluster, "data-plane")},
		TopologyKey:   "kubernetes.io/hostname",
	}}}}
}

func effectiveRegion(cluster *epochv1alpha1.EpochCluster) string {
	if region := strings.TrimSpace(cluster.Spec.Region); region != "" {
		return region
	}
	return "local"
}

func effectiveNodeClass(cluster *epochv1alpha1.EpochCluster) string {
	if nodeClass := strings.TrimSpace(cluster.Spec.NodeClass); nodeClass != "" {
		return nodeClass
	}
	return "general-purpose"
}

func effectiveCatalogReplicas(spec *epochv1alpha1.EpochClusterSpec) int32 {
	if spec.CatalogReplicas == 0 {
		return 3
	}
	return spec.CatalogReplicas
}

func containsAccessMode(modes []corev1.PersistentVolumeAccessMode, wanted corev1.PersistentVolumeAccessMode) bool {
	for _, mode := range modes {
		if mode == wanted {
			return true
		}
	}
	return false
}

func safeIdentifier(value string, maximum int) bool {
	if len(value) < 1 || len(value) > maximum {
		return false
	}
	for _, character := range []byte(value) {
		if !(character >= 'a' && character <= 'z') &&
			!(character >= 'A' && character <= 'Z') &&
			!(character >= '0' && character <= '9') &&
			character != '.' && character != '_' && character != '-' {
			return false
		}
	}
	return true
}

func sha256Hex(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func safeObjectName(value string) bool {
	return safeIdentifier(value, 255) && value != "." && value != ".."
}

func boundedStatusMessage(message string) string {
	message = strings.TrimSpace(message)
	if len(message) > 1024 {
		return message[:1024]
	}
	return message
}

func tlsSocketProbe(port string) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler:        corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString(port)}},
		InitialDelaySeconds: 5,
		PeriodSeconds:       5,
		TimeoutSeconds:      2,
		FailureThreshold:    6,
	}
}

func boolPointer(value bool) *bool    { return &value }
func int64Pointer(value int64) *int64 { return &value }
