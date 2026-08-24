package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	epochv1alpha1 "epoch.local/epoch/operator/api/v1alpha1"
)

const (
	conditionUpgrade       = "UpgradeReady"
	upgradeOwnerLabel      = "platform.epoch.dev/upgrade-owner"
	upgradeRequestLabel    = "platform.epoch.dev/upgrade-request"
	upgradeStageLabel      = "platform.epoch.dev/upgrade-stage"
	defaultBackupMaxAge    = time.Hour
	defaultStepDeadline    = 15 * time.Minute
	maintenanceStatusLimit = 4 * 1024
)

type maintenanceTerminationStatus struct {
	State               string   `json:"state"`
	Operation           string   `json:"operation"`
	TargetNodeID        uint64   `json:"target_node_id"`
	ObservedNodeIDs     []uint64 `json:"observed_node_ids"`
	GroupsChecked       int      `json:"groups_checked"`
	LeadershipTransfers int      `json:"leadership_transfers"`
}

func (reconciler *EpochClusterReconciler) reconcileUpgrade(
	ctx context.Context,
	cluster *epochv1alpha1.EpochCluster,
) error {
	if err := reconciler.initializeUpgradeStatus(ctx, cluster); err != nil {
		return err
	}
	status := &cluster.Status.Upgrade
	requestID := upgradeRequestID(cluster.Spec.NodeImage, cluster.Spec.Upgrade.RetryToken)

	if cluster.Spec.NodeImage == status.CurrentNodeImage {
		if status.Phase == epochv1alpha1.UpgradePhaseFailed &&
			(status.UpdatedNodes > 0 || status.TargetOrdinalUpdated) {
			startUpgradeStep(cluster, epochv1alpha1.UpgradePhaseRollbackPreflight, cluster.Spec.Replicas-1, "desired image reverted; preparing guarded rollback")
			return nil
		}
		resetStableUpgrade(status)
		return nil
	}
	if status.TargetNodeImage != cluster.Spec.NodeImage || status.RequestID != requestID {
		startUpgradeRequest(cluster, requestID)
		return nil
	}

	switch status.Phase {
	case epochv1alpha1.UpgradePhaseWaitingForBackup:
		return reconciler.startUpgradeAfterBackup(ctx, cluster)
	case epochv1alpha1.UpgradePhasePreflight:
		return reconciler.runForwardMaintenance(ctx, cluster, "preflight", "verify", epochv1alpha1.UpgradePhaseDraining)
	case epochv1alpha1.UpgradePhaseDraining:
		return reconciler.runForwardMaintenance(ctx, cluster, "drain", "drain", epochv1alpha1.UpgradePhaseUpdating)
	case epochv1alpha1.UpgradePhaseUpdating:
		return reconciler.waitForForwardPod(ctx, cluster)
	case epochv1alpha1.UpgradePhaseVerifying:
		return reconciler.finishForwardVerification(ctx, cluster)
	case epochv1alpha1.UpgradePhaseRollbackPreflight:
		return reconciler.prepareRollbackOrdinal(ctx, cluster)
	case epochv1alpha1.UpgradePhaseRollbackDraining:
		return reconciler.runRollbackMaintenance(ctx, cluster, "rollback-drain", "drain", epochv1alpha1.UpgradePhaseRollbackUpdating)
	case epochv1alpha1.UpgradePhaseRollbackUpdating:
		return reconciler.waitForRollbackPod(ctx, cluster)
	case epochv1alpha1.UpgradePhaseRollbackVerifying:
		return reconciler.finishRollbackVerification(ctx, cluster)
	case epochv1alpha1.UpgradePhaseFailed:
		return nil
	case epochv1alpha1.UpgradePhaseStable, "":
		startUpgradeRequest(cluster, requestID)
		return nil
	default:
		return fmt.Errorf("unsupported persisted upgrade phase %q", status.Phase)
	}
}

func (reconciler *EpochClusterReconciler) initializeUpgradeStatus(
	ctx context.Context,
	cluster *epochv1alpha1.EpochCluster,
) error {
	status := &cluster.Status.Upgrade
	if status.CurrentNodeImage != "" {
		return nil
	}
	currentImage := cluster.Spec.NodeImage
	nodes := nodeStatefulSetShell(cluster)
	if err := reconciler.Get(ctx, client.ObjectKeyFromObject(nodes), nodes); err == nil {
		if len(nodes.Spec.Template.Spec.Containers) == 0 || strings.TrimSpace(nodes.Spec.Template.Spec.Containers[0].Image) == "" {
			return fmt.Errorf("existing data StatefulSet has no node image")
		}
		currentImage = nodes.Spec.Template.Spec.Containers[0].Image
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	status.CurrentNodeImage = currentImage
	status.Phase = epochv1alpha1.UpgradePhaseStable
	status.Message = "data-plane image is stable"
	return nil
}

func startUpgradeRequest(cluster *epochv1alpha1.EpochCluster, requestID string) {
	now := metav1.Now()
	status := &cluster.Status.Upgrade
	status.TargetNodeImage = cluster.Spec.NodeImage
	status.RequestID = requestID
	status.Phase = epochv1alpha1.UpgradePhaseWaitingForBackup
	status.TargetOrdinal = nil
	status.StartedAt = &now
	status.StepStartedAt = &now
	status.FailureMessage = ""
	status.Message = "waiting for a successful encrypted backup captured after the upgrade request"
	status.UpdatedNodes = 0
	status.TargetOrdinalUpdated = false
}

func resetStableUpgrade(status *epochv1alpha1.UpgradeStatus) {
	status.TargetNodeImage = ""
	status.RequestID = ""
	status.Phase = epochv1alpha1.UpgradePhaseStable
	status.TargetOrdinal = nil
	status.StartedAt = nil
	status.StepStartedAt = nil
	status.FailureMessage = ""
	status.Message = "data-plane image is stable"
	status.UpdatedNodes = 0
	status.TargetOrdinalUpdated = false
}

func (reconciler *EpochClusterReconciler) startUpgradeAfterBackup(
	ctx context.Context,
	cluster *epochv1alpha1.EpochCluster,
) error {
	ready, err := reconciler.dataPlaneReady(ctx, cluster)
	if err != nil {
		return err
	}
	if !ready {
		cluster.Status.Upgrade.Message = "waiting for every data node to be ready before upgrade"
		return nil
	}
	if !freshUpgradeBackup(cluster, time.Now()) {
		cluster.Status.Upgrade.Message = "waiting for a fresh successful encrypted backup after the upgrade request"
		return nil
	}
	ordinal := cluster.Spec.Replicas - 1
	startUpgradeStep(cluster, epochv1alpha1.UpgradePhasePreflight, ordinal, "running preflight invariant checks")
	return nil
}

func freshUpgradeBackup(cluster *epochv1alpha1.EpochCluster, now time.Time) bool {
	backup := cluster.Status.Backup
	started := cluster.Status.Upgrade.StartedAt
	if started == nil || backup.LastSuccessfulTime == nil || backup.LastKeyID != cluster.Spec.Backup.KeyID {
		return false
	}
	if backup.LastSuccessfulTime.Time.Before(started.Time) || now.Sub(backup.LastSuccessfulTime.Time) > effectiveBackupMaxAge(cluster) {
		return false
	}
	return backup.LastFailureTime == nil || !backup.LastFailureTime.After(backup.LastSuccessfulTime.Time)
}

func (reconciler *EpochClusterReconciler) dataPlaneReady(
	ctx context.Context,
	cluster *epochv1alpha1.EpochCluster,
) (bool, error) {
	nodes := nodeStatefulSetShell(cluster)
	if err := reconciler.Get(ctx, client.ObjectKeyFromObject(nodes), nodes); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return nodes.Status.ReadyReplicas == cluster.Spec.Replicas &&
		nodes.Status.CurrentReplicas == cluster.Spec.Replicas, nil
}

func (reconciler *EpochClusterReconciler) runForwardMaintenance(
	ctx context.Context,
	cluster *epochv1alpha1.EpochCluster,
	stage string,
	operation string,
	next epochv1alpha1.UpgradePhase,
) error {
	ordinal, err := currentUpgradeOrdinal(cluster)
	if err != nil {
		return err
	}
	image := cluster.Status.Upgrade.CurrentNodeImage
	if cluster.Status.Upgrade.UpdatedNodes > 0 {
		image = cluster.Status.Upgrade.TargetNodeImage
	}
	state, message, err := reconciler.maintenanceJobState(ctx, cluster, stage, operation, ordinal, image)
	if err != nil {
		return err
	}
	switch state {
	case maintenancePending:
		if upgradeStepExpired(cluster, time.Now()) {
			beginUpgradeFailure(cluster, fmt.Sprintf("%s timed out: %s", stage, message))
		} else {
			cluster.Status.Upgrade.Message = message
		}
	case maintenanceFailed:
		beginUpgradeFailure(cluster, fmt.Sprintf("%s failed: %s", stage, message))
	case maintenanceSucceeded:
		if next == epochv1alpha1.UpgradePhaseUpdating {
			cluster.Status.Upgrade.TargetOrdinalUpdated = true
		}
		startUpgradeStep(cluster, next, ordinal, fmt.Sprintf("%s succeeded", stage))
	}
	return nil
}

func (reconciler *EpochClusterReconciler) waitForForwardPod(
	ctx context.Context,
	cluster *epochv1alpha1.EpochCluster,
) error {
	ordinal, err := currentUpgradeOrdinal(cluster)
	if err != nil {
		return err
	}
	ready, message, err := reconciler.podAtImage(ctx, cluster, ordinal, cluster.Status.Upgrade.TargetNodeImage)
	if err != nil {
		return err
	}
	if ready {
		startUpgradeStep(cluster, epochv1alpha1.UpgradePhaseVerifying, ordinal, "updated node is ready; verifying cluster invariants")
		return nil
	}
	if upgradeStepExpired(cluster, time.Now()) {
		beginUpgradeFailure(cluster, "node update timed out: "+message)
	} else {
		cluster.Status.Upgrade.Message = message
	}
	return nil
}

func (reconciler *EpochClusterReconciler) finishForwardVerification(
	ctx context.Context,
	cluster *epochv1alpha1.EpochCluster,
) error {
	ordinal, err := currentUpgradeOrdinal(cluster)
	if err != nil {
		return err
	}
	state, message, err := reconciler.maintenanceJobState(
		ctx,
		cluster,
		"postflight",
		"verify",
		ordinal,
		cluster.Status.Upgrade.TargetNodeImage,
	)
	if err != nil {
		return err
	}
	switch state {
	case maintenancePending:
		if upgradeStepExpired(cluster, time.Now()) {
			beginUpgradeFailure(cluster, "postflight verification timed out: "+message)
		} else {
			cluster.Status.Upgrade.Message = message
		}
	case maintenanceFailed:
		beginUpgradeFailure(cluster, "postflight verification failed: "+message)
	case maintenanceSucceeded:
		cluster.Status.Upgrade.UpdatedNodes++
		cluster.Status.Upgrade.TargetOrdinalUpdated = false
		if ordinal == 0 {
			cluster.Status.Upgrade.CurrentNodeImage = cluster.Status.Upgrade.TargetNodeImage
			resetStableUpgrade(&cluster.Status.Upgrade)
			return nil
		}
		startUpgradeStep(cluster, epochv1alpha1.UpgradePhasePreflight, ordinal-1, "advancing to the next node")
	}
	return nil
}

func beginUpgradeFailure(cluster *epochv1alpha1.EpochCluster, message string) {
	message = boundedStatusMessage(message)
	status := &cluster.Status.Upgrade
	status.FailureMessage = message
	if effectiveRollbackOnFailure(cluster) {
		startUpgradeStep(cluster, epochv1alpha1.UpgradePhaseRollbackPreflight, cluster.Spec.Replicas-1, "upgrade stopped; preparing guarded rollback")
		status.FailureMessage = message
		return
	}
	status.Phase = epochv1alpha1.UpgradePhaseFailed
	status.Message = "upgrade stopped without automatic rollback: " + message
}

func (reconciler *EpochClusterReconciler) prepareRollbackOrdinal(
	ctx context.Context,
	cluster *epochv1alpha1.EpochCluster,
) error {
	ordinal, err := currentUpgradeOrdinal(cluster)
	if err != nil {
		return err
	}
	podImage, ready, err := reconciler.podImageAndReadiness(ctx, cluster, ordinal)
	if err != nil {
		if upgradeStepExpired(cluster, time.Now()) {
			stopRollback(cluster, "could not inspect rollback target: "+err.Error())
			return nil
		}
		cluster.Status.Upgrade.Message = "waiting to inspect rollback target: " + err.Error()
		return nil
	}
	if podImage == cluster.Status.Upgrade.CurrentNodeImage && ready {
		cluster.Status.Upgrade.TargetOrdinalUpdated = false
		advanceRollback(cluster, ordinal)
		return nil
	}
	if podImage != cluster.Status.Upgrade.TargetNodeImage && podImage != cluster.Status.Upgrade.CurrentNodeImage {
		stopRollback(cluster, fmt.Sprintf("node %d runs unexpected image %q", ordinal, podImage))
		return nil
	}
	if !ready {
		startUpgradeStep(cluster, epochv1alpha1.UpgradePhaseRollbackUpdating, ordinal, "unready target cannot lead; restoring the stable image")
		return nil
	}
	state, message, err := reconciler.maintenanceJobState(
		ctx,
		cluster,
		"rollback-preflight",
		"verify",
		ordinal,
		cluster.Status.Upgrade.CurrentNodeImage,
	)
	if err != nil {
		return err
	}
	switch state {
	case maintenancePending:
		if upgradeStepExpired(cluster, time.Now()) {
			stopRollback(cluster, "rollback preflight timed out: "+message)
		} else {
			cluster.Status.Upgrade.Message = message
		}
	case maintenanceFailed:
		stopRollback(cluster, "rollback preflight failed: "+message)
	case maintenanceSucceeded:
		startUpgradeStep(cluster, epochv1alpha1.UpgradePhaseRollbackDraining, ordinal, "draining leadership before rollback")
	}
	return nil
}

func (reconciler *EpochClusterReconciler) runRollbackMaintenance(
	ctx context.Context,
	cluster *epochv1alpha1.EpochCluster,
	stage string,
	operation string,
	next epochv1alpha1.UpgradePhase,
) error {
	ordinal, err := currentUpgradeOrdinal(cluster)
	if err != nil {
		return err
	}
	state, message, err := reconciler.maintenanceJobState(
		ctx,
		cluster,
		stage,
		operation,
		ordinal,
		cluster.Status.Upgrade.CurrentNodeImage,
	)
	if err != nil {
		return err
	}
	switch state {
	case maintenancePending:
		if upgradeStepExpired(cluster, time.Now()) {
			stopRollback(cluster, stage+" timed out: "+message)
		} else {
			cluster.Status.Upgrade.Message = message
		}
	case maintenanceFailed:
		stopRollback(cluster, stage+" failed: "+message)
	case maintenanceSucceeded:
		startUpgradeStep(cluster, next, ordinal, stage+" succeeded")
	}
	return nil
}

func (reconciler *EpochClusterReconciler) waitForRollbackPod(
	ctx context.Context,
	cluster *epochv1alpha1.EpochCluster,
) error {
	ordinal, err := currentUpgradeOrdinal(cluster)
	if err != nil {
		return err
	}
	ready, message, err := reconciler.podAtImage(ctx, cluster, ordinal, cluster.Status.Upgrade.CurrentNodeImage)
	if err != nil {
		return err
	}
	if ready {
		startUpgradeStep(cluster, epochv1alpha1.UpgradePhaseRollbackVerifying, ordinal, "stable node image is ready; verifying rollback")
		return nil
	}
	if upgradeStepExpired(cluster, time.Now()) {
		stopRollback(cluster, "rollback update timed out: "+message)
	} else {
		cluster.Status.Upgrade.Message = message
	}
	return nil
}

func (reconciler *EpochClusterReconciler) finishRollbackVerification(
	ctx context.Context,
	cluster *epochv1alpha1.EpochCluster,
) error {
	ordinal, err := currentUpgradeOrdinal(cluster)
	if err != nil {
		return err
	}
	state, message, err := reconciler.maintenanceJobState(
		ctx,
		cluster,
		"rollback-postflight",
		"verify",
		ordinal,
		cluster.Status.Upgrade.CurrentNodeImage,
	)
	if err != nil {
		return err
	}
	switch state {
	case maintenancePending:
		if upgradeStepExpired(cluster, time.Now()) {
			stopRollback(cluster, "rollback verification timed out: "+message)
		} else {
			cluster.Status.Upgrade.Message = message
		}
	case maintenanceFailed:
		stopRollback(cluster, "rollback verification failed: "+message)
	case maintenanceSucceeded:
		if cluster.Status.Upgrade.UpdatedNodes > 0 {
			cluster.Status.Upgrade.UpdatedNodes--
		}
		advanceRollback(cluster, ordinal)
	}
	return nil
}

func advanceRollback(cluster *epochv1alpha1.EpochCluster, ordinal int32) {
	cluster.Status.Upgrade.TargetOrdinalUpdated = false
	if ordinal == 0 {
		failure := cluster.Status.Upgrade.FailureMessage
		cluster.Status.Upgrade.Phase = epochv1alpha1.UpgradePhaseFailed
		cluster.Status.Upgrade.TargetOrdinal = nil
		cluster.Status.Upgrade.StepStartedAt = nil
		cluster.Status.Upgrade.UpdatedNodes = 0
		cluster.Status.Upgrade.Message = "upgrade failed and every changed node was rolled back: " + failure
		return
	}
	startUpgradeStep(cluster, epochv1alpha1.UpgradePhaseRollbackPreflight, ordinal-1, "continuing guarded rollback")
}

func stopRollback(cluster *epochv1alpha1.EpochCluster, message string) {
	cluster.Status.Upgrade.Phase = epochv1alpha1.UpgradePhaseFailed
	cluster.Status.Upgrade.Message = boundedStatusMessage("automatic rollback stopped: " + message)
}

func startUpgradeStep(
	cluster *epochv1alpha1.EpochCluster,
	phase epochv1alpha1.UpgradePhase,
	ordinal int32,
	message string,
) {
	now := metav1.Now()
	cluster.Status.Upgrade.Phase = phase
	cluster.Status.Upgrade.TargetOrdinal = &ordinal
	cluster.Status.Upgrade.StepStartedAt = &now
	cluster.Status.Upgrade.Message = boundedStatusMessage(message)
}

func currentUpgradeOrdinal(cluster *epochv1alpha1.EpochCluster) (int32, error) {
	ordinal := cluster.Status.Upgrade.TargetOrdinal
	if ordinal == nil || *ordinal < 0 || *ordinal >= cluster.Spec.Replicas {
		return 0, fmt.Errorf("upgrade phase %s has invalid target ordinal", cluster.Status.Upgrade.Phase)
	}
	return *ordinal, nil
}

func upgradeStepExpired(cluster *epochv1alpha1.EpochCluster, now time.Time) bool {
	started := cluster.Status.Upgrade.StepStartedAt
	return started == nil || now.Sub(started.Time) > effectiveStepDeadline(cluster)
}

func effectiveBackupMaxAge(cluster *epochv1alpha1.EpochCluster) time.Duration {
	if cluster.Spec.Upgrade.BackupMaxAgeSeconds == 0 {
		return defaultBackupMaxAge
	}
	return time.Duration(cluster.Spec.Upgrade.BackupMaxAgeSeconds) * time.Second
}

func effectiveStepDeadline(cluster *epochv1alpha1.EpochCluster) time.Duration {
	if cluster.Spec.Upgrade.StepDeadlineSeconds == 0 {
		return defaultStepDeadline
	}
	return time.Duration(cluster.Spec.Upgrade.StepDeadlineSeconds) * time.Second
}

func effectiveRollbackOnFailure(cluster *epochv1alpha1.EpochCluster) bool {
	return cluster.Spec.Upgrade.RollbackOnFailure == nil || *cluster.Spec.Upgrade.RollbackOnFailure
}

func operationalNodeImage(cluster *epochv1alpha1.EpochCluster) string {
	if cluster.Status.Upgrade.CurrentNodeImage != "" {
		return cluster.Status.Upgrade.CurrentNodeImage
	}
	return cluster.Spec.NodeImage
}

func nodeRolloutImage(cluster *epochv1alpha1.EpochCluster) string {
	status := cluster.Status.Upgrade
	if status.CurrentNodeImage == "" {
		return cluster.Spec.NodeImage
	}
	switch status.Phase {
	case epochv1alpha1.UpgradePhaseUpdating,
		epochv1alpha1.UpgradePhaseVerifying:
		return status.TargetNodeImage
	case epochv1alpha1.UpgradePhasePreflight,
		epochv1alpha1.UpgradePhaseDraining:
		if status.UpdatedNodes > 0 {
			return status.TargetNodeImage
		}
	case epochv1alpha1.UpgradePhaseRollbackPreflight,
		epochv1alpha1.UpgradePhaseRollbackDraining,
		epochv1alpha1.UpgradePhaseRollbackUpdating,
		epochv1alpha1.UpgradePhaseRollbackVerifying:
		return status.CurrentNodeImage
	case epochv1alpha1.UpgradePhaseFailed:
		if !effectiveRollbackOnFailure(cluster) &&
			(status.UpdatedNodes > 0 || status.TargetOrdinalUpdated) {
			return status.TargetNodeImage
		}
	}
	return status.CurrentNodeImage
}

func nodeRolloutPartition(cluster *epochv1alpha1.EpochCluster) int32 {
	status := cluster.Status.Upgrade
	ordinal := cluster.Spec.Replicas
	if status.TargetOrdinal != nil {
		ordinal = *status.TargetOrdinal
	}
	switch status.Phase {
	case epochv1alpha1.UpgradePhaseStable, "":
		return 0
	case epochv1alpha1.UpgradePhaseWaitingForBackup:
		return cluster.Spec.Replicas
	case epochv1alpha1.UpgradePhasePreflight, epochv1alpha1.UpgradePhaseDraining:
		return cluster.Spec.Replicas - status.UpdatedNodes
	case epochv1alpha1.UpgradePhaseUpdating, epochv1alpha1.UpgradePhaseVerifying:
		return ordinal
	case epochv1alpha1.UpgradePhaseRollbackPreflight,
		epochv1alpha1.UpgradePhaseRollbackDraining:
		return min(ordinal+1, cluster.Spec.Replicas)
	case epochv1alpha1.UpgradePhaseRollbackUpdating,
		epochv1alpha1.UpgradePhaseRollbackVerifying:
		return ordinal
	case epochv1alpha1.UpgradePhaseFailed:
		if status.TargetOrdinal == nil {
			return 0
		}
		return ordinal
	default:
		return cluster.Spec.Replicas
	}
}

func upgradeRequestID(image, retryToken string) string {
	digest := sha256.Sum256([]byte(image + "\x00" + retryToken))
	return hex.EncodeToString(digest[:])[:12]
}

type maintenanceState int

const (
	maintenancePending maintenanceState = iota
	maintenanceSucceeded
	maintenanceFailed
)

func (reconciler *EpochClusterReconciler) maintenanceJobState(
	ctx context.Context,
	cluster *epochv1alpha1.EpochCluster,
	stage string,
	operation string,
	ordinal int32,
	image string,
) (maintenanceState, string, error) {
	job := maintenanceJob(cluster, stage, operation, ordinal, image)
	key := client.ObjectKeyFromObject(job)
	current := &batchv1.Job{}
	err := reconciler.Get(ctx, key, current)
	if apierrors.IsNotFound(err) {
		if err := controllerutil.SetControllerReference(cluster, job, reconciler.Scheme); err != nil {
			return maintenancePending, "", err
		}
		if err := reconciler.Create(ctx, job); err != nil {
			return maintenancePending, "", err
		}
		return maintenancePending, fmt.Sprintf("%s job %s was created", stage, job.Name), nil
	}
	if err != nil {
		return maintenancePending, "", err
	}
	if !maintenanceJobMatches(job, current) {
		return maintenanceFailed, "operator-owned maintenance Job drifted from its immutable plan", nil
	}
	for _, condition := range current.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			message := boundedStatusMessage(condition.Message)
			if message == "" {
				message = "maintenance Job failed"
			}
			return maintenanceFailed, message, nil
		}
	}
	if current.Status.Succeeded == 0 || current.Status.CompletionTime == nil {
		return maintenancePending, fmt.Sprintf("waiting for %s job %s", stage, current.Name), nil
	}
	receipt, err := reconciler.maintenanceTerminationStatus(ctx, current)
	if err != nil {
		return maintenancePending, "", err
	}
	if receipt == nil {
		return maintenanceFailed, "successful maintenance Job has no valid termination receipt", nil
	}
	if err := validateMaintenanceReceipt(receipt, cluster, operation, ordinal); err != nil {
		return maintenanceFailed, err.Error(), nil
	}
	return maintenanceSucceeded, fmt.Sprintf("%s job %s succeeded", stage, current.Name), nil
}

func maintenanceJobMatches(desired, current *batchv1.Job) bool {
	return current.Labels[upgradeOwnerLabel] == desired.Labels[upgradeOwnerLabel] &&
		current.Labels[upgradeRequestLabel] == desired.Labels[upgradeRequestLabel] &&
		current.Labels[upgradeStageLabel] == desired.Labels[upgradeStageLabel] &&
		len(current.Spec.Template.Spec.Containers) == 1 &&
		current.Spec.Template.Spec.Containers[0].Image == desired.Spec.Template.Spec.Containers[0].Image &&
		strings.Join(current.Spec.Template.Spec.Containers[0].Command, "\x00") == strings.Join(desired.Spec.Template.Spec.Containers[0].Command, "\x00") &&
		strings.Join(current.Spec.Template.Spec.Containers[0].Args, "\x00") == strings.Join(desired.Spec.Template.Spec.Containers[0].Args, "\x00")
}

func maintenanceJob(
	cluster *epochv1alpha1.EpochCluster,
	stage string,
	operation string,
	ordinal int32,
	image string,
) *batchv1.Job {
	labels := labels(cluster, "maintenance")
	labels[upgradeOwnerLabel] = cluster.Name
	labels[upgradeRequestLabel] = cluster.Status.Upgrade.RequestID
	labels[upgradeStageLabel] = stage
	backoff := int32(0)
	deadline := int64(effectiveStepDeadline(cluster).Seconds())
	ttl := int32(86_400)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      upgradeJobName(cluster, stage, ordinal),
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoff,
			ActiveDeadlineSeconds:   &deadline,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: boolPointer(true),
						FSGroup:      int64Pointer(10001),
					},
					Containers: []corev1.Container{{
						Name:            "epoch-maintenance",
						Image:           image,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Command:         []string{"/usr/local/bin/epoch-maintenance"},
						Args:            maintenanceArgs(cluster, operation, ordinal),
						Resources:       cluster.Spec.ControlResources,
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: boolPointer(false),
							ReadOnlyRootFilesystem:   boolPointer(true),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						VolumeMounts: []corev1.VolumeMount{{Name: "transport-tls", MountPath: "/etc/epoch/tls", ReadOnly: true}},
					}},
					Volumes: []corev1.Volume{{
						Name: "transport-tls",
						VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
							SecretName: cluster.Spec.TransportSecurity.ControlPlaneSecret,
						}},
					}},
				},
			},
		},
	}
}

func maintenanceArgs(cluster *epochv1alpha1.EpochCluster, operation string, ordinal int32) []string {
	endpoints := make([]string, cluster.Spec.Replicas)
	for index := range endpoints {
		endpoints[index] = fmt.Sprintf("https://%s-%d.%s:7701/", nodeName(cluster), index, peerName(cluster))
	}
	rounds := int(effectiveStepDeadline(cluster)/(500*time.Millisecond)) - 1
	if rounds < 1 {
		rounds = 1
	}
	if rounds > 1000 {
		rounds = 1000
	}
	return []string{
		"--endpoints", strings.Join(endpoints, ","),
		"--node-id", strconv.FormatInt(int64(ordinal+1), 10),
		"--tls-ca", "/etc/epoch/tls/" + tlsCAKey,
		"--tls-certificate", "/etc/epoch/tls/" + tlsCertificateKey,
		"--tls-private-key", "/etc/epoch/tls/" + tlsPrivateKeyKey,
		"--rounds", strconv.Itoa(rounds),
		"--status-path", "/dev/termination-log",
		operation,
	}
}

func upgradeJobName(cluster *epochv1alpha1.EpochCluster, stage string, ordinal int32) string {
	suffix := fmt.Sprintf("upg-%s-%d-%s", cluster.Status.Upgrade.RequestID, ordinal, stage)
	maximumPrefix := 63 - len(suffix) - 1
	prefix := strings.Trim(cluster.Name, "-")
	if len(prefix) > maximumPrefix {
		prefix = strings.TrimRight(prefix[:maximumPrefix], "-")
	}
	if prefix == "" {
		prefix = "epoch"
	}
	return prefix + "-" + suffix
}

func (reconciler *EpochClusterReconciler) maintenanceTerminationStatus(
	ctx context.Context,
	job *batchv1.Job,
) (*maintenanceTerminationStatus, error) {
	pods := &corev1.PodList{}
	if err := reconciler.List(ctx, pods, client.InNamespace(job.Namespace), client.MatchingLabels{"job-name": job.Name}); err != nil {
		return nil, err
	}
	for _, pod := range pods.Items {
		for _, container := range pod.Status.ContainerStatuses {
			terminated := container.State.Terminated
			if container.Name != "epoch-maintenance" || terminated == nil || terminated.ExitCode != 0 ||
				len(terminated.Message) == 0 || len(terminated.Message) > maintenanceStatusLimit {
				continue
			}
			status := &maintenanceTerminationStatus{}
			decoder := json.NewDecoder(strings.NewReader(terminated.Message))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(status); err != nil {
				continue
			}
			return status, nil
		}
	}
	return nil, nil
}

func validateMaintenanceReceipt(
	receipt *maintenanceTerminationStatus,
	cluster *epochv1alpha1.EpochCluster,
	operation string,
	ordinal int32,
) error {
	expectedState := "verified"
	if operation == "drain" {
		expectedState = "drained"
	}
	if receipt.State != expectedState || receipt.Operation != operation || receipt.TargetNodeID != uint64(ordinal+1) {
		return fmt.Errorf("maintenance Job returned a receipt for another operation or node")
	}
	if receipt.GroupsChecked < 0 || receipt.LeadershipTransfers < 0 || receipt.LeadershipTransfers > receipt.GroupsChecked {
		return fmt.Errorf("maintenance Job returned invalid bounded counters")
	}
	if len(receipt.ObservedNodeIDs) != int(cluster.Spec.Replicas) {
		return fmt.Errorf("maintenance Job did not observe every physical node")
	}
	for index, nodeID := range receipt.ObservedNodeIDs {
		if nodeID != uint64(index+1) {
			return fmt.Errorf("maintenance Job returned a non-canonical node inventory")
		}
	}
	return nil
}

func (reconciler *EpochClusterReconciler) podAtImage(
	ctx context.Context,
	cluster *epochv1alpha1.EpochCluster,
	ordinal int32,
	image string,
) (bool, string, error) {
	observedImage, ready, err := reconciler.podImageAndReadiness(ctx, cluster, ordinal)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, fmt.Sprintf("waiting for node pod ordinal %d", ordinal), nil
		}
		return false, "", err
	}
	if observedImage != image {
		return false, fmt.Sprintf("node pod ordinal %d still runs %q", ordinal, observedImage), nil
	}
	if !ready {
		return false, fmt.Sprintf("node pod ordinal %d is not Ready", ordinal), nil
	}
	return true, fmt.Sprintf("node pod ordinal %d is Ready at image %q", ordinal, image), nil
}

func (reconciler *EpochClusterReconciler) podImageAndReadiness(
	ctx context.Context,
	cluster *epochv1alpha1.EpochCluster,
	ordinal int32,
) (string, bool, error) {
	pod := &corev1.Pod{}
	if err := reconciler.Get(ctx, types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      fmt.Sprintf("%s-%d", nodeName(cluster), ordinal),
	}, pod); err != nil {
		return "", false, err
	}
	if pod.DeletionTimestamp != nil || len(pod.Spec.Containers) != 1 {
		return "", false, nil
	}
	ready := false
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			ready = true
		}
	}
	return pod.Spec.Containers[0].Image, ready, nil
}

func nodeStatefulSetShell(cluster *epochv1alpha1.EpochCluster) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name:      nodeName(cluster),
		Namespace: cluster.Namespace,
	}}
}
