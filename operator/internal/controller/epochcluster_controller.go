// Package controller reconciles EpochCluster resources into a regional data
// plane and its single-owner control-plane service.
package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
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
	policyKey          = "bootstrap-policy.json"
	credentialKey      = "regional-token"
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
		nodeStatefulSet(cluster),
		controlStatefulSet(cluster),
	} {
		if err := reconciler.reconcileObject(ctx, cluster, object); err != nil {
			return ctrl.Result{}, err
		}
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
		Complete(reconciler)
}

func validateSpec(spec *epochv1alpha1.EpochClusterSpec) error {
	if spec.Replicas != 3 {
		return fmt.Errorf("replicas must be exactly 3 for the current fixed-voter runtime")
	}
	for field, value := range map[string]string{
		"nodeImage":           spec.NodeImage,
		"controlImage":        spec.ControlImage,
		"authPolicyConfigMap": spec.AuthPolicyConfigMap,
		"credentialSecret":    spec.CredentialSecret,
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
	cluster.Status.Endpoint = fmt.Sprintf("http://%s.%s.svc:8080", controlName(cluster), cluster.Namespace)
	available := nodes.Status.ReadyReplicas == cluster.Spec.Replicas && cluster.Status.ControlReady
	status := metav1.ConditionFalse
	reason := "ComponentsPending"
	message := fmt.Sprintf("%d/%d data nodes and %d/1 control replicas are ready", nodes.Status.ReadyReplicas, cluster.Spec.Replicas, control.Status.ReadyReplicas)
	if available {
		status = metav1.ConditionTrue
		reason = "ComponentsReady"
		message = "fixed-voter data plane and control plane are ready"
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
		Status:             boolCondition(!available),
		Reason:             reason,
		Message:            message,
		ObservedGeneration: cluster.Generation,
	})
	return reconciler.Status().Update(ctx, cluster)
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

func peerService(cluster *epochv1alpha1.EpochCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: peerName(cluster), Namespace: cluster.Namespace, Labels: labels(cluster, "data-plane")},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  labels(cluster, "data-plane"),
			Ports: []corev1.ServicePort{
				{Name: "http", Port: 7601, TargetPort: intstr.FromString("http")},
				{Name: "peer", Port: 7701, TargetPort: intstr.FromString("peer")},
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
			Ports:    []corev1.ServicePort{{Name: "http", Port: 7601, TargetPort: intstr.FromString("http")}},
		},
	}
}

func controlService(cluster *epochv1alpha1.EpochCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: controlName(cluster), Namespace: cluster.Namespace, Labels: labels(cluster, "control-plane")},
		Spec: corev1.ServiceSpec{
			Selector: labels(cluster, "control-plane"),
			Ports: []corev1.ServicePort{
				{Name: "http", Port: 8080, TargetPort: intstr.FromString("http")},
				{Name: "grpc", Port: 8081, TargetPort: intstr.FromString("grpc")},
			},
		},
	}
}

func nodeStatefulSet(cluster *epochv1alpha1.EpochCluster) *appsv1.StatefulSet {
	name := nodeName(cluster)
	peers := make([]string, cluster.Spec.Replicas)
	for index := range peers {
		peers[index] = fmt.Sprintf("%d=http://%s-%d.%s:7701", index+1, name, index, peerName(cluster))
	}
	selector := labels(cluster, "data-plane")
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace, Labels: selector},
		Spec: appsv1.StatefulSetSpec{
			ServiceName:         peerName(cluster),
			Replicas:            &cluster.Spec.Replicas,
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Selector:            &metav1.LabelSelector{MatchLabels: selector},
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
						Image:           cluster.Spec.NodeImage,
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
							{Name: "EPOCH_REGIONAL_REGION", Value: effectiveRegion(cluster)},
							{Name: "EPOCH_REGIONAL_ZONE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"}}},
							{Name: "EPOCH_REGIONAL_NODE_CLASS", Value: effectiveNodeClass(cluster)},
						},
						Ports:          []corev1.ContainerPort{{Name: "http", ContainerPort: 7601}, {Name: "peer", ContainerPort: 7701}},
						Resources:      cluster.Spec.NodeResources,
						ReadinessProbe: httpProbe("/healthz", "http"),
						LivenessProbe:  httpProbe("/healthz", "http"),
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: boolPointer(false),
							ReadOnlyRootFilesystem:   boolPointer(true),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/var/lib/epoch"}, {Name: "auth-policy", MountPath: "/etc/epoch/auth", ReadOnly: true}},
					}},
					Volumes: []corev1.Volume{{Name: "auth-policy", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: cluster.Spec.AuthPolicyConfigMap}}}}},
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
}

func controlStatefulSet(cluster *epochv1alpha1.EpochCluster) *appsv1.StatefulSet {
	one := int32(1)
	selector := labels(cluster, "control-plane")
	endpoints := make([]string, cluster.Spec.Replicas)
	for index := range endpoints {
		endpoints[index] = fmt.Sprintf("http://%s-%d.%s:7601", nodeName(cluster), index, peerName(cluster))
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
						},
						Ports:           []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}, {Name: "grpc", ContainerPort: 8081}},
						Resources:       cluster.Spec.ControlResources,
						ReadinessProbe:  httpProbe("/healthz", "http"),
						LivenessProbe:   httpProbe("/healthz", "http"),
						SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: boolPointer(false), ReadOnlyRootFilesystem: boolPointer(true), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
						VolumeMounts:    []corev1.VolumeMount{{Name: "data", MountPath: "/var/lib/epoch-control"}, {Name: "auth-policy", MountPath: "/etc/epoch/auth", ReadOnly: true}},
					}},
					Volumes: []corev1.Volume{{Name: "auth-policy", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: cluster.Spec.AuthPolicyConfigMap}}}}},
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

func httpProbe(path, port string) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: path, Port: intstr.FromString(port)}},
		InitialDelaySeconds: 5,
		PeriodSeconds:       5,
		TimeoutSeconds:      2,
		FailureThreshold:    6,
	}
}

func boolPointer(value bool) *bool    { return &value }
func int64Pointer(value int64) *int64 { return &value }
