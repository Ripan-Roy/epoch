package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	epochv1alpha1 "epoch.local/epoch/operator/api/v1alpha1"
)

func TestReconcileCreatesRunnableFixedVoterTopology(t *testing.T) {
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
	container := nodes.Spec.Template.Spec.Containers[0]
	if !containsEnvironment(container.Env, "EPOCH_CONSENSUS_PEERS", "1=http://orders-node-0.orders-peer:7701,2=http://orders-node-1.orders-peer:7701,3=http://orders-node-2.orders-peer:7701") {
		t.Fatalf("fixed voter endpoints are absent: %#v", container.Env)
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
	for _, serviceName := range []string{peerName(cluster), publicName(cluster), controlName(cluster)} {
		service := &corev1.Service{}
		if err := client.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: serviceName}, service); err != nil {
			t.Fatalf("Service %s was not created: %v", serviceName, err)
		}
	}

	observed := &epochv1alpha1.EpochCluster{}
	if err := client.Get(context.Background(), request.NamespacedName, observed); err != nil {
		t.Fatal(err)
	}
	if observed.Status.ObservedGeneration != cluster.Generation || observed.Status.Endpoint != "http://orders-control.epoch-system.svc:8080" {
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

func TestValidateSpecRejectsTopologyThatRuntimeCannotHonor(t *testing.T) {
	t.Parallel()
	cluster := validCluster()
	cluster.Spec.Replicas = 5
	if err := validateSpec(&cluster.Spec); err == nil {
		t.Fatal("five replicas must be rejected while consensus membership is fixed at three")
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
		t.Fatal("drift in the fixed voter count must be repaired")
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
