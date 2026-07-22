// Package resources implements the declarative resource registry used by the
// initial managed-control-plane slice. It stores control metadata only; it does
// not read or mutate Epoch data-node memory, logs, or storage files.
package resources

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
)

// Kind identifies an Epoch control-plane resource type.
type Kind string

const (
	KindCache        Kind = "cache"
	KindTable        Kind = "table"
	KindStream       Kind = "stream"
	KindQueue        Kind = "queue"
	KindEventBus     Kind = "event_bus"
	KindSubscription Kind = "subscription"
	KindSchema       Kind = "schema"
	KindPipe         Kind = "pipe"
	KindConnector    Kind = "connector"
	KindPolicy       Kind = "policy"
)

// Valid reports whether the kind belongs to the initial resource model.
func (kind Kind) Valid() bool {
	switch kind {
	case KindCache, KindTable, KindStream, KindQueue, KindEventBus,
		KindSubscription, KindSchema, KindPipe, KindConnector, KindPolicy:
		return true
	default:
		return false
	}
}

// ResourceKey is the stable identity of a resource within this regional
// registry. Organization, project, and environment routing belongs above this
// initial regional slice.
type ResourceKey struct {
	Namespace string `json:"namespace"`
	Kind      Kind   `json:"kind"`
	Name      string `json:"name"`
}

// DesiredResource is the declarative input accepted by Apply. Spec is kept as
// canonical JSON until generated Protobuf bindings replace this HTTP boundary.
type DesiredResource struct {
	ResourceKey
	Labels map[string]string `json:"labels,omitempty"`
	Spec   json.RawMessage   `json:"spec"`
}

// ResourcePhase describes reconciliation state. The in-memory registry only
// accepts desired state, so new generations remain pending until a regional
// Rust data plane reports an observed generation through a future contract.
type ResourcePhase string

const (
	PhasePending ResourcePhase = "pending"
	PhaseReady   ResourcePhase = "ready"
	PhaseFailed  ResourcePhase = "failed"
)

// ResourceStatus is intentionally small in the initial slice and never
// implies that an unconnected data plane has achieved the requested state.
type ResourceStatus struct {
	Phase              ResourcePhase `json:"phase"`
	ObservedGeneration uint64        `json:"observed_generation"`
	Message            string        `json:"message,omitempty"`
}

// Resource is the registry's immutable response value.
type Resource struct {
	ResourceKey
	Labels     map[string]string `json:"labels,omitempty"`
	Spec       json.RawMessage   `json:"spec"`
	Generation uint64            `json:"generation"`
	Status     ResourceStatus    `json:"status"`
}

// ApplyRequest performs a declarative create or update. A nil expected
// generation is an unconditional apply, zero is create-only, and a positive
// value must match the current resource generation.
type ApplyRequest struct {
	RequestToken       string          `json:"request_token"`
	ExpectedGeneration *uint64         `json:"expected_generation,omitempty"`
	Resource           DesiredResource `json:"resource"`
}

// ApplyResult records whether Apply created or materially changed desired
// state. Replayed is true when a completed request token is seen again.
type ApplyResult struct {
	Resource Resource `json:"resource"`
	Created  bool     `json:"created"`
	Changed  bool     `json:"changed"`
	Replayed bool     `json:"replayed"`
}

// DeleteRequest deletes one resource with optional optimistic concurrency.
// Tokens make a successfully completed delete safe to retry.
type DeleteRequest struct {
	RequestToken       string      `json:"request_token"`
	ExpectedGeneration *uint64     `json:"expected_generation,omitempty"`
	Key                ResourceKey `json:"key"`
}

// DeleteResult reports the generation assigned to the delete mutation. The
// generation is retained as a tombstone counter so recreation remains
// monotonic for the life of this registry process.
type DeleteResult struct {
	Key        ResourceKey `json:"key"`
	Generation uint64      `json:"generation"`
	Deleted    bool        `json:"deleted"`
	Replayed   bool        `json:"replayed"`
}

// ListFilter limits a stable, key-sorted list operation.
type ListFilter struct {
	Namespace string
	Kind      Kind
}

// ErrorCode is stable across the Go registry and its HTTP translation.
type ErrorCode string

const (
	CodeInvalidArgument ErrorCode = "invalid_argument"
	CodeNotFound        ErrorCode = "not_found"
	CodeConflict        ErrorCode = "conflict"
	CodeInternal        ErrorCode = "internal"
)

// RegistryError carries safe, structured error details.
type RegistryError struct {
	Code               ErrorCode `json:"code"`
	Message            string    `json:"message"`
	ExpectedGeneration uint64    `json:"expected_generation,omitempty"`
	ActualGeneration   uint64    `json:"actual_generation,omitempty"`
}

func (err *RegistryError) Error() string {
	return err.Message
}

type tokenRecord struct {
	operation   string
	fingerprint string
	apply       *ApplyResult
	delete      *DeleteResult
}

// Registry is a concurrency-safe, in-memory declarative registry. It is an M1
// control-plane proving surface, not durable customer metadata storage.
type Registry struct {
	mu             sync.RWMutex
	resources      map[ResourceKey]Resource
	lastGeneration map[ResourceKey]uint64
	tokens         map[string]tokenRecord
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		resources:      make(map[ResourceKey]Resource),
		lastGeneration: make(map[ResourceKey]uint64),
		tokens:         make(map[string]tokenRecord),
	}
}

// Apply creates or updates desired state atomically.
func (registry *Registry) Apply(request ApplyRequest) (ApplyResult, error) {
	normalized, err := normalizeApply(request)
	if err != nil {
		return ApplyResult{}, err
	}

	fingerprint, err := fingerprint("apply", normalized)
	if err != nil {
		return ApplyResult{}, invalid("resource could not be encoded")
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()

	if prior, found := registry.tokens[normalized.RequestToken]; found {
		if prior.operation != "apply" || prior.fingerprint != fingerprint {
			return ApplyResult{}, conflict("request token was already used for a different operation", 0, 0)
		}
		result := cloneApplyResult(*prior.apply)
		result.Replayed = true
		return result, nil
	}

	key := normalized.Resource.ResourceKey
	current, exists := registry.resources[key]
	actualGeneration := uint64(0)
	if exists {
		actualGeneration = current.Generation
	}
	if normalized.ExpectedGeneration != nil && *normalized.ExpectedGeneration != actualGeneration {
		return ApplyResult{}, conflict(
			fmt.Sprintf("expected generation %d, found %d", *normalized.ExpectedGeneration, actualGeneration),
			*normalized.ExpectedGeneration,
			actualGeneration,
		)
	}

	result := ApplyResult{}
	switch {
	case !exists:
		generation, err := nextGeneration(registry.lastGeneration[key])
		if err != nil {
			return ApplyResult{}, err
		}
		resource := materialize(normalized.Resource, generation)
		registry.resources[key] = resource
		registry.lastGeneration[key] = generation
		result = ApplyResult{Resource: cloneResource(resource), Created: true, Changed: true}
	case desiredEqual(current, normalized.Resource):
		result = ApplyResult{Resource: cloneResource(current)}
	default:
		generation, err := nextGeneration(current.Generation)
		if err != nil {
			return ApplyResult{}, err
		}
		resource := materialize(normalized.Resource, generation)
		registry.resources[key] = resource
		registry.lastGeneration[key] = generation
		result = ApplyResult{Resource: cloneResource(resource), Changed: true}
	}

	stored := cloneApplyResult(result)
	registry.tokens[normalized.RequestToken] = tokenRecord{
		operation:   "apply",
		fingerprint: fingerprint,
		apply:       &stored,
	}
	return result, nil
}

// Get retrieves a defensive copy of a resource.
func (registry *Registry) Get(key ResourceKey) (Resource, error) {
	normalized, err := normalizeKey(key)
	if err != nil {
		return Resource{}, err
	}

	registry.mu.RLock()
	defer registry.mu.RUnlock()
	resource, found := registry.resources[normalized]
	if !found {
		return Resource{}, notFound(normalized)
	}
	return cloneResource(resource), nil
}

// List returns defensive copies in deterministic namespace/kind/name order.
func (registry *Registry) List(filter ListFilter) ([]Resource, error) {
	filter.Namespace = strings.TrimSpace(filter.Namespace)
	if filter.Kind != "" && !filter.Kind.Valid() {
		return nil, invalid(fmt.Sprintf("unknown resource kind %q", filter.Kind))
	}

	registry.mu.RLock()
	resources := make([]Resource, 0, len(registry.resources))
	for _, resource := range registry.resources {
		if filter.Namespace != "" && resource.Namespace != filter.Namespace {
			continue
		}
		if filter.Kind != "" && resource.Kind != filter.Kind {
			continue
		}
		resources = append(resources, cloneResource(resource))
	}
	registry.mu.RUnlock()

	sort.Slice(resources, func(left, right int) bool {
		if resources[left].Namespace != resources[right].Namespace {
			return resources[left].Namespace < resources[right].Namespace
		}
		if resources[left].Kind != resources[right].Kind {
			return resources[left].Kind < resources[right].Kind
		}
		return resources[left].Name < resources[right].Name
	})
	return resources, nil
}

// Delete removes desired state while retaining the monotonic generation
// counter. A retry with the same token returns the original result.
func (registry *Registry) Delete(request DeleteRequest) (DeleteResult, error) {
	normalizedKey, err := normalizeKey(request.Key)
	if err != nil {
		return DeleteResult{}, err
	}
	request.Key = normalizedKey
	request.RequestToken = strings.TrimSpace(request.RequestToken)
	if err := validateToken(request.RequestToken); err != nil {
		return DeleteResult{}, err
	}

	fingerprint, err := fingerprint("delete", request)
	if err != nil {
		return DeleteResult{}, invalid("delete request could not be encoded")
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()

	if prior, found := registry.tokens[request.RequestToken]; found {
		if prior.operation != "delete" || prior.fingerprint != fingerprint {
			return DeleteResult{}, conflict("request token was already used for a different operation", 0, 0)
		}
		result := *prior.delete
		result.Replayed = true
		return result, nil
	}

	current, found := registry.resources[request.Key]
	if !found {
		if request.ExpectedGeneration != nil && *request.ExpectedGeneration > 0 {
			return DeleteResult{}, conflict(
				fmt.Sprintf("expected generation %d, found 0", *request.ExpectedGeneration),
				*request.ExpectedGeneration,
				0,
			)
		}
		result := DeleteResult{
			Key:        request.Key,
			Generation: registry.lastGeneration[request.Key],
		}
		stored := result
		registry.tokens[request.RequestToken] = tokenRecord{
			operation:   "delete",
			fingerprint: fingerprint,
			delete:      &stored,
		}
		return result, nil
	}

	if request.ExpectedGeneration != nil && *request.ExpectedGeneration != current.Generation {
		return DeleteResult{}, conflict(
			fmt.Sprintf("expected generation %d, found %d", *request.ExpectedGeneration, current.Generation),
			*request.ExpectedGeneration,
			current.Generation,
		)
	}

	deleteGeneration, err := nextGeneration(current.Generation)
	if err != nil {
		return DeleteResult{}, err
	}
	delete(registry.resources, request.Key)
	registry.lastGeneration[request.Key] = deleteGeneration
	result := DeleteResult{Key: request.Key, Generation: deleteGeneration, Deleted: true}
	stored := result
	registry.tokens[request.RequestToken] = tokenRecord{
		operation:   "delete",
		fingerprint: fingerprint,
		delete:      &stored,
	}
	return result, nil
}

// Count returns the number of live resources.
func (registry *Registry) Count() int {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	return len(registry.resources)
}

func normalizeApply(request ApplyRequest) (ApplyRequest, error) {
	request.RequestToken = strings.TrimSpace(request.RequestToken)
	if err := validateToken(request.RequestToken); err != nil {
		return ApplyRequest{}, err
	}
	key, err := normalizeKey(request.Resource.ResourceKey)
	if err != nil {
		return ApplyRequest{}, err
	}
	request.Resource.ResourceKey = key
	request.Resource.Labels = cloneLabels(request.Resource.Labels)
	request.Resource.Spec, err = canonicalJSON(request.Resource.Spec)
	if err != nil {
		return ApplyRequest{}, invalid("spec must be one valid JSON object")
	}
	return request, nil
}

func normalizeKey(key ResourceKey) (ResourceKey, error) {
	key.Namespace = strings.TrimSpace(key.Namespace)
	key.Name = strings.TrimSpace(key.Name)
	if key.Namespace == "" {
		return ResourceKey{}, invalid("namespace is required")
	}
	if strings.Contains(key.Namespace, "/") {
		return ResourceKey{}, invalid("namespace cannot contain '/'")
	}
	if !key.Kind.Valid() {
		return ResourceKey{}, invalid(fmt.Sprintf("unknown resource kind %q", key.Kind))
	}
	if key.Name == "" {
		return ResourceKey{}, invalid("name is required")
	}
	if strings.Contains(key.Name, "/") {
		return ResourceKey{}, invalid("name cannot contain '/'")
	}
	return key, nil
}

func validateToken(token string) error {
	if token == "" {
		return invalid("request_token is required")
	}
	if len(token) > 256 {
		return invalid("request_token must be at most 256 bytes")
	}
	return nil
}

func canonicalJSON(raw json.RawMessage) (json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = json.RawMessage(`{}`)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return nil, fmt.Errorf("multiple JSON values")
	} else if err != io.EOF {
		return nil, err
	}
	object, ok := value.(map[string]any)
	if !ok || object == nil {
		return nil, fmt.Errorf("JSON value is not an object")
	}
	canonical, err := json.Marshal(object)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(canonical), nil
}

func fingerprint(operation string, value any) (string, error) {
	encoded, err := json.Marshal(struct {
		Operation string `json:"operation"`
		Value     any    `json:"value"`
	}{Operation: operation, Value: value})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func nextGeneration(current uint64) (uint64, error) {
	if current == math.MaxUint64 {
		return 0, conflict("resource generation exhausted", current, current)
	}
	return current + 1, nil
}

func materialize(desired DesiredResource, generation uint64) Resource {
	return Resource{
		ResourceKey: desired.ResourceKey,
		Labels:      cloneLabels(desired.Labels),
		Spec:        cloneJSON(desired.Spec),
		Generation:  generation,
		Status: ResourceStatus{
			Phase:   PhasePending,
			Message: "accepted by control plane; awaiting regional reconciliation",
		},
	}
}

func desiredEqual(current Resource, desired DesiredResource) bool {
	if !bytes.Equal(current.Spec, desired.Spec) || len(current.Labels) != len(desired.Labels) {
		return false
	}
	for key, value := range current.Labels {
		if desired.Labels[key] != value {
			return false
		}
	}
	return true
}

func cloneApplyResult(result ApplyResult) ApplyResult {
	result.Resource = cloneResource(result.Resource)
	return result
}

func cloneResource(resource Resource) Resource {
	resource.Labels = cloneLabels(resource.Labels)
	resource.Spec = cloneJSON(resource.Spec)
	return resource
}

func cloneLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(labels))
	for key, value := range labels {
		cloned[key] = value
	}
	return cloned
}

func cloneJSON(raw json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), raw...)
}

func invalid(message string) *RegistryError {
	return &RegistryError{Code: CodeInvalidArgument, Message: message}
}

func notFound(key ResourceKey) *RegistryError {
	return &RegistryError{
		Code:    CodeNotFound,
		Message: fmt.Sprintf("resource %s/%s/%s was not found", key.Namespace, key.Kind, key.Name),
	}
}

func conflict(message string, expected, actual uint64) *RegistryError {
	return &RegistryError{
		Code:               CodeConflict,
		Message:            message,
		ExpectedGeneration: expected,
		ActualGeneration:   actual,
	}
}
