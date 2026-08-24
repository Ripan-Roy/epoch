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
	"slices"
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

// ResourceKey is the stable identity of a resource. Organization, project, and
// environment are either all present for the regional contract or all absent
// for the legacy local HTTP slice.
type ResourceKey struct {
	Organization string `json:"organization,omitempty"`
	Project      string `json:"project,omitempty"`
	Environment  string `json:"environment,omitempty"`
	Namespace    string `json:"namespace"`
	Kind         Kind   `json:"kind"`
	Name         string `json:"name"`
}

// DesiredResource is the declarative input accepted by Apply. Spec is kept as
// canonical JSON until generated Protobuf bindings replace this HTTP boundary.
type DesiredResource struct {
	ResourceKey
	Labels     map[string]string   `json:"labels,omitempty"`
	Governance *ResourceGovernance `json:"governance,omitempty"`
	Spec       json.RawMessage     `json:"spec"`
}

// ResourcePhase describes reconciliation state. New desired generations remain
// pending until the regional Rust data plane reports an observed generation.
type ResourcePhase string

const (
	PhasePending  ResourcePhase = "pending"
	PhaseReady    ResourcePhase = "ready"
	PhaseDegraded ResourcePhase = "degraded"
	PhaseFailed   ResourcePhase = "failed"
)

// TabletStatus reports achieved regional routing state without inferring that
// desired replicas exist merely because the catalog requested them.
type TabletStatus struct {
	TabletID              uint64   `json:"tablet_id"`
	ConsensusGroupID      uint64   `json:"consensus_group_id"`
	ShardIndex            uint32   `json:"shard_index"`
	TabletEpoch           uint64   `json:"tablet_epoch"`
	ResourceGeneration    uint64   `json:"resource_generation"`
	DesiredReplicas       uint32   `json:"desired_replicas"`
	AssignedNodeIDs       []uint64 `json:"assigned_node_ids"`
	BootstrapVoterNodeIDs []uint64 `json:"bootstrap_voter_node_ids,omitempty"`
	TargetVoterNodeIDs    []uint64 `json:"target_voter_node_ids,omitempty"`
	VoterNodeIDs          []uint64 `json:"voter_node_ids"`
	ReachableVoterNodeIDs []uint64 `json:"reachable_voter_node_ids"`
	LeaderNodeID          uint64   `json:"leader_node_id,omitempty"`
}

// RegionalNodeStatus is one policy-protected configured-endpoint topology and
// capacity observation used to explain an admission decision.
type RegionalNodeStatus struct {
	NodeID                   uint64   `json:"node_id"`
	Region                   string   `json:"region"`
	Zone                     string   `json:"zone"`
	NodeClass                string   `json:"node_class"`
	ConsensusVoterNodeIDs    []uint64 `json:"consensus_voter_node_ids"`
	MaxConsensusGroups       uint32   `json:"max_consensus_groups"`
	UsedConsensusGroups      uint32   `json:"used_consensus_groups"`
	AvailableConsensusGroups uint32   `json:"available_consensus_groups"`
}

// PlacementStatus reports requested and achieved failure-domain constraints.
type PlacementStatus struct {
	AllowedRegions    []string             `json:"allowed_regions,omitempty"`
	MinimumZones      uint32               `json:"minimum_zones"`
	RequiredNodeClass string               `json:"required_node_class,omitempty"`
	AchievedZones     uint32               `json:"achieved_zones"`
	Nodes             []RegionalNodeStatus `json:"nodes"`
}

// ResourceStatus is intentionally small in the initial slice and never
// implies that an unconnected data plane has achieved the requested state.
type ResourceStatus struct {
	Phase              ResourcePhase    `json:"phase"`
	ObservedGeneration uint64           `json:"observed_generation"`
	ObservedShardCount uint32           `json:"observed_shard_count,omitempty"`
	Message            string           `json:"message,omitempty"`
	Tablets            []TabletStatus   `json:"tablets,omitempty"`
	Placement          *PlacementStatus `json:"placement,omitempty"`
}

// Resource is the registry's immutable response value.
type Resource struct {
	ResourceKey
	Labels     map[string]string   `json:"labels,omitempty"`
	Governance *ResourceGovernance `json:"governance,omitempty"`
	Spec       json.RawMessage     `json:"spec"`
	Generation uint64              `json:"generation"`
	Status     ResourceStatus      `json:"status"`
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
// monotonic across control-plane restarts when durable storage is configured.
type DeleteResult struct {
	Key        ResourceKey `json:"key"`
	Generation uint64      `json:"generation"`
	Deleted    bool        `json:"deleted"`
	Replayed   bool        `json:"replayed"`
}

// ListFilter limits a stable, key-sorted list operation.
type ListFilter struct {
	Organization   string
	Project        string
	Environment    string
	Namespace      string
	Kind           Kind
	Owner          string
	CostCenter     string
	Classification DataClassification
	Tags           map[string]string
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
	cause              error
}

func (err *RegistryError) Error() string {
	return err.Message
}

func (err *RegistryError) Unwrap() error {
	return err.cause
}

type tokenRecord struct {
	Operation   string        `json:"operation"`
	Fingerprint string        `json:"fingerprint"`
	Apply       *ApplyResult  `json:"apply,omitempty"`
	Delete      *DeleteResult `json:"delete,omitempty"`
}

type registryState struct {
	resources      map[ResourceKey]Resource
	lastGeneration map[ResourceKey]uint64
	tokens         map[string]tokenRecord
}

type registryMutation struct {
	resourceKey    ResourceKey
	resource       *Resource
	deleteResource bool
	generation     *uint64
	token          string
	tokenRecord    *tokenRecord
}

type registryPersistence interface {
	Commit(registryMutation) error
	Close() error
	Mode() string
}

// Registry is a concurrency-safe declarative registry. NewRegistry is
// intentionally memory-only for unit and embedded use; OpenDurableRegistry
// attaches the same state machine to versioned transactional storage.
type Registry struct {
	mu             sync.RWMutex
	resources      map[ResourceKey]Resource
	lastGeneration map[ResourceKey]uint64
	tokens         map[string]tokenRecord
	persistence    registryPersistence
	mode           string
	closed         bool
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return newRegistry(emptyRegistryState(), nil)
}

func emptyRegistryState() registryState {
	return registryState{
		resources:      make(map[ResourceKey]Resource),
		lastGeneration: make(map[ResourceKey]uint64),
		tokens:         make(map[string]tokenRecord),
	}
}

func newRegistry(state registryState, persistence registryPersistence) *Registry {
	mode := "memory"
	if persistence != nil {
		mode = persistence.Mode()
	}
	return &Registry{
		resources:      state.resources,
		lastGeneration: state.lastGeneration,
		tokens:         state.tokens,
		persistence:    persistence,
		mode:           mode,
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
	if registry.closed {
		return ApplyResult{}, internal("control metadata registry is closed", nil)
	}

	if prior, found := registry.tokens[normalized.RequestToken]; found {
		if prior.Operation != "apply" || prior.Fingerprint != fingerprint {
			return ApplyResult{}, conflict("request token was already used for a different operation", 0, 0)
		}
		result := cloneApplyResult(*prior.Apply)
		result.Replayed = true
		return result, nil
	}

	key := normalized.Resource.ResourceKey
	if key.Organization != "" && normalized.Resource.Governance == nil {
		return ApplyResult{}, invalid("governance is required for a managed regional resource")
	}
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

	var (
		result              ApplyResult
		nextResource        *Resource
		persistedGeneration *uint64
	)
	switch {
	case !exists:
		generation, err := nextGeneration(registry.lastGeneration[key])
		if err != nil {
			return ApplyResult{}, err
		}
		resource := materialize(normalized.Resource, generation, ResourceStatus{})
		result = ApplyResult{Resource: cloneResource(resource), Created: true, Changed: true}
		nextResource = &resource
		persistedGeneration = &generation
	case desiredEqual(current, normalized.Resource):
		result = ApplyResult{Resource: cloneResource(current)}
	default:
		generation, err := nextGeneration(current.Generation)
		if err != nil {
			return ApplyResult{}, err
		}
		resource := materialize(normalized.Resource, generation, current.Status)
		result = ApplyResult{Resource: cloneResource(resource), Changed: true}
		nextResource = &resource
		persistedGeneration = &generation
	}

	stored := cloneApplyResult(result)
	record := tokenRecord{
		Operation:   "apply",
		Fingerprint: fingerprint,
		Apply:       &stored,
	}
	if err := registry.commitLocked(registryMutation{
		resourceKey: key,
		resource:    nextResource,
		generation:  persistedGeneration,
		token:       normalized.RequestToken,
		tokenRecord: &record,
	}); err != nil {
		return ApplyResult{}, err
	}
	if nextResource != nil {
		registry.resources[key] = cloneResource(*nextResource)
	}
	if persistedGeneration != nil {
		registry.lastGeneration[key] = *persistedGeneration
	}
	registry.tokens[normalized.RequestToken] = cloneTokenRecord(record)
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
	filter.Organization = strings.TrimSpace(filter.Organization)
	filter.Project = strings.TrimSpace(filter.Project)
	filter.Environment = strings.TrimSpace(filter.Environment)
	filter.Namespace = strings.TrimSpace(filter.Namespace)
	if filter.Kind != "" && !filter.Kind.Valid() {
		return nil, invalid(fmt.Sprintf("unknown resource kind %q", filter.Kind))
	}
	var err error
	filter, err = normalizeGovernanceFilter(filter)
	if err != nil {
		return nil, err
	}

	registry.mu.RLock()
	resources := make([]Resource, 0, len(registry.resources))
	for _, resource := range registry.resources {
		if filter.Organization != "" && resource.Organization != filter.Organization {
			continue
		}
		if filter.Project != "" && resource.Project != filter.Project {
			continue
		}
		if filter.Environment != "" && resource.Environment != filter.Environment {
			continue
		}
		if filter.Namespace != "" && resource.Namespace != filter.Namespace {
			continue
		}
		if filter.Kind != "" && resource.Kind != filter.Kind {
			continue
		}
		if !governanceMatches(resource.Governance, filter) {
			continue
		}
		resources = append(resources, cloneResource(resource))
	}
	registry.mu.RUnlock()

	sort.Slice(resources, func(left, right int) bool {
		if resources[left].Organization != resources[right].Organization {
			return resources[left].Organization < resources[right].Organization
		}
		if resources[left].Project != resources[right].Project {
			return resources[left].Project < resources[right].Project
		}
		if resources[left].Environment != resources[right].Environment {
			return resources[left].Environment < resources[right].Environment
		}
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
	if registry.closed {
		return DeleteResult{}, internal("control metadata registry is closed", nil)
	}

	if prior, found := registry.tokens[request.RequestToken]; found {
		if prior.Operation != "delete" || prior.Fingerprint != fingerprint {
			return DeleteResult{}, conflict("request token was already used for a different operation", 0, 0)
		}
		result := *prior.Delete
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
		record := tokenRecord{
			Operation:   "delete",
			Fingerprint: fingerprint,
			Delete:      &stored,
		}
		if err := registry.commitLocked(registryMutation{
			token:       request.RequestToken,
			tokenRecord: &record,
		}); err != nil {
			return DeleteResult{}, err
		}
		registry.tokens[request.RequestToken] = cloneTokenRecord(record)
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
	result := DeleteResult{Key: request.Key, Generation: deleteGeneration, Deleted: true}
	stored := result
	record := tokenRecord{
		Operation:   "delete",
		Fingerprint: fingerprint,
		Delete:      &stored,
	}
	if err := registry.commitLocked(registryMutation{
		resourceKey:    request.Key,
		deleteResource: true,
		generation:     &deleteGeneration,
		token:          request.RequestToken,
		tokenRecord:    &record,
	}); err != nil {
		return DeleteResult{}, err
	}
	delete(registry.resources, request.Key)
	registry.lastGeneration[request.Key] = deleteGeneration
	registry.tokens[request.RequestToken] = cloneTokenRecord(record)
	return result, nil
}

// Count returns the number of live resources.
func (registry *Registry) Count() int {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	return len(registry.resources)
}

// UpdateStatus atomically records an observation only while the desired
// generation still matches. A late reconciler cannot mark a newer desired
// state ready with evidence gathered for an older generation.
func (registry *Registry) UpdateStatus(
	key ResourceKey,
	desiredGeneration uint64,
	status ResourceStatus,
) (Resource, error) {
	normalized, err := normalizeKey(key)
	if err != nil {
		return Resource{}, err
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return Resource{}, internal("control metadata registry is closed", nil)
	}
	current, found := registry.resources[normalized]
	if !found {
		return Resource{}, notFound(normalized)
	}
	if current.Generation != desiredGeneration {
		return Resource{}, conflict(
			fmt.Sprintf("expected generation %d, found %d", desiredGeneration, current.Generation),
			desiredGeneration,
			current.Generation,
		)
	}
	if err := validateStatus(status, desiredGeneration); err != nil {
		return Resource{}, err
	}
	next := cloneResource(current)
	next.Status = cloneStatus(status)
	if statusEqual(current.Status, next.Status) {
		return cloneResource(current), nil
	}
	if err := registry.commitLocked(registryMutation{
		resourceKey: normalized,
		resource:    &next,
	}); err != nil {
		return Resource{}, err
	}
	registry.resources[normalized] = cloneResource(next)
	return cloneResource(next), nil
}

// Mode reports the configured metadata storage implementation.
func (registry *Registry) Mode() string {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	return registry.mode
}

// Close releases durable registry ownership. It is safe to call more than once.
func (registry *Registry) Close() error {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return nil
	}
	registry.closed = true
	if registry.persistence == nil {
		return nil
	}
	return registry.persistence.Close()
}

func (registry *Registry) commitLocked(mutation registryMutation) error {
	if registry.closed {
		return internal("control metadata registry is closed", nil)
	}
	if registry.persistence == nil {
		return nil
	}
	if err := registry.persistence.Commit(mutation); err != nil {
		return internal("control metadata commit failed", err)
	}
	return nil
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
	request.Resource.Governance, err = NormalizeGovernance(request.Resource.Governance)
	if err != nil {
		return ApplyRequest{}, err
	}
	request.Resource.Spec, err = canonicalJSON(request.Resource.Spec)
	if err != nil {
		return ApplyRequest{}, invalid("spec must be one valid JSON object")
	}
	return request, nil
}

func normalizeKey(key ResourceKey) (ResourceKey, error) {
	key.Organization = strings.TrimSpace(key.Organization)
	key.Project = strings.TrimSpace(key.Project)
	key.Environment = strings.TrimSpace(key.Environment)
	key.Namespace = strings.TrimSpace(key.Namespace)
	key.Name = strings.TrimSpace(key.Name)
	regionalComponents := []struct {
		name  string
		value string
	}{
		{name: "organization", value: key.Organization},
		{name: "project", value: key.Project},
		{name: "environment", value: key.Environment},
	}
	regional := false
	for _, component := range regionalComponents {
		regional = regional || component.value != ""
	}
	if regional {
		for _, component := range regionalComponents {
			if component.value == "" {
				return ResourceKey{}, invalid(
					"organization, project, and environment must be provided together",
				)
			}
			if strings.Contains(component.value, "/") {
				return ResourceKey{}, invalid(component.name + " cannot contain '/'")
			}
		}
	}
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

// NormalizeKey validates and canonicalizes a resource identity without
// mutating registry state.
func NormalizeKey(key ResourceKey) (ResourceKey, error) {
	return normalizeKey(key)
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

func materialize(
	desired DesiredResource,
	generation uint64,
	previousStatus ResourceStatus,
) Resource {
	status := cloneStatus(previousStatus)
	status.Phase = PhasePending
	status.Message = "accepted by control plane; awaiting regional reconciliation"
	return Resource{
		ResourceKey: desired.ResourceKey,
		Labels:      cloneLabels(desired.Labels),
		Governance:  cloneGovernance(desired.Governance),
		Spec:        cloneJSON(desired.Spec),
		Generation:  generation,
		Status:      status,
	}
}

func desiredEqual(current Resource, desired DesiredResource) bool {
	if !bytes.Equal(current.Spec, desired.Spec) ||
		len(current.Labels) != len(desired.Labels) ||
		!governanceEqual(current.Governance, desired.Governance) {
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

func cloneTokenRecord(record tokenRecord) tokenRecord {
	if record.Apply != nil {
		apply := cloneApplyResult(*record.Apply)
		record.Apply = &apply
	}
	if record.Delete != nil {
		deleted := *record.Delete
		record.Delete = &deleted
	}
	return record
}

func cloneResource(resource Resource) Resource {
	resource.Labels = cloneLabels(resource.Labels)
	resource.Governance = cloneGovernance(resource.Governance)
	resource.Spec = cloneJSON(resource.Spec)
	resource.Status = cloneStatus(resource.Status)
	return resource
}

func cloneStatus(status ResourceStatus) ResourceStatus {
	if len(status.Tablets) == 0 {
		status.Tablets = nil
	} else {
		status.Tablets = append([]TabletStatus(nil), status.Tablets...)
		for index := range status.Tablets {
			status.Tablets[index].AssignedNodeIDs = append(
				[]uint64(nil),
				status.Tablets[index].AssignedNodeIDs...,
			)
			status.Tablets[index].BootstrapVoterNodeIDs = append(
				[]uint64(nil),
				status.Tablets[index].BootstrapVoterNodeIDs...,
			)
			status.Tablets[index].TargetVoterNodeIDs = append(
				[]uint64(nil),
				status.Tablets[index].TargetVoterNodeIDs...,
			)
			status.Tablets[index].VoterNodeIDs = append(
				[]uint64(nil),
				status.Tablets[index].VoterNodeIDs...,
			)
			status.Tablets[index].ReachableVoterNodeIDs = append(
				[]uint64(nil),
				status.Tablets[index].ReachableVoterNodeIDs...,
			)
		}
	}
	if status.Placement != nil {
		placement := *status.Placement
		placement.AllowedRegions = append([]string(nil), placement.AllowedRegions...)
		placement.Nodes = append([]RegionalNodeStatus(nil), placement.Nodes...)
		for index := range placement.Nodes {
			placement.Nodes[index].ConsensusVoterNodeIDs = append(
				[]uint64(nil),
				placement.Nodes[index].ConsensusVoterNodeIDs...,
			)
		}
		status.Placement = &placement
	}
	return status
}

func statusEqual(left, right ResourceStatus) bool {
	if left.Phase != right.Phase ||
		left.ObservedGeneration != right.ObservedGeneration ||
		left.ObservedShardCount != right.ObservedShardCount ||
		left.Message != right.Message ||
		!placementStatusEqual(left.Placement, right.Placement) ||
		len(left.Tablets) != len(right.Tablets) {
		return false
	}
	for index := range left.Tablets {
		leftTablet := left.Tablets[index]
		rightTablet := right.Tablets[index]
		if leftTablet.TabletID != rightTablet.TabletID ||
			leftTablet.ConsensusGroupID != rightTablet.ConsensusGroupID ||
			leftTablet.ShardIndex != rightTablet.ShardIndex ||
			leftTablet.TabletEpoch != rightTablet.TabletEpoch ||
			leftTablet.ResourceGeneration != rightTablet.ResourceGeneration ||
			leftTablet.DesiredReplicas != rightTablet.DesiredReplicas ||
			leftTablet.LeaderNodeID != rightTablet.LeaderNodeID ||
			!slices.Equal(leftTablet.AssignedNodeIDs, rightTablet.AssignedNodeIDs) ||
			!slices.Equal(leftTablet.BootstrapVoterNodeIDs, rightTablet.BootstrapVoterNodeIDs) ||
			!slices.Equal(leftTablet.TargetVoterNodeIDs, rightTablet.TargetVoterNodeIDs) ||
			!slices.Equal(leftTablet.VoterNodeIDs, rightTablet.VoterNodeIDs) ||
			!slices.Equal(leftTablet.ReachableVoterNodeIDs, rightTablet.ReachableVoterNodeIDs) {
			return false
		}
	}
	return true
}

func placementStatusEqual(left, right *PlacementStatus) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	if left.MinimumZones != right.MinimumZones ||
		left.RequiredNodeClass != right.RequiredNodeClass ||
		left.AchievedZones != right.AchievedZones ||
		len(left.AllowedRegions) != len(right.AllowedRegions) ||
		len(left.Nodes) != len(right.Nodes) {
		return false
	}
	for index := range left.AllowedRegions {
		if left.AllowedRegions[index] != right.AllowedRegions[index] {
			return false
		}
	}
	for index := range left.Nodes {
		leftNode := left.Nodes[index]
		rightNode := right.Nodes[index]
		if leftNode.NodeID != rightNode.NodeID ||
			leftNode.Region != rightNode.Region ||
			leftNode.Zone != rightNode.Zone ||
			leftNode.NodeClass != rightNode.NodeClass ||
			leftNode.MaxConsensusGroups != rightNode.MaxConsensusGroups ||
			leftNode.UsedConsensusGroups != rightNode.UsedConsensusGroups ||
			leftNode.AvailableConsensusGroups != rightNode.AvailableConsensusGroups ||
			!slices.Equal(leftNode.ConsensusVoterNodeIDs, rightNode.ConsensusVoterNodeIDs) {
			return false
		}
	}
	return true
}

func validateStatus(status ResourceStatus, desiredGeneration uint64) error {
	switch status.Phase {
	case PhasePending, PhaseReady, PhaseDegraded, PhaseFailed:
	default:
		return invalid(fmt.Sprintf("unknown resource phase %q", status.Phase))
	}
	if status.ObservedGeneration > desiredGeneration {
		return invalid("observed generation cannot exceed desired generation")
	}
	if status.Phase == PhaseReady && status.ObservedGeneration != desiredGeneration {
		return invalid("ready status must observe the current desired generation")
	}
	if status.ObservedShardCount != 0 &&
		status.ObservedShardCount < uint32(len(status.Tablets)) {
		return invalid("observed shard count cannot be smaller than the reported tablet set")
	}
	if status.Placement != nil {
		if err := validatePlacementStatus(*status.Placement); err != nil {
			return err
		}
	}
	tabletIDs := make(map[uint64]struct{}, len(status.Tablets))
	groupIDs := make(map[uint64]struct{}, len(status.Tablets))
	shards := make(map[uint32]struct{}, len(status.Tablets))
	for _, tablet := range status.Tablets {
		if tablet.TabletID == 0 || tablet.ConsensusGroupID == 0 ||
			tablet.TabletEpoch == 0 || tablet.DesiredReplicas == 0 {
			return invalid("tablet identity, epoch, group, and desired replicas must be non-zero")
		}
		if tablet.ResourceGeneration != status.ObservedGeneration {
			return invalid("tablet resource generation must match the observed generation")
		}
		if _, exists := tabletIDs[tablet.TabletID]; exists {
			return invalid("tablet IDs must be unique")
		}
		if _, exists := groupIDs[tablet.ConsensusGroupID]; exists {
			return invalid("consensus group IDs must be unique")
		}
		if _, exists := shards[tablet.ShardIndex]; exists {
			return invalid("tablet shard indexes must be unique")
		}
		tabletIDs[tablet.TabletID] = struct{}{}
		groupIDs[tablet.ConsensusGroupID] = struct{}{}
		shards[tablet.ShardIndex] = struct{}{}
		assigned, err := tabletNodeSet(tablet.AssignedNodeIDs, "assigned")
		if err != nil {
			return err
		}
		if len(tablet.AssignedNodeIDs) > 0 && len(tablet.AssignedNodeIDs) != int(tablet.DesiredReplicas) {
			return invalid("assigned node IDs must match desired replicas")
		}
		bootstrap, err := tabletNodeSet(tablet.BootstrapVoterNodeIDs, "bootstrap voter")
		if err != nil {
			return err
		}
		if len(bootstrap) > 0 && len(bootstrap) != int(tablet.DesiredReplicas) {
			return invalid("bootstrap voter node IDs must match desired replicas")
		}
		target, err := tabletNodeSet(tablet.TargetVoterNodeIDs, "target voter")
		if err != nil {
			return err
		}
		if len(target) > 0 {
			if status.Phase == PhaseReady {
				return invalid("ready status cannot contain an active voter replacement")
			}
			if len(target) != int(tablet.DesiredReplicas) ||
				!singleTabletVoterReplacement(assigned, target) {
				return invalid("target voter node IDs must replace exactly one assigned voter")
			}
		}
		voters, err := tabletNodeSet(tablet.VoterNodeIDs, "voter")
		if err != nil {
			return err
		}
		if len(voters) > 0 && !sameNodeSet(voters, assigned) &&
			(len(target) == 0 || !sameNodeSet(voters, target)) {
			return invalid("observed voters must match the current or target assigned node set")
		}
		reachable, err := tabletNodeSet(tablet.ReachableVoterNodeIDs, "reachable voter")
		if err != nil {
			return err
		}
		for nodeID := range reachable {
			if _, committed := voters[nodeID]; !committed {
				return invalid("reachable voters must belong to committed membership")
			}
		}
		if tablet.LeaderNodeID != 0 {
			if _, exists := reachable[tablet.LeaderNodeID]; !exists {
				return invalid("tablet leader must be a reachable committed voter")
			}
		}
	}
	return nil
}

func tabletNodeSet(values []uint64, label string) (map[uint64]struct{}, error) {
	if !slices.IsSorted(values) {
		return nil, invalid(label + " node IDs must be sorted")
	}
	result := make(map[uint64]struct{}, len(values))
	for _, nodeID := range values {
		if nodeID == 0 {
			return nil, invalid(label + " node IDs must be non-zero")
		}
		if _, duplicate := result[nodeID]; duplicate {
			return nil, invalid(label + " node IDs must be unique per tablet")
		}
		result[nodeID] = struct{}{}
	}
	return result, nil
}

func sameNodeSet(left, right map[uint64]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for nodeID := range left {
		if _, exists := right[nodeID]; !exists {
			return false
		}
	}
	return true
}

func singleTabletVoterReplacement(current, target map[uint64]struct{}) bool {
	if len(current) != len(target) || len(current) == 0 {
		return false
	}
	removed := 0
	for nodeID := range current {
		if _, retained := target[nodeID]; !retained {
			removed++
		}
	}
	added := 0
	for nodeID := range target {
		if _, retained := current[nodeID]; !retained {
			added++
		}
	}
	return removed == 1 && added == 1
}

func validatePlacementStatus(status PlacementStatus) error {
	if status.MinimumZones == 0 ||
		status.AchievedZones < status.MinimumZones ||
		len(status.Nodes) == 0 {
		return invalid("placement status must contain satisfied zone and node evidence")
	}
	if status.RequiredNodeClass != "" && !validPlacementLabel(status.RequiredNodeClass) {
		return invalid("placement status contains an invalid required node class")
	}
	regions := make(map[string]struct{}, len(status.AllowedRegions))
	for _, region := range status.AllowedRegions {
		if !validPlacementLabel(region) {
			return invalid("placement status contains an invalid allowed region")
		}
		if _, duplicate := regions[region]; duplicate {
			return invalid("placement status allowed regions must be unique")
		}
		regions[region] = struct{}{}
	}
	nodes := make(map[uint64]struct{}, len(status.Nodes))
	zones := make(map[string]struct{}, len(status.Nodes))
	for _, node := range status.Nodes {
		if node.NodeID == 0 {
			return invalid("placement node IDs must be non-zero")
		}
		if _, duplicate := nodes[node.NodeID]; duplicate {
			return invalid("placement node IDs must be unique")
		}
		nodes[node.NodeID] = struct{}{}
		if !validPlacementLabel(node.Region) ||
			!validPlacementLabel(node.Zone) ||
			!validPlacementLabel(node.NodeClass) {
			return invalid("placement node contains an invalid topology label")
		}
		if node.MaxConsensusGroups == 0 ||
			node.UsedConsensusGroups > node.MaxConsensusGroups ||
			node.AvailableConsensusGroups != node.MaxConsensusGroups-node.UsedConsensusGroups {
			return invalid("placement node contains inconsistent group capacity")
		}
		voters := make(map[uint64]struct{}, len(node.ConsensusVoterNodeIDs))
		for _, voter := range node.ConsensusVoterNodeIDs {
			if voter == 0 {
				return invalid("placement voter IDs must be non-zero")
			}
			if _, duplicate := voters[voter]; duplicate {
				return invalid("placement voter IDs must be unique")
			}
			voters[voter] = struct{}{}
		}
		zones[node.Zone] = struct{}{}
	}
	if status.AchievedZones > uint32(len(zones)) {
		return invalid("placement achieved zones cannot exceed inventory failure domains")
	}
	return nil
}

func validPlacementLabel(value string) bool {
	if value == "" || len(value) > 63 {
		return false
	}
	for index := range len(value) {
		character := value[index]
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			(index > 0 && (character == '.' || character == '_' || character == '-')) {
			continue
		}
		return false
	}
	return true
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
	scope := ""
	if key.Organization != "" {
		scope = fmt.Sprintf(
			"%s/%s/%s/",
			key.Organization,
			key.Project,
			key.Environment,
		)
	}
	return &RegistryError{
		Code: CodeNotFound,
		Message: fmt.Sprintf(
			"resource %s%s/%s/%s was not found",
			scope,
			key.Namespace,
			key.Kind,
			key.Name,
		),
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

func internal(message string, cause error) *RegistryError {
	return &RegistryError{
		Code:    CodeInternal,
		Message: message,
		cause:   cause,
	}
}
