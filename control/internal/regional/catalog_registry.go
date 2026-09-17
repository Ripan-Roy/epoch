package regional

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"epoch.local/epoch/control/internal/resources"
)

const (
	controlResourcesPath        = "/experimental/v1/regional/control/resources"
	controlImportPath           = "/experimental/v1/regional/control/import"
	controlLeasePath            = "/experimental/v1/regional/control/lease"
	controlMaterializationsPath = "/experimental/v1/regional/control/materializations"
	controlOperationsPath       = "/experimental/v1/regional/control/operations"
	controlChangesPath          = "/experimental/v1/regional/control/changes"
	controlLeaseTTL             = 10 * time.Second
	controlCallTimeout          = 10 * time.Second
	maxControlOwnerBytes        = 128
)

var errManagedResourceNotFound = errors.New("managed resource was not found")

type controlResourceNameDocument struct {
	Organization string `json:"organization"`
	Project      string `json:"project"`
	Environment  string `json:"environment"`
	Namespace    string `json:"namespace"`
	Kind         string `json:"kind"`
	Name         string `json:"name"`
}

type controlDesiredWriteDocument struct {
	Name               controlResourceNameDocument `json:"name"`
	ExpectedGeneration *string                     `json:"expected_generation,omitempty"`
	Desired            json.RawMessage             `json:"desired"`
}

type controlApplyDesiredBody struct {
	RequestToken string                        `json:"request_token"`
	Resources    []controlDesiredWriteDocument `json:"resources"`
}

type controlDeleteDesiredBody struct {
	RequestToken       string  `json:"request_token"`
	ExpectedGeneration *string `json:"expected_generation,omitempty"`
}

type controlDeleteManagedBody struct {
	RequestToken              string                    `json:"request_token"`
	Lease                     controlLeaseGuardDocument `json:"lease"`
	ExpectedDesiredGeneration string                    `json:"expected_desired_generation"`
	ExpectedCatalogGeneration string                    `json:"expected_catalog_generation"`
}

type controlImportResourceDocument struct {
	Name       controlResourceNameDocument `json:"name"`
	Generation string                      `json:"generation"`
	Desired    json.RawMessage             `json:"desired"`
	Status     json.RawMessage             `json:"status"`
}

type controlImportGenerationDocument struct {
	Name       controlResourceNameDocument `json:"name"`
	Generation string                      `json:"generation"`
}

type controlImportBody struct {
	RequestToken string                            `json:"request_token"`
	Resources    []controlImportResourceDocument   `json:"resources"`
	Generations  []controlImportGenerationDocument `json:"generations"`
}

type controlManagedResourceDocument struct {
	Name              controlResourceNameDocument `json:"name"`
	Generation        decimalUint64               `json:"generation"`
	Desired           json.RawMessage             `json:"desired"`
	Status            json.RawMessage             `json:"status"`
	DeletionRequested bool                        `json:"deletion_requested"`
}

type controlApplyResultDocument struct {
	Resource controlManagedResourceDocument `json:"resource"`
	Created  bool                           `json:"created"`
	Changed  bool                           `json:"changed"`
}

type controlMutationDocument struct {
	Kind              string                          `json:"kind"`
	Resources         []controlApplyResultDocument    `json:"resources"`
	Resource          *controlManagedResourceDocument `json:"resource"`
	Name              *controlResourceNameDocument    `json:"name"`
	Generation        decimalUint64                   `json:"generation"`
	DesiredGeneration decimalUint64                   `json:"desired_generation"`
	CatalogGeneration decimalUint64                   `json:"catalog_generation"`
	Deleted           bool                            `json:"deleted"`
	Replayed          bool                            `json:"replayed"`
	Lease             *controlLeaseDocument           `json:"lease"`
	Code              string                          `json:"code"`
	Message           string                          `json:"message"`
}

type controlMutationReceiptDocument struct {
	RequestReplayed bool                    `json:"request_replayed"`
	Mutation        controlMutationDocument `json:"mutation"`
}

type controlResourceListDocument struct {
	LatestChangeCursor decimalUint64                    `json:"latest_change_cursor"`
	Resources          []controlManagedResourceDocument `json:"resources"`
}

type ControlOperationState string

const (
	ControlOperationPending   ControlOperationState = "pending"
	ControlOperationSucceeded ControlOperationState = "succeeded"
	ControlOperationFailed    ControlOperationState = "failed"
)

type controlOperationDocument struct {
	RequestToken      string                        `json:"request_token"`
	ProposalID        decimalUint64                 `json:"proposal_id"`
	State             ControlOperationState         `json:"state"`
	ResourceNames     []controlResourceNameDocument `json:"resource_names"`
	Mutation          *controlMutationDocument      `json:"mutation"`
	FirstChangeCursor decimalUint64                 `json:"first_change_cursor"`
	LastChangeCursor  decimalUint64                 `json:"last_change_cursor"`
}

type ControlOperation struct {
	RequestToken      string
	ProposalID        uint64
	State             ControlOperationState
	ResourceKeys      []resources.ResourceKey
	FailureCode       string
	FailureMessage    string
	FirstChangeCursor uint64
	LastChangeCursor  uint64
}

type ControlChangeKind string

const (
	ControlChangeDesiredApplied ControlChangeKind = "desired_applied"
	ControlChangeDesiredDeleted ControlChangeKind = "desired_deleted"
	ControlChangeStatusUpdated  ControlChangeKind = "status_updated"
)

type controlChangeDocument struct {
	Cursor     decimalUint64               `json:"cursor"`
	Kind       ControlChangeKind           `json:"kind"`
	Name       controlResourceNameDocument `json:"name"`
	Generation decimalUint64               `json:"generation"`
}

type controlChangesDocument struct {
	EarliestCursor decimalUint64           `json:"earliest_cursor"`
	LatestCursor   decimalUint64           `json:"latest_cursor"`
	Changes        []controlChangeDocument `json:"changes"`
}

type ControlChange struct {
	Cursor     uint64
	Kind       ControlChangeKind
	Key        resources.ResourceKey
	Generation uint64
}

type ControlChangePage struct {
	EarliestCursor uint64
	LatestCursor   uint64
	Changes        []ControlChange
}

type BatchApplyRequest struct {
	RequestToken string
	Resources    []resources.ApplyRequest
}

type BatchApplyResult struct {
	Results  []resources.ApplyResult
	Replayed bool
}

type controlLeaseDocument struct {
	OwnerID      string        `json:"owner_id"`
	Fence        decimalUint64 `json:"fence"`
	ValidUntilMS decimalUint64 `json:"valid_until_ms"`
}

type controlLeaseBody struct {
	RequestToken string `json:"request_token"`
	OwnerID      string `json:"owner_id"`
	NowMS        string `json:"now_ms"`
	TTLMS        string `json:"ttl_ms"`
}

type controlLeaseGuardDocument struct {
	OwnerID string `json:"owner_id"`
	Fence   string `json:"fence"`
	NowMS   string `json:"now_ms"`
}

type controlStatusBody struct {
	RequestToken       string                    `json:"request_token"`
	Lease              controlLeaseGuardDocument `json:"lease"`
	ExpectedGeneration string                    `json:"expected_generation"`
	Status             resources.ResourceStatus  `json:"status"`
}

// CatalogRegistry stores all managed desired state in the replicated Rust
// Catalog. Its only local state is a replaceable lease cache and an advisory
// health count; no acknowledged resource or request outcome depends on this
// process surviving.
type CatalogRegistry struct {
	authority *HTTPAuthority
	ownerID   string
	now       func() time.Time

	leaseMu sync.Mutex
	lease   controlLeaseDocument
	closed  bool
	count   atomic.Int64
}

var _ resources.Store = (*CatalogRegistry)(nil)

func NewCatalogRegistry(authority *HTTPAuthority, ownerID string) (*CatalogRegistry, error) {
	return newCatalogRegistry(authority, ownerID, time.Now)
}

func newCatalogRegistry(
	authority *HTTPAuthority,
	ownerID string,
	now func() time.Time,
) (*CatalogRegistry, error) {
	if authority == nil {
		return nil, fmt.Errorf("regional catalog registry requires an authority")
	}
	if !validControlOwner(ownerID) {
		return nil, fmt.Errorf(
			"control instance ID must be a canonical 1-%d byte identifier",
			maxControlOwnerBytes,
		)
	}
	if now == nil {
		return nil, fmt.Errorf("control clock is required")
	}
	return &CatalogRegistry{authority: authority, ownerID: ownerID, now: now}, nil
}

func (registry *CatalogRegistry) Apply(request resources.ApplyRequest) (resources.ApplyResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), controlCallTimeout)
	defer cancel()
	batch, err := registry.BatchApply(ctx, BatchApplyRequest{
		RequestToken: request.RequestToken,
		Resources:    []resources.ApplyRequest{request},
	})
	if err != nil {
		return resources.ApplyResult{}, err
	}
	if len(batch.Results) != 1 {
		return resources.ApplyResult{}, storeUnavailable(
			"decode desired apply",
			fmt.Errorf("catalog returned an inconsistent desired mutation"),
		)
	}
	return batch.Results[0], nil
}

// BatchApply atomically commits a strictly sorted set of desired resources.
// The returned results follow canonical resource-name order.
func (registry *CatalogRegistry) BatchApply(
	ctx context.Context,
	request BatchApplyRequest,
) (BatchApplyResult, error) {
	token, err := resources.NormalizeRequestToken(request.RequestToken)
	if err != nil {
		return BatchApplyResult{}, err
	}
	if len(request.Resources) == 0 || len(request.Resources) > 128 {
		return BatchApplyResult{}, resources.NewStoreError(
			resources.CodeInvalidArgument,
			"managed batches must contain between 1 and 128 resources",
			0,
			0,
			nil,
		)
	}
	if err := registry.ensureOpen(); err != nil {
		return BatchApplyResult{}, err
	}
	normalized := make([]resources.ApplyRequest, 0, len(request.Resources))
	for _, item := range request.Resources {
		item.RequestToken = token
		item, err = resources.NormalizeApplyRequest(item)
		if err != nil {
			return BatchApplyResult{}, err
		}
		if err := requireRegionalKey(item.Resource.ResourceKey); err != nil {
			return BatchApplyResult{}, err
		}
		normalized = append(normalized, item)
	}
	sort.Slice(normalized, func(left, right int) bool {
		return controlNameLess(
			controlName(normalized[left].Resource.ResourceKey),
			controlName(normalized[right].Resource.ResourceKey),
		)
	})
	body := controlApplyDesiredBody{
		RequestToken: token,
		Resources:    make([]controlDesiredWriteDocument, 0, len(normalized)),
	}
	for index, item := range normalized {
		if index > 0 && item.Resource.ResourceKey == normalized[index-1].Resource.ResourceKey {
			return BatchApplyResult{}, resources.NewStoreError(
				resources.CodeInvalidArgument,
				"managed batch resource names must be distinct",
				0,
				0,
				nil,
			)
		}
		desired, encodeErr := json.Marshal(item.Resource)
		if encodeErr != nil {
			return BatchApplyResult{}, storeUnavailable("encode desired resource", encodeErr)
		}
		body.Resources = append(body.Resources, controlDesiredWriteDocument{
			Name:               controlName(item.Resource.ResourceKey),
			ExpectedGeneration: decimalPointer(item.ExpectedGeneration),
			Desired:            desired,
		})
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return BatchApplyResult{}, storeUnavailable("encode desired apply", err)
	}
	ctx, cancel := context.WithTimeout(ctx, controlCallTimeout)
	defer cancel()
	response, err := registry.authority.requestAny(
		ctx,
		http.MethodPut,
		controlResourcesPath,
		encoded,
	)
	if err != nil {
		return BatchApplyResult{}, registry.mapMutationError(
			err,
			normalized[0].Resource.ResourceKey,
			normalized[0].ExpectedGeneration,
		)
	}
	var receipt controlMutationReceiptDocument
	if err := decodeAuthorityJSON(response, &receipt); err != nil {
		return BatchApplyResult{}, storeUnavailable("decode desired apply", err)
	}
	if receipt.Mutation.Kind != "desired_applied" || len(receipt.Mutation.Resources) != len(normalized) {
		return BatchApplyResult{}, storeUnavailable(
			"decode desired apply",
			fmt.Errorf("catalog returned an inconsistent desired mutation"),
		)
	}
	replayed := receipt.RequestReplayed || receipt.Mutation.Replayed
	batch := BatchApplyResult{Results: make([]resources.ApplyResult, 0, len(normalized)), Replayed: replayed}
	createdCount := int64(0)
	for index, result := range receipt.Mutation.Resources {
		resource, decodeErr := managedResourceFromDocument(result.Resource)
		if decodeErr != nil {
			return BatchApplyResult{}, decodeErr
		}
		if resource.ResourceKey != normalized[index].Resource.ResourceKey {
			return BatchApplyResult{}, storeUnavailable(
				"decode desired apply",
				fmt.Errorf("catalog returned a different resource identity"),
			)
		}
		if result.Created && !replayed {
			createdCount++
		}
		batch.Results = append(batch.Results, resources.ApplyResult{
			Resource: resource,
			Created:  result.Created,
			Changed:  result.Changed,
			Replayed: replayed,
		})
	}
	registry.count.Add(createdCount)
	return batch, nil
}

// ImportLegacy atomically seeds an empty replicated Catalog from the previous
// single-owner registry while preserving every live and tombstoned generation.
func (registry *CatalogRegistry) ImportLegacy(snapshot resources.LegacyRegistrySnapshot) error {
	if len(snapshot.Generations) == 0 {
		return nil
	}
	if err := registry.ensureOpen(); err != nil {
		return err
	}
	body := controlImportBody{
		Resources:   make([]controlImportResourceDocument, 0, len(snapshot.Resources)),
		Generations: make([]controlImportGenerationDocument, 0, len(snapshot.Generations)),
	}
	for _, resource := range snapshot.Resources {
		normalized, err := resources.NormalizeApplyRequest(resources.ApplyRequest{
			RequestToken: "legacy-import-validation",
			Resource: resources.DesiredResource{
				ResourceKey: resource.ResourceKey,
				Labels:      resource.Labels,
				Governance:  resource.Governance,
				Spec:        resource.Spec,
			},
		})
		if err != nil {
			return fmt.Errorf("validate legacy resource %s: %w", resource.Name, err)
		}
		if err := requireRegionalKey(normalized.Resource.ResourceKey); err != nil {
			return err
		}
		if resource.Generation == 0 {
			return resources.NewStoreError(
				resources.CodeInvalidArgument,
				"legacy resources must have a positive generation",
				0,
				0,
				nil,
			)
		}
		desired, err := json.Marshal(normalized.Resource)
		if err != nil {
			return storeUnavailable("encode legacy desired resource", err)
		}
		status, err := json.Marshal(resource.Status)
		if err != nil {
			return storeUnavailable("encode legacy resource status", err)
		}
		body.Resources = append(body.Resources, controlImportResourceDocument{
			Name:       controlName(normalized.Resource.ResourceKey),
			Generation: strconv.FormatUint(resource.Generation, 10),
			Desired:    desired,
			Status:     status,
		})
	}
	for _, generation := range snapshot.Generations {
		normalized, err := resources.NormalizeKey(generation.Key)
		if err != nil {
			return err
		}
		if err := requireRegionalKey(normalized); err != nil {
			return err
		}
		if generation.Generation == 0 {
			return resources.NewStoreError(
				resources.CodeInvalidArgument,
				"legacy generation records must be positive",
				0,
				0,
				nil,
			)
		}
		body.Generations = append(body.Generations, controlImportGenerationDocument{
			Name:       controlName(normalized),
			Generation: strconv.FormatUint(generation.Generation, 10),
		})
	}
	sort.Slice(body.Resources, func(left, right int) bool {
		return controlNameLess(body.Resources[left].Name, body.Resources[right].Name)
	})
	sort.Slice(body.Generations, func(left, right int) bool {
		return controlNameLess(body.Generations[left].Name, body.Generations[right].Name)
	})
	fingerprint, err := json.Marshal(struct {
		Resources   []controlImportResourceDocument   `json:"resources"`
		Generations []controlImportGenerationDocument `json:"generations"`
	}{body.Resources, body.Generations})
	if err != nil {
		return storeUnavailable("fingerprint legacy registry", err)
	}
	digest := sha256.Sum256(fingerprint)
	body.RequestToken = "epoch-control.legacy-import.v1." + hex.EncodeToString(digest[:])
	encoded, err := json.Marshal(body)
	if err != nil {
		return storeUnavailable("encode legacy registry", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), controlCallTimeout)
	defer cancel()
	response, err := registry.authority.requestAny(ctx, http.MethodPost, controlImportPath, encoded)
	if err != nil {
		return mapAuthorityStoreError(err, 0, 0)
	}
	var receipt controlMutationReceiptDocument
	if err := decodeAuthorityJSON(response, &receipt); err != nil {
		return storeUnavailable("decode legacy registry import", err)
	}
	if receipt.Mutation.Kind != "desired_applied" ||
		len(receipt.Mutation.Resources) != len(body.Resources) {
		return storeUnavailable(
			"decode legacy registry import",
			fmt.Errorf("catalog returned an inconsistent import mutation"),
		)
	}
	for index, result := range receipt.Mutation.Resources {
		if result.Resource.Name != body.Resources[index].Name ||
			strconv.FormatUint(uint64(result.Resource.Generation), 10) != body.Resources[index].Generation {
			return storeUnavailable(
				"verify legacy registry import",
				fmt.Errorf("catalog returned inconsistent imported identity or generation"),
			)
		}
	}
	registry.count.Store(int64(len(body.Resources)))
	return nil
}

func (registry *CatalogRegistry) Get(key resources.ResourceKey) (resources.Resource, error) {
	normalized, err := resources.NormalizeKey(key)
	if err != nil {
		return resources.Resource{}, err
	}
	if err := requireRegionalKey(normalized); err != nil {
		return resources.Resource{}, err
	}
	if err := registry.ensureOpen(); err != nil {
		return resources.Resource{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), controlCallTimeout)
	defer cancel()
	response, err := registry.authority.readControlAny(ctx, controlResourcePath(normalized))
	if errors.Is(err, errManagedResourceNotFound) {
		return resources.Resource{}, resources.NewStoreError(
			resources.CodeNotFound,
			fmt.Sprintf("resource %s was not found", normalized.Name),
			0,
			0,
			err,
		)
	}
	if err != nil {
		return resources.Resource{}, mapAuthorityStoreError(err, 0, 0)
	}
	var document controlManagedResourceDocument
	if err := decodeAuthorityJSON(response, &document); err != nil {
		return resources.Resource{}, storeUnavailable("decode desired resource", err)
	}
	resource, err := managedResourceFromDocument(document)
	if err != nil {
		return resources.Resource{}, err
	}
	if resource.ResourceKey != normalized {
		return resources.Resource{}, storeUnavailable(
			"decode desired resource",
			fmt.Errorf("catalog returned a different resource identity"),
		)
	}
	return resource, nil
}

func (registry *CatalogRegistry) List(filter resources.ListFilter) ([]resources.Resource, error) {
	if err := registry.ensureOpen(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), controlCallTimeout)
	defer cancel()
	response, err := registry.authority.readControlAny(ctx, controlResourcesPath)
	if err != nil {
		return nil, mapAuthorityStoreError(err, 0, 0)
	}
	var document controlResourceListDocument
	if err := decodeAuthorityJSON(response, &document); err != nil {
		return nil, storeUnavailable("decode desired inventory", err)
	}
	listed := make([]resources.Resource, 0, len(document.Resources))
	for _, encoded := range document.Resources {
		resource, err := managedResourceFromDocument(encoded)
		if err != nil {
			return nil, err
		}
		listed = append(listed, resource)
	}
	registry.count.Store(int64(len(listed)))
	return resources.FilterResourceList(listed, filter)
}

// ControlOperation performs a linearizable lookup of one durable Catalog
// request outcome, including affected identities required for authorization.
func (registry *CatalogRegistry) ControlOperation(
	ctx context.Context,
	requestToken string,
) (ControlOperation, error) {
	token, err := resources.NormalizeRequestToken(requestToken)
	if err != nil {
		return ControlOperation{}, err
	}
	document, err := registry.readControlOperation(ctx, token)
	if errors.Is(err, errManagedResourceNotFound) {
		return ControlOperation{}, resources.NewStoreError(
			resources.CodeNotFound,
			"control operation was not found",
			0,
			0,
			err,
		)
	}
	if err != nil {
		return ControlOperation{}, mapAuthorityStoreError(err, 0, 0)
	}
	if document.RequestToken != token || !validControlOperationState(document.State) {
		return ControlOperation{}, storeUnavailable(
			"decode control operation",
			fmt.Errorf("catalog returned an inconsistent operation"),
		)
	}
	operation := ControlOperation{
		RequestToken:      token,
		ProposalID:        uint64(document.ProposalID),
		State:             document.State,
		ResourceKeys:      make([]resources.ResourceKey, 0, len(document.ResourceNames)),
		FirstChangeCursor: uint64(document.FirstChangeCursor),
		LastChangeCursor:  uint64(document.LastChangeCursor),
	}
	for index, name := range document.ResourceNames {
		key, decodeErr := keyFromControlName(name)
		if decodeErr != nil || requireRegionalKey(key) != nil ||
			(index > 0 && !controlNameLess(document.ResourceNames[index-1], name)) {
			return ControlOperation{}, storeUnavailable(
				"decode control operation",
				fmt.Errorf("catalog returned invalid affected resource identities"),
			)
		}
		operation.ResourceKeys = append(operation.ResourceKeys, key)
	}
	if document.Mutation != nil && document.Mutation.Kind == "rejected" {
		operation.FailureCode = document.Mutation.Code
		operation.FailureMessage = document.Mutation.Message
	}
	if operation.State == ControlOperationFailed && operation.FailureMessage == "" {
		return ControlOperation{}, storeUnavailable(
			"decode control operation",
			fmt.Errorf("failed operation omitted its rejection"),
		)
	}
	if operation.FirstChangeCursor > operation.LastChangeCursor && operation.LastChangeCursor != 0 {
		return ControlOperation{}, storeUnavailable(
			"decode control operation",
			fmt.Errorf("catalog returned invalid change cursor bounds"),
		)
	}
	return operation, nil
}

func (registry *CatalogRegistry) readControlOperation(
	ctx context.Context,
	token string,
) (controlOperationDocument, error) {
	if err := registry.ensureOpen(); err != nil {
		return controlOperationDocument{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, controlCallTimeout)
	defer cancel()
	response, err := registry.authority.readControlAny(
		ctx,
		controlOperationsPath+"/"+url.PathEscape(token),
	)
	if err != nil {
		return controlOperationDocument{}, err
	}
	var document controlOperationDocument
	if err := decodeAuthorityJSON(response, &document); err != nil {
		return controlOperationDocument{}, storeUnavailable("decode control operation", err)
	}
	return document, nil
}

// ControlChanges reads one bounded, resumable page after the supplied global
// cursor. The caller owns tenant filtering but must always advance through the
// last scanned global cursor so hidden tenant activity cannot be replayed.
func (registry *CatalogRegistry) ControlChanges(
	ctx context.Context,
	after uint64,
	limit uint32,
) (ControlChangePage, error) {
	if limit == 0 || limit > 1_000 {
		return ControlChangePage{}, resources.NewStoreError(
			resources.CodeInvalidArgument,
			"change page size must be between 1 and 1000",
			0,
			0,
			nil,
		)
	}
	if err := registry.ensureOpen(); err != nil {
		return ControlChangePage{}, err
	}
	query := url.Values{}
	query.Set("after", strconv.FormatUint(after, 10))
	query.Set("limit", strconv.FormatUint(uint64(limit), 10))
	ctx, cancel := context.WithTimeout(ctx, controlCallTimeout)
	defer cancel()
	response, err := registry.authority.readControlAny(
		ctx,
		controlChangesPath+"?"+query.Encode(),
	)
	if err != nil {
		return ControlChangePage{}, mapAuthorityStoreError(err, 0, 0)
	}
	var document controlChangesDocument
	if err := decodeAuthorityJSON(response, &document); err != nil {
		return ControlChangePage{}, storeUnavailable("decode control changes", err)
	}
	page := ControlChangePage{
		EarliestCursor: uint64(document.EarliestCursor),
		LatestCursor:   uint64(document.LatestCursor),
		Changes:        make([]ControlChange, 0, len(document.Changes)),
	}
	var previous = after
	for _, encoded := range document.Changes {
		key, decodeErr := keyFromControlName(encoded.Name)
		cursor := uint64(encoded.Cursor)
		generation := uint64(encoded.Generation)
		if decodeErr != nil || requireRegionalKey(key) != nil ||
			!validControlChangeKind(encoded.Kind) || generation == 0 ||
			cursor <= previous || cursor > page.LatestCursor {
			return ControlChangePage{}, storeUnavailable(
				"decode control changes",
				fmt.Errorf("catalog returned an invalid change page"),
			)
		}
		page.Changes = append(page.Changes, ControlChange{
			Cursor: cursor, Kind: encoded.Kind, Key: key, Generation: generation,
		})
		previous = cursor
	}
	if page.EarliestCursor == 0 || (page.LatestCursor != 0 && page.EarliestCursor > page.LatestCursor) {
		return ControlChangePage{}, storeUnavailable(
			"decode control changes",
			fmt.Errorf("catalog returned invalid retained cursor bounds"),
		)
	}
	return page, nil
}

func validControlOperationState(state ControlOperationState) bool {
	return state == ControlOperationPending ||
		state == ControlOperationSucceeded ||
		state == ControlOperationFailed
}

func validControlChangeKind(kind ControlChangeKind) bool {
	return kind == ControlChangeDesiredApplied ||
		kind == ControlChangeDesiredDeleted ||
		kind == ControlChangeStatusUpdated
}

func (registry *CatalogRegistry) Delete(request resources.DeleteRequest) (resources.DeleteResult, error) {
	request, err := resources.NormalizeDeleteRequest(request)
	if err != nil {
		return resources.DeleteResult{}, err
	}
	key := request.Key
	if err := requireRegionalKey(key); err != nil {
		return resources.DeleteResult{}, err
	}
	if err := registry.ensureOpen(); err != nil {
		return resources.DeleteResult{}, err
	}
	body := controlDeleteDesiredBody{
		RequestToken:       request.RequestToken,
		ExpectedGeneration: decimalPointer(request.ExpectedGeneration),
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return resources.DeleteResult{}, storeUnavailable("encode desired delete", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), controlCallTimeout)
	defer cancel()
	response, err := registry.authority.requestAny(
		ctx,
		http.MethodDelete,
		controlResourcePath(key),
		encoded,
	)
	if err != nil {
		return resources.DeleteResult{}, registry.mapMutationError(err, key, request.ExpectedGeneration)
	}
	var receipt controlMutationReceiptDocument
	if err := decodeAuthorityJSON(response, &receipt); err != nil {
		return resources.DeleteResult{}, storeUnavailable("decode desired delete", err)
	}
	if receipt.Mutation.Kind != "desired_deleted" || receipt.Mutation.Name == nil {
		return resources.DeleteResult{}, storeUnavailable(
			"decode desired delete",
			fmt.Errorf("catalog returned an inconsistent delete mutation"),
		)
	}
	deletedKey, err := keyFromControlName(*receipt.Mutation.Name)
	if err != nil || deletedKey != key {
		return resources.DeleteResult{}, storeUnavailable(
			"decode desired delete",
			fmt.Errorf("catalog returned a different resource identity"),
		)
	}
	if receipt.Mutation.Deleted && !receipt.RequestReplayed && !receipt.Mutation.Replayed {
		registry.count.Add(-1)
	}
	return resources.DeleteResult{
		Key:        key,
		Generation: uint64(receipt.Mutation.Generation),
		Deleted:    receipt.Mutation.Deleted,
		Replayed:   receipt.RequestReplayed || receipt.Mutation.Replayed,
	}, nil
}

// DeleteManaged removes replicated desired state and its native Catalog
// materialization in one lease-fenced command.
func (registry *CatalogRegistry) DeleteManaged(
	ctx context.Context,
	request resources.DeleteRequest,
	desiredGeneration uint64,
	catalogGeneration uint64,
) (resources.DeleteResult, error) {
	request, err := resources.NormalizeDeleteRequest(request)
	if err != nil {
		return resources.DeleteResult{}, err
	}
	if err := requireRegionalKey(request.Key); err != nil {
		return resources.DeleteResult{}, err
	}
	if replayed, found, replayErr := registry.ReplayManagedDelete(ctx, request); replayErr != nil || found {
		return replayed, replayErr
	}
	expectedGeneration := desiredGeneration
	if request.ExpectedGeneration != nil {
		expectedGeneration = *request.ExpectedGeneration
	}
	if desiredGeneration == 0 || desiredGeneration == ^uint64(0) ||
		(request.ExpectedGeneration != nil && *request.ExpectedGeneration != desiredGeneration) {
		return resources.DeleteResult{}, resources.NewStoreError(
			resources.CodeConflict,
			"delete expected generation does not match desired state",
			expectedGeneration,
			desiredGeneration,
			nil,
		)
	}
	lease, nowMS, err := registry.ensureLease()
	if err != nil {
		return resources.DeleteResult{}, err
	}
	body := controlDeleteManagedBody{
		RequestToken: managedDeleteToken(request.RequestToken),
		Lease: controlLeaseGuardDocument{
			OwnerID: registry.ownerID,
			Fence:   strconv.FormatUint(uint64(lease.Fence), 10),
			NowMS:   strconv.FormatUint(nowMS, 10),
		},
		ExpectedDesiredGeneration: strconv.FormatUint(desiredGeneration, 10),
		ExpectedCatalogGeneration: strconv.FormatUint(catalogGeneration, 10),
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return resources.DeleteResult{}, storeUnavailable("encode managed delete", err)
	}
	response, err := registry.authority.requestAny(
		ctx,
		http.MethodDelete,
		controlMaterializationsPath+"/"+resourceSegments(request.Key),
		encoded,
	)
	if err != nil {
		if replayed, found, replayErr := registry.ReplayManagedDelete(ctx, request); replayErr != nil || found {
			return replayed, replayErr
		}
		return resources.DeleteResult{}, registry.mapMutationError(
			err,
			request.Key,
			request.ExpectedGeneration,
		)
	}
	var receipt controlMutationReceiptDocument
	if err := decodeAuthorityJSON(response, &receipt); err != nil {
		return resources.DeleteResult{}, storeUnavailable("decode managed delete", err)
	}
	if receipt.Mutation.Kind != "managed_deleted" || receipt.Mutation.Name == nil {
		return resources.DeleteResult{}, storeUnavailable(
			"decode managed delete",
			fmt.Errorf("catalog returned an inconsistent managed delete mutation"),
		)
	}
	deletedKey, err := keyFromControlName(*receipt.Mutation.Name)
	if err != nil || deletedKey != request.Key ||
		uint64(receipt.Mutation.DesiredGeneration) != desiredGeneration+1 {
		return resources.DeleteResult{}, storeUnavailable(
			"verify managed delete",
			fmt.Errorf("catalog returned a different identity or generation"),
		)
	}
	if receipt.Mutation.Deleted && !receipt.RequestReplayed && !receipt.Mutation.Replayed {
		registry.count.Add(-1)
	}
	return resources.DeleteResult{
		Key:        request.Key,
		Generation: uint64(receipt.Mutation.DesiredGeneration),
		Deleted:    receipt.Mutation.Deleted,
		Replayed:   receipt.RequestReplayed || receipt.Mutation.Replayed,
	}, nil
}

// ReplayManagedDelete resolves a completed managed delete without requiring
// the deleted desired record or the lease fence that originally committed it.
// The stable internal token remains scoped by the caller-supplied resource key.
func (registry *CatalogRegistry) ReplayManagedDelete(
	ctx context.Context,
	request resources.DeleteRequest,
) (resources.DeleteResult, bool, error) {
	request, err := resources.NormalizeDeleteRequest(request)
	if err != nil {
		return resources.DeleteResult{}, false, err
	}
	if err := requireRegionalKey(request.Key); err != nil {
		return resources.DeleteResult{}, false, err
	}
	token := managedDeleteToken(request.RequestToken)
	document, err := registry.readControlOperation(ctx, token)
	if errors.Is(err, errManagedResourceNotFound) {
		return resources.DeleteResult{}, false, nil
	}
	if err != nil {
		return resources.DeleteResult{}, false, mapAuthorityStoreError(err, 0, 0)
	}
	if document.RequestToken != token || !validControlOperationState(document.State) {
		return resources.DeleteResult{}, true, storeUnavailable(
			"decode managed delete outcome",
			fmt.Errorf("catalog returned an inconsistent operation"),
		)
	}
	if document.State == ControlOperationPending {
		return resources.DeleteResult{}, true, resources.NewStoreError(
			resources.CodeUnavailable,
			"managed delete outcome is still pending",
			0,
			0,
			nil,
		)
	}
	if len(document.ResourceNames) != 1 {
		return resources.DeleteResult{}, true, storeUnavailable(
			"authorize managed delete outcome",
			fmt.Errorf("catalog outcome omitted its exact resource identity"),
		)
	}
	key, decodeErr := keyFromControlName(document.ResourceNames[0])
	if decodeErr != nil || key != request.Key {
		return resources.DeleteResult{}, true, resources.NewStoreError(
			resources.CodeConflict,
			"request token is already bound to a different managed delete",
			0,
			0,
			decodeErr,
		)
	}
	if document.Mutation == nil {
		return resources.DeleteResult{}, true, storeUnavailable(
			"decode managed delete outcome",
			fmt.Errorf("catalog operation omitted its mutation"),
		)
	}
	if document.State == ControlOperationFailed {
		return resources.DeleteResult{}, true, storeErrorFromControlRejection(*document.Mutation)
	}
	mutation := *document.Mutation
	if mutation.Kind != "managed_deleted" || !mutation.Deleted ||
		uint64(mutation.DesiredGeneration) == 0 {
		return resources.DeleteResult{}, true, storeUnavailable(
			"decode managed delete outcome",
			fmt.Errorf("catalog returned an inconsistent successful delete"),
		)
	}
	if request.ExpectedGeneration != nil &&
		(*request.ExpectedGeneration == ^uint64(0) ||
			uint64(mutation.DesiredGeneration) != *request.ExpectedGeneration+1) {
		return resources.DeleteResult{}, true, resources.NewStoreError(
			resources.CodeConflict,
			"request token is already bound to a different desired generation",
			*request.ExpectedGeneration,
			uint64(mutation.DesiredGeneration)-1,
			nil,
		)
	}
	return resources.DeleteResult{
		Key:        request.Key,
		Generation: uint64(mutation.DesiredGeneration),
		Deleted:    true,
		Replayed:   true,
	}, true, nil
}

func (registry *CatalogRegistry) UpdateStatus(
	key resources.ResourceKey,
	desiredGeneration uint64,
	status resources.ResourceStatus,
) (resources.Resource, error) {
	normalized, err := resources.NormalizeKey(key)
	if err != nil {
		return resources.Resource{}, err
	}
	lease, nowMS, err := registry.ensureLease()
	if err != nil {
		return resources.Resource{}, err
	}
	body := controlStatusBody{
		RequestToken: statusMutationToken(
			normalized,
			desiredGeneration,
			uint64(lease.Fence),
			status,
		),
		Lease: controlLeaseGuardDocument{
			OwnerID: registry.ownerID,
			Fence:   strconv.FormatUint(uint64(lease.Fence), 10),
			NowMS:   strconv.FormatUint(nowMS, 10),
		},
		ExpectedGeneration: strconv.FormatUint(desiredGeneration, 10),
		Status:             status,
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return resources.Resource{}, storeUnavailable("encode managed status", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), controlCallTimeout)
	defer cancel()
	response, err := registry.authority.requestAny(
		ctx,
		http.MethodPut,
		controlStatusPath(normalized),
		encoded,
	)
	if err != nil {
		return resources.Resource{}, registry.mapMutationError(err, normalized, &desiredGeneration)
	}
	var receipt controlMutationReceiptDocument
	if err := decodeAuthorityJSON(response, &receipt); err != nil {
		return resources.Resource{}, storeUnavailable("decode managed status", err)
	}
	if receipt.Mutation.Kind != "managed_status_updated" || receipt.Mutation.Resource == nil {
		return resources.Resource{}, storeUnavailable(
			"decode managed status",
			fmt.Errorf("catalog returned an inconsistent status mutation"),
		)
	}
	return managedResourceFromDocument(*receipt.Mutation.Resource)
}

func (registry *CatalogRegistry) Count() int {
	count := registry.count.Load()
	if count < 0 {
		return 0
	}
	return int(count)
}

func (registry *CatalogRegistry) ControlLease() (AuthorityControlLease, error) {
	lease, nowMS, err := registry.ensureLease()
	if err != nil {
		return AuthorityControlLease{}, err
	}
	return AuthorityControlLease{
		OwnerID: lease.OwnerID,
		Fence:   uint64(lease.Fence),
		NowMS:   nowMS,
	}, nil
}

func (*CatalogRegistry) Mode() string {
	return "catalog_consensus_v1"
}

func (registry *CatalogRegistry) Close() error {
	registry.leaseMu.Lock()
	defer registry.leaseMu.Unlock()
	registry.closed = true
	return nil
}

func (registry *CatalogRegistry) ensureOpen() error {
	registry.leaseMu.Lock()
	defer registry.leaseMu.Unlock()
	if registry.closed {
		return storeUnavailable("use replicated registry", fmt.Errorf("registry is closed"))
	}
	return nil
}

func (registry *CatalogRegistry) ensureLease() (controlLeaseDocument, uint64, error) {
	registry.leaseMu.Lock()
	defer registry.leaseMu.Unlock()
	if registry.closed {
		return controlLeaseDocument{}, 0, storeUnavailable(
			"acquire control lease",
			fmt.Errorf("registry is closed"),
		)
	}
	nowUnixMS := registry.now().UTC().UnixMilli()
	if nowUnixMS < 0 {
		return controlLeaseDocument{}, 0, storeUnavailable(
			"acquire control lease",
			fmt.Errorf("control clock precedes the Unix epoch"),
		)
	}
	nowMS := uint64(nowUnixMS)
	if registry.lease.OwnerID == registry.ownerID &&
		uint64(registry.lease.ValidUntilMS) > nowMS+uint64(controlLeaseTTL.Milliseconds()/3) {
		return registry.lease, nowMS, nil
	}
	readContext, readCancel := context.WithTimeout(context.Background(), controlCallTimeout)
	currentResponse, readErr := registry.authority.readControlAny(readContext, controlLeasePath)
	readCancel()
	if readErr == nil {
		var current controlLeaseDocument
		if err := decodeAuthorityJSON(currentResponse, &current); err != nil {
			return controlLeaseDocument{}, 0, storeUnavailable("decode current control lease", err)
		}
		if uint64(current.ValidUntilMS) > nowMS {
			if current.OwnerID != registry.ownerID {
				return controlLeaseDocument{}, 0, resources.NewStoreError(
					resources.CodeUnavailable,
					"another control instance currently owns reconciliation",
					0,
					0,
					nil,
				)
			}
			registry.lease = current
			if uint64(current.ValidUntilMS) > nowMS+uint64(controlLeaseTTL.Milliseconds()/3) {
				return registry.lease, nowMS, nil
			}
		}
	} else if !errors.Is(readErr, errManagedResourceNotFound) {
		return controlLeaseDocument{}, 0, mapAuthorityStoreError(readErr, 0, 0)
	}
	body := controlLeaseBody{
		RequestToken: leaseMutationToken(registry.ownerID, nowMS),
		OwnerID:      registry.ownerID,
		NowMS:        strconv.FormatUint(nowMS, 10),
		TTLMS:        strconv.FormatInt(controlLeaseTTL.Milliseconds(), 10),
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return controlLeaseDocument{}, 0, storeUnavailable("encode control lease", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), controlCallTimeout)
	defer cancel()
	response, err := registry.authority.requestAny(ctx, http.MethodPost, controlLeasePath, encoded)
	if err != nil {
		return controlLeaseDocument{}, 0, mapAuthorityStoreError(err, 0, 0)
	}
	var receipt controlMutationReceiptDocument
	if err := decodeAuthorityJSON(response, &receipt); err != nil {
		return controlLeaseDocument{}, 0, storeUnavailable("decode control lease", err)
	}
	if receipt.Mutation.Kind != "control_lease_acquired" || receipt.Mutation.Lease == nil {
		return controlLeaseDocument{}, 0, storeUnavailable(
			"decode control lease",
			fmt.Errorf("catalog returned an inconsistent lease mutation"),
		)
	}
	registry.lease = *receipt.Mutation.Lease
	return registry.lease, nowMS, nil
}

func validControlOwner(ownerID string) bool {
	if ownerID == "" || len(ownerID) > maxControlOwnerBytes || strings.TrimSpace(ownerID) != ownerID {
		return false
	}
	for index := 0; index < len(ownerID); index++ {
		character := ownerID[index]
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			strings.ContainsRune("._:@/-", rune(character)) {
			continue
		}
		return false
	}
	return true
}

func (registry *CatalogRegistry) mapMutationError(
	err error,
	key resources.ResourceKey,
	expected *uint64,
) error {
	expectedGeneration := uint64(0)
	if expected != nil {
		expectedGeneration = *expected
	}
	actualGeneration := uint64(0)
	var authority *authorityError
	if errors.As(err, &authority) && authority.kind == authorityConflict {
		if current, getErr := registry.Get(key); getErr == nil {
			actualGeneration = current.Generation
		}
	}
	return mapAuthorityStoreError(err, expectedGeneration, actualGeneration)
}

func (authority *HTTPAuthority) readControlAny(ctx context.Context, path string) ([]byte, error) {
	var failures []string
	for _, endpoint := range authority.endpoints {
		response, status, err := authority.requestEndpoint(ctx, endpoint, http.MethodGet, path, nil)
		if err != nil {
			failures = append(failures, err.Error())
			continue
		}
		switch {
		case status >= 200 && status < 300:
			return response, nil
		case status == http.StatusConflict && authorityErrorCode(response) == "not_leader":
			failures = append(failures, authorityErrorMessage(response, status))
		case status == http.StatusNotFound:
			return nil, errManagedResourceNotFound
		case status >= 500 || status == http.StatusTooManyRequests:
			failures = append(failures, authorityErrorMessage(response, status))
		case status == http.StatusConflict:
			return nil, conflictError(authorityErrorMessage(response, status))
		default:
			return nil, invalidAuthorityError(authorityErrorMessage(response, status))
		}
	}
	return nil, availabilityError(
		"no regional authority endpoint completed the linearizable read: " +
			strings.Join(failures, "; "),
	)
}

func managedResourceFromDocument(
	document controlManagedResourceDocument,
) (resources.Resource, error) {
	key, err := keyFromControlName(document.Name)
	if err != nil {
		return resources.Resource{}, storeUnavailable("decode managed resource name", err)
	}
	var desired resources.DesiredResource
	if err := json.Unmarshal(document.Desired, &desired); err != nil {
		return resources.Resource{}, storeUnavailable("decode managed desired state", err)
	}
	normalized, err := resources.NormalizeApplyRequest(resources.ApplyRequest{
		RequestToken: "catalog-read",
		Resource:     desired,
	})
	if err != nil {
		return resources.Resource{}, storeUnavailable("validate managed desired state", err)
	}
	if normalized.Resource.ResourceKey != key {
		return resources.Resource{}, storeUnavailable(
			"validate managed desired state",
			fmt.Errorf("desired identity does not match catalog key"),
		)
	}
	var status resources.ResourceStatus
	if err := json.Unmarshal(document.Status, &status); err != nil {
		return resources.Resource{}, storeUnavailable("decode managed resource status", err)
	}
	return resources.Resource{
		ResourceKey: key,
		Labels:      normalized.Resource.Labels,
		Governance:  normalized.Resource.Governance,
		Spec:        append(json.RawMessage(nil), normalized.Resource.Spec...),
		Generation:  uint64(document.Generation),
		Status:      status,
	}, nil
}

func controlName(key resources.ResourceKey) controlResourceNameDocument {
	return controlResourceNameDocument{
		Organization: key.Organization,
		Project:      key.Project,
		Environment:  key.Environment,
		Namespace:    key.Namespace,
		Kind:         string(key.Kind),
		Name:         key.Name,
	}
}

func keyFromControlName(name controlResourceNameDocument) (resources.ResourceKey, error) {
	return resources.NormalizeKey(resources.ResourceKey{
		Organization: name.Organization,
		Project:      name.Project,
		Environment:  name.Environment,
		Namespace:    name.Namespace,
		Kind:         resources.Kind(name.Kind),
		Name:         name.Name,
	})
}

func requireRegionalKey(key resources.ResourceKey) error {
	if key.Organization == "" || key.Project == "" || key.Environment == "" {
		return resources.NewStoreError(
			resources.CodeInvalidArgument,
			"replicated Catalog resources require organization, project, and environment",
			0,
			0,
			nil,
		)
	}
	return nil
}

func decimalPointer(value *uint64) *string {
	if value == nil {
		return nil
	}
	encoded := strconv.FormatUint(*value, 10)
	return &encoded
}

func controlResourcePath(key resources.ResourceKey) string {
	return controlResourcesPath + "/" + resourceSegments(key)
}

func controlStatusPath(key resources.ResourceKey) string {
	return controlResourcePath(key) + "/status"
}

func leaseMutationToken(ownerID string, nowMS uint64) string {
	digest := sha256.Sum256([]byte(ownerID + "\x00" + strconv.FormatUint(nowMS, 10)))
	return "epoch-control.lease.v1." + hex.EncodeToString(digest[:])
}

func statusMutationToken(
	key resources.ResourceKey,
	generation uint64,
	leaseFence uint64,
	status resources.ResourceStatus,
) string {
	encoded, err := json.Marshal(struct {
		Key        resources.ResourceKey    `json:"key"`
		Generation uint64                   `json:"generation"`
		LeaseFence uint64                   `json:"lease_fence"`
		Status     resources.ResourceStatus `json:"status"`
	}{key, generation, leaseFence, status})
	if err != nil {
		panic("validated status must encode")
	}
	digest := sha256.Sum256(encoded)
	return "epoch-control.status.v1." + hex.EncodeToString(digest[:])
}

func managedDeleteToken(base string) string {
	digest := sha256.Sum256([]byte(base))
	return "epoch-control.delete.v2." + hex.EncodeToString(digest[:])
}

func storeErrorFromControlRejection(mutation controlMutationDocument) error {
	if mutation.Kind != "rejected" || mutation.Message == "" {
		return storeUnavailable(
			"decode managed delete rejection",
			fmt.Errorf("catalog returned an inconsistent rejection"),
		)
	}
	code := resources.CodeConflict
	if mutation.Code == "invalid_argument" {
		code = resources.CodeInvalidArgument
	}
	return resources.NewStoreError(code, mutation.Message, 0, 0, nil)
}

func mapAuthorityStoreError(err error, expected, actual uint64) error {
	var authority *authorityError
	if errors.As(err, &authority) {
		switch authority.kind {
		case authorityInvalid:
			return resources.NewStoreError(
				resources.CodeInvalidArgument,
				authority.message,
				expected,
				actual,
				err,
			)
		case authorityConflict:
			return resources.NewStoreError(
				resources.CodeConflict,
				authority.message,
				expected,
				actual,
				err,
			)
		case authorityUnavailable:
			return storeUnavailable("access replicated Catalog", err)
		}
	}
	return storeUnavailable("access replicated Catalog", err)
}

func storeUnavailable(operation string, cause error) error {
	return resources.NewStoreError(
		resources.CodeUnavailable,
		operation+" failed",
		0,
		0,
		cause,
	)
}

// Stable sort helper used by future multi-resource API calls.
func sortDesiredWrites(writes []controlDesiredWriteDocument) {
	sort.Slice(writes, func(left, right int) bool {
		return controlNameLess(writes[left].Name, writes[right].Name)
	})
}

func controlNameLess(left, right controlResourceNameDocument) bool {
	leftParts := []string{left.Organization, left.Project, left.Environment, left.Namespace, left.Kind, left.Name}
	rightParts := []string{right.Organization, right.Project, right.Environment, right.Namespace, right.Kind, right.Name}
	for index := range leftParts {
		if leftParts[index] != rightParts[index] {
			return leftParts[index] < rightParts[index]
		}
	}
	return false
}
