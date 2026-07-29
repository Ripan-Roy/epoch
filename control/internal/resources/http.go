package resources

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const maxRequestBody = 1 << 20

var defaultBrowserOrigins = []string{
	"http://127.0.0.1:5173",
	"http://localhost:5173",
	"http://127.0.0.1:4173",
	"http://localhost:4173",
}

// NewHTTPHandler exposes the initial health and declarative resource API. It
// deliberately depends only on this control registry and never on data-path
// storage packages.
func NewHTTPHandler(registry *Registry) http.Handler {
	handler, err := NewHTTPHandlerWithOrigins(registry, defaultBrowserOrigins)
	if err != nil {
		panic("resources: invalid built-in browser origin: " + err.Error())
	}
	return handler
}

// NewHTTPHandlerWithOrigins exposes the managed HTTP API to an exact set of
// browser origins. An empty set keeps the API available to non-browser clients
// without granting cross-origin access.
func NewHTTPHandlerWithOrigins(registry *Registry, allowedOrigins []string) (http.Handler, error) {
	if registry == nil {
		panic("resources: nil registry")
	}
	origins, err := validateAllowedOrigins(allowedOrigins)
	if err != nil {
		return nil, err
	}
	handler := &httpHandler{registry: registry}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", handler.health)
	mux.HandleFunc("/v1/resources", handler.collection)
	mux.HandleFunc("/v1/resources/", handler.item)
	mux.HandleFunc("/v1/regional/resources", handler.regionalInventory)
	return withCORS(mux, origins), nil
}

type httpHandler struct {
	registry *Registry
}

func (handler *httpHandler) health(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"status":           "ok",
		"component":        "epoch-control",
		"role":             "managed_control_plane",
		"data_path_owner":  "rust",
		"registry":         handler.registry.Mode(),
		"registry_durable": handler.registry.Mode() != "memory",
		"resource_count":   handler.registry.Count(),
	})
}

func (handler *httpHandler) collection(writer http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case http.MethodGet:
		handler.list(writer, request)
	case http.MethodPut:
		handler.apply(writer, request)
	default:
		methodNotAllowed(writer, http.MethodGet, http.MethodPut)
	}
}

func (handler *httpHandler) item(writer http.ResponseWriter, request *http.Request) {
	key, err := keyFromPath(request.URL.Path)
	if err != nil {
		writeError(writer, err)
		return
	}
	switch request.Method {
	case http.MethodGet:
		resource, err := handler.registry.Get(key)
		if err != nil {
			writeError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, resource)
	case http.MethodDelete:
		handler.delete(writer, request, key)
	default:
		methodNotAllowed(writer, http.MethodGet, http.MethodDelete)
	}
}

func (handler *httpHandler) apply(writer http.ResponseWriter, request *http.Request) {
	var apply ApplyRequest
	if err := readJSON(writer, request, &apply, false); err != nil {
		writeError(writer, invalid(err.Error()))
		return
	}
	if apply.RequestToken == "" {
		apply.RequestToken = request.Header.Get("Idempotency-Key")
	}
	if apply.ExpectedGeneration == nil {
		expected, err := expectedGenerationHeader(request.Header.Get("If-Match"))
		if err != nil {
			writeError(writer, err)
			return
		}
		apply.ExpectedGeneration = expected
	}

	result, err := handler.registry.Apply(apply)
	if err != nil {
		writeError(writer, err)
		return
	}
	status := http.StatusOK
	if result.Created && !result.Replayed {
		status = http.StatusCreated
	}
	writer.Header().Set("ETag", strconv.FormatUint(result.Resource.Generation, 10))
	writeJSON(writer, status, result)
}

func (handler *httpHandler) list(writer http.ResponseWriter, request *http.Request) {
	filter := ListFilter{
		Namespace: request.URL.Query().Get("namespace"),
		Kind:      Kind(request.URL.Query().Get("kind")),
	}
	resources, err := handler.registry.List(filter)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, struct {
		Resources []Resource `json:"resources"`
		Count     int        `json:"count"`
	}{Resources: resources, Count: len(resources)})
}

type regionalInventoryResponse struct {
	Resources []regionalResourceView `json:"resources"`
	Count     int                    `json:"count"`
}

type regionalResourceView struct {
	CanonicalName      string               `json:"canonical_name"`
	Organization       string               `json:"organization"`
	Project            string               `json:"project"`
	Environment        string               `json:"environment"`
	Namespace          string               `json:"namespace"`
	Kind               Kind                 `json:"kind"`
	Name               string               `json:"name"`
	Generation         string               `json:"generation"`
	ObservedGeneration string               `json:"observed_generation"`
	WorkloadProfile    string               `json:"workload_profile"`
	ShardCount         uint32               `json:"shard_count"`
	Phase              ResourcePhase        `json:"phase"`
	Message            string               `json:"message,omitempty"`
	Tablets            []regionalTabletView `json:"tablets"`
}

type regionalTabletView struct {
	TabletID           string   `json:"tablet_id"`
	ConsensusGroupID   string   `json:"consensus_group_id"`
	ShardIndex         uint32   `json:"shard_index"`
	TabletEpoch        string   `json:"tablet_epoch"`
	ResourceGeneration string   `json:"resource_generation"`
	DesiredReplicas    uint32   `json:"desired_replicas"`
	VoterNodeIDs       []string `json:"voter_node_ids"`
	LeaderNodeID       *string  `json:"leader_node_id"`
}

func (handler *httpHandler) regionalInventory(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	all, err := handler.registry.List(ListFilter{})
	if err != nil {
		writeError(writer, err)
		return
	}
	response := regionalInventoryResponse{
		Resources: make([]regionalResourceView, 0, len(all)),
	}
	for _, resource := range all {
		if resource.Organization == "" {
			continue
		}
		response.Resources = append(response.Resources, regionalResourceForBrowser(resource))
	}
	response.Count = len(response.Resources)
	writeJSON(writer, http.StatusOK, response)
}

func regionalResourceForBrowser(resource Resource) regionalResourceView {
	tablets := make([]regionalTabletView, 0, len(resource.Status.Tablets))
	for _, tablet := range resource.Status.Tablets {
		voters := make([]string, 0, len(tablet.VoterNodeIDs))
		for _, voter := range tablet.VoterNodeIDs {
			voters = append(voters, strconv.FormatUint(voter, 10))
		}
		var leader *string
		if tablet.LeaderNodeID != 0 {
			encoded := strconv.FormatUint(tablet.LeaderNodeID, 10)
			leader = &encoded
		}
		tablets = append(tablets, regionalTabletView{
			TabletID:           strconv.FormatUint(tablet.TabletID, 10),
			ConsensusGroupID:   strconv.FormatUint(tablet.ConsensusGroupID, 10),
			ShardIndex:         tablet.ShardIndex,
			TabletEpoch:        strconv.FormatUint(tablet.TabletEpoch, 10),
			ResourceGeneration: strconv.FormatUint(tablet.ResourceGeneration, 10),
			DesiredReplicas:    tablet.DesiredReplicas,
			VoterNodeIDs:       voters,
			LeaderNodeID:       leader,
		})
	}
	return regionalResourceView{
		CanonicalName: strings.Join([]string{
			resource.Organization,
			resource.Project,
			resource.Environment,
			resource.Namespace,
			string(resource.Kind),
			resource.Name,
		}, "/"),
		Organization:       resource.Organization,
		Project:            resource.Project,
		Environment:        resource.Environment,
		Namespace:          resource.Namespace,
		Kind:               resource.Kind,
		Name:               resource.Name,
		Generation:         strconv.FormatUint(resource.Generation, 10),
		ObservedGeneration: strconv.FormatUint(resource.Status.ObservedGeneration, 10),
		WorkloadProfile:    browserWorkloadProfile(resource.Kind),
		ShardCount:         browserShardCount(resource.Spec),
		Phase:              resource.Status.Phase,
		Message:            resource.Status.Message,
		Tablets:            tablets,
	}
}

func browserShardCount(raw json.RawMessage) uint32 {
	var spec struct {
		ShardCount    uint32 `json:"shard_count"`
		Configuration struct {
			ShardCount uint32 `json:"shard_count"`
		} `json:"configuration"`
	}
	if err := json.Unmarshal(raw, &spec); err != nil {
		return 0
	}
	if spec.ShardCount != 0 {
		return spec.ShardCount
	}
	return spec.Configuration.ShardCount
}

func browserWorkloadProfile(kind Kind) string {
	switch kind {
	case KindCache:
		return "cache"
	case KindTable:
		return "state_table"
	case KindStream:
		return "stream_log"
	case KindQueue:
		return "work_queue"
	case KindEventBus:
		return "event_bus"
	default:
		return string(kind)
	}
}

func (handler *httpHandler) delete(writer http.ResponseWriter, request *http.Request, key ResourceKey) {
	var payload struct {
		RequestToken       string  `json:"request_token"`
		ExpectedGeneration *uint64 `json:"expected_generation,omitempty"`
	}
	if err := readJSON(writer, request, &payload, true); err != nil {
		writeError(writer, invalid(err.Error()))
		return
	}
	if payload.RequestToken == "" {
		payload.RequestToken = request.Header.Get("Idempotency-Key")
	}
	if payload.ExpectedGeneration == nil {
		expected, err := expectedGenerationHeader(request.Header.Get("If-Match"))
		if err != nil {
			writeError(writer, err)
			return
		}
		payload.ExpectedGeneration = expected
	}
	result, err := handler.registry.Delete(DeleteRequest{
		RequestToken:       payload.RequestToken,
		ExpectedGeneration: payload.ExpectedGeneration,
		Key:                key,
	})
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func keyFromPath(path string) (ResourceKey, error) {
	remainder := strings.TrimPrefix(path, "/v1/resources/")
	parts := strings.Split(remainder, "/")
	if len(parts) != 3 {
		return ResourceKey{}, invalid("resource path must be /v1/resources/{namespace}/{kind}/{name}")
	}
	for index := range parts {
		decoded, err := url.PathUnescape(parts[index])
		if err != nil {
			return ResourceKey{}, invalid("resource path contains invalid escaping")
		}
		parts[index] = decoded
	}
	return normalizeKey(ResourceKey{Namespace: parts[0], Kind: Kind(parts[1]), Name: parts[2]})
}

func expectedGenerationHeader(value string) (*uint64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	value = strings.Trim(value, `"`)
	expected, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return nil, invalid("If-Match must be an unsigned resource generation")
	}
	return &expected, nil
}

func readJSON(writer http.ResponseWriter, request *http.Request, target any, allowEmpty bool) error {
	if request.Body == nil || request.ContentLength == 0 {
		if allowEmpty {
			return nil
		}
		return fmt.Errorf("JSON request body is required")
	}
	defer request.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, maxRequestBody))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("request body must contain one JSON value")
		}
		return fmt.Errorf("invalid trailing JSON: %w", err)
	}
	return nil
}

func writeError(writer http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	payload := &RegistryError{Code: CodeInternal, Message: "internal control-plane error"}
	var registryError *RegistryError
	if errors.As(err, &registryError) {
		payload = registryError
		switch registryError.Code {
		case CodeInvalidArgument:
			status = http.StatusBadRequest
		case CodeNotFound:
			status = http.StatusNotFound
		case CodeConflict:
			status = http.StatusConflict
		}
	}
	writeJSON(writer, status, payload)
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func methodNotAllowed(writer http.ResponseWriter, methods ...string) {
	writer.Header().Set("Allow", strings.Join(methods, ", "))
	writeJSON(writer, http.StatusMethodNotAllowed, &RegistryError{
		Code:    CodeInvalidArgument,
		Message: "method not allowed",
	})
}

func validateAllowedOrigins(values []string) (map[string]struct{}, error) {
	origins := make(map[string]struct{}, len(values))
	for _, value := range values {
		origin := strings.TrimSpace(value)
		if origin == "" || strings.Contains(origin, "*") {
			return nil, fmt.Errorf("allowed browser origin %q must be an exact HTTP(S) origin", value)
		}
		parsed, err := url.Parse(origin)
		if err != nil ||
			(parsed.Scheme != "http" && parsed.Scheme != "https") ||
			parsed.Host == "" ||
			parsed.Opaque != "" ||
			parsed.User != nil ||
			parsed.Path != "" ||
			parsed.RawPath != "" ||
			parsed.RawQuery != "" ||
			parsed.ForceQuery ||
			parsed.Fragment != "" {
			return nil, fmt.Errorf(
				"allowed browser origin %q must contain only an HTTP(S) scheme and authority",
				value,
			)
		}
		origins[origin] = struct{}{}
	}
	return origins, nil
}

func withCORS(next http.Handler, allowedOrigins map[string]struct{}) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Add("Vary", "Origin")
		origin := request.Header.Get("Origin")
		_, allowed := allowedOrigins[origin]
		if origin != "" && allowed {
			writer.Header().Set("Access-Control-Allow-Origin", origin)
			writer.Header().Set(
				"Access-Control-Allow-Methods",
				strings.Join(
					[]string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodOptions},
					", ",
				),
			)
			writer.Header().Set(
				"Access-Control-Allow-Headers",
				"Accept, Content-Type, Idempotency-Key, If-Match",
			)
		}
		if request.Method == http.MethodOptions {
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(writer, request)
	})
}
