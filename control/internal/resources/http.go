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

// NewHTTPHandler exposes the initial health and declarative resource API. It
// deliberately depends only on this control registry and never on data-path
// storage packages.
func NewHTTPHandler(registry *Registry) http.Handler {
	if registry == nil {
		panic("resources: nil registry")
	}
	handler := &httpHandler{registry: registry}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", handler.health)
	mux.HandleFunc("/v1/resources", handler.collection)
	mux.HandleFunc("/v1/resources/", handler.item)
	return mux
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
		"status":          "ok",
		"component":       "epoch-control",
		"role":            "managed_control_plane",
		"data_path_owner": "rust",
		"registry":        "in_memory",
		"resource_count":  handler.registry.Count(),
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
