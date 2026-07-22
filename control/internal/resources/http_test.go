package resources

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPResourceLifecycle(t *testing.T) {
	registry := NewRegistry()
	handler := NewHTTPHandler(registry)

	health := performRequest(t, handler, http.MethodGet, "/healthz", nil, nil)
	if health.Code != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, body = %s", health.Code, health.Body.String())
	}
	var healthBody map[string]any
	decodeResponse(t, health, &healthBody)
	if healthBody["data_path_owner"] != "rust" || healthBody["registry"] != "in_memory" {
		t.Fatalf("health response = %#v", healthBody)
	}

	createBody := []byte(`{
		"request_token":"create-jobs",
		"expected_generation":0,
		"resource":{
			"namespace":"prod",
			"kind":"queue",
			"name":"jobs",
			"labels":{"owner":"platform"},
			"spec":{"max_attempts":8}
		}
	}`)
	created := performRequest(t, handler, http.MethodPut, "/v1/resources", createBody, nil)
	if created.Code != http.StatusCreated {
		t.Fatalf("PUT create status = %d, body = %s", created.Code, created.Body.String())
	}
	if created.Header().Get("ETag") != "1" {
		t.Fatalf("PUT create ETag = %q", created.Header().Get("ETag"))
	}

	got := performRequest(t, handler, http.MethodGet, "/v1/resources/prod/queue/jobs", nil, nil)
	if got.Code != http.StatusOK {
		t.Fatalf("GET resource status = %d, body = %s", got.Code, got.Body.String())
	}
	var resource Resource
	decodeResponse(t, got, &resource)
	if resource.Generation != 1 || resource.Status.Phase != PhasePending {
		t.Fatalf("GET resource = %+v", resource)
	}

	listed := performRequest(t, handler, http.MethodGet, "/v1/resources?namespace=prod&kind=queue", nil, nil)
	if listed.Code != http.StatusOK {
		t.Fatalf("GET list status = %d, body = %s", listed.Code, listed.Body.String())
	}
	var listBody struct {
		Resources []Resource `json:"resources"`
		Count     int        `json:"count"`
	}
	decodeResponse(t, listed, &listBody)
	if listBody.Count != 1 || len(listBody.Resources) != 1 {
		t.Fatalf("GET list = %+v", listBody)
	}

	staleBody := []byte(`{
		"request_token":"stale-jobs",
		"expected_generation":0,
		"resource":{"namespace":"prod","kind":"queue","name":"jobs","spec":{"max_attempts":9}}
	}`)
	stale := performRequest(t, handler, http.MethodPut, "/v1/resources", staleBody, nil)
	if stale.Code != http.StatusConflict {
		t.Fatalf("PUT stale status = %d, body = %s", stale.Code, stale.Body.String())
	}
	var staleError RegistryError
	decodeResponse(t, stale, &staleError)
	if staleError.Code != CodeConflict || staleError.ActualGeneration != 1 {
		t.Fatalf("PUT stale error = %+v", staleError)
	}

	deleted := performRequest(t, handler, http.MethodDelete, "/v1/resources/prod/queue/jobs", nil, map[string]string{
		"Idempotency-Key": "delete-jobs",
		"If-Match":        `"1"`,
	})
	if deleted.Code != http.StatusOK {
		t.Fatalf("DELETE status = %d, body = %s", deleted.Code, deleted.Body.String())
	}
	var deleteResult DeleteResult
	decodeResponse(t, deleted, &deleteResult)
	if !deleteResult.Deleted || deleteResult.Generation != 2 {
		t.Fatalf("DELETE response = %+v", deleteResult)
	}

	missing := performRequest(t, handler, http.MethodGet, "/v1/resources/prod/queue/jobs", nil, nil)
	if missing.Code != http.StatusNotFound {
		t.Fatalf("GET deleted status = %d, body = %s", missing.Code, missing.Body.String())
	}
}

func TestHTTPApplyAcceptsIdempotencyAndIfMatchHeaders(t *testing.T) {
	registry := NewRegistry()
	handler := NewHTTPHandler(registry)
	body := []byte(`{"resource":{"namespace":"prod","kind":"stream","name":"events","spec":{}}}`)
	headers := map[string]string{"Idempotency-Key": "create-events", "If-Match": "0"}
	created := performRequest(t, handler, http.MethodPut, "/v1/resources", body, headers)
	if created.Code != http.StatusCreated {
		t.Fatalf("first PUT status = %d, body = %s", created.Code, created.Body.String())
	}
	replayed := performRequest(t, handler, http.MethodPut, "/v1/resources", body, headers)
	if replayed.Code != http.StatusOK {
		t.Fatalf("replayed PUT status = %d, body = %s", replayed.Code, replayed.Body.String())
	}
	var result ApplyResult
	decodeResponse(t, replayed, &result)
	if !result.Replayed || result.Resource.Generation != 1 {
		t.Fatalf("replayed PUT = %+v", result)
	}
}

func TestHTTPRejectsUnknownFieldsAndMethods(t *testing.T) {
	handler := NewHTTPHandler(NewRegistry())
	unknown := performRequest(t, handler, http.MethodPut, "/v1/resources", []byte(`{"unknown":true}`), nil)
	if unknown.Code != http.StatusBadRequest {
		t.Fatalf("unknown field status = %d, body = %s", unknown.Code, unknown.Body.String())
	}
	method := performRequest(t, handler, http.MethodPost, "/v1/resources", nil, nil)
	if method.Code != http.StatusMethodNotAllowed || method.Header().Get("Allow") == "" {
		t.Fatalf("method response = %d, Allow=%q", method.Code, method.Header().Get("Allow"))
	}
}

func performRequest(
	t *testing.T,
	handler http.Handler,
	method string,
	path string,
	body []byte,
	headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, bytes.NewReader(body))
	if len(body) > 0 {
		request.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func decodeResponse(t *testing.T, response *httptest.ResponseRecorder, target any) {
	t.Helper()
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		t.Fatalf("decode response: %v; body = %s", err, response.Body.String())
	}
}
