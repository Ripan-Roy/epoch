package resources

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestHTTPRegionalInventoryIsBrowserSafeAndExcludesLocalResources(t *testing.T) {
	registry := NewRegistry()
	regionalKey := ResourceKey{
		Organization: "acme",
		Project:      "payments",
		Environment:  "production",
		Namespace:    "orders",
		Kind:         KindStream,
		Name:         "events",
	}
	created, err := registry.Apply(ApplyRequest{
		RequestToken:       "create-regional-events",
		ExpectedGeneration: uint64Pointer(0),
		Resource: DesiredResource{
			ResourceKey: regionalKey,
			Spec: json.RawMessage(
				`{"workload_profile":"WORKLOAD_PROFILE_STREAM_LOG","replicas":3,"configuration":{"shard_count":1}}`,
			),
		},
	})
	if err != nil {
		t.Fatalf("create regional resource: %v", err)
	}
	if _, err := registry.Apply(ApplyRequest{
		RequestToken:       "create-local-events",
		ExpectedGeneration: uint64Pointer(0),
		Resource: DesiredResource{
			ResourceKey: ResourceKey{
				Namespace: "orders",
				Kind:      KindStream,
				Name:      "local-events",
			},
			Spec: json.RawMessage(`{"shard_count":1,"replica_count":3}`),
		},
	}); err != nil {
		t.Fatalf("create local resource: %v", err)
	}
	const largeID = uint64(9_007_199_254_740_993)
	if _, err := registry.UpdateStatus(regionalKey, created.Resource.Generation, ResourceStatus{
		Phase:              PhaseReady,
		ObservedGeneration: created.Resource.Generation,
		Message:            "regional placement converged",
		Tablets: []TabletStatus{{
			TabletID:           largeID,
			ConsensusGroupID:   largeID + 1,
			ShardIndex:         0,
			TabletEpoch:        largeID + 2,
			ResourceGeneration: created.Resource.Generation,
			DesiredReplicas:    3,
			VoterNodeIDs:       []uint64{largeID + 3, largeID + 4, largeID + 5},
			LeaderNodeID:       largeID + 4,
		}},
	}); err != nil {
		t.Fatalf("update regional status: %v", err)
	}

	handler, err := NewHTTPHandlerWithOrigins(registry, []string{"https://console.example.com"})
	if err != nil {
		t.Fatalf("NewHTTPHandlerWithOrigins() error = %v", err)
	}
	response := performRequest(
		t,
		handler,
		http.MethodGet,
		"/v1/regional/resources",
		nil,
		map[string]string{"Origin": "https://console.example.com"},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("GET regional inventory status = %d, body = %s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Access-Control-Allow-Origin"); got != "https://console.example.com" {
		t.Fatalf("Access-Control-Allow-Origin = %q", got)
	}
	if !strings.Contains(response.Header().Get("Vary"), "Origin") {
		t.Fatalf("Vary = %q", response.Header().Get("Vary"))
	}

	var payload map[string]any
	decodeResponse(t, response, &payload)
	if payload["count"] != float64(1) {
		t.Fatalf("regional count = %#v", payload["count"])
	}
	list, ok := payload["resources"].([]any)
	if !ok || len(list) != 1 {
		t.Fatalf("regional resources = %#v", payload["resources"])
	}
	resource, ok := list[0].(map[string]any)
	if !ok {
		t.Fatalf("regional resource = %#v", list[0])
	}
	assertJSONString(t, resource, "generation", "1")
	assertJSONString(t, resource, "observed_generation", "1")
	if resource["canonical_name"] != "acme/payments/production/orders/stream/events" ||
		resource["phase"] != "ready" ||
		resource["shard_count"] != float64(1) ||
		resource["workload_profile"] != "stream_log" {
		t.Fatalf("regional resource = %#v", resource)
	}
	tablets, ok := resource["tablets"].([]any)
	if !ok || len(tablets) != 1 {
		t.Fatalf("regional tablets = %#v", resource["tablets"])
	}
	tablet, ok := tablets[0].(map[string]any)
	if !ok {
		t.Fatalf("regional tablet = %#v", tablets[0])
	}
	assertJSONString(t, tablet, "tablet_id", "9007199254740993")
	assertJSONString(t, tablet, "consensus_group_id", "9007199254740994")
	assertJSONString(t, tablet, "tablet_epoch", "9007199254740995")
	assertJSONString(t, tablet, "resource_generation", "1")
	assertJSONString(t, tablet, "leader_node_id", "9007199254740997")
	voters, ok := tablet["voter_node_ids"].([]any)
	if !ok || len(voters) != 3 ||
		voters[0] != "9007199254740996" ||
		voters[1] != "9007199254740997" ||
		voters[2] != "9007199254740998" {
		t.Fatalf("regional voter_node_ids = %#v", tablet["voter_node_ids"])
	}
}

func TestHTTPRegionalInventoryReportsPendingWithoutInventingPlacement(t *testing.T) {
	registry := NewRegistry()
	if _, err := registry.Apply(ApplyRequest{
		RequestToken:       "create-pending-regional-queue",
		ExpectedGeneration: uint64Pointer(0),
		Resource: DesiredResource{
			ResourceKey: ResourceKey{
				Organization: "acme",
				Project:      "payments",
				Environment:  "staging",
				Namespace:    "orders",
				Kind:         KindQueue,
				Name:         "jobs",
			},
			Spec: json.RawMessage(`{"shard_count":2,"replica_count":3}`),
		},
	}); err != nil {
		t.Fatalf("create pending regional resource: %v", err)
	}
	handler, err := NewHTTPHandlerWithOrigins(registry, nil)
	if err != nil {
		t.Fatalf("NewHTTPHandlerWithOrigins() error = %v", err)
	}
	response := performRequest(t, handler, http.MethodGet, "/v1/regional/resources", nil, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("GET regional inventory status = %d, body = %s", response.Code, response.Body.String())
	}
	var payload struct {
		Resources []struct {
			Phase              ResourcePhase `json:"phase"`
			Generation         string        `json:"generation"`
			ObservedGeneration string        `json:"observed_generation"`
			ShardCount         uint32        `json:"shard_count"`
			Tablets            []any         `json:"tablets"`
		} `json:"resources"`
	}
	decodeResponse(t, response, &payload)
	if len(payload.Resources) != 1 {
		t.Fatalf("regional resources = %+v", payload.Resources)
	}
	resource := payload.Resources[0]
	if resource.Phase != PhasePending ||
		resource.Generation != "1" ||
		resource.ObservedGeneration != "0" ||
		resource.ShardCount != 2 ||
		resource.Tablets == nil ||
		len(resource.Tablets) != 0 {
		t.Fatalf("pending regional resource = %+v", resource)
	}
}

func TestHTTPCORSRequiresAnExactConfiguredOrigin(t *testing.T) {
	handler, err := NewHTTPHandlerWithOrigins(
		NewRegistry(),
		[]string{"http://127.0.0.1:5173"},
	)
	if err != nil {
		t.Fatalf("NewHTTPHandlerWithOrigins() error = %v", err)
	}
	preflight := performRequest(
		t,
		handler,
		http.MethodOptions,
		"/v1/regional/resources",
		nil,
		map[string]string{
			"Origin":                         "http://127.0.0.1:5173",
			"Access-Control-Request-Method":  http.MethodGet,
			"Access-Control-Request-Headers": "content-type",
		},
	)
	if preflight.Code != http.StatusNoContent ||
		preflight.Header().Get("Access-Control-Allow-Origin") != "http://127.0.0.1:5173" ||
		!strings.Contains(preflight.Header().Get("Access-Control-Allow-Methods"), http.MethodGet) ||
		!strings.Contains(strings.ToLower(preflight.Header().Get("Access-Control-Allow-Headers")), "content-type") {
		t.Fatalf("preflight = status %d, headers %#v", preflight.Code, preflight.Header())
	}

	untrusted := performRequest(
		t,
		handler,
		http.MethodGet,
		"/v1/regional/resources",
		nil,
		map[string]string{"Origin": "https://attacker.example"},
	)
	if untrusted.Code != http.StatusOK {
		t.Fatalf("untrusted non-browser request status = %d", untrusted.Code)
	}
	if got := untrusted.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("untrusted Access-Control-Allow-Origin = %q", got)
	}
}

func TestHTTPHandlerRejectsUnsafeAllowedOrigins(t *testing.T) {
	for _, origin := range []string{
		"*",
		"https://*.example.com",
		"https://user:secret@example.com",
		"https://console.example.com/path",
		"https://console.example.com?tenant=acme",
		"file:///tmp/console.html",
	} {
		t.Run(origin, func(t *testing.T) {
			if _, err := NewHTTPHandlerWithOrigins(NewRegistry(), []string{origin}); err == nil {
				t.Fatalf("NewHTTPHandlerWithOrigins(%q) succeeded", origin)
			}
		})
	}
}

func assertJSONString(t *testing.T, object map[string]any, key, expected string) {
	t.Helper()
	value, ok := object[key].(string)
	if !ok || value != expected {
		t.Fatalf("%s = %#v, want JSON string %q", key, object[key], expected)
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
