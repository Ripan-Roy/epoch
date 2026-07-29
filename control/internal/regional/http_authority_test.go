package regional

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"epoch.local/epoch/control/internal/resources"
)

func TestHTTPAuthorityAppliesThroughAvailableNodeAndObservesPlacement(t *testing.T) {
	var mu sync.Mutex
	var received map[string]any
	servers := make([]*httptest.Server, 0, 3)
	for node := 1; node <= 3; node++ {
		node := node
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			switch {
			case request.Method == http.MethodPut:
				if node == 1 {
					writeAuthorityJSON(writer, http.StatusConflict, map[string]any{
						"code": "not_leader", "message": "retry another node",
					})
					return
				}
				mu.Lock()
				defer mu.Unlock()
				if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
					t.Errorf("decode request: %v", err)
				}
				writeAuthorityJSON(writer, http.StatusCreated, appliedDocument(2, 3))
			case request.Method == http.MethodGet:
				writeAuthorityJSON(
					writer,
					http.StatusOK,
					routeResponseDocument(node, 2, 2, request.URL.Path),
				)
			default:
				t.Errorf("unexpected request: %s %s", request.Method, request.URL.Path)
				http.Error(writer, "unexpected", http.StatusInternalServerError)
			}
		}))
		servers = append(servers, server)
		t.Cleanup(server.Close)
	}
	authority, err := NewHTTPAuthority(serverURLs(servers), nil)
	if err != nil {
		t.Fatalf("NewHTTPAuthority() error = %v", err)
	}

	observation, err := authority.Apply(t.Context(), AuthorityApplyRequest{
		RequestToken:       "apply-orders",
		Key:                regionalKey(resources.KindStream, "orders"),
		ExpectedGeneration: 1,
		ShardCount:         2,
		ReplicaCount:       3,
	})
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if received["request_token"] != "apply-orders" ||
		received["expected_generation"] != "1" ||
		received["shard_count"] != float64(2) ||
		received["replica_count"] != float64(3) {
		t.Fatalf("request body = %#v", received)
	}
	if observation.Generation != 2 || len(observation.Tablets) != 2 {
		t.Fatalf("observation = %+v", observation)
	}
	for _, tablet := range observation.Tablets {
		if !slices.Equal(tablet.VoterNodeIDs, []uint64{1, 2, 3}) || tablet.LeaderNodeID != 2 {
			t.Fatalf("tablet placement = %+v", tablet)
		}
	}
}

func TestHTTPAuthorityClassifiesFollowerResponsesAsRetryable(t *testing.T) {
	servers := make([]*httptest.Server, 0, 2)
	for range 2 {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writeAuthorityJSON(writer, http.StatusConflict, map[string]any{
				"code": "not_leader", "message": "leader election is in progress",
			})
		}))
		servers = append(servers, server)
		t.Cleanup(server.Close)
	}
	authority, err := NewHTTPAuthority(serverURLs(servers), nil)
	if err != nil {
		t.Fatalf("NewHTTPAuthority() error = %v", err)
	}

	_, err = authority.Apply(t.Context(), AuthorityApplyRequest{
		RequestToken: "apply-during-election",
		Key:          regionalKey(resources.KindStream, "orders"),
		ShardCount:   1,
		ReplicaCount: 3,
	})
	if err == nil || !IsRetryable(err) {
		t.Fatalf("Apply() error = %v, want retryable follower response", err)
	}
}

func TestHTTPAuthorityObserveReturnsTruthfulIncompletePlacement(t *testing.T) {
	var servers []*httptest.Server
	for node := 1; node <= 3; node++ {
		node := node
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if request.URL.Path == catalogResourcePath(regionalKey(resources.KindQueue, "jobs")) {
				writeAuthorityJSON(writer, http.StatusOK, resourceDocument(1, 3))
				return
			}
			if node == 3 {
				writeAuthorityJSON(writer, http.StatusServiceUnavailable, map[string]any{
					"code": "route_unavailable",
				})
				return
			}
			writeAuthorityJSON(
				writer,
				http.StatusOK,
				routeResponseDocument(node, 1, 1, request.URL.Path),
			)
		}))
		servers = append(servers, server)
		t.Cleanup(server.Close)
	}
	authority, err := NewHTTPAuthority(serverURLs(servers), nil)
	if err != nil {
		t.Fatalf("NewHTTPAuthority() error = %v", err)
	}

	observation, err := authority.Observe(
		t.Context(),
		regionalKey(resources.KindQueue, "jobs"),
	)
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if !slices.Equal(observation.Tablets[0].VoterNodeIDs, []uint64{1, 2}) ||
		observation.Tablets[0].LeaderNodeID != 1 {
		t.Fatalf("incomplete placement was overclaimed: %+v", observation.Tablets[0])
	}
}

func TestHTTPAuthorityClassifiesDefinitiveCatalogConflict(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writeAuthorityJSON(writer, http.StatusConflict, map[string]any{
			"code":    "catalog_conflict",
			"message": "expected generation 1, found 2",
		})
	}))
	t.Cleanup(server.Close)
	authority, err := NewHTTPAuthority([]string{server.URL}, nil)
	if err != nil {
		t.Fatalf("NewHTTPAuthority() error = %v", err)
	}
	_, err = authority.Apply(t.Context(), AuthorityApplyRequest{
		RequestToken: "conflict",
		Key:          regionalKey(resources.KindCache, "sessions"),
		ShardCount:   1,
		ReplicaCount: 3,
	})
	if err == nil || IsRetryable(err) {
		t.Fatalf("Apply() error = %v, want definitive conflict", err)
	}
}

func TestHTTPAuthorityDeletesWithGenerationFence(t *testing.T) {
	var received map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodDelete {
			t.Errorf("method = %s, want DELETE", request.Method)
		}
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeAuthorityJSON(writer, http.StatusOK, map[string]any{
			"mutation": map[string]any{
				"kind":       "deleted",
				"generation": "2",
				"deleted":    true,
			},
		})
	}))
	t.Cleanup(server.Close)
	authority, err := NewHTTPAuthority([]string{server.URL}, nil)
	if err != nil {
		t.Fatalf("NewHTTPAuthority() error = %v", err)
	}
	deleted, err := authority.Delete(t.Context(), AuthorityDeleteRequest{
		RequestToken:       "delete-orders",
		Key:                regionalKey(resources.KindStream, "orders"),
		ExpectedGeneration: 1,
	})
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if !deleted.Deleted || deleted.Generation != 2 ||
		received["request_token"] != "delete-orders" ||
		received["expected_generation"] != "1" {
		t.Fatalf("Delete() = %+v, body = %#v", deleted, received)
	}
}

func TestAuthenticatedHTTPAuthoritySendsBearerToEveryRegionalRequest(t *testing.T) {
	const token = "regional-control-test-token"
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		if got := request.Header.Get("Authorization"); got != "Bearer "+token {
			t.Errorf("Authorization = %q", got)
			writeAuthorityJSON(writer, http.StatusUnauthorized, map[string]any{
				"code": "unauthenticated", "message": "bearer credential required",
			})
			return
		}
		if request.URL.Path == catalogResourcePath(regionalKey(resources.KindQueue, "jobs")) {
			writeAuthorityJSON(writer, http.StatusOK, resourceDocument(1, 3))
			return
		}
		writeAuthorityJSON(
			writer,
			http.StatusOK,
			routeResponseDocument(1, 1, 1, request.URL.Path),
		)
	}))
	t.Cleanup(server.Close)
	authority, err := NewAuthenticatedHTTPAuthority([]string{server.URL}, nil, token)
	if err != nil {
		t.Fatalf("NewAuthenticatedHTTPAuthority() error = %v", err)
	}
	if _, err := authority.Observe(
		t.Context(),
		regionalKey(resources.KindQueue, "jobs"),
	); err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if requests != 3 {
		t.Fatalf("regional requests = %d, want catalog plus two routes", requests)
	}
}

func TestAuthenticatedHTTPAuthorityRejectsUnsafeBearerTokens(t *testing.T) {
	for _, token := range []string{"", " ", "contains whitespace", "contains\nnewline"} {
		if _, err := NewAuthenticatedHTTPAuthority(
			[]string{"http://127.0.0.1:7601"},
			nil,
			token,
		); err == nil {
			t.Fatalf("NewAuthenticatedHTTPAuthority(token=%q) succeeded", token)
		}
	}
}

func TestHTTPAuthorityRejectsUnsafeOrEmptyEndpoints(t *testing.T) {
	for _, endpoints := range [][]string{
		nil,
		{"ftp://region.example"},
		{"https://user@region.example"},
		{"https://region.example/path"},
		{"https://region.example?query=true"},
	} {
		if _, err := NewHTTPAuthority(endpoints, nil); err == nil {
			t.Fatalf("NewHTTPAuthority(%v) succeeded", endpoints)
		}
	}
}

func serverURLs(servers []*httptest.Server) []string {
	result := make([]string, len(servers))
	for index, server := range servers {
		result[index] = server.URL
	}
	return result
}

func appliedDocument(generation uint64, replicas uint16) map[string]any {
	return map[string]any{
		"mutation": map[string]any{
			"kind":     "applied",
			"resource": resourceDocument(generation, replicas),
		},
	}
}

func resourceDocument(generation uint64, replicas uint16) map[string]any {
	return map[string]any{
		"generation":    generationString(generation),
		"replica_count": replicas,
		"tablets": []map[string]any{
			{
				"tablet_id":           "10",
				"consensus_group_id":  "20",
				"shard_index":         0,
				"tablet_epoch":        "1",
				"resource_generation": generationString(generation),
				"replica_count":       replicas,
			},
			{
				"tablet_id":           "11",
				"consensus_group_id":  "21",
				"shard_index":         1,
				"tablet_epoch":        "1",
				"resource_generation": generationString(generation),
				"replica_count":       replicas,
			},
		},
	}
}

func routeResponseDocument(node, leader int, generation uint64, path string) map[string]any {
	tabletID := uint64(10)
	groupID := uint64(20)
	if strings.HasSuffix(path, "/shards/1") {
		tabletID++
		groupID++
	}
	return map[string]any{
		"resource_generation": generationString(generation),
		"tablet_id":           generationString(tabletID),
		"consensus_group_id":  generationString(groupID),
		"tablet_epoch":        "1",
		"local_node_id":       generationString(uint64(node)),
		"leader_node_id":      generationString(uint64(leader)),
		"accepts_writes":      node == leader,
	}
}

func generationString(value uint64) string {
	return strconv.FormatUint(value, 10)
}

func writeAuthorityJSON(writer http.ResponseWriter, status int, document any) {
	writer.Header().Set("content-type", "application/json")
	writer.WriteHeader(status)
	if err := json.NewEncoder(writer).Encode(document); err != nil {
		panic(err)
	}
}
