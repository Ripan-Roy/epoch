package regional

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"epoch.local/epoch/control/internal/resources"
)

func TestCatalogRegistryProvidesReplicatedApplyListGetAndDelete(t *testing.T) {
	var (
		mu          sync.Mutex
		document    controlManagedResourceDocument
		applyCalls  int
		deleteCalls int
		deleted     bool
	)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		writer.Header().Set("content-type", "application/json")
		switch {
		case request.Method == http.MethodPut && request.URL.Path == controlResourcesPath:
			var body controlApplyDesiredBody
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode apply: %v", err)
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			if len(body.Resources) != 1 || body.RequestToken != "create-orders" {
				t.Errorf("unexpected apply body: %+v", body)
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			applyCalls++
			document = controlManagedResourceDocument{
				Name:       body.Resources[0].Name,
				Generation: 1,
				Desired:    append(json.RawMessage(nil), body.Resources[0].Desired...),
				Status:     json.RawMessage(`{}`),
			}
			deleted = false
			writeJSON(t, writer, controlMutationReceiptDocument{
				RequestReplayed: applyCalls > 1,
				Mutation: controlMutationDocument{
					Kind: "desired_applied",
					Resources: []controlApplyResultDocument{{
						Resource: document,
						Created:  true,
						Changed:  true,
					}},
					Replayed: applyCalls > 1,
				},
			})
		case request.Method == http.MethodGet && request.URL.Path == controlResourcesPath:
			listed := controlResourceListDocument{LatestChangeCursor: 1}
			if !deleted && len(document.Desired) != 0 {
				listed.Resources = []controlManagedResourceDocument{document}
			}
			writeJSON(t, writer, listed)
		case request.Method == http.MethodGet && request.URL.Path == controlResourcePath(regionalKey(resources.KindStream, "orders")):
			if deleted || len(document.Desired) == 0 {
				writer.WriteHeader(http.StatusNotFound)
				writeJSON(t, writer, map[string]any{"code": "not_found", "message": "missing"})
				return
			}
			writeJSON(t, writer, document)
		case request.Method == http.MethodDelete && request.URL.Path == controlResourcePath(regionalKey(resources.KindStream, "orders")):
			deleteCalls++
			deleted = true
			writeJSON(t, writer, controlMutationReceiptDocument{
				RequestReplayed: deleteCalls > 1,
				Mutation: controlMutationDocument{
					Kind:       "desired_deleted",
					Name:       &document.Name,
					Generation: 2,
					Deleted:    true,
					Replayed:   deleteCalls > 1,
				},
			})
		default:
			t.Errorf("unexpected request: %s %s", request.Method, request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	authority, err := NewHTTPAuthority([]string{server.URL}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	registry, err := newCatalogRegistry(authority, "epoch-control-0", func() time.Time {
		return time.UnixMilli(1_000)
	})
	if err != nil {
		t.Fatal(err)
	}
	request := resources.ApplyRequest{
		RequestToken:       "create-orders",
		ExpectedGeneration: uint64Pointer(0),
		Resource: resources.DesiredResource{
			ResourceKey: regionalKey(resources.KindStream, "orders"),
			Governance:  testGovernance(),
			Spec:        json.RawMessage(`{"shard_count":1,"replica_count":3}`),
		},
	}
	created, err := registry.Apply(request)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if !created.Created || !created.Changed || created.Replayed || created.Resource.Generation != 1 {
		t.Fatalf("Apply() = %+v", created)
	}
	replayed, err := registry.Apply(request)
	if err != nil {
		t.Fatalf("Apply(replay) error = %v", err)
	}
	if !replayed.Replayed || registry.Count() != 1 {
		t.Fatalf("Apply(replay) = %+v, count = %d", replayed, registry.Count())
	}
	got, err := registry.Get(request.Resource.ResourceKey)
	if err != nil || got.Generation != 1 || got.Name != "orders" {
		t.Fatalf("Get() = %+v, %v", got, err)
	}
	listed, err := registry.List(resources.ListFilter{Organization: "acme", Namespace: "core"})
	if err != nil || len(listed) != 1 || listed[0].Name != "orders" {
		t.Fatalf("List() = %+v, %v", listed, err)
	}
	deletedResult, err := registry.Delete(resources.DeleteRequest{
		RequestToken:       "delete-orders",
		ExpectedGeneration: uint64Pointer(1),
		Key:                request.Resource.ResourceKey,
	})
	if err != nil || !deletedResult.Deleted || deletedResult.Generation != 2 || registry.Count() != 0 {
		t.Fatalf("Delete() = %+v, %v, count = %d", deletedResult, err, registry.Count())
	}
}

func TestCatalogRegistryPagesTheCatalogInventoryBeforeFiltering(t *testing.T) {
	keys := []resources.ResourceKey{
		regionalKey(resources.KindStream, "audit"),
		regionalKey(resources.KindStream, "orders"),
		regionalKey(resources.KindStream, "payments"),
	}
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != controlResourcesPath {
			t.Errorf("unexpected request: %s %s", request.Method, request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		requests++
		if request.URL.Query().Get("limit") != "128" {
			t.Errorf("page limit = %q", request.URL.Query().Get("limit"))
		}
		start := 0
		if encoded := request.URL.Query().Get("after"); encoded != "" {
			var after controlResourceNameDocument
			if err := json.Unmarshal([]byte(encoded), &after); err != nil {
				t.Errorf("decode page cursor: %v", err)
			}
			if after != controlName(keys[1]) {
				t.Errorf("page cursor = %+v", after)
			}
			start = 2
		}
		listed := make([]map[string]any, 0, 2)
		end := min(start+2, len(keys))
		for _, key := range keys[start:end] {
			desired, _ := json.Marshal(resources.DesiredResource{
				ResourceKey: key,
				Governance:  testGovernance(),
				Spec:        json.RawMessage(`{"shard_count":1,"replica_count":3}`),
			})
			listed = append(listed, map[string]any{
				"name": controlName(key), "generation": "1", "desired": json.RawMessage(desired),
				"status": json.RawMessage(`{"phase":"pending"}`), "deletion_requested": false,
			})
		}
		response := map[string]any{"latest_change_cursor": "3", "resources": listed}
		if end < len(keys) {
			response["next_page_after"] = controlName(keys[end-1])
		}
		writeJSON(t, writer, response)
	}))
	defer server.Close()
	authority, err := NewHTTPAuthority([]string{server.URL}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewCatalogRegistry(authority, "epoch-control-0")
	if err != nil {
		t.Fatal(err)
	}

	listed, err := registry.List(resources.ListFilter{Namespace: "core"})
	if err != nil || len(listed) != 3 || listed[2].Name != "payments" {
		t.Fatalf("List() = %+v, %v", listed, err)
	}
	if requests != 2 || registry.Count() != 3 {
		t.Fatalf("inventory requests = %d, count = %d", requests, registry.Count())
	}
}

func TestCatalogRegistryPreservesEscapedOperationTokens(t *testing.T) {
	const token = "release/a%b c"
	var escapedPath string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		escapedPath = request.URL.EscapedPath()
		writeJSON(t, writer, controlOperationDocument{
			RequestToken: token,
			ProposalID:   9,
			State:        ControlOperationSucceeded,
		})
	}))
	defer server.Close()
	authority, err := NewHTTPAuthority([]string{server.URL}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewCatalogRegistry(authority, "epoch-control-0")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := registry.ControlOperation(t.Context(), token); err != nil {
		t.Fatalf("ControlOperation() error = %v", err)
	}
	want := controlOperationsPath + "/release%2Fa%25b%20c"
	if escapedPath != want {
		t.Fatalf("escaped request path = %q, want %q", escapedPath, want)
	}
}

func TestCatalogRegistryProvidesAtomicBatchOperationsAndResumableChanges(t *testing.T) {
	keys := []resources.ResourceKey{
		regionalKey(resources.KindStream, "audit"),
		regionalKey(resources.KindStream, "orders"),
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("content-type", "application/json")
		switch {
		case request.Method == http.MethodPut && request.URL.Path == controlResourcesPath:
			var body controlApplyDesiredBody
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode batch: %v", err)
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			if body.RequestToken != "batch-1" || len(body.Resources) != 2 ||
				body.Resources[0].Name.Name != "audit" || body.Resources[1].Name.Name != "orders" {
				t.Errorf("batch was not canonical: %+v", body)
			}
			results := make([]controlApplyResultDocument, 0, len(body.Resources))
			for _, item := range body.Resources {
				results = append(results, controlApplyResultDocument{
					Resource: controlManagedResourceDocument{
						Name: item.Name, Generation: 1, Desired: item.Desired, Status: json.RawMessage(`{}`),
					},
					Created: true,
					Changed: true,
				})
			}
			writeJSON(t, writer, controlMutationReceiptDocument{Mutation: controlMutationDocument{
				Kind: "desired_applied", Resources: results,
			}})
		case request.Method == http.MethodGet && request.URL.Path == controlOperationsPath+"/batch-1":
			writeJSON(t, writer, controlOperationDocument{
				RequestToken: "batch-1",
				ProposalID:   42,
				State:        ControlOperationSucceeded,
				ResourceNames: []controlResourceNameDocument{
					controlName(keys[0]), controlName(keys[1]),
				},
				FirstChangeCursor: 5,
				LastChangeCursor:  6,
			})
		case request.Method == http.MethodGet && request.URL.Path == controlChangesPath:
			if request.URL.Query().Get("after") != "4" || request.URL.Query().Get("limit") != "10" {
				t.Errorf("change query = %q", request.URL.RawQuery)
			}
			writeJSON(t, writer, controlChangesDocument{
				EarliestCursor: 1,
				LatestCursor:   6,
				Changes: []controlChangeDocument{
					{Cursor: 5, Kind: ControlChangeDesiredApplied, Name: controlName(keys[0]), Generation: 1},
					{Cursor: 6, Kind: ControlChangeDesiredApplied, Name: controlName(keys[1]), Generation: 1},
				},
			})
		default:
			t.Errorf("unexpected request: %s %s?%s", request.Method, request.URL.Path, request.URL.RawQuery)
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	authority, err := NewHTTPAuthority([]string{server.URL}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewCatalogRegistry(authority, "epoch-control-0")
	if err != nil {
		t.Fatal(err)
	}
	requests := []resources.ApplyRequest{
		{ExpectedGeneration: uint64Pointer(0), Resource: resources.DesiredResource{
			ResourceKey: keys[1], Governance: testGovernance(), Spec: json.RawMessage(`{"shard_count":1,"replica_count":3}`),
		}},
		{ExpectedGeneration: uint64Pointer(0), Resource: resources.DesiredResource{
			ResourceKey: keys[0], Governance: testGovernance(), Spec: json.RawMessage(`{"shard_count":1,"replica_count":3}`),
		}},
	}
	batch, err := registry.BatchApply(t.Context(), BatchApplyRequest{
		RequestToken: "batch-1",
		Resources:    requests,
	})
	if err != nil || len(batch.Results) != 2 || batch.Results[0].Resource.ResourceKey != keys[0] ||
		batch.Results[1].Resource.ResourceKey != keys[1] || registry.Count() != 2 {
		t.Fatalf("BatchApply() = %+v, %v, count = %d", batch, err, registry.Count())
	}
	operation, err := registry.ControlOperation(t.Context(), "batch-1")
	if err != nil || operation.State != ControlOperationSucceeded || operation.ProposalID != 42 ||
		len(operation.ResourceKeys) != 2 || operation.FirstChangeCursor != 5 || operation.LastChangeCursor != 6 {
		t.Fatalf("ControlOperation() = %+v, %v", operation, err)
	}
	changes, err := registry.ControlChanges(t.Context(), 4, 10)
	if err != nil || changes.EarliestCursor != 1 || changes.LatestCursor != 6 ||
		len(changes.Changes) != 2 || changes.Changes[0].Key != keys[0] || changes.Changes[1].Cursor != 6 {
		t.Fatalf("ControlChanges() = %+v, %v", changes, err)
	}
}

func TestCatalogRegistryLeasePreflightAndStatusAreFenced(t *testing.T) {
	var leasePosts int
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("content-type", "application/json")
		switch {
		case request.Method == http.MethodGet && request.URL.Path == controlLeasePath:
			writer.WriteHeader(http.StatusNotFound)
			writeJSON(t, writer, map[string]any{"code": "not_found", "message": "no lease"})
		case request.Method == http.MethodPost && request.URL.Path == controlLeasePath:
			leasePosts++
			writeJSON(t, writer, controlMutationReceiptDocument{Mutation: controlMutationDocument{
				Kind: "control_lease_acquired",
				Lease: &controlLeaseDocument{
					OwnerID:      "epoch-control-0",
					Fence:        7,
					ValidUntilMS: 11_000,
				},
			}})
		case request.Method == http.MethodPut && request.URL.Path == controlStatusPath(regionalKey(resources.KindStream, "orders")):
			var body controlStatusBody
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode status: %v", err)
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			if body.Lease.OwnerID != "epoch-control-0" || body.Lease.Fence != "7" || body.Lease.NowMS != "1000" {
				t.Errorf("unexpected status lease: %+v", body.Lease)
			}
			desired, _ := json.Marshal(resources.DesiredResource{
				ResourceKey: regionalKey(resources.KindStream, "orders"),
				Governance:  testGovernance(),
				Spec:        json.RawMessage(`{"shard_count":1,"replica_count":3}`),
			})
			status, _ := json.Marshal(body.Status)
			writeJSON(t, writer, controlMutationReceiptDocument{Mutation: controlMutationDocument{
				Kind: "managed_status_updated",
				Resource: &controlManagedResourceDocument{
					Name:       controlName(regionalKey(resources.KindStream, "orders")),
					Generation: 1,
					Desired:    desired,
					Status:     status,
				},
			}})
		default:
			t.Errorf("unexpected request: %s %s", request.Method, request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	authority, err := NewHTTPAuthority([]string{server.URL}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	registry, err := newCatalogRegistry(authority, "epoch-control-0", func() time.Time {
		return time.UnixMilli(1_000)
	})
	if err != nil {
		t.Fatal(err)
	}
	status := resources.ResourceStatus{Phase: resources.PhaseReady, ObservedGeneration: 1, CatalogGeneration: 1}
	updated, err := registry.UpdateStatus(regionalKey(resources.KindStream, "orders"), 1, status)
	if err != nil {
		t.Fatalf("UpdateStatus() error = %v", err)
	}
	if updated.Status.Phase != resources.PhaseReady || leasePosts != 1 {
		t.Fatalf("UpdateStatus() = %+v, lease posts = %d", updated, leasePosts)
	}
	if _, err := registry.ControlLease(); err != nil || leasePosts != 1 {
		t.Fatalf("ControlLease() error = %v, lease posts = %d", err, leasePosts)
	}
}

func TestCatalogRegistryStatusTokenBindsTheCompleteLeaseGuard(t *testing.T) {
	key := regionalKey(resources.KindStream, "orders")
	nowMS := int64(1_000)
	var tokens []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == controlLeasePath:
			writer.WriteHeader(http.StatusNotFound)
			writeJSON(t, writer, map[string]any{"code": "not_found", "message": "missing"})
		case request.Method == http.MethodPost && request.URL.Path == controlLeasePath:
			writeJSON(t, writer, controlMutationReceiptDocument{Mutation: controlMutationDocument{
				Kind:  "control_lease_acquired",
				Lease: &controlLeaseDocument{OwnerID: "epoch-control-0", Fence: 7, ValidUntilMS: 11_000},
			}})
		case request.Method == http.MethodPut && request.URL.Path == controlStatusPath(key):
			var body controlStatusBody
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode status: %v", err)
			}
			tokens = append(tokens, body.RequestToken)
			desired, _ := json.Marshal(resources.DesiredResource{
				ResourceKey: key,
				Governance:  testGovernance(),
				Spec:        json.RawMessage(`{"shard_count":1,"replica_count":3}`),
			})
			status, _ := json.Marshal(body.Status)
			writeJSON(t, writer, controlMutationReceiptDocument{Mutation: controlMutationDocument{
				Kind: "managed_status_updated",
				Resource: &controlManagedResourceDocument{
					Name: controlName(key), Generation: 1, Desired: desired, Status: status,
				},
			}})
		default:
			t.Errorf("unexpected request: %s %s", request.Method, request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	authority, err := NewHTTPAuthority([]string{server.URL}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	registry, err := newCatalogRegistry(authority, "epoch-control-0", func() time.Time {
		return time.UnixMilli(nowMS)
	})
	if err != nil {
		t.Fatal(err)
	}
	status := resources.ResourceStatus{
		Phase: resources.PhaseReady, ObservedGeneration: 1, CatalogGeneration: 1,
	}
	if _, err := registry.UpdateStatus(key, 1, status); err != nil {
		t.Fatal(err)
	}
	nowMS = 2_000
	if _, err := registry.UpdateStatus(key, 1, status); err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 2 || tokens[0] == tokens[1] {
		t.Fatalf("status tokens did not bind lease now_ms: %v", tokens)
	}
}

func TestCatalogRegistryImportsLegacyGenerationsWithStableReplayToken(t *testing.T) {
	var firstToken string
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("content-type", "application/json")
		if request.Method != http.MethodPost || request.URL.Path != controlImportPath {
			t.Errorf("unexpected request: %s %s", request.Method, request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		var body controlImportBody
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode import: %v", err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		requests++
		if firstToken == "" {
			firstToken = body.RequestToken
		} else if body.RequestToken != firstToken {
			t.Errorf("import token changed: %q != %q", body.RequestToken, firstToken)
		}
		if len(body.Resources) != 1 || len(body.Generations) != 2 ||
			body.Generations[0].Name.Name != "audit" || body.Generations[1].Name.Name != "orders" ||
			body.Resources[0].Generation != "7" {
			t.Errorf("unexpected import: %+v", body)
		}
		writeJSON(t, writer, controlMutationReceiptDocument{
			RequestReplayed: requests > 1,
			Mutation: controlMutationDocument{
				Kind: "desired_applied",
				Resources: []controlApplyResultDocument{{
					Resource: controlManagedResourceDocument{
						Name:       body.Resources[0].Name,
						Generation: 7,
						Desired:    body.Resources[0].Desired,
						Status:     body.Resources[0].Status,
					},
					Created: true,
					Changed: true,
				}},
				Replayed: requests > 1,
			},
		})
	}))
	defer server.Close()
	authority, err := NewHTTPAuthority([]string{server.URL}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewCatalogRegistry(authority, "epoch-control-0")
	if err != nil {
		t.Fatal(err)
	}
	snapshot := resources.LegacyRegistrySnapshot{
		Resources: []resources.Resource{{
			ResourceKey: regionalKey(resources.KindStream, "orders"),
			Governance:  testGovernance(),
			Spec:        json.RawMessage(`{"shard_count":1,"replica_count":3}`),
			Generation:  7,
			Status: resources.ResourceStatus{
				Phase:              resources.PhaseReady,
				ObservedGeneration: 7,
				CatalogGeneration:  7,
			},
		}},
		Generations: []resources.LegacyGeneration{
			{Key: regionalKey(resources.KindStream, "orders"), Generation: 7},
			{Key: regionalKey(resources.KindStream, "audit"), Generation: 4},
		},
	}
	if err := registry.ImportLegacy(snapshot); err != nil {
		t.Fatalf("ImportLegacy() error = %v", err)
	}
	if err := registry.ImportLegacy(snapshot); err != nil {
		t.Fatalf("ImportLegacy(replay) error = %v", err)
	}
	if requests != 2 || registry.Count() != 1 {
		t.Fatalf("requests = %d, count = %d", requests, registry.Count())
	}
}

func TestCatalogRegistryManagedDeleteIsLeaseFencedAndExactlyReplayable(t *testing.T) {
	leasePosts := 0
	deletePosts := 0
	var deleteToken string
	key := regionalKey(resources.KindStream, "orders")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("content-type", "application/json")
		switch {
		case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, controlOperationsPath+"/"):
			if deletePosts == 0 {
				writer.WriteHeader(http.StatusNotFound)
				writeJSON(t, writer, map[string]any{"code": "not_found", "message": "no outcome"})
				return
			}
			name := controlName(key)
			writeJSON(t, writer, controlOperationDocument{
				RequestToken:  deleteToken,
				ProposalID:    37,
				State:         ControlOperationSucceeded,
				ResourceNames: []controlResourceNameDocument{name},
				Mutation: &controlMutationDocument{
					Kind:              "managed_deleted",
					Name:              &name,
					DesiredGeneration: 5,
					CatalogGeneration: 4,
					Deleted:           true,
				},
			})
		case request.Method == http.MethodGet && request.URL.Path == controlLeasePath:
			writer.WriteHeader(http.StatusNotFound)
			writeJSON(t, writer, map[string]any{"code": "not_found", "message": "no lease"})
		case request.Method == http.MethodPost && request.URL.Path == controlLeasePath:
			leasePosts++
			writeJSON(t, writer, controlMutationReceiptDocument{Mutation: controlMutationDocument{
				Kind: "control_lease_acquired",
				Lease: &controlLeaseDocument{
					OwnerID:      "epoch-control-0",
					Fence:        9,
					ValidUntilMS: 11_000,
				},
			}})
		case request.Method == http.MethodDelete && request.URL.Path == controlMaterializationsPath+"/"+resourceSegments(key):
			var body controlDeleteManagedBody
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode managed delete: %v", err)
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			deletePosts++
			if body.RequestToken != "delete-orders" {
				t.Errorf("managed delete did not preserve public token: %q", body.RequestToken)
			}
			if deleteToken == "" {
				deleteToken = body.RequestToken
			} else if body.RequestToken != deleteToken {
				t.Errorf("managed delete token changed: %q != %q", body.RequestToken, deleteToken)
			}
			if body.Lease.Fence != "9" || body.ExpectedDesiredGeneration != "4" ||
				body.ExpectedCatalogGeneration != "3" {
				t.Errorf("unexpected managed delete body: %+v", body)
			}
			name := controlName(key)
			writeJSON(t, writer, controlMutationReceiptDocument{
				RequestReplayed: deletePosts > 1,
				Mutation: controlMutationDocument{
					Kind:              "managed_deleted",
					Name:              &name,
					DesiredGeneration: 5,
					CatalogGeneration: 4,
					Deleted:           true,
					Replayed:          deletePosts > 1,
				},
			})
		default:
			t.Errorf("unexpected request: %s %s", request.Method, request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	authority, err := NewHTTPAuthority([]string{server.URL}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	registry, err := newCatalogRegistry(authority, "epoch-control-0", func() time.Time {
		return time.UnixMilli(1_000)
	})
	if err != nil {
		t.Fatal(err)
	}
	registry.count.Store(1)
	expected := uint64(4)
	request := resources.DeleteRequest{
		RequestToken:       "delete-orders",
		ExpectedGeneration: &expected,
		Key:                key,
	}
	deleted, err := registry.DeleteManaged(t.Context(), request, 4, 3)
	if err != nil || !deleted.Deleted || deleted.Generation != 5 || deleted.Replayed {
		t.Fatalf("DeleteManaged() = %+v, %v", deleted, err)
	}
	registry.leaseMu.Lock()
	registry.lease = controlLeaseDocument{}
	registry.leaseMu.Unlock()
	replayed, err := registry.DeleteManaged(t.Context(), request, 4, 3)
	if err != nil || !replayed.Replayed || replayed.Generation != 5 {
		t.Fatalf("DeleteManaged(replay) = %+v, %v", replayed, err)
	}
	if leasePosts != 1 || deletePosts != 1 || registry.Count() != 0 {
		t.Fatalf("lease posts = %d, delete posts = %d, count = %d", leasePosts, deletePosts, registry.Count())
	}
	wrongGeneration := uint64(3)
	request.ExpectedGeneration = &wrongGeneration
	_, err = registry.DeleteManaged(t.Context(), request, 3, 3)
	assertStoreCode(t, err, resources.CodeConflict)
}

func TestCatalogRegistryDoesNotChallengeAnotherLiveOwner(t *testing.T) {
	leasePosts := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("content-type", "application/json")
		if request.Method == http.MethodGet && request.URL.Path == controlLeasePath {
			writeJSON(t, writer, controlLeaseDocument{OwnerID: "epoch-control-0", Fence: 3, ValidUntilMS: 20_000})
			return
		}
		if request.Method == http.MethodPost && request.URL.Path == controlLeasePath {
			leasePosts++
		}
		writer.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	authority, err := NewHTTPAuthority([]string{server.URL}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	registry, err := newCatalogRegistry(authority, "epoch-control-1", func() time.Time {
		return time.UnixMilli(1_000)
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = registry.ControlLease()
	assertStoreCode(t, err, resources.CodeUnavailable)
	if leasePosts != 0 {
		t.Fatalf("standby posted %d lease challenges", leasePosts)
	}
}

func TestCatalogRegistryReplaysMissingDeleteByItsPublicToken(t *testing.T) {
	key := regionalKey(resources.KindStream, "missing")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.EscapedPath() != controlOperationsPath+"/delete-missing" {
			t.Errorf("unexpected request: %s %s", request.Method, request.URL.EscapedPath())
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		name := controlName(key)
		writeJSON(t, writer, controlOperationDocument{
			RequestToken:  "delete-missing",
			ProposalID:    12,
			State:         ControlOperationSucceeded,
			ResourceNames: []controlResourceNameDocument{name},
			Mutation: &controlMutationDocument{
				Kind: "desired_deleted", Name: &name, Generation: 0, Deleted: false,
			},
		})
	}))
	defer server.Close()
	authority, err := NewHTTPAuthority([]string{server.URL}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewCatalogRegistry(authority, "epoch-control-1")
	if err != nil {
		t.Fatal(err)
	}

	replayed, found, err := registry.ReplayManagedDelete(t.Context(), resources.DeleteRequest{
		RequestToken: "delete-missing",
		Key:          key,
	})
	if err != nil || !found || replayed.Deleted || !replayed.Replayed {
		t.Fatalf("ReplayManagedDelete() = %+v, %t, %v", replayed, found, err)
	}
}

func TestCatalogRegistryUsesTheActiveLeaseForStandbyDeletes(t *testing.T) {
	key := regionalKey(resources.KindStream, "orders")
	deleteCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, controlOperationsPath+"/"):
			writer.WriteHeader(http.StatusNotFound)
			writeJSON(t, writer, map[string]any{"code": "not_found", "message": "missing"})
		case request.Method == http.MethodGet && request.URL.Path == controlLeasePath:
			writeJSON(t, writer, controlLeaseDocument{
				OwnerID: "epoch-control-0", Fence: 4, ValidUntilMS: 20_000,
			})
		case request.Method == http.MethodDelete && request.URL.Path == controlMaterializationsPath+"/"+resourceSegments(key):
			var body controlDeleteManagedBody
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode managed delete: %v", err)
			}
			deleteCalls++
			if body.Lease.OwnerID != "epoch-control-0" || body.Lease.Fence != "4" {
				t.Errorf("standby did not delegate through active lease: %+v", body.Lease)
			}
			name := controlName(key)
			writeJSON(t, writer, controlMutationReceiptDocument{Mutation: controlMutationDocument{
				Kind: "managed_deleted", Name: &name, DesiredGeneration: 3,
				CatalogGeneration: 2, Deleted: true,
			}})
		default:
			t.Errorf("unexpected request: %s %s", request.Method, request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	authority, err := NewHTTPAuthority([]string{server.URL}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	registry, err := newCatalogRegistry(authority, "epoch-control-1", func() time.Time {
		return time.UnixMilli(1_000)
	})
	if err != nil {
		t.Fatal(err)
	}
	registry.count.Store(1)

	result, err := registry.DeleteManaged(t.Context(), resources.DeleteRequest{
		RequestToken: "delete-orders", Key: key,
	}, 2, 1)
	if err != nil || !result.Deleted || deleteCalls != 1 {
		t.Fatalf("DeleteManaged() = %+v, %v, calls = %d", result, err, deleteCalls)
	}
}

func TestCatalogRegistryRejectsNoncanonicalOwnerAndPreEpochClock(t *testing.T) {
	for _, owner := range []string{"", " owner", "owner ", "owner with space", "owner#1"} {
		if _, err := newCatalogRegistry(&HTTPAuthority{}, owner, time.Now); err == nil {
			t.Fatalf("owner %q was accepted", owner)
		}
	}
	registry, err := newCatalogRegistry(&HTTPAuthority{}, "epoch-control-0", func() time.Time {
		return time.UnixMilli(-1)
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = registry.ControlLease()
	assertStoreCode(t, err, resources.CodeUnavailable)
}

func writeJSON(t *testing.T, writer http.ResponseWriter, value any) {
	t.Helper()
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		t.Errorf("encode response: %v", err)
	}
}

func assertStoreCode(t *testing.T, err error, code resources.ErrorCode) {
	t.Helper()
	var storeError *resources.RegistryError
	if !errors.As(err, &storeError) || storeError.Code != code {
		t.Fatalf("error = %v, want store code %s", err, code)
	}
}
