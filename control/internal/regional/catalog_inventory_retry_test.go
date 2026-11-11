package regional

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"epoch.local/epoch/control/internal/resources"
	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func inventoryResource(t *testing.T, name string, generation uint64, owner ...string) controlManagedResourceDocument {
	t.Helper()
	key := regionalKey(resources.KindStream, name)
	governance := testGovernance()
	if len(owner) != 0 {
		governance.Owner = owner[0]
	}
	desired, err := json.Marshal(resources.DesiredResource{
		ResourceKey: key, Governance: governance, Spec: json.RawMessage(`{"shard_count":1,"replica_count":3}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	return controlManagedResourceDocument{
		Name: controlName(key), Generation: decimalUint64(generation), Desired: desired, Status: json.RawMessage(`{"phase":"pending"}`),
	}
}

func inventoryRegistry(t *testing.T, handler http.HandlerFunc) *CatalogRegistry {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	authority, err := NewHTTPAuthority([]string{server.URL}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewCatalogRegistry(authority, "inventory-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	return registry
}

func TestCatalogInventoryRestartsChangedSnapshotWithoutPublishingMixedPages(t *testing.T) {
	first := inventoryResource(t, "a", 1, "team:inventory")
	last := inventoryResource(t, "b", 1)
	foreign := inventoryResource(t, "c", 1, "team:inventory")
	var foreignDesired resources.DesiredResource
	if err := json.Unmarshal(foreign.Desired, &foreignDesired); err != nil {
		t.Fatal(err)
	}
	foreignDesired.Namespace, foreign.Name.Namespace = "external", "external"
	var err error
	foreign.Desired, err = json.Marshal(foreignDesired)
	if err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int32
	registry := inventoryRegistry(t, func(writer http.ResponseWriter, request *http.Request) {
		index := requests.Add(1)
		if request.Method != http.MethodGet || request.URL.Path != controlResourcesPath || request.URL.Query().Get("limit") != "128" {
			t.Errorf("unexpected inventory request %s %s", request.Method, request.URL)
		}
		var after controlResourceNameDocument
		if encoded := request.URL.Query().Get("after"); encoded != "" {
			if err := json.Unmarshal([]byte(encoded), &after); err != nil {
				t.Error(err)
			}
		}
		if index%2 == 1 {
			if after != (controlResourceNameDocument{}) {
				t.Error("a restarted inventory must begin without a cursor")
			}
			document := first
			cursor := decimalUint64(10)
			if index > 2 {
				document.Generation, cursor = 2, 11
			}
			writeJSON(t, writer, controlResourceListDocument{LatestChangeCursor: cursor, Resources: []controlManagedResourceDocument{document}, NextPageAfter: &first.Name})
		} else {
			if after != first.Name {
				t.Errorf("continuation = %+v", after)
			}
			document, foreignDocument := last, foreign
			if index > 2 {
				document.Generation = 2
				foreignDocument.Generation = 2
			}
			writeJSON(t, writer, controlResourceListDocument{LatestChangeCursor: 11, Resources: []controlManagedResourceDocument{document, foreignDocument}})
		}
	})
	listed, err := registry.List(resources.ListFilter{Namespace: "core", Kind: resources.KindStream, Owner: "team:inventory"})
	if err != nil {
		t.Fatalf("transient inventory churn should recover with a fresh coherent scan: %v", err)
	}
	if requests.Load() != 4 || len(listed) != 1 || registry.Count() != 3 {
		t.Fatalf("requests=%d resources=%+v count=%d", requests.Load(), listed, registry.Count())
	}
	if listed[0].Generation != 2 || listed[0].Name != "a" {
		t.Fatalf("mixed inventory generations escaped: %+v", listed)
	}
}

func TestCatalogInventoryMalformedPagesAreNeverRetriedAwayAsChurn(t *testing.T) {
	first, last := inventoryResource(t, "a", 1), inventoryResource(t, "b", 1)
	for _, scenario := range []struct {
		name    string
		page    int
		malform func(*controlResourceListDocument)
	}{
		{"empty nonterminal", 1, func(page *controlResourceListDocument) { page.Resources = nil }},
		{"oversized", 1, func(page *controlResourceListDocument) { page.Resources = make([]controlManagedResourceDocument, 129) }},
		{"inconsistent continuation", 1, func(page *controlResourceListDocument) { page.NextPageAfter = &last.Name }},
		{"repeated key during churn", 2, func(page *controlResourceListDocument) { page.Resources = []controlManagedResourceDocument{first} }},
		{"invalid desired during churn", 2, func(page *controlResourceListDocument) { page.Resources[0].Desired = json.RawMessage(`{}`) }},
		{"foreign desired identity during churn", 2, func(page *controlResourceListDocument) { page.Resources[0].Desired = first.Desired }},
		{"inconsistent continuation during churn", 2, func(page *controlResourceListDocument) { page.NextPageAfter = &first.Name }},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			requests := 0
			registry := transportInventoryRegistry(t, func(_ *http.Request) controlResourceListDocument {
				requests++
				page := controlResourceListDocument{LatestChangeCursor: 2, Resources: []controlManagedResourceDocument{last}}
				if requests%2 == 1 {
					page.LatestChangeCursor, page.Resources, page.NextPageAfter = 1, []controlManagedResourceDocument{first}, &first.Name
				}
				if requests == scenario.page {
					scenario.malform(&page)
				}
				return page
			})
			registry.count.Store(17)
			listed, err := registry.List(resources.ListFilter{})
			if err == nil || listed != nil || requests != scenario.page || registry.Count() != 17 {
				t.Fatalf("malformed scan was retried or published: resources=%+v err=%v requests=%d count=%d", listed, err, requests, registry.Count())
			}
		})
	}
}

func TestCatalogInventoryCursorRegressionFailsWithoutRetryOrPublishing(t *testing.T) {
	first, last := inventoryResource(t, "a", 1), inventoryResource(t, "b", 1)
	for _, scenario := range []struct {
		name    string
		cursors []decimalUint64
		failAt  int
	}{
		{"within one scan", []decimalUint64{11, 10, 11, 11}, 2},
		{"across a fresh scan", []decimalUint64{10, 12, 11, 11}, 3},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			requests := 0
			registry := transportInventoryRegistry(t, func(_ *http.Request) controlResourceListDocument {
				requests++
				index := min(requests-1, len(scenario.cursors)-1)
				page := controlResourceListDocument{LatestChangeCursor: scenario.cursors[index], Resources: []controlManagedResourceDocument{last}}
				if requests%2 == 1 {
					page.Resources, page.NextPageAfter = []controlManagedResourceDocument{first}, &first.Name
				}
				return page
			})
			registry.count.Store(17)
			listed, err := registry.List(resources.ListFilter{})
			if err == nil || errors.Is(err, errCatalogInventoryChanged) || listed != nil || requests != scenario.failAt || registry.Count() != 17 {
				t.Fatalf("regressing inventory was retried or published: resources=%+v err=%v requests=%d count=%d", listed, err, requests, registry.Count())
			}
		})
	}
}

func TestCatalogInventoryDoesNotRetryAnUnavailableAuthorityAsSnapshotChurn(t *testing.T) {
	var requests atomic.Int32
	registry := inventoryRegistry(t, func(writer http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		writer.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(t, writer, map[string]string{"code": "unavailable", "message": "fixture unavailable"})
	})
	registry.count.Store(17)
	listed, err := registry.List(resources.ListFilter{})
	if err == nil || listed != nil || requests.Load() != 1 || registry.Count() != 17 {
		t.Fatalf("unavailable authority was retried as churn: resources=%+v err=%v requests=%d count=%d", listed, err, requests.Load(), registry.Count())
	}
}

func TestCatalogInventoryGRPCPassesTheUnexpiredOriginalDeadlineIntoEveryPage(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	expected, _ := ctx.Deadline()
	registry := transportInventoryRegistry(t, func(request *http.Request) controlResourceListDocument {
		deadline, exists := request.Context().Deadline()
		if !exists || !deadline.Equal(expected) {
			t.Errorf("gRPC caller deadline lost: got %v, want %v", deadline, expected)
		}
		return controlResourceListDocument{}
	})
	server := &RegionalAdminServer{registry: registry}
	if _, err := server.ListResources(ctx, &epochv1.ListResourcesRequest{}); err != nil {
		t.Fatal(err)
	}
}

func TestCatalogInventoryPersistentChurnIsBoundedAndNeverPublishesPartialCount(t *testing.T) {
	first, last := inventoryResource(t, "a", 1), inventoryResource(t, "b", 1)
	var requests atomic.Int32
	registry := inventoryRegistry(t, func(writer http.ResponseWriter, _ *http.Request) {
		index := requests.Add(1)
		page := controlResourceListDocument{LatestChangeCursor: decimalUint64(index), Resources: []controlManagedResourceDocument{last}}
		if index%2 == 1 {
			page.Resources, page.NextPageAfter = []controlManagedResourceDocument{first}, &first.Name
		}
		writeJSON(t, writer, page)
	})
	registry.count.Store(17)
	listed, err := registry.List(resources.ListFilter{})
	if err == nil || listed != nil {
		t.Fatalf("persistent churn returned a partial inventory: %+v, %v", listed, err)
	}
	if requests.Load() != 6 || registry.Count() != 17 {
		t.Fatalf("three bounded scans must not publish partial counts: requests=%d count=%d", requests.Load(), registry.Count())
	}
}

type inventoryRoundTripFunc func(*http.Request) (*http.Response, error)

func (function inventoryRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func transportInventoryRegistry(t *testing.T, response func(*http.Request) controlResourceListDocument) *CatalogRegistry {
	t.Helper()
	client := &http.Client{Transport: inventoryRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		body, err := json.Marshal(response(request))
		if err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(body)), Request: request}, nil
	})}
	authority, err := NewHTTPAuthority([]string{"http://127.0.0.1:1"}, client)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewCatalogRegistry(authority, "inventory-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	return registry
}

func TestCatalogInventoryReusesTheCallerDeadlineAcrossWholeScanRestarts(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	expectedDeadline, _ := ctx.Deadline()
	first, last := inventoryResource(t, "a", 1), inventoryResource(t, "b", 1)
	requests := 0
	registry := transportInventoryRegistry(t, func(request *http.Request) controlResourceListDocument {
		requests++
		deadline, exists := request.Context().Deadline()
		if !exists || !deadline.Equal(expectedDeadline) {
			t.Errorf("original caller deadline was reset: got %v, want %v", deadline, expectedDeadline)
		}
		page := controlResourceListDocument{LatestChangeCursor: 2, Resources: []controlManagedResourceDocument{last}}
		if requests%2 == 1 {
			page.Resources, page.NextPageAfter = []controlManagedResourceDocument{first}, &first.Name
			if requests == 1 {
				page.LatestChangeCursor = 1
			}
		}
		return page
	})
	if _, err := registry.ListContext(ctx, resources.ListFilter{}); err != nil {
		t.Fatal(err)
	}
	if requests != 4 {
		t.Fatalf("requests=%d, want one discarded and one coherent scan", requests)
	}
}

func TestCatalogInventoryBackgroundCallKeepsOneExistingControlCallBudget(t *testing.T) {
	first, last := inventoryResource(t, "a", 1), inventoryResource(t, "b", 1)
	var firstDeadline time.Time
	requests := 0
	registry := transportInventoryRegistry(t, func(request *http.Request) controlResourceListDocument {
		requests++
		deadline, exists := request.Context().Deadline()
		remaining := time.Until(deadline)
		if !exists || remaining <= 0 || remaining > controlCallTimeout {
			t.Errorf("inventory must retain the existing bounded control budget: exists=%v remaining=%v", exists, remaining)
		}
		if requests == 1 {
			firstDeadline = deadline
		} else if !deadline.Equal(firstDeadline) {
			t.Error("each inventory page must share one deadline, not receive a fresh budget")
		}
		page := controlResourceListDocument{LatestChangeCursor: 1, Resources: []controlManagedResourceDocument{last}}
		if requests == 1 {
			page.Resources, page.NextPageAfter = []controlManagedResourceDocument{first}, &first.Name
		}
		return page
	})
	if _, err := registry.List(resources.ListFilter{}); err != nil {
		t.Fatal(err)
	}
	if requests != 2 {
		t.Fatalf("requests=%d, want one two-page inventory", requests)
	}
}

func TestCatalogInventoryCancellationDiscardsStagedPagesWithoutAnotherRequest(t *testing.T) {
	for _, continuation := range []bool{false, true} {
		t.Run(fmt.Sprintf("continuation-%v", continuation), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			first := inventoryResource(t, "a", 1)
			requests := 0
			registry := transportInventoryRegistry(t, func(_ *http.Request) controlResourceListDocument {
				requests++
				cancel()
				page := controlResourceListDocument{LatestChangeCursor: 1, Resources: []controlManagedResourceDocument{first}}
				if continuation && requests == 1 {
					page.NextPageAfter = &first.Name
				}
				return page
			})
			registry.count.Store(17)
			listed, err := registry.ListContext(ctx, resources.ListFilter{})
			if !errors.Is(err, context.Canceled) || listed != nil || requests != 1 || registry.Count() != 17 {
				t.Fatalf("cancelled scan returned resources=%+v err=%v requests=%d count=%d", listed, err, requests, registry.Count())
			}
		})
	}
}

func TestCatalogInventoryGRPCPropagatesCancelledAndExpiredCallerContexts(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprintf("deadline-%v", deadline), func(t *testing.T) {
			var ctx context.Context
			var cancel context.CancelFunc
			if deadline {
				ctx, cancel = context.WithDeadline(t.Context(), time.Unix(1, 0))
			} else {
				ctx, cancel = context.WithCancel(t.Context())
				cancel()
			}
			defer cancel()
			requests := 0
			registry := transportInventoryRegistry(t, func(_ *http.Request) controlResourceListDocument { requests++; return controlResourceListDocument{} })
			server := &RegionalAdminServer{registry: registry}
			_, err := server.ListResources(ctx, &epochv1.ListResourcesRequest{})
			expected := codes.Canceled
			if deadline {
				expected = codes.DeadlineExceeded
			}
			if status.Code(err) != expected || requests != 0 {
				t.Fatalf("status=%s requests=%d, want %s before any authority call", status.Code(err), requests, expected)
			}
		})
	}
}
