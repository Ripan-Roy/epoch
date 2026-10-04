package regional

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"epoch.local/epoch/control/internal/resources"
)

func TestManagedDeleteReplayBindsOriginalOptionalPrecondition(t *testing.T) {
	zero, four := uint64(0), uint64(4)
	cases := []struct {
		name              string
		stored, requested *uint64
		conflict          bool
	}{
		{"omitted_exact", nil, nil, false},
		{"explicit_exact", &four, &four, false},
		{"omitted_is_not_zero", nil, &zero, true},
		{"explicit_is_not_omitted", &four, nil, true},
		{"different_generation", &four, &zero, true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			key := regionalKey(resources.KindStream, "orders")
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.Method != http.MethodGet || request.URL.Path != controlOperationsPath+"/original-delete" {
					t.Errorf("replay tried mutation or unrelated lookup: %s %s", request.Method, request.URL.Path)
					writer.WriteHeader(http.StatusBadRequest)
					return
				}
				name := controlName(key)
				writeJSON(t, writer, controlOperationDocument{
					RequestToken: "original-delete", ProposalID: 17, State: ControlOperationSucceeded,
					CommandKind: "delete_managed", ResourceNames: []controlResourceNameDocument{name},
					ExpectedGeneration: decimalPointerForTest(test.stored),
					Mutation:           &controlMutationDocument{Kind: "managed_deleted", Name: &name, DesiredGeneration: 5, CatalogGeneration: 4, Deleted: true},
				})
			}))
			defer server.Close()
			authority, err := NewHTTPAuthority([]string{server.URL}, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			registry, err := NewCatalogRegistry(authority, "test-controller")
			if err != nil {
				t.Fatal(err)
			}
			result, found, err := registry.ReplayManagedDelete(t.Context(), resources.DeleteRequest{Key: key, RequestToken: "original-delete", ExpectedGeneration: test.requested})
			if !found {
				t.Fatal("completed delete was not found")
			}
			if test.conflict {
				assertStoreCode(t, err, resources.CodeConflict)
				return
			}
			if err != nil || !result.Replayed || !result.Deleted || result.Generation != 5 {
				t.Fatalf("exact delete replay = %+v, %v", result, err)
			}
		})
	}
}

func decimalPointerForTest(value *uint64) *decimalUint64 {
	if value == nil {
		return nil
	}
	encoded := decimalUint64(*value)
	return &encoded
}

func TestReconcilerReplaysCompletedDeleteBeforeNewIncarnationPrecondition(t *testing.T) {
	local := resources.NewRegistry()
	key := regionalKey(resources.KindStream, "orders")
	created := applyDesired(t, local, "create-original", key, 1, 3)
	registry := &managedDeleteRegistry{Store: local}
	reconciler := NewReconciler(registry, &fakeAuthority{})
	request := resources.DeleteRequest{Key: key, RequestToken: "delete-original", ExpectedGeneration: &created.Generation}
	deleted, err := reconciler.Delete(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	recreated := applyDesired(t, local, "recreate-new", key, 1, 3)
	if recreated.Generation <= deleted.Generation {
		t.Fatal("resource incarnation did not advance")
	}
	replayed, err := reconciler.Delete(t.Context(), request)
	if err != nil || !replayed.Replayed || replayed.Generation != deleted.Generation {
		t.Fatalf("completed delete replay after recreation = %+v, %v", replayed, err)
	}
	current, err := local.Get(key)
	if err != nil || current.Generation != recreated.Generation {
		t.Fatalf("old token removed or changed new incarnation: %+v, %v", current, err)
	}
}
