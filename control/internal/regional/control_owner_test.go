package regional

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"epoch.local/epoch/control/internal/resources"
)

func TestProductionRegistriesFenceDistinctProcessIncarnations(t *testing.T) {
	t.Parallel()
	for _, instanceID := range []string{"epoch-control-0", strings.Repeat("a", maxControlOwnerBytes)} {
		t.Run(instanceID[:1], func(t *testing.T) {
			first, err := NewCatalogRegistry(&HTTPAuthority{}, instanceID)
			if err != nil {
				t.Fatal(err)
			}
			second, err := NewCatalogRegistry(&HTTPAuthority{}, instanceID)
			if err != nil {
				t.Fatal(err)
			}
			if first.ownerID == second.ownerID {
				t.Fatalf("two process incarnations share reconciliation owner %q", first.ownerID)
			}
			for _, registry := range []*CatalogRegistry{first, second} {
				if !validControlOwner(registry.ownerID) || !strings.Contains(registry.ownerID, "@") {
					t.Fatalf("process owner is not canonical and incarnation-bound: %q", registry.ownerID)
				}
				if registry.ownerID != registry.ControlOwnerID() {
					t.Fatal("reported owner does not match the lease identity")
				}
			}
		})
	}
}

func TestProcessControlOwnerIDIsBoundedCanonicalAndExact(t *testing.T) {
	t.Parallel()
	for _, instanceID := range []string{"epoch-control-0", strings.Repeat("a", maxControlOwnerBytes)} {
		ownerID, err := processControlOwnerID(instanceID, bytes.NewReader(bytes.Repeat([]byte{0xab}, controlOwnerNonceBytes)))
		if err != nil {
			t.Fatal(err)
		}
		prefix := instanceID[:min(len(instanceID), maxControlOwnerBytes-1-controlOwnerNonceBytes*2)]
		if ownerID != prefix+"@"+strings.Repeat("ab", controlOwnerNonceBytes) || !validControlOwner(ownerID) {
			t.Fatalf("unexpected process owner %q", ownerID)
		}
	}
}

func TestProcessControlOwnerIDFailsClosedWithoutEntropyOrValidLabel(t *testing.T) {
	t.Parallel()
	ownerID, err := processControlOwnerID("epoch-control-0", bytes.NewReader([]byte{1}))
	if ownerID != "" || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short entropy produced owner %q, error %v", ownerID, err)
	}
	for _, label := range []string{"", " invalid", "invalid label", "unicode-é", strings.Repeat("a", maxControlOwnerBytes+1)} {
		if owner, err := processControlOwnerID(label, bytes.NewReader(make([]byte, controlOwnerNonceBytes))); err == nil || owner != "" {
			t.Fatalf("invalid label %q produced owner %q, error %v", label, owner, err)
		}
	}
}

func TestProductionRegistryCannotAdoptItsPredecessorsLiveLease(t *testing.T) {
	t.Parallel()
	predecessor, err := NewCatalogRegistry(&HTTPAuthority{}, "epoch-control-0")
	if err != nil {
		t.Fatal(err)
	}
	var leasePosts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodGet && request.URL.Path == controlLeasePath {
			writeJSON(t, writer, controlLeaseDocument{OwnerID: predecessor.ControlOwnerID(), Fence: 3, ValidUntilMS: 20_000})
			return
		}
		if request.Method == http.MethodPost {
			leasePosts.Add(1)
		}
		writer.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	authority, err := NewHTTPAuthority([]string{server.URL}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := NewCatalogRegistry(authority, "epoch-control-0")
	if err != nil {
		t.Fatal(err)
	}
	replacement.now = func() time.Time { return time.UnixMilli(1_000) }
	_, err = replacement.ControlLease()
	assertStoreCode(t, err, resources.CodeUnavailable)
	if leasePosts.Load() != 0 || replacement.ownerID == predecessor.ownerID {
		t.Fatal("replacement challenged or reused its predecessor's live lease")
	}
}
