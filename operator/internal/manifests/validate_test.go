package manifests

import (
	"strings"
	"testing"
)

const serviceManifest = `
apiVersion: v1
kind: Service
metadata:
  name: epoch
  namespace: epoch-system
spec:
  selector:
    app.kubernetes.io/name: epoch
  ports:
    - name: http
      port: 80
      targetPort: 8080
`

func TestValidateAcceptsStrictTypedDocuments(t *testing.T) {
	t.Parallel()
	count, err := Validate(strings.NewReader(serviceManifest))
	if err != nil {
		t.Fatalf("validate manifest: %v", err)
	}
	if count != 1 {
		t.Fatalf("document count = %d, want 1", count)
	}
}

func TestValidateRejectsUnknownFieldsAndDuplicateObjects(t *testing.T) {
	t.Parallel()
	unknown := strings.Replace(serviceManifest, "spec:\n", "spec:\n  unsupported: true\n", 1)
	if _, err := Validate(strings.NewReader(unknown)); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown typed fields must fail closed: %v", err)
	}

	if _, err := Validate(strings.NewReader(serviceManifest + "\n---\n" + serviceManifest)); err == nil || !strings.Contains(err.Error(), "duplicate object") {
		t.Fatalf("duplicate identities must fail closed: %v", err)
	}
}

func TestValidateBoundsInput(t *testing.T) {
	t.Parallel()
	if _, err := Validate(strings.NewReader(strings.Repeat("x", maxManifestBytes+1))); err == nil || !strings.Contains(err.Error(), "exceeds 4 MiB") {
		t.Fatalf("oversized render must fail closed: %v", err)
	}
}
