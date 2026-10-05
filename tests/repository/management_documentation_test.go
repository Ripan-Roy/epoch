package repository_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestManagementContractDocumentationMatchesGeneratedService(t *testing.T) {
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join("..", "..", path))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	contract := read("spec/proto/epoch/v1/regional_admin.proto")
	document := read("docs/API_CONTRACTS.md")
	server := read("control/internal/regional/grpc_service.go")
	sdk := read("sdk/go/epoch/management_unary.go") + read("sdk/go/epoch/management_watch.go")
	sdkDocument := read("docs/MANAGEMENT_SDK.md")
	methods := regexp.MustCompile(`(?m)^\s*rpc\s+(\w+)\(`).FindAllStringSubmatch(contract, -1)
	if len(methods) != 7 {
		t.Fatalf("review management documentation for %d generated methods", len(methods))
	}
	for _, method := range methods {
		name := method[1]
		if !strings.Contains(document, "`"+name+"`") {
			t.Errorf("management documentation omits generated method %s", name)
		}
		if !strings.Contains(server, "func (server *RegionalAdminServer) "+name+"(") {
			t.Errorf("generated management method %s lacks a Go implementation", name)
		}
		if !strings.Contains(sdk, "func (client *ManagementClient) "+name+"(") {
			t.Errorf("generated management method %s lacks a public Go SDK implementation", name)
		}
		if !strings.Contains(sdkDocument, "`"+name+"`") {
			t.Errorf("management SDK guide omits generated method %s", name)
		}
	}
	if strings.Contains(document, "serves the current four-method RegionalAdmin") {
		t.Error("management documentation still describes the superseded four-method scaffold")
	}
}

func TestManagementSDKDocsDisplayTheCompiledExampleWithExplicitLimits(t *testing.T) {
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join("..", "..", path))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	content := read("console/src/docs/content.ts")
	page := strings.Join(strings.Fields(read("console/src/docs/pages.tsx")), " ")
	registry := read("console/src/docs/registry.ts")
	if !strings.Contains(content, `../../../sdk/go/epoch/management_example_test.go?raw`) || !strings.Contains(content, "export const goManagementExample") {
		t.Error("management docs must use the exact example compiled by Go tests")
	}
	for _, marker := range []string{`id="management-sdk"`, "value={goManagementExample}", "MANAGEMENT_SDK.md", "Not a live Catalog quickstart", "Python and Java management clients remain open"} {
		if !strings.Contains(page, marker) {
			t.Errorf("management SDK page lacks %q", marker)
		}
	}
	if !strings.Contains(registry, `{ id: "management-sdk", label: "Management SDK" }`) {
		t.Error("management SDK topic is missing from the page navigation")
	}
}
