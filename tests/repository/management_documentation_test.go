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
	}
	if strings.Contains(document, "serves the current four-method RegionalAdmin") {
		t.Error("management documentation still describes the superseded four-method scaffold")
	}
}
