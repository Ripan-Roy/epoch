package epoch

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
)

// CI sets the interpreter explicitly after installing the pinned management
// extras. Ordinary Go-only SDK users need no Python installation. This fixture
// is generated-wire trust/transport evidence, not a Rust Catalog campaign.
func TestManagementPythonTLS13GeneratedWire(t *testing.T) {
	python := os.Getenv("EPOCH_PYTHON_MANAGEMENT_PROBE")
	if python == "" {
		t.Skip("set EPOCH_PYTHON_MANAGEMENT_PROBE to run the required cross-language CI probe")
	}
	material := generateMutualTLSMaterial(t)
	untrusted := generateMutualTLSMaterial(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(material.caPEM)
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13,
		Certificates: []tls.Certificate{material.serverIdentity},
		ClientAuth:   tls.RequireAndVerifyClientCert, ClientCAs: roots,
	})), grpc.UnaryInterceptor(func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if err := authorizeManagementWireContext(ctx); err != nil {
			return nil, err
		}
		return next(ctx, request)
	}))
	implementation := &managementWireServer{requests: make(chan proto.Message, 16), watchClosed: make(chan struct{})}
	epochv1.RegisterRegionalAdminServiceServer(server, implementation)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, python, filepath.Join(root, "sdk", "python", "tests", "management_tls_probe.py"),
		listener.Addr().String(), material.caPath, material.clientCertificatePath, material.clientPrivateKeyPath, untrusted.caPath)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "sdk", "python", "src"))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("Python management TLS 1.3 probe: %v\n%s", err, output)
	}
	select {
	case <-implementation.watchClosed:
	case <-time.After(time.Second):
		t.Fatal("Python Close did not cancel the remote TLS watch")
	}
	seen := map[string]bool{}
	for len(implementation.requests) != 0 {
		request := <-implementation.requests
		seen[string(request.ProtoReflect().Descriptor().Name())] = true
	}
	if len(seen) != 7 {
		t.Fatalf("Python probe did not execute all seven generated methods: %v", seen)
	}
}
