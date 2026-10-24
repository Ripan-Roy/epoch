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

// Required in Java CI with an explicitly compiled classpath. Ordinary Go-only
// users do not need a JDK. This is real trust/wire evidence, not Rust Catalog HA.
func TestManagementJavaTLS13GeneratedWire(t *testing.T) {
	java := os.Getenv("EPOCH_JAVA_MANAGEMENT_PROBE")
	if java == "" {
		t.Skip("set EPOCH_JAVA_MANAGEMENT_PROBE to run the required cross-language CI probe")
	}
	classpath := os.Getenv("EPOCH_JAVA_MANAGEMENT_CLASSPATH")
	if classpath == "" {
		t.Fatal("Java probe requires EPOCH_JAVA_MANAGEMENT_CLASSPATH")
	}
	material := generateMutualTLSMaterial(t)
	untrusted := generateMutualTLSMaterial(t)
	identity := filepath.Join(t.TempDir(), "client.p12")
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	convert := exec.CommandContext(ctx, "openssl", "pkcs12", "-export",
		"-inkey", material.clientPrivateKeyPath, "-in", material.clientCertificatePath,
		"-out", identity, "-passout", "pass:epoch-wire-test")
	if output, err := convert.CombinedOutput(); err != nil {
		t.Fatalf("create test-owned Java PKCS12 identity: %v\n%s", err, output)
	}
	if err := os.Chmod(identity, 0o600); err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(material.caPEM) {
		t.Fatal("test CA is invalid")
	}
	serve := func(version uint16, authorize bool) (string, *managementWireServer) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		options := []grpc.ServerOption{grpc.Creds(credentials.NewTLS(&tls.Config{
			MinVersion: version, MaxVersion: version,
			Certificates: []tls.Certificate{material.serverIdentity},
			ClientAuth:   tls.RequireAndVerifyClientCert, ClientCAs: roots,
		}))}
		if authorize {
			options = append(options, grpc.UnaryInterceptor(func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
				if err := authorizeManagementWireContext(ctx); err != nil {
					return nil, err
				}
				return next(ctx, request)
			}))
		}
		server := grpc.NewServer(options...)
		implementation := &managementWireServer{requests: make(chan proto.Message, 16), watchClosed: make(chan struct{})}
		epochv1.RegisterRegionalAdminServiceServer(server, implementation)
		go func() { _ = server.Serve(listener) }()
		t.Cleanup(func() { server.Stop(); _ = listener.Close() })
		return listener.Addr().String(), implementation
	}
	endpoint, implementation := serve(tls.VersionTLS13, true)
	tls12, rejected := serve(tls.VersionTLS12, false)
	command := exec.CommandContext(ctx, java, "-cp", classpath, "io.epoch.sdk.ManagementTLSProbe",
		endpoint, material.caPath, identity, untrusted.caPath, tls12)
	command.Env = append(os.Environ(), "https_proxy=http://127.0.0.1:9", "http_proxy=http://127.0.0.1:9", "grpc_proxy=http://127.0.0.1:9")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("Java management TLS 1.3 probe: %v\n%s", err, output)
	}
	select {
	case <-implementation.watchClosed:
	case <-time.After(time.Second):
		t.Fatal("Java Close did not cancel the remote TLS watch")
	}
	if len(rejected.requests) != 0 {
		t.Fatal("Java dispatched application data to a TLS 1.2 server")
	}
	seen := map[string]bool{}
	for len(implementation.requests) != 0 {
		request := <-implementation.requests
		seen[string(request.ProtoReflect().Descriptor().Name())] = true
		switch typed := request.(type) {
		case *epochv1.ApplyResourceRequest:
			if typed.ExpectedGeneration == nil || *typed.ExpectedGeneration != 0 || typed.RequestToken != "java-wire-apply" {
				t.Fatal("Java lost apply token/OCC presence")
			}
		case *epochv1.DeleteResourceRequest:
			if typed.ExpectedGeneration == nil || *typed.ExpectedGeneration != 0 || typed.RequestToken != "java-wire-delete" {
				t.Fatal("Java lost delete token/OCC presence")
			}
		}
	}
	if len(seen) != 7 {
		t.Fatalf("Java probe did not execute all seven generated methods: %v", seen)
	}
}
