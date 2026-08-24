package epoch

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSecureHTTPTransportPerformsMutualTLSAndRejectsAnonymousClient(t *testing.T) {
	material := generateMutualTLSMaterial(t)
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(material.caPEM) {
		t.Fatal("test CA did not parse")
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))
	server.TLS = &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{material.serverIdentity},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    roots,
	}
	server.StartTLS()
	defer server.Close()

	secure, err := NewSecureHTTPTransport(server.URL, 2*time.Second, TLSConfig{
		RootCAPath:      material.caPath,
		CertificatePath: material.clientCertificatePath,
		PrivateKeyPath:  material.clientPrivateKeyPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := secure.Do(context.Background(), Request{Method: "GET", Path: "/healthz"}, nil); err != nil {
		t.Fatalf("trusted mTLS request failed: %v", err)
	}

	anonymous, err := NewSecureHTTPTransport(server.URL, 2*time.Second, TLSConfig{RootCAPath: material.caPath})
	if err != nil {
		t.Fatal(err)
	}
	if err := anonymous.Do(context.Background(), Request{Method: "GET", Path: "/healthz"}, nil); err == nil {
		t.Fatal("server accepted an anonymous TLS client")
	}
}

type mutualTLSMaterial struct {
	caPEM                 []byte
	caPath                string
	serverIdentity        tls.Certificate
	clientCertificatePath string
	clientPrivateKeyPath  string
}

func generateMutualTLSMaterial(t *testing.T) mutualTLSMaterial {
	t.Helper()
	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Epoch test CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})

	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	serverCertificate, serverPrivateKey := issueTestIdentity(t, serverTemplate, caTemplate, caKey)
	serverIdentity, err := tls.X509KeyPair(serverCertificate, serverPrivateKey)
	if err != nil {
		t.Fatal(err)
	}

	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "epoch-test-client"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientCertificate, clientPrivateKey := issueTestIdentity(t, clientTemplate, caTemplate, caKey)
	directory := t.TempDir()
	caPath := filepath.Join(directory, "ca.crt")
	clientCertificatePath := filepath.Join(directory, "client.crt")
	clientPrivateKeyPath := filepath.Join(directory, "client.key")
	for path, contents := range map[string][]byte{
		caPath: caPEM, clientCertificatePath: clientCertificate, clientPrivateKeyPath: clientPrivateKey,
	} {
		if err := os.WriteFile(path, contents, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return mutualTLSMaterial{
		caPEM:                 caPEM,
		caPath:                caPath,
		serverIdentity:        serverIdentity,
		clientCertificatePath: clientCertificatePath,
		clientPrivateKeyPath:  clientPrivateKeyPath,
	}
}

func issueTestIdentity(
	t *testing.T,
	template *x509.Certificate,
	caTemplate *x509.Certificate,
	caKey *ecdsa.PrivateKey,
) ([]byte, []byte) {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificateDER, err := x509.CreateCertificate(
		rand.Reader, template, caTemplate, &privateKey.PublicKey, caKey,
	)
	if err != nil {
		t.Fatal(err)
	}
	privateKeyDER, err := x509.MarshalECPrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDER}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privateKeyDER})
}

func TestHTTPTransportSendsJSONAndDecodesSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != "POST" || request.URL.Path != "/v1/streams/orders/records" {
			t.Errorf("unexpected request: %s %s", request.Method, request.URL.Path)
		}
		if request.URL.Query().Get("partition") != "1" {
			t.Errorf("unexpected query: %s", request.URL.RawQuery)
		}
		if request.Header.Get("User-Agent") != "epoch-go/0.2.0-beta.5" {
			t.Errorf("unexpected user agent: %s", request.Header.Get("User-Agent"))
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		if string(body) != `{"value":"ok"}` {
			t.Errorf("unexpected body: %s", body)
		}
		writer.Header().Set("content-type", "application/json")
		_, _ = writer.Write([]byte(`{"offset":7}`))
	}))
	defer server.Close()

	transport, err := NewHTTPTransport(server.URL, 2*time.Second)
	if err != nil {
		t.Fatalf("NewHTTPTransport returned an error: %v", err)
	}
	var result struct {
		Offset uint64 `json:"offset"`
	}
	err = transport.Do(context.Background(), Request{
		Method: "POST",
		Path:   "/v1/streams/orders/records",
		Query:  url.Values{"partition": {"1"}},
		Body:   Document{"value": "ok"},
	}, &result)
	if err != nil {
		t.Fatalf("Do returned an error: %v", err)
	}
	if result.Offset != 7 {
		t.Fatalf("unexpected response: %#v", result)
	}
}

func TestHTTPTransportAcceptsEmptySuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	transport, err := NewHTTPTransport(server.URL, 2*time.Second)
	if err != nil {
		t.Fatalf("NewHTTPTransport returned an error: %v", err)
	}
	var result Document
	if err := transport.Do(context.Background(), Request{Method: "DELETE", Path: "/resource"}, &result); err != nil {
		t.Fatalf("Do returned an error: %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil result, got %#v", result)
	}
}

func TestHTTPTransportReturnsStructuredAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("content-type", "application/json")
		writer.WriteHeader(http.StatusServiceUnavailable)
		_, _ = writer.Write([]byte(`{"error":{"code":"unavailable","detail":"try later"}}`))
	}))
	defer server.Close()

	transport, err := NewHTTPTransport(server.URL, 2*time.Second)
	if err != nil {
		t.Fatalf("NewHTTPTransport returned an error: %v", err)
	}
	err = transport.Do(context.Background(), Request{Method: "GET", Path: "/resource"}, nil)
	var apiError *APIError
	if !errors.As(err, &apiError) {
		t.Fatalf("expected APIError, got %T: %v", err, err)
	}
	if apiError.StatusCode != 503 || apiError.Code != "unavailable" || apiError.Detail != "try later" {
		t.Fatalf("unexpected API error: %#v", apiError)
	}
	if !apiError.Retryable() {
		t.Fatal("503 should be classified as retryable")
	}
	var decoded map[string]any
	if err := json.Unmarshal(apiError.Body, &decoded); err != nil {
		t.Fatalf("error body was not preserved: %v", err)
	}
}

func TestHTTPTransportPreservesNonJSONProxyError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusBadGateway)
		_, _ = writer.Write([]byte("upstream reset"))
	}))
	defer server.Close()

	transport, err := NewHTTPTransport(server.URL, 2*time.Second)
	if err != nil {
		t.Fatalf("NewHTTPTransport returned an error: %v", err)
	}
	err = transport.Do(context.Background(), Request{Method: "GET", Path: "/resource"}, nil)
	var apiError *APIError
	if !errors.As(err, &apiError) {
		t.Fatalf("expected APIError, got %T: %v", err, err)
	}
	if apiError.StatusCode != 502 || apiError.Code != "http_error" {
		t.Fatalf("unexpected API error: %#v", apiError)
	}
	if strings.TrimSpace(string(apiError.Body)) != "upstream reset" {
		t.Fatalf("proxy body was not preserved: %q", apiError.Body)
	}
}

func TestHTTPTransportRejectsInvalidSuccessJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte("not-json"))
	}))
	defer server.Close()

	transport, err := NewHTTPTransport(server.URL, 2*time.Second)
	if err != nil {
		t.Fatalf("NewHTTPTransport returned an error: %v", err)
	}
	var result Document
	err = transport.Do(context.Background(), Request{Method: "GET", Path: "/resource"}, &result)
	if err == nil || !strings.Contains(err.Error(), "invalid JSON") {
		t.Fatalf("expected invalid JSON error, got %v", err)
	}
}

func TestHTTPTransportDoesNotFollowRedirects(t *testing.T) {
	redirected := false
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirected = true
	}))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()

	transport, err := NewHTTPTransport(server.URL, 2*time.Second)
	if err != nil {
		t.Fatalf("NewHTTPTransport returned an error: %v", err)
	}
	err = transport.Do(context.Background(), Request{Method: "POST", Path: "/mutation", Body: Document{"value": 1}}, nil)
	var apiError *APIError
	if !errors.As(err, &apiError) || apiError.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("expected redirect APIError, got %T: %v", err, err)
	}
	if redirected {
		t.Fatal("transport followed a redirect")
	}
}
