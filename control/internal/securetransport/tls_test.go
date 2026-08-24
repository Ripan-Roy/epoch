package securetransport

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRequiredServerTLSRejectsIncompleteMaterial(t *testing.T) {
	t.Parallel()
	if _, err := LoadServerTLS(ServerOptions{Required: true}); err == nil {
		t.Fatal("required TLS must reject absent certificate material")
	}
	if _, err := LoadServerTLS(ServerOptions{CertificatePath: "only-cert"}); err == nil {
		t.Fatal("a certificate without its key must be rejected")
	}
}

func TestMutualTLSAuthenticatesTrustedWorkloadsAndRejectsUnknownOnes(t *testing.T) {
	t.Parallel()
	fixture := newTLSFixture(t)
	serverTLS, err := LoadServerTLS(ServerOptions{
		Required:        true,
		CertificatePath: fixture.serverCertificate,
		PrivateKeyPath:  fixture.serverKey,
		ClientCAPath:    fixture.caCertificate,
	})
	if err != nil {
		t.Fatal(err)
	}
	if serverTLS.MinVersion != MinVersion || serverTLS.ClientAuth != RequireVerifiedClientCertificate {
		t.Fatalf("unexpected server policy: min=%d client_auth=%d", serverTLS.MinVersion, serverTLS.ClientAuth)
	}

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))
	server.TLS = serverTLS
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)

	trustedTLS, err := LoadClientTLS(ClientOptions{
		Required:        true,
		CAPath:          fixture.caCertificate,
		CertificatePath: fixture.clientCertificate,
		PrivateKeyPath:  fixture.clientKey,
		ServerName:      "localhost",
	})
	if err != nil {
		t.Fatal(err)
	}
	trusted := &http.Client{Transport: &http.Transport{TLSClientConfig: trustedTLS, ForceAttemptHTTP2: true}}
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := trusted.Do(request)
	if err != nil {
		t.Fatalf("trusted workload handshake failed: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", response.StatusCode)
	}

	unidentifiedTLS, err := LoadClientTLS(ClientOptions{
		Required:   true,
		CAPath:     fixture.caCertificate,
		ServerName: "localhost",
	})
	if err != nil {
		t.Fatal(err)
	}
	unidentified := &http.Client{Transport: &http.Transport{TLSClientConfig: unidentifiedTLS, ForceAttemptHTTP2: true}}
	request, err = http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unidentified.Do(request); err == nil {
		t.Fatal("server requiring workload identity accepted a client without a certificate")
	}
}

func TestClientTLSRejectsAmbiguousIdentityAndMissingTrust(t *testing.T) {
	t.Parallel()
	fixture := newTLSFixture(t)
	if _, err := LoadClientTLS(ClientOptions{Required: true}); err == nil {
		t.Fatal("required TLS must reject an absent trust bundle")
	}
	if _, err := LoadClientTLS(ClientOptions{
		CAPath:          fixture.caCertificate,
		CertificatePath: fixture.clientCertificate,
	}); err == nil {
		t.Fatal("a client certificate without its key must be rejected")
	}
}

type tlsFixture struct {
	caCertificate     string
	serverCertificate string
	serverKey         string
	clientCertificate string
	clientKey         string
}

func newTLSFixture(t *testing.T) tlsFixture {
	t.Helper()
	directory := t.TempDir()
	caKey := newPrivateKey(t)
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Epoch test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caPath := writePEM(t, directory, "ca.crt", "CERTIFICATE", caDER)

	serverCertificate, serverKey := issueCertificate(t, directory, caTemplate, caKey, 2, "server", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	clientCertificate, clientKey := issueCertificate(t, directory, caTemplate, caKey, 3, "client", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	return tlsFixture{
		caCertificate:     caPath,
		serverCertificate: serverCertificate,
		serverKey:         serverKey,
		clientCertificate: clientCertificate,
		clientKey:         clientKey,
	}
}

func issueCertificate(
	t *testing.T,
	directory string,
	ca *x509.Certificate,
	caKey *ecdsa.PrivateKey,
	serial int64,
	name string,
	usage []x509.ExtKeyUsage,
) (string, string) {
	t.Helper()
	key := newPrivateKey(t)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: "epoch-" + name},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  usage,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return writePEM(t, directory, name+".crt", "CERTIFICATE", der),
		writePEM(t, directory, name+".key", "PRIVATE KEY", keyDER)
}

func newPrivateKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func writePEM(t *testing.T, directory, name, blockType string, bytes []byte) string {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: bytes}), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
