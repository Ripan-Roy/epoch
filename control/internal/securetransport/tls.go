// Package securetransport loads bounded TLS material for Epoch's Go control
// plane and its regional data-plane client.
package securetransport

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
)

const (
	// MinVersion is the only supported minimum for the beta deployment path.
	MinVersion = tls.VersionTLS13
	// RequireVerifiedClientCertificate documents the server's workload-identity policy.
	RequireVerifiedClientCertificate = tls.RequireAndVerifyClientCert
	maxTLSFileBytes                  = 4 << 20
)

// ServerOptions names the server identity and optional client trust bundle.
type ServerOptions struct {
	Required        bool
	CertificatePath string
	PrivateKeyPath  string
	ClientCAPath    string
}

// ClientOptions names the server trust bundle and optional workload identity.
type ClientOptions struct {
	Required        bool
	CAPath          string
	CertificatePath string
	PrivateKeyPath  string
	ServerName      string
}

// LoadServerTLS returns nil only for an explicitly optional plaintext server.
func LoadServerTLS(options ServerOptions) (*tls.Config, error) {
	options.CertificatePath = strings.TrimSpace(options.CertificatePath)
	options.PrivateKeyPath = strings.TrimSpace(options.PrivateKeyPath)
	options.ClientCAPath = strings.TrimSpace(options.ClientCAPath)
	configured := options.CertificatePath != "" || options.PrivateKeyPath != "" || options.ClientCAPath != ""
	if !configured {
		if options.Required {
			return nil, fmt.Errorf("secure transport: server certificate and private key are required")
		}
		return nil, nil
	}
	if options.CertificatePath == "" || options.PrivateKeyPath == "" {
		return nil, fmt.Errorf("secure transport: server certificate and private key must be configured together")
	}
	certificate, err := loadKeyPair(options.CertificatePath, options.PrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("secure transport: load server identity: %w", err)
	}
	config := &tls.Config{
		MinVersion:   MinVersion,
		Certificates: []tls.Certificate{certificate},
		NextProtos:   []string{"h2", "http/1.1"},
	}
	if options.ClientCAPath != "" {
		clientCAs, err := loadCertPool(options.ClientCAPath)
		if err != nil {
			return nil, fmt.Errorf("secure transport: load client trust bundle: %w", err)
		}
		config.ClientCAs = clientCAs
		config.ClientAuth = RequireVerifiedClientCertificate
	}
	return config, nil
}

// LoadClientTLS returns nil only when TLS is optional and no TLS input exists.
func LoadClientTLS(options ClientOptions) (*tls.Config, error) {
	options.CAPath = strings.TrimSpace(options.CAPath)
	options.CertificatePath = strings.TrimSpace(options.CertificatePath)
	options.PrivateKeyPath = strings.TrimSpace(options.PrivateKeyPath)
	options.ServerName = strings.TrimSpace(options.ServerName)
	configured := options.CAPath != "" || options.CertificatePath != "" || options.PrivateKeyPath != "" || options.ServerName != ""
	if !configured {
		if options.Required {
			return nil, fmt.Errorf("secure transport: a server trust bundle is required")
		}
		return nil, nil
	}
	if options.Required && options.CAPath == "" {
		return nil, fmt.Errorf("secure transport: a server trust bundle is required")
	}
	if (options.CertificatePath == "") != (options.PrivateKeyPath == "") {
		return nil, fmt.Errorf("secure transport: client certificate and private key must be configured together")
	}
	config := &tls.Config{
		MinVersion: MinVersion,
		ServerName: options.ServerName,
		NextProtos: []string{"h2", "http/1.1"},
	}
	if options.CAPath != "" {
		roots, err := loadCertPool(options.CAPath)
		if err != nil {
			return nil, fmt.Errorf("secure transport: load server trust bundle: %w", err)
		}
		config.RootCAs = roots
	}
	if options.CertificatePath != "" {
		certificate, err := loadKeyPair(options.CertificatePath, options.PrivateKeyPath)
		if err != nil {
			return nil, fmt.Errorf("secure transport: load client identity: %w", err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return config, nil
}

func loadKeyPair(certificatePath, privateKeyPath string) (tls.Certificate, error) {
	if err := validateTLSFile(certificatePath); err != nil {
		return tls.Certificate{}, err
	}
	if err := validateTLSFile(privateKeyPath); err != nil {
		return tls.Certificate{}, err
	}
	return tls.LoadX509KeyPair(certificatePath, privateKeyPath)
}

func loadCertPool(path string) (*x509.CertPool, error) {
	if err := validateTLSFile(path); err != nil {
		return nil, err
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(payload) {
		return nil, fmt.Errorf("%s does not contain a PEM certificate", path)
	}
	return pool, nil
}

func validateTLSFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", path)
	}
	if info.Size() <= 0 || info.Size() > maxTLSFileBytes {
		return fmt.Errorf("%s must contain between 1 and %d bytes", path, maxTLSFileBytes)
	}
	return nil
}
