//! Bounded TLS 1.3 material loading and an Axum-compatible rustls listener.

use std::{
    fmt::{self, Formatter},
    io,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use axum::serve::Listener;
use rustls::{RootCertStore, ServerConfig, server::WebPkiClientVerifier, version::TLS13};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, server::TlsStream};

const MAX_TLS_FILE_BYTES: u64 = 4 * 1024 * 1024;
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const ACCEPT_ERROR_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerTlsFiles {
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub client_ca: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientTlsFiles {
    pub ca: PathBuf,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
}

#[derive(Debug, Error)]
pub enum TransportSecurityError {
    #[error("TLS file {path} is invalid: {reason}")]
    InvalidFile { path: PathBuf, reason: String },
    #[error("TLS certificate material is invalid: {0}")]
    InvalidCertificate(String),
    #[error("TLS private-key material is invalid: {0}")]
    InvalidPrivateKey(String),
    #[error("TLS trust bundle is invalid: {0}")]
    InvalidTrust(String),
    #[error("TLS configuration could not be built: {0}")]
    Configuration(String),
}

pub fn load_server_config(
    files: &ServerTlsFiles,
) -> Result<Arc<ServerConfig>, TransportSecurityError> {
    let certificates = load_certificates(&files.certificate)?;
    let private_key = load_private_key(&files.private_key)?;
    // Connector libraries intentionally bring their own Rustls providers. Do
    // not ask Rustls to infer a process default when both are compiled in: the
    // supported Epoch listener is pinned to ring just like its direct reqwest
    // client boundary.
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&[&TLS13])
        .map_err(|error| TransportSecurityError::Configuration(error.to_string()))?;
    let mut config = if let Some(client_ca) = &files.client_ca {
        let roots = load_trust_store(client_ca)?;
        let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
            .build()
            .map_err(|error| TransportSecurityError::Configuration(error.to_string()))?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certificates, private_key)
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(certificates, private_key)
    }
    .map_err(|error| TransportSecurityError::Configuration(error.to_string()))?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

pub fn configure_client_builder(
    builder: reqwest::ClientBuilder,
    files: &ClientTlsFiles,
) -> Result<reqwest::ClientBuilder, TransportSecurityError> {
    let ca_pem = read_bounded(&files.ca)?;
    let certificate_pem = read_bounded(&files.certificate)?;
    let private_key_pem = read_bounded(&files.private_key)?;
    // Parse independently before combining so a missing half cannot be hidden
    // by an unrelated PEM section.
    let _ = load_certificates(&files.certificate)?;
    let _ = load_private_key(&files.private_key)?;
    let roots = reqwest::Certificate::from_pem_bundle(&ca_pem)
        .map_err(|error| TransportSecurityError::InvalidTrust(error.to_string()))?;
    if roots.is_empty() {
        return Err(TransportSecurityError::InvalidTrust(
            "trust bundle contains no certificates".into(),
        ));
    }
    let mut identity_pem = certificate_pem;
    identity_pem.extend_from_slice(&private_key_pem);
    let identity = reqwest::Identity::from_pem(&identity_pem)
        .map_err(|error| TransportSecurityError::InvalidCertificate(error.to_string()))?;
    Ok(roots.into_iter().fold(
        builder
            .https_only(true)
            .min_tls_version(reqwest::tls::Version::TLS_1_3)
            .tls_built_in_root_certs(false)
            .identity(identity),
        reqwest::ClientBuilder::add_root_certificate,
    ))
}

fn load_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, TransportSecurityError> {
    let encoded = read_bounded(path)?;
    let certificates = CertificateDer::pem_slice_iter(&encoded)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| TransportSecurityError::InvalidCertificate(error.to_string()))?;
    if certificates.is_empty() {
        return Err(TransportSecurityError::InvalidCertificate(format!(
            "{} contains no certificates",
            path.display()
        )));
    }
    Ok(certificates)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, TransportSecurityError> {
    let encoded = read_bounded(path)?;
    let mut keys = PrivateKeyDer::pem_slice_iter(&encoded);
    let key = keys
        .next()
        .transpose()
        .map_err(|error| TransportSecurityError::InvalidPrivateKey(error.to_string()))?
        .ok_or_else(|| {
            TransportSecurityError::InvalidPrivateKey(format!(
                "{} contains no supported private key",
                path.display()
            ))
        })?;
    if keys.next().is_some() {
        return Err(TransportSecurityError::InvalidPrivateKey(format!(
            "{} contains more than one private key",
            path.display()
        )));
    }
    Ok(key)
}

fn load_trust_store(path: &Path) -> Result<RootCertStore, TransportSecurityError> {
    let certificates = load_certificates(path)?;
    let mut roots = RootCertStore::empty();
    let (added, rejected) = roots.add_parsable_certificates(certificates);
    if added == 0 || rejected != 0 {
        return Err(TransportSecurityError::InvalidTrust(format!(
            "{} contains {added} accepted and {rejected} rejected certificates",
            path.display()
        )));
    }
    Ok(roots)
}

fn read_bounded(path: &Path) -> Result<Vec<u8>, TransportSecurityError> {
    let metadata = std::fs::metadata(path).map_err(|error| invalid_file(path, error))?;
    if !metadata.is_file() {
        return Err(TransportSecurityError::InvalidFile {
            path: path.to_path_buf(),
            reason: "not a regular file".into(),
        });
    }
    if metadata.len() == 0 || metadata.len() > MAX_TLS_FILE_BYTES {
        return Err(TransportSecurityError::InvalidFile {
            path: path.to_path_buf(),
            reason: format!("size must be between 1 and {MAX_TLS_FILE_BYTES} bytes"),
        });
    }
    std::fs::read(path).map_err(|error| invalid_file(path, error))
}

fn invalid_file(path: &Path, error: impl fmt::Display) -> TransportSecurityError {
    TransportSecurityError::InvalidFile {
        path: path.to_path_buf(),
        reason: error.to_string(),
    }
}

pub struct TlsListener {
    inner: TcpListener,
    acceptor: TlsAcceptor,
}

impl fmt::Debug for TlsListener {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TlsListener")
            .field("local_addr", &self.inner.local_addr())
            .finish_non_exhaustive()
    }
}

impl TlsListener {
    pub fn new(inner: TcpListener, config: Arc<ServerConfig>) -> Self {
        Self {
            inner,
            acceptor: TlsAcceptor::from(config),
        }
    }
}

impl Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let (stream, address) = match self.inner.accept().await {
                Ok(accepted) => accepted,
                Err(error) => {
                    tracing::error!(%error, "TLS listener could not accept a TCP connection");
                    tokio::time::sleep(ACCEPT_ERROR_DELAY).await;
                    continue;
                }
            };
            match tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, self.acceptor.accept(stream)).await {
                Ok(Ok(secured)) => return (secured, address),
                Ok(Err(error)) => {
                    tracing::warn!(%address, %error, "rejected TLS connection");
                }
                Err(_) => {
                    tracing::warn!(%address, "TLS handshake timed out");
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use axum::{Router, http::StatusCode, routing::get};
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa,
        KeyPair, KeyUsagePurpose,
    };
    use tempfile::{NamedTempFile, TempDir};

    use super::*;

    #[test]
    fn missing_and_oversized_tls_files_fail_closed() {
        let missing = PathBuf::from("definitely-missing-epoch-tls-file");
        let error = load_server_config(&ServerTlsFiles {
            certificate: missing.clone(),
            private_key: missing,
            client_ca: None,
        })
        .unwrap_err();
        assert!(matches!(error, TransportSecurityError::InvalidFile { .. }));

        let mut oversized = NamedTempFile::new().unwrap();
        oversized
            .as_file_mut()
            .set_len(MAX_TLS_FILE_BYTES + 1)
            .unwrap();
        let error = read_bounded(oversized.path()).unwrap_err();
        assert!(error.to_string().contains("size must be between"));
    }

    #[test]
    fn malformed_identity_and_trust_material_are_rejected() {
        let mut malformed = NamedTempFile::new().unwrap();
        malformed.write_all(b"not a PEM document").unwrap();
        let error = load_server_config(&ServerTlsFiles {
            certificate: malformed.path().to_path_buf(),
            private_key: malformed.path().to_path_buf(),
            client_ca: None,
        })
        .unwrap_err();
        assert!(matches!(
            error,
            TransportSecurityError::InvalidCertificate(_)
        ));
    }

    #[tokio::test]
    async fn mtls_listener_accepts_trusted_identity_and_rejects_anonymous_client() {
        let material = generate_test_tls_material();
        let directory = TempDir::new().unwrap();
        let ca = write_fixture(&directory, "ca.crt", &material.ca_certificate);
        let server_certificate =
            write_fixture(&directory, "server.crt", &material.server_certificate);
        let server_key = write_fixture(&directory, "server.key", &material.server_key);
        let client_certificate =
            write_fixture(&directory, "client.crt", &material.client_certificate);
        let client_key = write_fixture(&directory, "client.key", &material.client_key);
        let config = load_server_config(&ServerTlsFiles {
            certificate: server_certificate,
            private_key: server_key,
            client_ca: Some(ca.clone()),
        })
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                TlsListener::new(listener, config),
                Router::new().route("/healthz", get(|| async { StatusCode::NO_CONTENT })),
            )
            .await
            .unwrap();
        });
        let endpoint = format!("https://{address}/healthz");

        let anonymous = reqwest::Client::builder()
            .https_only(true)
            .tls_built_in_root_certs(false)
            .add_root_certificate(
                reqwest::Certificate::from_pem(material.ca_certificate.as_bytes()).unwrap(),
            )
            .build()
            .unwrap();
        assert!(anonymous.get(&endpoint).send().await.is_err());

        let authenticated = configure_client_builder(
            reqwest::Client::builder(),
            &ClientTlsFiles {
                ca,
                certificate: client_certificate,
                private_key: client_key,
            },
        )
        .unwrap()
        .build()
        .unwrap();
        assert_eq!(
            authenticated.get(endpoint).send().await.unwrap().status(),
            StatusCode::NO_CONTENT
        );
        server.abort();
        let _ = server.await;
    }

    struct TestTlsMaterial {
        ca_certificate: String,
        server_certificate: String,
        server_key: String,
        client_certificate: String,
        client_key: String,
    }

    fn generate_test_tls_material() -> TestTlsMaterial {
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let ca = CertifiedIssuer::self_signed(ca_params, KeyPair::generate().unwrap()).unwrap();

        let mut server_params =
            CertificateParams::new(vec!["localhost".to_owned(), "127.0.0.1".to_owned()]).unwrap();
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_key = KeyPair::generate().unwrap();
        let server_certificate = server_params.signed_by(&server_key, &ca).unwrap();

        let mut client_params =
            CertificateParams::new(vec!["epoch-test-client".to_owned()]).unwrap();
        client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client_key = KeyPair::generate().unwrap();
        let client_certificate = client_params.signed_by(&client_key, &ca).unwrap();

        TestTlsMaterial {
            ca_certificate: ca.pem(),
            server_certificate: server_certificate.pem(),
            server_key: server_key.serialize_pem(),
            client_certificate: client_certificate.pem(),
            client_key: client_key.serialize_pem(),
        }
    }

    fn write_fixture(directory: &TempDir, name: &str, contents: &str) -> PathBuf {
        let path = directory.path().join(name);
        std::fs::write(&path, contents).unwrap();
        path
    }
}
