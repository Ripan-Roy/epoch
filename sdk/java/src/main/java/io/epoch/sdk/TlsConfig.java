package io.epoch.sdk;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Collection;
import java.util.Objects;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/** Explicit CA trust and optional PKCS#12 client identity for an Epoch HTTPS endpoint. */
public record TlsConfig(Path rootCa, Path clientKeyStore, char[] clientKeyStorePassword) {
  public TlsConfig {
    Objects.requireNonNull(rootCa, "rootCa");
    if ((clientKeyStore == null) != (clientKeyStorePassword == null)) {
      throw new IllegalArgumentException(
          "clientKeyStore and clientKeyStorePassword must be configured together");
    }
    clientKeyStorePassword = clientKeyStorePassword == null ? null : clientKeyStorePassword.clone();
  }

  public TlsConfig(Path rootCa) {
    this(rootCa, null, null);
  }

  @Override
  public char[] clientKeyStorePassword() {
    return clientKeyStorePassword == null ? null : clientKeyStorePassword.clone();
  }

  SSLContext sslContext() throws IOException {
    try {
      KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
      trustStore.load(null);
      Collection<? extends Certificate> certificates;
      try (InputStream input = Files.newInputStream(rootCa)) {
        certificates = CertificateFactory.getInstance("X.509").generateCertificates(input);
      }
      if (certificates.isEmpty()) {
        throw new IOException("TLS root CA contains no certificates");
      }
      int index = 0;
      for (Certificate certificate : certificates) {
        trustStore.setCertificateEntry("epoch-root-" + index++, certificate);
      }
      TrustManagerFactory trustManagers =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trustManagers.init(trustStore);

      KeyManager[] keyManagers = null;
      if (clientKeyStore != null) {
        KeyStore identity = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(clientKeyStore)) {
          identity.load(input, clientKeyStorePassword);
        }
        KeyManagerFactory factory =
            KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        factory.init(identity, clientKeyStorePassword);
        keyManagers = factory.getKeyManagers();
      }
      SSLContext context = SSLContext.getInstance("TLSv1.3");
      context.init(keyManagers, trustManagers.getTrustManagers(), null);
      return context;
    } catch (GeneralSecurityException error) {
      throw new IOException("could not build Epoch TLS context", error);
    }
  }
}
