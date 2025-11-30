//! TLS configuration and utilities
//!
//! Provides TLS support using rustls for secure connections.
//! Supports both server-side TLS and mutual TLS (mTLS) for client authentication.

use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::danger::ClientCertVerifier;
use rustls::server::WebPkiClientVerifier;
use rustls::RootCertStore;
use rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

use super::error::NetworkError;

/// Minimum TLS protocol version
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TlsVersion {
    /// TLS 1.2 (legacy support)
    Tls12,
    /// TLS 1.3 (recommended, default)
    #[default]
    Tls13,
}

/// TLS configuration for secure connections
///
/// # Example
///
/// ```rust,no_run
/// use gorilla_tsdb::ingestion::network::TlsConfig;
/// use std::path::PathBuf;
///
/// let tls = TlsConfig {
///     cert_path: PathBuf::from("/path/to/cert.pem"),
///     key_path: PathBuf::from("/path/to/key.pem"),
///     client_ca_path: None, // No mTLS
///     min_version: Default::default(),
///     require_client_cert: false,
/// };
/// ```
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Path to server certificate file (PEM format)
    /// Can contain certificate chain (server cert first, then intermediates)
    pub cert_path: PathBuf,

    /// Path to server private key file (PEM format)
    /// Supports RSA, ECDSA, and Ed25519 keys
    pub key_path: PathBuf,

    /// Path to CA certificate for client verification (mTLS)
    /// If set, enables client certificate verification
    pub client_ca_path: Option<PathBuf>,

    /// Minimum TLS protocol version (default: TLS 1.3)
    pub min_version: TlsVersion,

    /// Require valid client certificate (only applies when client_ca_path is set)
    /// If false, client certs are optional but validated if provided
    pub require_client_cert: bool,
}

impl TlsConfig {
    /// Create a new TLS configuration with default settings
    ///
    /// # Arguments
    ///
    /// * `cert_path` - Path to server certificate
    /// * `key_path` - Path to server private key
    pub fn new(cert_path: PathBuf, key_path: PathBuf) -> Self {
        Self {
            cert_path,
            key_path,
            client_ca_path: None,
            min_version: TlsVersion::default(),
            require_client_cert: false,
        }
    }

    /// Enable mutual TLS (mTLS) with client certificate verification
    ///
    /// # Arguments
    ///
    /// * `ca_path` - Path to CA certificate for verifying client certs
    /// * `require` - Whether to require client certificates
    pub fn with_mtls(mut self, ca_path: PathBuf, require: bool) -> Self {
        self.client_ca_path = Some(ca_path);
        self.require_client_cert = require;
        self
    }

    /// Set minimum TLS version
    pub fn with_min_version(mut self, version: TlsVersion) -> Self {
        self.min_version = version;
        self
    }

    /// Validate the TLS configuration
    pub fn validate(&self) -> Result<(), String> {
        // Check certificate file exists and is readable
        if !self.cert_path.exists() {
            return Err(format!(
                "Certificate file not found: {}",
                self.cert_path.display()
            ));
        }

        // Check key file exists and is readable
        if !self.key_path.exists() {
            return Err(format!(
                "Private key file not found: {}",
                self.key_path.display()
            ));
        }

        // Check CA file if mTLS is configured
        if let Some(ref ca_path) = self.client_ca_path {
            if !ca_path.exists() {
                return Err(format!(
                    "CA certificate file not found: {}",
                    ca_path.display()
                ));
            }
        }

        Ok(())
    }

    /// Build a rustls ServerConfig from this configuration
    pub fn build_server_config(&self) -> Result<ServerConfig, NetworkError> {
        // Load certificate chain
        let certs = self.load_certs()?;

        // Load private key
        let key = self.load_private_key()?;

        // Build server config based on mTLS settings
        let config = if let Some(ref ca_path) = self.client_ca_path {
            // mTLS enabled - configure client verification
            let client_verifier = self.build_client_verifier(ca_path)?;
            ServerConfig::builder()
                .with_client_cert_verifier(client_verifier)
                .with_single_cert(certs, key)
                .map_err(|e| NetworkError::TlsConfig(e.to_string()))?
        } else {
            // No mTLS - no client verification
            ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|e| NetworkError::TlsConfig(e.to_string()))?
        };

        Ok(config)
    }

    /// Create a TLS acceptor from this configuration
    pub fn build_acceptor(&self) -> Result<TlsAcceptor, NetworkError> {
        let config = self.build_server_config()?;
        Ok(TlsAcceptor::from(Arc::new(config)))
    }

    /// Load certificate chain from PEM file
    fn load_certs(&self) -> Result<Vec<CertificateDer<'static>>, NetworkError> {
        let cert_file = File::open(&self.cert_path).map_err(|e| {
            NetworkError::Certificate(format!(
                "Failed to open certificate file {}: {}",
                self.cert_path.display(),
                e
            ))
        })?;

        let mut reader = BufReader::new(cert_file);
        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                NetworkError::Certificate(format!("Failed to parse certificates: {}", e))
            })?;

        if certs.is_empty() {
            return Err(NetworkError::Certificate(
                "No certificates found in file".to_string(),
            ));
        }

        Ok(certs)
    }

    /// Load private key from PEM file
    fn load_private_key(&self) -> Result<PrivateKeyDer<'static>, NetworkError> {
        let key_file = File::open(&self.key_path).map_err(|e| {
            NetworkError::PrivateKey(format!(
                "Failed to open private key file {}: {}",
                self.key_path.display(),
                e
            ))
        })?;

        let mut reader = BufReader::new(key_file);

        // Try to read any private key format (RSA, PKCS8, EC)
        let key = rustls_pemfile::private_key(&mut reader)
            .map_err(|e| NetworkError::PrivateKey(format!("Failed to parse private key: {}", e)))?
            .ok_or_else(|| NetworkError::PrivateKey("No private key found in file".to_string()))?;

        Ok(key)
    }

    /// Build client certificate verifier for mTLS
    fn build_client_verifier(
        &self,
        ca_path: &PathBuf,
    ) -> Result<Arc<dyn ClientCertVerifier>, NetworkError> {
        let ca_file = File::open(ca_path).map_err(|e| {
            NetworkError::Certificate(format!(
                "Failed to open CA certificate file {}: {}",
                ca_path.display(),
                e
            ))
        })?;

        let mut reader = BufReader::new(ca_file);
        let ca_certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                NetworkError::Certificate(format!("Failed to parse CA certificates: {}", e))
            })?;

        // Build root cert store
        let mut root_store = RootCertStore::empty();
        for cert in ca_certs {
            root_store.add(cert).map_err(|e| {
                NetworkError::Certificate(format!("Failed to add CA certificate: {}", e))
            })?;
        }

        // Build verifier based on whether client certs are required
        let verifier = if self.require_client_cert {
            WebPkiClientVerifier::builder(Arc::new(root_store))
                .build()
                .map_err(|e| NetworkError::TlsConfig(format!("Failed to build verifier: {}", e)))?
        } else {
            WebPkiClientVerifier::builder(Arc::new(root_store))
                .allow_unauthenticated()
                .build()
                .map_err(|e| NetworkError::TlsConfig(format!("Failed to build verifier: {}", e)))?
        };

        Ok(verifier)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // TlsVersion Tests
    // =========================================================================

    #[test]
    fn test_tls_version_default() {
        assert_eq!(TlsVersion::default(), TlsVersion::Tls13);
    }

    #[test]
    fn test_tls_version_variants() {
        let v12 = TlsVersion::Tls12;
        let v13 = TlsVersion::Tls13;

        assert_ne!(v12, v13);
        assert_eq!(v12, TlsVersion::Tls12);
        assert_eq!(v13, TlsVersion::Tls13);
    }

    #[test]
    fn test_tls_version_clone() {
        let v1 = TlsVersion::Tls12;
        let v2 = v1;
        assert_eq!(v1, v2);
    }

    #[test]
    fn test_tls_version_debug() {
        let v = TlsVersion::Tls13;
        let debug_str = format!("{:?}", v);
        assert!(debug_str.contains("Tls13"));
    }

    // =========================================================================
    // TlsConfig Constructor Tests
    // =========================================================================

    #[test]
    fn test_tls_config_new() {
        let config = TlsConfig::new(PathBuf::from("cert.pem"), PathBuf::from("key.pem"));

        assert_eq!(config.cert_path, PathBuf::from("cert.pem"));
        assert_eq!(config.key_path, PathBuf::from("key.pem"));
        assert!(config.client_ca_path.is_none());
        assert_eq!(config.min_version, TlsVersion::Tls13);
        assert!(!config.require_client_cert);
    }

    #[test]
    fn test_tls_config_builder() {
        let config = TlsConfig::new(PathBuf::from("cert.pem"), PathBuf::from("key.pem"))
            .with_min_version(TlsVersion::Tls12);

        assert_eq!(config.min_version, TlsVersion::Tls12);
        assert!(config.client_ca_path.is_none());
    }

    #[test]
    fn test_tls_config_mtls() {
        let config = TlsConfig::new(PathBuf::from("cert.pem"), PathBuf::from("key.pem"))
            .with_mtls(PathBuf::from("ca.pem"), true);

        assert!(config.client_ca_path.is_some());
        assert_eq!(
            config.client_ca_path.as_ref().unwrap(),
            &PathBuf::from("ca.pem")
        );
        assert!(config.require_client_cert);
    }

    #[test]
    fn test_tls_config_mtls_optional_client_cert() {
        let config = TlsConfig::new(PathBuf::from("cert.pem"), PathBuf::from("key.pem"))
            .with_mtls(PathBuf::from("ca.pem"), false);

        assert!(config.client_ca_path.is_some());
        assert!(!config.require_client_cert);
    }

    #[test]
    fn test_tls_config_chained_builder() {
        let config = TlsConfig::new(PathBuf::from("cert.pem"), PathBuf::from("key.pem"))
            .with_min_version(TlsVersion::Tls12)
            .with_mtls(PathBuf::from("ca.pem"), true);

        assert_eq!(config.min_version, TlsVersion::Tls12);
        assert!(config.client_ca_path.is_some());
        assert!(config.require_client_cert);
    }

    #[test]
    fn test_tls_config_clone() {
        let config1 = TlsConfig::new(PathBuf::from("cert.pem"), PathBuf::from("key.pem"))
            .with_mtls(PathBuf::from("ca.pem"), true);

        let config2 = config1.clone();
        assert_eq!(config2.cert_path, config1.cert_path);
        assert_eq!(config2.key_path, config1.key_path);
        assert_eq!(config2.client_ca_path, config1.client_ca_path);
        assert_eq!(config2.min_version, config1.min_version);
        assert_eq!(config2.require_client_cert, config1.require_client_cert);
    }

    #[test]
    fn test_tls_config_debug() {
        let config = TlsConfig::new(PathBuf::from("cert.pem"), PathBuf::from("key.pem"));
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("TlsConfig"));
        assert!(debug_str.contains("cert_path"));
        assert!(debug_str.contains("key_path"));
    }

    // =========================================================================
    // TlsConfig Validation Tests
    // =========================================================================

    #[test]
    fn test_tls_config_validation_missing_cert() {
        let config = TlsConfig {
            cert_path: PathBuf::from("/nonexistent/cert.pem"),
            key_path: PathBuf::from("/nonexistent/key.pem"),
            client_ca_path: None,
            min_version: TlsVersion::Tls13,
            require_client_cert: false,
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_tls_config_validation_missing_key() {
        // Create a temp file for cert (simulating it exists)
        let temp_dir = std::env::temp_dir();
        let cert_path = temp_dir.join("test_cert.pem");
        std::fs::write(&cert_path, "dummy cert").unwrap();

        let config = TlsConfig {
            cert_path,
            key_path: PathBuf::from("/nonexistent/key.pem"),
            client_ca_path: None,
            min_version: TlsVersion::Tls13,
            require_client_cert: false,
        };

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Private key file not found"));

        // Cleanup
        std::fs::remove_file(&config.cert_path).ok();
    }

    #[test]
    fn test_tls_config_validation_missing_ca() {
        let temp_dir = std::env::temp_dir();
        let cert_path = temp_dir.join("test_cert2.pem");
        let key_path = temp_dir.join("test_key2.pem");
        std::fs::write(&cert_path, "dummy cert").unwrap();
        std::fs::write(&key_path, "dummy key").unwrap();

        let config = TlsConfig {
            cert_path: cert_path.clone(),
            key_path: key_path.clone(),
            client_ca_path: Some(PathBuf::from("/nonexistent/ca.pem")),
            min_version: TlsVersion::Tls13,
            require_client_cert: true,
        };

        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("CA certificate file not found"));

        // Cleanup
        std::fs::remove_file(&cert_path).ok();
        std::fs::remove_file(&key_path).ok();
    }

    #[test]
    fn test_tls_config_validation_all_files_exist() {
        let temp_dir = std::env::temp_dir();
        let cert_path = temp_dir.join("test_cert3.pem");
        let key_path = temp_dir.join("test_key3.pem");
        let ca_path = temp_dir.join("test_ca3.pem");
        std::fs::write(&cert_path, "dummy cert").unwrap();
        std::fs::write(&key_path, "dummy key").unwrap();
        std::fs::write(&ca_path, "dummy ca").unwrap();

        let config = TlsConfig {
            cert_path: cert_path.clone(),
            key_path: key_path.clone(),
            client_ca_path: Some(ca_path.clone()),
            min_version: TlsVersion::Tls13,
            require_client_cert: true,
        };

        // Validation should pass (files exist)
        assert!(config.validate().is_ok());

        // Cleanup
        std::fs::remove_file(&cert_path).ok();
        std::fs::remove_file(&key_path).ok();
        std::fs::remove_file(&ca_path).ok();
    }

    #[test]
    fn test_tls_config_validation_error_messages() {
        let config = TlsConfig::new(
            PathBuf::from("/nonexistent/cert.pem"),
            PathBuf::from("/nonexistent/key.pem"),
        );

        let err = config.validate().unwrap_err();
        assert!(err.contains("Certificate file not found"));
        assert!(err.contains("/nonexistent/cert.pem"));
    }

    // =========================================================================
    // Path Handling Tests
    // =========================================================================

    #[test]
    fn test_tls_config_paths_with_spaces() {
        let config = TlsConfig::new(
            PathBuf::from("/path/with spaces/cert.pem"),
            PathBuf::from("/another path/key.pem"),
        );

        assert_eq!(
            config.cert_path.to_str().unwrap(),
            "/path/with spaces/cert.pem"
        );
        assert_eq!(config.key_path.to_str().unwrap(), "/another path/key.pem");
    }

    #[test]
    fn test_tls_config_paths_unicode() {
        let config = TlsConfig::new(
            PathBuf::from("/путь/到/证书/cert.pem"),
            PathBuf::from("/密钥/key.pem"),
        );

        assert!(config.cert_path.to_str().is_some());
        assert!(config.key_path.to_str().is_some());
    }

    #[test]
    fn test_tls_config_relative_paths() {
        let config = TlsConfig::new(
            PathBuf::from("./certs/cert.pem"),
            PathBuf::from("../keys/key.pem"),
        );

        assert_eq!(config.cert_path.to_str().unwrap(), "./certs/cert.pem");
        assert_eq!(config.key_path.to_str().unwrap(), "../keys/key.pem");
    }

    // =========================================================================
    // Edge Case Tests
    // =========================================================================

    #[test]
    fn test_tls_config_empty_paths() {
        let config = TlsConfig::new(PathBuf::from(""), PathBuf::from(""));

        // Empty paths should fail validation
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_tls_config_with_min_version_preserves_other_fields() {
        let config = TlsConfig::new(PathBuf::from("cert.pem"), PathBuf::from("key.pem"))
            .with_mtls(PathBuf::from("ca.pem"), true)
            .with_min_version(TlsVersion::Tls12);

        // Ensure with_min_version doesn't affect other fields
        assert_eq!(config.cert_path, PathBuf::from("cert.pem"));
        assert_eq!(config.key_path, PathBuf::from("key.pem"));
        assert!(config.client_ca_path.is_some());
        assert!(config.require_client_cert);
        assert_eq!(config.min_version, TlsVersion::Tls12);
    }

    #[test]
    fn test_tls_config_mtls_overwrites_previous() {
        let config = TlsConfig::new(PathBuf::from("cert.pem"), PathBuf::from("key.pem"))
            .with_mtls(PathBuf::from("ca1.pem"), true)
            .with_mtls(PathBuf::from("ca2.pem"), false);

        // Second mtls call should overwrite the first
        assert_eq!(
            config.client_ca_path.as_ref().unwrap(),
            &PathBuf::from("ca2.pem")
        );
        assert!(!config.require_client_cert);
    }
}
