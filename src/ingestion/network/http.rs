//! HTTP listener for REST API ingestion
//!
//! Provides an HTTP/HTTPS server for data ingestion via REST endpoints.
//! Supports multiple protocols (Line Protocol, JSON, Protobuf) with
//! automatic content-type detection.
//!
//! # Endpoints
//!
//! - `POST /write` - Write data points (main ingestion endpoint)
//! - `POST /api/v2/write` - InfluxDB v2 compatible endpoint
//! - `GET /ping` - Health check endpoint
//! - `GET /health` - Detailed health status
//! - `GET /metrics` - Prometheus metrics
//!
//! # Example
//!
//! ```rust,no_run
//! use gorilla_tsdb::ingestion::network::HttpListener;
//! use std::net::SocketAddr;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let addr: SocketAddr = "0.0.0.0:8087".parse()?;
//! let listener = HttpListener::new(addr, None, Default::default()).await?;
//! listener.run().await?;
//! # Ok(())
//! # }
//! ```

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    extract::{ConnectInfo, DefaultBodyLimit, State},
    http::{header, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::broadcast;
use tokio_rustls::TlsAcceptor;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;
use tracing::{debug, error, info, warn};

use super::error::NetworkError;
use super::rate_limit::RateLimiter;
use super::tls::TlsConfig;
use crate::ingestion::protocol::{
    JsonParser, LineProtocolParser, ProtobufParser, Protocol, ProtocolParser,
};

/// HTTP listener configuration
#[derive(Debug, Clone)]
pub struct HttpConfig {
    /// Maximum request body size (default: 10 MB)
    pub max_body_size: usize,
    /// Request timeout (default: 30 seconds)
    pub request_timeout: Duration,
    /// Enable gzip decompression (default: true)
    pub enable_gzip: bool,
    /// Enable request logging (default: true)
    pub enable_logging: bool,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            max_body_size: 10 * 1024 * 1024, // 10 MB
            request_timeout: Duration::from_secs(30),
            enable_gzip: true,
            enable_logging: true,
        }
    }
}

impl HttpConfig {
    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.max_body_size == 0 {
            return Err("max_body_size must be > 0".to_string());
        }
        if self.request_timeout.is_zero() {
            return Err("request_timeout must be > 0".to_string());
        }
        Ok(())
    }
}

/// Shared state for HTTP handlers
#[derive(Clone)]
pub struct AppState {
    /// Rate limiter for traffic control
    rate_limiter: Arc<RateLimiter>,
    /// HTTP configuration
    config: HttpConfig,
    /// Line protocol parser instance
    line_parser: LineProtocolParser,
    /// JSON parser instance
    json_parser: JsonParser,
    /// Protobuf parser instance
    protobuf_parser: ProtobufParser,
}

/// HTTP listener for REST API ingestion
///
/// Implements an HTTP server using axum for handling data ingestion
/// via REST endpoints. Supports TLS for secure connections.
pub struct HttpListener {
    /// Bind address
    addr: SocketAddr,
    /// TLS acceptor (if TLS is enabled)
    tls_acceptor: Option<TlsAcceptor>,
    /// Application state shared across handlers
    state: AppState,
    /// Shutdown signal receiver
    shutdown_rx: Option<broadcast::Receiver<()>>,
}

impl HttpListener {
    /// Create a new HTTP listener
    ///
    /// # Arguments
    ///
    /// * `addr` - Socket address to bind to
    /// * `tls_config` - Optional TLS configuration
    /// * `config` - HTTP configuration
    ///
    /// # Errors
    ///
    /// Returns error if configuration is invalid or TLS setup fails
    pub async fn new(
        addr: SocketAddr,
        tls_config: Option<&TlsConfig>,
        config: HttpConfig,
    ) -> Result<Self, NetworkError> {
        config.validate().map_err(NetworkError::Config)?;

        // Build TLS acceptor if configured
        let tls_acceptor = match tls_config {
            Some(tls) => Some(tls.build_acceptor()?),
            None => None,
        };

        // Use rate limiter with background cleanup task to prevent unbounded memory growth
        let rate_limiter = RateLimiter::new_with_cleanup(Default::default());

        let state = AppState {
            rate_limiter,
            config,
            line_parser: LineProtocolParser::new(),
            json_parser: JsonParser::new(),
            protobuf_parser: ProtobufParser::new(),
        };

        Ok(Self {
            addr,
            tls_acceptor,
            state,
            shutdown_rx: None,
        })
    }

    /// Set the rate limiter
    pub fn with_rate_limiter(mut self, rate_limiter: Arc<RateLimiter>) -> Self {
        self.state.rate_limiter = rate_limiter;
        self
    }

    /// Set the shutdown receiver for graceful shutdown
    pub fn with_shutdown(mut self, shutdown_rx: broadcast::Receiver<()>) -> Self {
        self.shutdown_rx = Some(shutdown_rx);
        self
    }

    /// Run the HTTP listener
    ///
    /// Starts accepting connections and serving requests.
    /// Blocks until shutdown signal is received.
    ///
    /// If TLS is configured, connections are upgraded to TLS before processing.
    pub async fn run(self) -> Result<(), NetworkError> {
        let listener =
            TokioTcpListener::bind(self.addr)
                .await
                .map_err(|e| NetworkError::BindFailed {
                    addr: self.addr,
                    reason: e.to_string(),
                })?;

        let local_addr = listener.local_addr()?;
        let has_tls = self.tls_acceptor.is_some();
        info!(
            addr = %local_addr,
            tls = has_tls,
            "HTTP listener started"
        );

        // If TLS is configured, use manual accept loop with TLS handshake
        // Extract all fields before consuming self to avoid partial move
        let HttpListener {
            addr: _,
            tls_acceptor,
            state,
            shutdown_rx,
        } = self;

        let router = Self::build_router_from_state(state);

        if let Some(acceptor) = tls_acceptor {
            Self::run_with_tls_internal(listener, acceptor, router, shutdown_rx).await
        } else {
            Self::run_plaintext_internal(listener, router, shutdown_rx).await
        }
    }

    /// Build the router from state (static helper method)
    fn build_router_from_state(state: AppState) -> Router {
        let max_body_size = state.config.max_body_size;

        Router::new()
            // Ingestion endpoints
            .route("/write", post(handle_write))
            .route("/api/v2/write", post(handle_write))
            // Health endpoints
            .route("/ping", get(handle_ping))
            .route("/health", get(handle_health))
            .route("/ready", get(handle_ready))
            // Metrics endpoint (placeholder - full implementation in observability module)
            .route("/metrics", get(handle_metrics))
            // Apply middleware - DefaultBodyLimit BEFORE buffering to prevent memory exhaustion
            .layer(
                ServiceBuilder::new()
                    .layer(DefaultBodyLimit::max(max_body_size))
                    .layer(TraceLayer::new_for_http())
                    .layer(middleware::from_fn_with_state(
                        state.clone(),
                        rate_limit_middleware,
                    )),
            )
            .with_state(state)
    }

    /// Run HTTP listener without TLS (internal static method)
    ///
    /// This is a static method to avoid ownership issues with self
    async fn run_plaintext_internal(
        listener: TokioTcpListener,
        router: Router,
        shutdown_rx: Option<broadcast::Receiver<()>>,
    ) -> Result<(), NetworkError> {
        if let Some(mut shutdown_rx) = shutdown_rx {
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.recv().await;
                info!("HTTP listener shutting down");
            })
            .await
            .map_err(|e| NetworkError::Io(std::io::Error::other(e.to_string())))?;
        } else {
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .map_err(|e| NetworkError::Io(std::io::Error::other(e.to_string())))?;
        }

        Ok(())
    }

    /// Run HTTP listener with TLS (internal static method)
    ///
    /// This is a static method to avoid ownership issues with self.
    /// Uses manual accept loop with TLS handshake for each connection.
    async fn run_with_tls_internal(
        listener: TokioTcpListener,
        tls_acceptor: TlsAcceptor,
        router: Router,
        mut shutdown_rx: Option<broadcast::Receiver<()>>,
    ) -> Result<(), NetworkError> {
        loop {
            let accept_result = tokio::select! {
                result = listener.accept() => result,
                _ = async {
                    if let Some(ref mut rx) = shutdown_rx {
                        let _ = rx.recv().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    info!("HTTP listener shutting down");
                    break;
                }
            };

            let (stream, peer_addr) = match accept_result {
                Ok(conn) => conn,
                Err(e) => {
                    error!(error = %e, "Accept error");
                    continue;
                }
            };

            let tls = tls_acceptor.clone();
            let router = router.clone();

            // Spawn TLS connection handler for each incoming connection
            tokio::spawn(async move {
                match tls.accept(stream).await {
                    Ok(tls_stream) => {
                        let io = TokioIo::new(tls_stream);

                        // Create the service for this connection
                        // Note: ConnectInfo won't be available for TLS connections with this approach
                        // but we maintain TLS functionality
                        let service = router.into_service();

                        // Wrap tower service for hyper compatibility
                        let hyper_service = hyper_util::service::TowerToHyperService::new(service);

                        // Use hyper to serve the TLS connection
                        if let Err(e) = hyper_util::server::conn::auto::Builder::new(
                            hyper_util::rt::TokioExecutor::new(),
                        )
                        .serve_connection_with_upgrades(io, hyper_service)
                        .await
                        {
                            debug!(peer = %peer_addr, error = %e, "Connection error");
                        }
                    }
                    Err(e) => {
                        warn!(peer = %peer_addr, error = %e, "TLS handshake failed");
                    }
                }
            });
        }

        Ok(())
    }
}

/// Rate limiting middleware
///
/// Checks rate limits before processing requests.
/// Returns 429 Too Many Requests if limit is exceeded.
async fn rate_limit_middleware(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    request: Request<Body>,
    next: Next,
) -> Response {
    // Check rate limit for this IP
    if !state.rate_limiter.check_ip(addr.ip(), 1) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            "Rate limit exceeded. Please slow down.\n",
        )
            .into_response();
    }

    next.run(request).await
}

/// Write endpoint handler
///
/// Accepts data in multiple formats based on Content-Type header:
/// - text/plain: Line Protocol
/// - application/json: JSON format
/// - application/x-protobuf: Protocol Buffers
async fn handle_write(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    // Check body size
    if body.len() > state.config.max_body_size {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "Request body too large. Max size: {} bytes\n",
                state.config.max_body_size
            ),
        )
            .into_response();
    }

    // Detect protocol from Content-Type header
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("text/plain");

    let protocol = detect_protocol(content_type);

    // Parse the data
    let result = match protocol {
        Protocol::LineProtocol => state.line_parser.parse(&body),
        Protocol::Json => state.json_parser.parse(&body),
        Protocol::Protobuf => state.protobuf_parser.parse(&body),
    };

    match result {
        Ok(points) => {
            let count = points.len();
            debug!(protocol = %protocol, points = count, "Parsed data points");

            // TODO: Send points to ingestion pipeline
            // For now, just return success with count

            (StatusCode::NO_CONTENT, "").into_response()
        }
        Err(e) => {
            warn!(protocol = %protocol, error = %e, "Parse error");
            (StatusCode::BAD_REQUEST, format!("Parse error: {}\n", e)).into_response()
        }
    }
}

/// Detect protocol from Content-Type header
fn detect_protocol(content_type: &str) -> Protocol {
    let ct_lower = content_type.to_lowercase();

    if ct_lower.contains("application/json") {
        Protocol::Json
    } else if ct_lower.contains("application/x-protobuf")
        || ct_lower.contains("application/protobuf")
    {
        Protocol::Protobuf
    } else {
        // Default to line protocol for text/plain or unknown
        Protocol::LineProtocol
    }
}

/// Ping endpoint handler
///
/// Simple health check that returns 204 No Content
async fn handle_ping() -> Response {
    (StatusCode::NO_CONTENT, "").into_response()
}

/// Health endpoint handler
///
/// Returns detailed health status in JSON format
async fn handle_health() -> Response {
    let health = serde_json::json!({
        "status": "healthy",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_seconds": 0, // TODO: Track actual uptime
    });

    (StatusCode::OK, axum::Json(health)).into_response()
}

/// Ready endpoint handler
///
/// Returns 200 if the server is ready to accept traffic
async fn handle_ready() -> Response {
    // TODO: Check if all subsystems are ready
    (StatusCode::OK, "ready\n").into_response()
}

/// Metrics endpoint handler (placeholder)
///
/// Returns Prometheus-formatted metrics
async fn handle_metrics() -> Response {
    // Placeholder - full implementation in observability module
    let metrics = "# HELP gorilla_tsdb_up Indicates if the server is up\n\
                   # TYPE gorilla_tsdb_up gauge\n\
                   gorilla_tsdb_up 1\n";

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        metrics,
    )
        .into_response()
}

/// Write response for successful ingestion
#[derive(serde::Serialize)]
#[allow(dead_code)]
struct WriteResponse {
    /// Number of points written
    points_written: usize,
}

/// Error response for failed requests
#[derive(serde::Serialize)]
#[allow(dead_code)]
struct ErrorResponse {
    /// Error message
    error: String,
    /// Error code for programmatic handling
    code: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // HttpConfig Tests
    // =========================================================================

    #[test]
    fn test_http_config_default() {
        let config = HttpConfig::default();
        assert_eq!(config.max_body_size, 10 * 1024 * 1024);
        assert_eq!(config.request_timeout, Duration::from_secs(30));
        assert!(config.enable_gzip);
        assert!(config.enable_logging);
    }

    #[test]
    fn test_http_config_all_fields() {
        let config = HttpConfig {
            max_body_size: 5 * 1024 * 1024,
            request_timeout: Duration::from_secs(60),
            enable_gzip: false,
            enable_logging: false,
        };

        assert_eq!(config.max_body_size, 5 * 1024 * 1024);
        assert_eq!(config.request_timeout, Duration::from_secs(60));
        assert!(!config.enable_gzip);
        assert!(!config.enable_logging);
    }

    #[test]
    fn test_http_config_clone() {
        let config1 = HttpConfig {
            max_body_size: 1024,
            request_timeout: Duration::from_secs(10),
            enable_gzip: true,
            enable_logging: false,
        };

        let config2 = config1.clone();
        assert_eq!(config2.max_body_size, config1.max_body_size);
        assert_eq!(config2.request_timeout, config1.request_timeout);
        assert_eq!(config2.enable_gzip, config1.enable_gzip);
        assert_eq!(config2.enable_logging, config1.enable_logging);
    }

    #[test]
    fn test_http_config_debug() {
        let config = HttpConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("HttpConfig"));
        assert!(debug_str.contains("max_body_size"));
        assert!(debug_str.contains("request_timeout"));
    }

    // =========================================================================
    // HttpConfig Validation Tests
    // =========================================================================

    #[test]
    fn test_http_config_validation() {
        let config = HttpConfig::default();
        assert!(config.validate().is_ok());

        let bad_config = HttpConfig {
            max_body_size: 0,
            ..Default::default()
        };
        assert!(bad_config.validate().is_err());

        let bad_config2 = HttpConfig {
            request_timeout: Duration::ZERO,
            ..Default::default()
        };
        assert!(bad_config2.validate().is_err());
    }

    #[test]
    fn test_http_config_validation_error_messages() {
        let bad_config = HttpConfig {
            max_body_size: 0,
            ..Default::default()
        };
        let err = bad_config.validate().unwrap_err();
        assert!(err.contains("max_body_size must be > 0"));

        let bad_config2 = HttpConfig {
            request_timeout: Duration::ZERO,
            ..Default::default()
        };
        let err2 = bad_config2.validate().unwrap_err();
        assert!(err2.contains("request_timeout must be > 0"));
    }

    #[test]
    fn test_http_config_validation_edge_cases() {
        // Minimum valid values
        let min_config = HttpConfig {
            max_body_size: 1,
            request_timeout: Duration::from_nanos(1),
            enable_gzip: false,
            enable_logging: false,
        };
        assert!(min_config.validate().is_ok());

        // Large values
        let large_config = HttpConfig {
            max_body_size: usize::MAX,
            request_timeout: Duration::from_secs(3600),
            enable_gzip: true,
            enable_logging: true,
        };
        assert!(large_config.validate().is_ok());
    }

    // =========================================================================
    // Protocol Detection Tests
    // =========================================================================

    #[test]
    fn test_detect_protocol() {
        assert_eq!(detect_protocol("text/plain"), Protocol::LineProtocol);
        assert_eq!(
            detect_protocol("text/plain; charset=utf-8"),
            Protocol::LineProtocol
        );
        assert_eq!(detect_protocol("application/json"), Protocol::Json);
        assert_eq!(
            detect_protocol("application/json; charset=utf-8"),
            Protocol::Json
        );
        assert_eq!(
            detect_protocol("application/x-protobuf"),
            Protocol::Protobuf
        );
        assert_eq!(detect_protocol("application/protobuf"), Protocol::Protobuf);
        // Unknown defaults to line protocol
        assert_eq!(
            detect_protocol("application/octet-stream"),
            Protocol::LineProtocol
        );
    }

    #[test]
    fn test_detect_protocol_case_insensitive() {
        assert_eq!(detect_protocol("APPLICATION/JSON"), Protocol::Json);
        assert_eq!(detect_protocol("Application/Json"), Protocol::Json);
        assert_eq!(
            detect_protocol("APPLICATION/X-PROTOBUF"),
            Protocol::Protobuf
        );
        assert_eq!(detect_protocol("TEXT/PLAIN"), Protocol::LineProtocol);
    }

    #[test]
    fn test_detect_protocol_with_parameters() {
        assert_eq!(
            detect_protocol("application/json; charset=utf-8; boundary=something"),
            Protocol::Json
        );
        assert_eq!(
            detect_protocol("application/x-protobuf; version=1"),
            Protocol::Protobuf
        );
        assert_eq!(
            detect_protocol("text/plain; format=fixed"),
            Protocol::LineProtocol
        );
    }

    #[test]
    fn test_detect_protocol_empty_and_unknown() {
        // Empty string defaults to line protocol
        assert_eq!(detect_protocol(""), Protocol::LineProtocol);

        // Unknown content types default to line protocol
        assert_eq!(detect_protocol("video/mp4"), Protocol::LineProtocol);
        assert_eq!(detect_protocol("image/png"), Protocol::LineProtocol);
        assert_eq!(
            detect_protocol("multipart/form-data"),
            Protocol::LineProtocol
        );
    }

    #[test]
    fn test_detect_protocol_partial_matches() {
        // Note: detect_protocol uses contains() so these will also match
        // This documents the actual behavior (application/jsonl contains "application/json")
        assert_eq!(detect_protocol("application/jsonl"), Protocol::Json);

        // text/json doesn't contain "application/json" so it defaults to line protocol
        assert_eq!(detect_protocol("text/json"), Protocol::LineProtocol);

        // These should match
        assert_eq!(detect_protocol("application/json"), Protocol::Json);
        assert_eq!(
            detect_protocol("application/x-protobuf"),
            Protocol::Protobuf
        );
        assert_eq!(detect_protocol("application/protobuf"), Protocol::Protobuf);
    }

    // =========================================================================
    // HttpListener Tests
    // =========================================================================

    #[tokio::test]
    async fn test_http_listener_creation() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = HttpConfig::default();

        let listener = HttpListener::new(addr, None, config).await;
        assert!(listener.is_ok());
    }

    #[tokio::test]
    async fn test_http_listener_creation_with_custom_config() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = HttpConfig {
            max_body_size: 1024 * 1024, // 1 MB
            request_timeout: Duration::from_secs(60),
            enable_gzip: false,
            enable_logging: false,
        };

        let listener = HttpListener::new(addr, None, config).await;
        assert!(listener.is_ok());
    }

    #[tokio::test]
    async fn test_http_listener_creation_with_invalid_config() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = HttpConfig {
            max_body_size: 0, // Invalid
            ..Default::default()
        };

        let listener = HttpListener::new(addr, None, config).await;
        assert!(listener.is_err());
    }

    #[tokio::test]
    async fn test_http_listener_with_rate_limiter() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = HttpConfig::default();

        let listener = HttpListener::new(addr, None, config).await.unwrap();
        let rate_limiter = Arc::new(RateLimiter::new(Default::default()));

        let listener = listener.with_rate_limiter(rate_limiter);
        // Listener should still be valid after adding rate limiter
        assert!(listener.tls_acceptor.is_none());
    }

    #[tokio::test]
    async fn test_http_listener_with_shutdown() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = HttpConfig::default();

        let listener = HttpListener::new(addr, None, config).await.unwrap();
        let (tx, rx) = broadcast::channel(1);

        let listener = listener.with_shutdown(rx);
        assert!(listener.shutdown_rx.is_some());

        // Cleanup
        drop(tx);
    }

    // =========================================================================
    // Response Structure Tests
    // =========================================================================

    #[test]
    fn test_write_response_serialization() {
        let response = WriteResponse {
            points_written: 100,
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("points_written"));
        assert!(json.contains("100"));
    }

    #[test]
    fn test_error_response_serialization() {
        let response = ErrorResponse {
            error: "Test error message".to_string(),
            code: "ERR_TEST".to_string(),
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("Test error message"));
        assert!(json.contains("ERR_TEST"));
    }

    // =========================================================================
    // AppState Tests
    // =========================================================================

    #[test]
    fn test_app_state_clone() {
        let rate_limiter = Arc::new(RateLimiter::new(Default::default()));
        let state = AppState {
            rate_limiter,
            config: HttpConfig::default(),
            line_parser: LineProtocolParser::new(),
            json_parser: JsonParser::new(),
            protobuf_parser: ProtobufParser::new(),
        };

        // AppState should be Clone
        let state2 = state.clone();
        assert_eq!(state2.config.max_body_size, state.config.max_body_size);
    }

    // =========================================================================
    // Address Parsing Tests
    // =========================================================================

    #[test]
    fn test_socket_addr_parsing_ipv4() {
        let addr: Result<SocketAddr, _> = "127.0.0.1:8080".parse();
        assert!(addr.is_ok());

        let addr: Result<SocketAddr, _> = "0.0.0.0:8080".parse();
        assert!(addr.is_ok());
    }

    #[test]
    fn test_socket_addr_parsing_ipv6() {
        let addr: Result<SocketAddr, _> = "[::1]:8080".parse();
        assert!(addr.is_ok());

        let addr: Result<SocketAddr, _> = "[::]:8080".parse();
        assert!(addr.is_ok());
    }

    #[test]
    fn test_socket_addr_parsing_invalid() {
        let addr: Result<SocketAddr, _> = "invalid".parse();
        assert!(addr.is_err());

        let addr: Result<SocketAddr, _> = "127.0.0.1".parse(); // Missing port
        assert!(addr.is_err());
    }
}
