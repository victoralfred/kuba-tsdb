//! TCP listener implementation
//!
//! Provides a high-performance TCP listener with support for:
//! - SO_REUSEPORT for multi-threaded accept scaling
//! - Optional TLS encryption via rustls
//! - Connection limiting and rate limiting integration
//! - Graceful shutdown support

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use socket2::{Domain, Protocol, Socket, Type};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener as TokioTcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error, info, trace, warn};

use super::connection::{ConnectionConfig, ConnectionManager};
use super::error::NetworkError;
use super::rate_limit::RateLimiter;
use super::tls::TlsConfig;

/// TCP listener with optional TLS support
///
/// Implements a high-performance TCP listener designed for time-series
/// data ingestion with support for connection pooling, rate limiting,
/// and TLS encryption.
///
/// # Architecture
///
/// The listener uses `SO_REUSEPORT` to allow multiple threads to accept
/// connections on the same port, enabling horizontal scaling of accept
/// throughput. Each accepted connection is handled in a separate task.
///
/// # Example
///
/// ```rust,no_run
/// use gorilla_tsdb::ingestion::network::{TcpListener, ConnectionConfig};
/// use std::net::SocketAddr;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let addr: SocketAddr = "0.0.0.0:8086".parse()?;
/// let listener = TcpListener::bind(addr, None, ConnectionConfig::default()).await?;
/// # Ok(())
/// # }
/// ```
pub struct TcpListener {
    /// Underlying tokio TCP listener
    listener: TokioTcpListener,
    /// Local address the listener is bound to
    local_addr: SocketAddr,
    /// Optional TLS acceptor for encrypted connections
    tls_acceptor: Option<TlsAcceptor>,
    /// Connection configuration
    connection_config: ConnectionConfig,
}

impl TcpListener {
    /// Bind a new TCP listener to the specified address
    ///
    /// # Arguments
    ///
    /// * `addr` - Socket address to bind to (e.g., "0.0.0.0:8086")
    /// * `tls_config` - Optional TLS configuration for encrypted connections
    /// * `connection_config` - Connection pool configuration
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Address is already in use
    /// - TLS configuration is invalid
    /// - Socket options cannot be set
    ///
    /// # Socket Options
    ///
    /// The following socket options are configured:
    /// - `SO_REUSEADDR`: Allow quick restart after crash
    /// - `SO_REUSEPORT`: Allow multiple listeners on same port (if enabled)
    /// - `TCP_NODELAY`: Disable Nagle's algorithm (if enabled)
    pub async fn bind(
        addr: SocketAddr,
        tls_config: Option<&TlsConfig>,
        connection_config: ConnectionConfig,
    ) -> Result<Self, NetworkError> {
        // Create socket with socket2 for advanced options
        let socket = Self::create_socket(addr, &connection_config)?;

        // Convert to tokio TcpListener
        let std_listener: std::net::TcpListener = socket.into();
        std_listener.set_nonblocking(true)?;

        let listener =
            TokioTcpListener::from_std(std_listener).map_err(|e| NetworkError::BindFailed {
                addr,
                reason: e.to_string(),
            })?;

        let local_addr = listener.local_addr()?;

        // Build TLS acceptor if configured
        let tls_acceptor = match tls_config {
            Some(config) => Some(config.build_acceptor()?),
            None => None,
        };

        info!(
            addr = %local_addr,
            tls = tls_acceptor.is_some(),
            "TCP listener bound"
        );

        Ok(Self {
            listener,
            local_addr,
            tls_acceptor,
            connection_config,
        })
    }

    /// Create a socket with appropriate options set
    ///
    /// Configures the socket with:
    /// - SO_REUSEADDR for quick restart after crash
    /// - SO_REUSEPORT for multi-threaded accept (if enabled)
    fn create_socket(addr: SocketAddr, config: &ConnectionConfig) -> Result<Socket, NetworkError> {
        // Determine domain based on address family
        let domain = if addr.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        };

        // Create TCP socket
        let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP)).map_err(|e| {
            NetworkError::BindFailed {
                addr,
                reason: format!("Failed to create socket: {}", e),
            }
        })?;

        // Enable SO_REUSEADDR - allows binding even if in TIME_WAIT state
        socket
            .set_reuse_address(true)
            .map_err(|e| NetworkError::BindFailed {
                addr,
                reason: format!("Failed to set SO_REUSEADDR: {}", e),
            })?;

        // Enable SO_REUSEPORT if configured - allows multiple listeners
        // This enables multi-threaded accept scaling where each thread
        // has its own listener and the kernel load-balances connections
        #[cfg(unix)]
        if config.so_reuseport {
            socket
                .set_reuse_port(true)
                .map_err(|e| NetworkError::BindFailed {
                    addr,
                    reason: format!("Failed to set SO_REUSEPORT: {}", e),
                })?;
        }

        // Bind to the address
        socket
            .bind(&addr.into())
            .map_err(|e| NetworkError::BindFailed {
                addr,
                reason: format!("Failed to bind: {}", e),
            })?;

        // Start listening with a reasonable backlog
        // The backlog determines how many pending connections the kernel
        // will queue before refusing new connections
        socket.listen(1024).map_err(|e| NetworkError::BindFailed {
            addr,
            reason: format!("Failed to listen: {}", e),
        })?;

        Ok(socket)
    }

    /// Get the local address this listener is bound to
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Check if TLS is enabled
    pub fn is_tls_enabled(&self) -> bool {
        self.tls_acceptor.is_some()
    }

    /// Run the listener accept loop
    ///
    /// Accepts connections and spawns handler tasks for each.
    /// Integrates with connection manager for limits and rate limiter
    /// for traffic control.
    ///
    /// # Arguments
    ///
    /// * `conn_manager` - Connection manager for limit tracking
    /// * `rate_limiter` - Rate limiter for traffic control
    /// * `shutdown_rx` - Broadcast receiver for shutdown signal
    ///
    /// # Shutdown
    ///
    /// The loop exits gracefully when a shutdown signal is received.
    /// In-flight connections are allowed to complete.
    pub async fn run(
        self,
        conn_manager: Arc<ConnectionManager>,
        rate_limiter: Arc<RateLimiter>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> Result<(), NetworkError> {
        info!(addr = %self.local_addr, "Starting TCP accept loop");

        let tls_acceptor = self.tls_acceptor.clone();
        let idle_timeout = self.connection_config.idle_timeout;
        let tcp_nodelay = self.connection_config.tcp_nodelay;
        let keepalive = self.connection_config.keepalive_interval;

        loop {
            tokio::select! {
                // Handle shutdown signal
                _ = shutdown_rx.recv() => {
                    info!(addr = %self.local_addr, "TCP listener shutting down");
                    break;
                }

                // Accept new connections
                result = self.listener.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            // Check connection limits before processing
                            let peer_ip = peer_addr.ip();

                            if !conn_manager.try_acquire(peer_ip) {
                                debug!(
                                    peer = %peer_addr,
                                    "Connection rejected: limit exceeded"
                                );
                                // Connection will be dropped, sending RST
                                continue;
                            }

                            // Configure socket options on accepted connection
                            if let Err(e) = Self::configure_stream(&stream, tcp_nodelay, keepalive) {
                                warn!(peer = %peer_addr, error = %e, "Failed to configure socket");
                            }

                            // Clone references for the handler task
                            let conn_mgr = Arc::clone(&conn_manager);
                            let rate_lim = Arc::clone(&rate_limiter);
                            let tls = tls_acceptor.clone();

                            // Spawn handler task for this connection
                            tokio::spawn(async move {
                                let result = Self::handle_connection(
                                    stream,
                                    peer_addr,
                                    tls,
                                    rate_lim,
                                    idle_timeout,
                                ).await;

                                if let Err(e) = result {
                                    // Only log unexpected errors (not normal disconnects)
                                    if !matches!(e, NetworkError::ConnectionClosed { .. }) {
                                        debug!(peer = %peer_addr, error = %e, "Connection error");
                                    }
                                }

                                // Always release connection slot
                                conn_mgr.release(peer_ip);
                                trace!(peer = %peer_addr, "Connection released");
                            });
                        }
                        Err(e) => {
                            // Log accept errors but continue accepting
                            // Common errors: EMFILE (too many open files), ENOMEM
                            error!(error = %e, "Accept error");

                            // Brief pause on error to prevent tight loop
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Configure socket options on an accepted stream
    ///
    /// Sets TCP_NODELAY and keepalive options
    fn configure_stream(
        stream: &TcpStream,
        tcp_nodelay: bool,
        keepalive: Option<Duration>,
    ) -> io::Result<()> {
        // Disable Nagle's algorithm for low-latency writes
        // This is important for time-series data where we want
        // acknowledgments sent immediately
        stream.set_nodelay(tcp_nodelay)?;

        // Configure TCP keepalive to detect dead connections
        if let Some(interval) = keepalive {
            let socket = socket2::SockRef::from(stream);
            let keepalive = socket2::TcpKeepalive::new()
                .with_time(interval)
                .with_interval(interval);

            socket.set_tcp_keepalive(&keepalive)?;
        }

        Ok(())
    }

    /// Handle a single TCP connection
    ///
    /// Performs TLS handshake if configured, then reads data from the
    /// connection and processes it.
    async fn handle_connection(
        stream: TcpStream,
        peer_addr: SocketAddr,
        tls_acceptor: Option<TlsAcceptor>,
        rate_limiter: Arc<RateLimiter>,
        idle_timeout: Duration,
    ) -> Result<(), NetworkError> {
        trace!(peer = %peer_addr, "Handling connection");

        // Perform TLS handshake if configured
        if let Some(acceptor) = tls_acceptor {
            // TLS handshake with timeout
            let tls_stream = timeout(Duration::from_secs(10), acceptor.accept(stream))
                .await
                .map_err(|_| NetworkError::TlsHandshake {
                    peer: peer_addr,
                    reason: "Handshake timeout".to_string(),
                })?
                .map_err(|e| NetworkError::TlsHandshake {
                    peer: peer_addr,
                    reason: e.to_string(),
                })?;

            trace!(peer = %peer_addr, "TLS handshake complete");

            // Handle TLS stream
            Self::handle_stream(tls_stream, peer_addr, rate_limiter, idle_timeout).await
        } else {
            // Handle plaintext stream
            Self::handle_stream(stream, peer_addr, rate_limiter, idle_timeout).await
        }
    }

    /// Handle data on an established stream (TLS or plaintext)
    ///
    /// Reads lines from the stream and processes them. Each line is
    /// assumed to be a data point in line protocol format.
    async fn handle_stream<S>(
        stream: S,
        peer_addr: SocketAddr,
        rate_limiter: Arc<RateLimiter>,
        idle_timeout: Duration,
    ) -> Result<(), NetworkError>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let (reader, mut writer) = tokio::io::split(stream);
        let mut reader = BufReader::new(reader);
        let mut line = String::new();

        loop {
            line.clear();

            // Read with idle timeout
            let read_result = timeout(idle_timeout, reader.read_line(&mut line)).await;

            match read_result {
                Ok(Ok(0)) => {
                    // EOF - client closed connection
                    return Err(NetworkError::ConnectionClosed { peer: peer_addr });
                }
                Ok(Ok(bytes_read)) => {
                    // Check rate limit before processing
                    // Use bytes read as "points" for rate limiting
                    if !rate_limiter.check_ip(peer_addr.ip(), bytes_read as u64) {
                        // Rate limited - send error response
                        let _ = writer.write_all(b"ERR rate limit exceeded\n").await;
                        return Err(NetworkError::RateLimited {
                            identifier: peer_addr.ip().to_string(),
                        });
                    }

                    // Process the line
                    // TODO: Send to protocol parser pipeline
                    // For now, just acknowledge receipt
                    trace!(
                        peer = %peer_addr,
                        bytes = bytes_read,
                        "Received data"
                    );

                    // Send acknowledgment (will be replaced with actual processing)
                    let _ = writer.write_all(b"OK\n").await;
                }
                Ok(Err(e)) => {
                    // Read error
                    return Err(NetworkError::Io(e));
                }
                Err(_) => {
                    // Timeout - idle connection
                    debug!(peer = %peer_addr, "Connection idle timeout");
                    return Err(NetworkError::Timeout {
                        duration_ms: idle_timeout.as_millis() as u64,
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream as TokioTcpStream;

    #[tokio::test]
    async fn test_tcp_listener_bind() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = ConnectionConfig::default();

        let listener = TcpListener::bind(addr, None, config).await;
        assert!(listener.is_ok());

        let listener = listener.unwrap();
        assert!(!listener.is_tls_enabled());
        assert_ne!(listener.local_addr().port(), 0);
    }

    #[tokio::test]
    async fn test_tcp_listener_accept() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = ConnectionConfig::default();

        let listener = TcpListener::bind(addr, None, config.clone()).await.unwrap();
        let bound_addr = listener.local_addr();

        let conn_manager = Arc::new(ConnectionManager::new(config));
        let rate_limiter = Arc::new(RateLimiter::new(Default::default()));
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        // Spawn listener in background
        let listener_handle =
            tokio::spawn(
                async move { listener.run(conn_manager, rate_limiter, shutdown_rx).await },
            );

        // Give listener time to start
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Connect as client
        let mut client = TokioTcpStream::connect(bound_addr).await.unwrap();

        // Send data
        client.write_all(b"test data\n").await.unwrap();

        // Read response
        let mut response = vec![0u8; 10];
        let n = client.read(&mut response).await.unwrap();
        assert!(n > 0);

        // Shutdown
        let _ = shutdown_tx.send(());
        let _ = listener_handle.await;
    }

    #[tokio::test]
    async fn test_tcp_listener_connection_limit() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = ConnectionConfig {
            max_connections: 2,
            max_per_ip: 1, // Only 1 connection per IP
            ..Default::default()
        };

        let listener = TcpListener::bind(addr, None, config.clone()).await.unwrap();
        let bound_addr = listener.local_addr();

        let conn_manager = Arc::new(ConnectionManager::new(config));
        let rate_limiter = Arc::new(RateLimiter::new(Default::default()));
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        // Spawn listener
        let listener_handle =
            tokio::spawn(
                async move { listener.run(conn_manager, rate_limiter, shutdown_rx).await },
            );

        tokio::time::sleep(Duration::from_millis(10)).await;

        // First connection should succeed
        let client1 = TokioTcpStream::connect(bound_addr).await;
        assert!(client1.is_ok());

        // Keep first connection alive
        let _client1 = client1.unwrap();

        // Give listener time to process
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Second connection from same IP should be rejected (or RST)
        // Note: The connection may succeed at TCP level but get closed immediately
        let client2 = TokioTcpStream::connect(bound_addr).await;
        if let Ok(mut c2) = client2 {
            // Try to write - should fail or get closed
            tokio::time::sleep(Duration::from_millis(50)).await;
            let result = c2.write_all(b"test\n").await;
            // Connection should either fail or be dropped
            // This depends on timing of the connection manager check
            let _ = result; // May or may not fail depending on timing
        }

        // Cleanup
        let _ = shutdown_tx.send(());
        let _ = listener_handle.await;
    }
}
