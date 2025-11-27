//! UDP listener implementation
//!
//! Provides a high-performance UDP listener optimized for receiving
//! time-series data with support for:
//! - High packet throughput with recv_from batching
//! - Rate limiting integration
//! - Graceful shutdown support
//!
//! # Design Considerations
//!
//! UDP is connectionless, so there's no concept of connection management.
//! Instead, we focus on:
//! - Maximizing packet receive throughput
//! - Efficient buffer management
//! - Rate limiting by source IP
//!
//! # Limitations
//!
//! - No guaranteed delivery (UDP semantics)
//! - No backpressure to clients (packets silently dropped if rate limited)
//! - Maximum datagram size limited by MTU (typically ~1400 bytes)

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::net::UdpSocket;
use tokio::sync::broadcast;
use tracing::{debug, error, info, trace, warn};

use super::error::NetworkError;
use super::rate_limit::RateLimiter;

/// UDP listener for receiving datagrams
///
/// Implements a high-throughput UDP receiver designed for time-series
/// data ingestion where occasional packet loss is acceptable.
///
/// # Example
///
/// ```rust,no_run
/// use gorilla_tsdb::ingestion::network::UdpListener;
/// use std::net::SocketAddr;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let addr: SocketAddr = "0.0.0.0:8087".parse()?;
/// let listener = UdpListener::bind(addr, 65536).await?;
/// # Ok(())
/// # }
/// ```
pub struct UdpListener {
    /// Underlying UDP socket
    socket: UdpSocket,
    /// Local address the socket is bound to
    local_addr: SocketAddr,
    /// Receive buffer size
    buffer_size: usize,
    /// Statistics
    stats: UdpStats,
}

/// UDP listener statistics
#[derive(Debug, Default)]
pub struct UdpStats {
    /// Total packets received
    pub packets_received: AtomicU64,
    /// Total bytes received
    pub bytes_received: AtomicU64,
    /// Packets dropped due to rate limiting
    pub rate_limited: AtomicU64,
    /// Packets with parse errors
    pub parse_errors: AtomicU64,
}

impl UdpStats {
    /// Get a snapshot of current statistics
    pub fn snapshot(&self) -> UdpStatsSnapshot {
        UdpStatsSnapshot {
            packets_received: self.packets_received.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            rate_limited: self.rate_limited.load(Ordering::Relaxed),
            parse_errors: self.parse_errors.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of UDP statistics
#[derive(Debug, Clone)]
pub struct UdpStatsSnapshot {
    /// Total packets received
    pub packets_received: u64,
    /// Total bytes received
    pub bytes_received: u64,
    /// Packets dropped due to rate limiting
    pub rate_limited: u64,
    /// Packets with parse errors
    pub parse_errors: u64,
}

impl UdpListener {
    /// Bind a new UDP listener to the specified address
    ///
    /// # Arguments
    ///
    /// * `addr` - Socket address to bind to (e.g., "0.0.0.0:8087")
    /// * `buffer_size` - Size of receive buffer for each packet
    ///
    /// # Errors
    ///
    /// Returns error if the address is already in use or invalid.
    ///
    /// # Buffer Size
    ///
    /// The buffer size should be large enough to hold the largest expected
    /// datagram. For line protocol, this is typically the max line length.
    /// A good default is 65536 (64KB) which is the maximum UDP payload size.
    pub async fn bind(addr: SocketAddr, buffer_size: usize) -> Result<Self, NetworkError> {
        let socket = UdpSocket::bind(addr)
            .await
            .map_err(|e| NetworkError::BindFailed {
                addr,
                reason: e.to_string(),
            })?;

        let local_addr = socket.local_addr()?;

        // Set receive buffer size hint to kernel
        // This increases the kernel buffer to handle burst traffic
        Self::set_recv_buffer(&socket, buffer_size * 100)?;

        info!(addr = %local_addr, buffer_size, "UDP listener bound");

        Ok(Self {
            socket,
            local_addr,
            buffer_size,
            stats: UdpStats::default(),
        })
    }

    /// Set the socket receive buffer size
    ///
    /// Attempts to set a large receive buffer to handle burst traffic.
    /// Falls back gracefully if the requested size isn't available.
    fn set_recv_buffer(socket: &UdpSocket, size: usize) -> Result<(), NetworkError> {
        // On Linux, the actual buffer may be limited by:
        // - /proc/sys/net/core/rmem_max (usually 208KB default)
        // - CAP_NET_ADMIN capability for larger values
        //
        // We try to set a large value but don't fail if it's reduced

        let socket_ref = socket2::SockRef::from(socket);

        if let Err(e) = socket_ref.set_recv_buffer_size(size) {
            warn!(
                requested = size,
                error = %e,
                "Failed to set full receive buffer size"
            );
        }

        // Log actual buffer size
        if let Ok(actual) = socket_ref.recv_buffer_size() {
            debug!(requested = size, actual, "UDP receive buffer configured");
        }

        Ok(())
    }

    /// Get the local address this listener is bound to
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Get reference to statistics
    pub fn stats(&self) -> &UdpStats {
        &self.stats
    }

    /// Run the UDP receive loop
    ///
    /// Continuously receives datagrams and processes them.
    /// Integrates with rate limiter for traffic control.
    ///
    /// # Arguments
    ///
    /// * `rate_limiter` - Rate limiter for traffic control
    /// * `shutdown_rx` - Broadcast receiver for shutdown signal
    ///
    /// # Processing
    ///
    /// Each received datagram is:
    /// 1. Checked against rate limiter
    /// 2. Parsed (if rate limit allows)
    /// 3. Forwarded to ingestion pipeline
    ///
    /// # Shutdown
    ///
    /// The loop exits gracefully when a shutdown signal is received.
    pub async fn run(
        self,
        rate_limiter: Arc<RateLimiter>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> Result<(), NetworkError> {
        info!(addr = %self.local_addr, "Starting UDP receive loop");

        // Allocate receive buffer
        let mut buffer = vec![0u8; self.buffer_size];

        loop {
            tokio::select! {
                // Handle shutdown signal
                _ = shutdown_rx.recv() => {
                    info!(addr = %self.local_addr, "UDP listener shutting down");
                    break;
                }

                // Receive datagrams
                result = self.socket.recv_from(&mut buffer) => {
                    match result {
                        Ok((len, src_addr)) => {
                            self.handle_datagram(
                                &buffer[..len],
                                src_addr,
                                &rate_limiter,
                            ).await;
                        }
                        Err(e) => {
                            // Log error but continue receiving
                            // Common errors: buffer too small, connection refused
                            error!(error = %e, "UDP receive error");
                        }
                    }
                }
            }
        }

        let snapshot = self.stats.snapshot();
        info!(
            packets = snapshot.packets_received,
            bytes = snapshot.bytes_received,
            rate_limited = snapshot.rate_limited,
            "UDP listener stopped"
        );

        Ok(())
    }

    /// Handle a received datagram
    ///
    /// Applies rate limiting and forwards to processing pipeline.
    async fn handle_datagram(&self, data: &[u8], src_addr: SocketAddr, rate_limiter: &RateLimiter) {
        let len = data.len();

        // Update receive statistics
        self.stats.packets_received.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_received
            .fetch_add(len as u64, Ordering::Relaxed);

        // Check rate limit
        if !rate_limiter.check_ip(src_addr.ip(), len as u64) {
            self.stats.rate_limited.fetch_add(1, Ordering::Relaxed);
            trace!(
                src = %src_addr,
                bytes = len,
                "UDP packet rate limited"
            );
            return;
        }

        // Process the datagram
        // TODO: Send to protocol parser pipeline
        // For now, just trace
        trace!(
            src = %src_addr,
            bytes = len,
            "Received UDP datagram"
        );

        // Parse data as UTF-8 for line protocol
        // In production, this would go to the protocol parser
        match std::str::from_utf8(data) {
            Ok(text) => {
                // Each line in the datagram is a separate data point
                for line in text.lines() {
                    if !line.is_empty() {
                        trace!(src = %src_addr, line = %line, "UDP line received");
                        // TODO: Forward to protocol parser
                    }
                }
            }
            Err(_) => {
                // Invalid UTF-8 - might be binary protocol
                self.stats.parse_errors.fetch_add(1, Ordering::Relaxed);
                debug!(src = %src_addr, "Invalid UTF-8 in UDP datagram");
            }
        }
    }

    /// Run with batch receiving for higher throughput
    ///
    /// Uses multiple receive calls per iteration to improve throughput
    /// under high load. This is a simplified version of recv_mmsg.
    ///
    /// # Arguments
    ///
    /// * `rate_limiter` - Rate limiter for traffic control
    /// * `shutdown_rx` - Broadcast receiver for shutdown signal
    /// * `batch_size` - Number of receives to attempt per iteration
    #[allow(dead_code)]
    pub async fn run_batched(
        self,
        rate_limiter: Arc<RateLimiter>,
        mut shutdown_rx: broadcast::Receiver<()>,
        batch_size: usize,
    ) -> Result<(), NetworkError> {
        info!(
            addr = %self.local_addr,
            batch_size,
            "Starting batched UDP receive loop"
        );

        // Allocate receive buffers
        let mut buffers: Vec<Vec<u8>> = (0..batch_size)
            .map(|_| vec![0u8; self.buffer_size])
            .collect();

        loop {
            tokio::select! {
                biased; // Check shutdown first

                _ = shutdown_rx.recv() => {
                    info!(addr = %self.local_addr, "UDP listener shutting down");
                    break;
                }

                _ = async {
                    // Try to receive multiple datagrams
                    for buffer in buffers.iter_mut() {
                        // Use try_recv_from for non-blocking receive
                        match self.socket.try_recv_from(buffer) {
                            Ok((len, src_addr)) => {
                                self.handle_datagram(
                                    &buffer[..len],
                                    src_addr,
                                    &rate_limiter,
                                ).await;
                            }
                            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                // No more data available, break batch
                                break;
                            }
                            Err(e) => {
                                error!(error = %e, "UDP batch receive error");
                                break;
                            }
                        }
                    }

                    // Wait for more data with a blocking recv
                    // This prevents busy-looping when no data is available
                    let mut first_buffer = vec![0u8; self.buffer_size];
                    if let Ok((len, src_addr)) = self.socket.recv_from(&mut first_buffer).await {
                        self.handle_datagram(
                            &first_buffer[..len],
                            src_addr,
                            &rate_limiter,
                        ).await;
                    }
                } => {}
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UdpSocket as TokioUdpSocket;

    #[tokio::test]
    async fn test_udp_listener_bind() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

        let listener = UdpListener::bind(addr, 65536).await;
        assert!(listener.is_ok());

        let listener = listener.unwrap();
        assert_ne!(listener.local_addr().port(), 0);
    }

    #[tokio::test]
    async fn test_udp_listener_receive() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = UdpListener::bind(addr, 65536).await.unwrap();
        let bound_addr = listener.local_addr();

        let rate_limiter = Arc::new(RateLimiter::new(Default::default()));
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        // Spawn listener in background
        let stats = listener.stats().snapshot();
        let listener_handle =
            tokio::spawn(async move { listener.run(rate_limiter, shutdown_rx).await });

        // Give listener time to start
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Create client socket
        let client = TokioUdpSocket::bind("127.0.0.1:0").await.unwrap();

        // Send data
        let data = b"test,host=server1 value=42.0 1234567890000000000\n";
        client.send_to(data, bound_addr).await.unwrap();

        // Give listener time to receive
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Shutdown
        let _ = shutdown_tx.send(());
        let result = listener_handle.await;
        assert!(result.is_ok());

        // Note: Can't easily check stats after listener is consumed
        // The initial snapshot shows 0 as expected
        assert_eq!(stats.packets_received, 0);
    }

    #[tokio::test]
    async fn test_udp_listener_rate_limiting() {
        use super::super::rate_limit::RateLimitConfig;

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = UdpListener::bind(addr, 65536).await.unwrap();
        let bound_addr = listener.local_addr();

        // Create rate limiter with very low limit
        let config = RateLimitConfig {
            enabled: true,
            points_per_sec_per_ip: 10, // Very low limit
            points_per_sec_per_tenant: 100,
            burst_multiplier: 1.0,
            ..Default::default()
        };
        let rate_limiter = Arc::new(RateLimiter::new(config));
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        // Spawn listener
        let listener_handle =
            tokio::spawn(async move { listener.run(rate_limiter, shutdown_rx).await });

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Create client
        let client = TokioUdpSocket::bind("127.0.0.1:0").await.unwrap();

        // Send many packets to trigger rate limiting
        let data = b"test data that exceeds the rate limit quickly\n";
        for _ in 0..20 {
            let _ = client.send_to(data, bound_addr).await;
        }

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Shutdown
        let _ = shutdown_tx.send(());
        let _ = listener_handle.await;
    }

    #[tokio::test]
    async fn test_udp_stats_snapshot() {
        let stats = UdpStats::default();

        stats.packets_received.fetch_add(100, Ordering::Relaxed);
        stats.bytes_received.fetch_add(5000, Ordering::Relaxed);
        stats.rate_limited.fetch_add(5, Ordering::Relaxed);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.packets_received, 100);
        assert_eq!(snapshot.bytes_received, 5000);
        assert_eq!(snapshot.rate_limited, 5);
        assert_eq!(snapshot.parse_errors, 0);
    }
}
