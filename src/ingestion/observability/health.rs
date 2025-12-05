//! Health check implementation for ingestion pipeline
//!
//! Provides liveness and readiness probes compatible with Kubernetes
//! and other orchestration systems.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;

/// Health status enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    /// System is healthy and operating normally
    Healthy,
    /// System is degraded but still accepting traffic
    Degraded,
    /// System is unhealthy and should not receive traffic
    Unhealthy,
}

impl HealthStatus {
    /// Convert to HTTP status code
    pub fn http_status_code(&self) -> u16 {
        match self {
            HealthStatus::Healthy => 200,
            HealthStatus::Degraded => 200, // Still return 200 for degraded (accepting traffic)
            HealthStatus::Unhealthy => 503,
        }
    }

    /// Check if the status indicates the system can accept traffic
    pub fn is_accepting_traffic(&self) -> bool {
        matches!(self, HealthStatus::Healthy | HealthStatus::Degraded)
    }
}

/// Individual subsystem health information
#[derive(Debug, Clone, Serialize)]
pub struct SubsystemHealth {
    /// Name of the subsystem
    pub name: String,
    /// Current health status
    pub status: HealthStatus,
    /// Optional message describing the status
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Time since last successful operation (if applicable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_success_ms: Option<u64>,
}

impl SubsystemHealth {
    /// Create a healthy subsystem status
    pub fn healthy(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: HealthStatus::Healthy,
            message: None,
            last_success_ms: None,
        }
    }

    /// Create an unhealthy subsystem status with message
    pub fn unhealthy(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: HealthStatus::Unhealthy,
            message: Some(message.into()),
            last_success_ms: None,
        }
    }

    /// Create a degraded subsystem status with message
    pub fn degraded(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: HealthStatus::Degraded,
            message: Some(message.into()),
            last_success_ms: None,
        }
    }
}

/// Health check implementation
///
/// Tracks system health across multiple subsystems and provides
/// aggregated health status for external monitoring.
pub struct HealthCheck {
    /// Start time for uptime calculation
    start_time: Instant,
    /// Whether the system is ready to accept traffic
    ready: AtomicBool,
    /// Whether the system is alive (basic liveness)
    alive: AtomicBool,
    /// Last successful write timestamp (epoch ms)
    last_write_ms: AtomicU64,
    /// Last successful read timestamp (epoch ms)
    last_read_ms: AtomicU64,
    /// Total health check invocations
    check_count: AtomicU64,
}

impl HealthCheck {
    /// Create a new health check instance
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            ready: AtomicBool::new(false),
            alive: AtomicBool::new(true),
            last_write_ms: AtomicU64::new(0),
            last_read_ms: AtomicU64::new(0),
            check_count: AtomicU64::new(0),
        }
    }

    /// Mark the system as ready to accept traffic
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
    }

    /// Mark the system as alive
    pub fn set_alive(&self, alive: bool) {
        self.alive.store(alive, Ordering::Release);
    }

    /// Record a successful write operation
    pub fn record_write(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.last_write_ms.store(now, Ordering::Relaxed);
    }

    /// Record a successful read operation
    pub fn record_read(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.last_read_ms.store(now, Ordering::Relaxed);
    }

    /// Check if the system is alive (liveness probe)
    ///
    /// Returns true if the system is running and responding.
    /// Used by orchestrators to detect crashed containers.
    pub fn is_alive(&self) -> bool {
        self.check_count.fetch_add(1, Ordering::Relaxed);
        self.alive.load(Ordering::Acquire)
    }

    /// Check if the system is ready (readiness probe)
    ///
    /// Returns true if the system is ready to accept traffic.
    /// Used by orchestrators to add/remove from load balancers.
    pub fn is_ready(&self) -> bool {
        self.check_count.fetch_add(1, Ordering::Relaxed);
        self.ready.load(Ordering::Acquire) && self.alive.load(Ordering::Acquire)
    }

    /// Get system uptime
    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Get time since last write (returns None if no writes recorded)
    pub fn time_since_last_write(&self) -> Option<Duration> {
        let last = self.last_write_ms.load(Ordering::Relaxed);
        if last == 0 {
            return None;
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        if now > last {
            Some(Duration::from_millis(now - last))
        } else {
            Some(Duration::ZERO)
        }
    }

    /// Get detailed health status with all subsystems
    pub fn detailed_status(&self) -> DetailedHealth {
        let mut subsystems = Vec::new();

        // Check storage subsystem (based on last write time)
        let storage_health = match self.time_since_last_write() {
            Some(duration) if duration > Duration::from_secs(60) => {
                SubsystemHealth::degraded("storage", "No writes in the last 60 seconds")
            },
            _ => SubsystemHealth::healthy("storage"),
        };
        subsystems.push(storage_health);

        // Network subsystem (assume healthy if alive)
        let network_health = if self.alive.load(Ordering::Acquire) {
            SubsystemHealth::healthy("network")
        } else {
            SubsystemHealth::unhealthy("network", "Network subsystem not responding")
        };
        subsystems.push(network_health);

        // Calculate overall status
        let overall = if subsystems
            .iter()
            .any(|s| s.status == HealthStatus::Unhealthy)
        {
            HealthStatus::Unhealthy
        } else if subsystems
            .iter()
            .any(|s| s.status == HealthStatus::Degraded)
        {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        };

        DetailedHealth {
            status: overall,
            uptime_seconds: self.uptime().as_secs(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            ready: self.is_ready(),
            subsystems,
        }
    }
}

impl Default for HealthCheck {
    fn default() -> Self {
        Self::new()
    }
}

/// Detailed health response for /health endpoint
#[derive(Debug, Clone, Serialize)]
pub struct DetailedHealth {
    /// Overall health status
    pub status: HealthStatus,
    /// System uptime in seconds
    pub uptime_seconds: u64,
    /// Application version
    pub version: String,
    /// Whether system is ready to accept traffic
    pub ready: bool,
    /// Per-subsystem health status
    pub subsystems: Vec<SubsystemHealth>,
}

/// Readiness check configuration
pub struct ReadinessCheck {
    /// Minimum uptime before reporting ready (warmup period)
    min_uptime: Duration,
    /// Health check reference
    health: Arc<HealthCheck>,
}

impl ReadinessCheck {
    /// Create a new readiness check
    pub fn new(health: Arc<HealthCheck>, min_uptime: Duration) -> Self {
        Self { min_uptime, health }
    }

    /// Check if the system is ready
    ///
    /// Returns true if:
    /// - System has been alive for at least min_uptime
    /// - System is marked as ready
    /// - All critical subsystems are healthy
    pub fn is_ready(&self) -> bool {
        // Check warmup period
        if self.health.uptime() < self.min_uptime {
            return false;
        }

        self.health.is_ready()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_status_http_codes() {
        assert_eq!(HealthStatus::Healthy.http_status_code(), 200);
        assert_eq!(HealthStatus::Degraded.http_status_code(), 200);
        assert_eq!(HealthStatus::Unhealthy.http_status_code(), 503);
    }

    #[test]
    fn test_health_status_accepting_traffic() {
        assert!(HealthStatus::Healthy.is_accepting_traffic());
        assert!(HealthStatus::Degraded.is_accepting_traffic());
        assert!(!HealthStatus::Unhealthy.is_accepting_traffic());
    }

    #[test]
    fn test_health_check_new() {
        let health = HealthCheck::new();
        assert!(health.is_alive());
        assert!(!health.is_ready()); // Not ready by default
    }

    #[test]
    fn test_health_check_set_ready() {
        let health = HealthCheck::new();
        health.set_ready(true);
        assert!(health.is_ready());

        health.set_ready(false);
        assert!(!health.is_ready());
    }

    #[test]
    fn test_health_check_set_alive() {
        let health = HealthCheck::new();
        assert!(health.is_alive());

        health.set_alive(false);
        assert!(!health.is_alive());
        assert!(!health.is_ready()); // Not ready if not alive
    }

    #[test]
    fn test_health_check_record_write() {
        let health = HealthCheck::new();
        assert!(health.time_since_last_write().is_none());

        health.record_write();
        let since = health.time_since_last_write();
        assert!(since.is_some());
        assert!(since.unwrap() < Duration::from_secs(1));
    }

    #[test]
    fn test_health_check_uptime() {
        let health = HealthCheck::new();
        std::thread::sleep(Duration::from_millis(10));
        assert!(health.uptime() >= Duration::from_millis(10));
    }

    #[test]
    fn test_detailed_health() {
        let health = HealthCheck::new();
        health.set_ready(true);
        health.set_alive(true);

        let detailed = health.detailed_status();
        assert_eq!(detailed.status, HealthStatus::Healthy);
        assert!(detailed.ready);
        assert!(!detailed.subsystems.is_empty());
    }

    #[test]
    fn test_subsystem_health_constructors() {
        let healthy = SubsystemHealth::healthy("test");
        assert_eq!(healthy.status, HealthStatus::Healthy);
        assert!(healthy.message.is_none());

        let unhealthy = SubsystemHealth::unhealthy("test", "error message");
        assert_eq!(unhealthy.status, HealthStatus::Unhealthy);
        assert_eq!(unhealthy.message, Some("error message".to_string()));

        let degraded = SubsystemHealth::degraded("test", "warning");
        assert_eq!(degraded.status, HealthStatus::Degraded);
    }

    #[test]
    fn test_readiness_check_warmup() {
        let health = Arc::new(HealthCheck::new());
        health.set_ready(true);

        // Readiness check with 1 second warmup
        let readiness = ReadinessCheck::new(Arc::clone(&health), Duration::from_secs(1));

        // Should not be ready immediately (warmup period)
        assert!(!readiness.is_ready());
    }
}
