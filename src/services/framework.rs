//! Service Framework
//!
//! Provides the core framework for managing background services including:
//! - Service trait for implementing custom services
//! - ServiceManager for coordinating service lifecycle
//! - Graceful shutdown handling
//! - Service dependency management
//! - Restart policies for fault tolerance

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::{broadcast, Notify};
use tokio::task::JoinHandle;
use tokio::time::Instant;

// ============================================================================
// Service Trait
// ============================================================================

/// Trait for implementing background services
///
/// Services are long-running background tasks that perform maintenance,
/// monitoring, or other periodic operations.
#[async_trait::async_trait]
pub trait Service: Send + Sync {
    /// Start the service
    ///
    /// This method should initialize the service and begin its main loop.
    /// It should respect the shutdown signal for graceful termination.
    async fn start(&self, shutdown: broadcast::Receiver<()>) -> Result<(), ServiceError>;

    /// Get the service name for logging and identification
    fn name(&self) -> &'static str;

    /// Get the current status of the service
    fn status(&self) -> ServiceStatus;

    /// Get service dependencies (services that must start before this one)
    fn dependencies(&self) -> Vec<&'static str> {
        vec![]
    }

    /// Get the restart policy for this service
    fn restart_policy(&self) -> RestartPolicy {
        RestartPolicy::OnFailure {
            max_retries: 3,
            backoff: Duration::from_secs(5),
        }
    }
}

// ============================================================================
// Service Status
// ============================================================================

/// Status of a service
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServiceStatus {
    /// Service is initializing
    Starting,

    /// Service is running normally
    Running,

    /// Service is shutting down
    Stopping,

    /// Service has stopped
    Stopped,

    /// Service failed with an error
    Failed(String),
}

impl ServiceStatus {
    /// Check if the service is in a healthy state
    pub fn is_healthy(&self) -> bool {
        matches!(self, ServiceStatus::Running)
    }

    /// Check if the service has stopped (normally or due to failure)
    pub fn is_stopped(&self) -> bool {
        matches!(self, ServiceStatus::Stopped | ServiceStatus::Failed(_))
    }
}

// ============================================================================
// Restart Policy
// ============================================================================

/// Policy for restarting failed services
#[derive(Debug, Clone)]
pub enum RestartPolicy {
    /// Never restart the service
    Never,

    /// Always restart the service
    Always {
        /// Delay between restarts
        backoff: Duration,
    },

    /// Restart only on failure (not on clean shutdown)
    OnFailure {
        /// Maximum number of restart attempts
        max_retries: u32,
        /// Delay between restarts (multiplied by attempt number)
        backoff: Duration,
    },
}

// ============================================================================
// Service Error
// ============================================================================

/// Errors that can occur in services
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    /// Service failed during initialization phase
    #[error("Service initialization failed: {0}")]
    InitializationFailed(String),

    /// Service encountered an error during execution
    #[error("Service runtime error: {0}")]
    RuntimeError(String),

    /// Service failed to shut down cleanly
    #[error("Service shutdown error: {0}")]
    ShutdownError(String),

    /// A required dependency is not available or not running
    #[error("Dependency not satisfied: {0}")]
    DependencyError(String),

    /// Attempted to start a service that is already running
    #[error("Service already running")]
    AlreadyRunning,

    /// The requested service was not found in the registry
    #[error("Service not found: {0}")]
    NotFound(String),

    /// An internal error occurred in the service framework
    #[error("Internal error: {0}")]
    Internal(String),
}

// ============================================================================
// Service Config
// ============================================================================

/// Configuration for the service manager
#[derive(Debug, Clone)]
pub struct ServiceConfig {
    /// Timeout for graceful shutdown
    pub shutdown_timeout: Duration,

    /// Enable automatic restart of failed services
    pub auto_restart: bool,

    /// Interval for health checks
    pub health_check_interval: Duration,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            shutdown_timeout: Duration::from_secs(30),
            auto_restart: true,
            health_check_interval: Duration::from_secs(10),
        }
    }
}

// ============================================================================
// Service Handle
// ============================================================================

/// Handle for a running service
struct ServiceHandle {
    /// The service instance
    service: Arc<dyn Service>,

    /// Task handle for the running service
    task: Option<JoinHandle<Result<(), ServiceError>>>,

    /// Number of restart attempts (used for restart policy enforcement)
    #[allow(dead_code)]
    restart_count: u32,

    /// Last start time
    started_at: Option<Instant>,
}

// ============================================================================
// Service Manager
// ============================================================================

/// Manager for coordinating background services
///
/// The ServiceManager handles:
/// - Starting and stopping services in dependency order
/// - Graceful shutdown with configurable timeout
/// - Automatic restart of failed services
/// - Health monitoring
pub struct ServiceManager {
    /// Configuration
    config: ServiceConfig,

    /// Registered services
    services: RwLock<HashMap<&'static str, ServiceHandle>>,

    /// Shutdown signal sender
    shutdown_tx: broadcast::Sender<()>,

    /// Shutdown notification for waiting
    shutdown_notify: Arc<Notify>,

    /// Manager is running
    running: RwLock<bool>,
}

/// Shared service manager for use across threads
pub type SharedServiceManager = Arc<ServiceManager>;

impl ServiceManager {
    /// Create a new service manager
    pub fn new(config: ServiceConfig) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);

        Self {
            config,
            services: RwLock::new(HashMap::new()),
            shutdown_tx,
            shutdown_notify: Arc::new(Notify::new()),
            running: RwLock::new(false),
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(ServiceConfig::default())
    }

    /// Register a service with the manager
    pub fn register(&self, service: Arc<dyn Service>) -> Result<(), ServiceError> {
        let name = service.name();
        let mut services = self.services.write();

        if services.contains_key(name) {
            return Err(ServiceError::AlreadyRunning);
        }

        services.insert(
            name,
            ServiceHandle {
                service,
                task: None,
                restart_count: 0,
                started_at: None,
            },
        );

        tracing::debug!(service = name, "Service registered");
        Ok(())
    }

    /// Validate that all declared dependencies exist
    ///
    /// This should be called before starting services to catch configuration
    /// errors early. Returns an error listing all missing dependencies.
    pub fn validate_dependencies(&self) -> Result<(), ServiceError> {
        let services = self.services.read();
        let mut missing: Vec<String> = Vec::new();

        for (name, handle) in services.iter() {
            for dep in handle.service.dependencies() {
                if !services.contains_key(dep) {
                    missing.push(format!(
                        "'{}' depends on '{}' which is not registered",
                        name, dep
                    ));
                }
            }
        }

        if missing.is_empty() {
            Ok(())
        } else {
            Err(ServiceError::DependencyError(missing.join("; ")))
        }
    }

    /// Start all registered services
    ///
    /// Services are started in dependency order. If a dependency fails to start,
    /// dependent services will not be started.
    pub async fn start_all(&self) -> Result<(), ServiceError> {
        // Validate all dependencies exist before starting anything
        self.validate_dependencies()?;

        // Check and set running flag with explicit scope to release lock before await
        {
            let mut running = self.running.write();
            if *running {
                return Err(ServiceError::AlreadyRunning);
            }
            *running = true;
        }

        // Get services in dependency order
        let order = self.topological_sort()?;

        for name in order {
            self.start_service(name).await?;
        }

        tracing::debug!("All services started");
        Ok(())
    }

    /// Start a specific service
    ///
    /// This method holds a write lock during the entire dependency check and
    /// task spawn to prevent TOCTOU race conditions where dependencies could
    /// stop between the check and the start.
    pub async fn start_service(&self, name: &'static str) -> Result<(), ServiceError> {
        // All lock operations happen in this block - lock is released before any await
        let service_ref = {
            // Hold write lock for the entire operation to prevent TOCTOU race
            let mut services = self.services.write();

            // Get the service and its dependencies
            let handle = match services.get(name) {
                Some(h) => h,
                None => return Err(ServiceError::NotFound(name.to_string())),
            };
            let service = handle.service.clone();
            let deps = handle.service.dependencies();

            // Check all dependencies are running while still holding the lock
            for dep in deps {
                match services.get(dep) {
                    Some(dep_handle) => {
                        if !dep_handle.service.status().is_healthy() {
                            return Err(ServiceError::DependencyError(format!(
                                "Dependency '{}' is not running",
                                dep
                            )));
                        }
                    },
                    None => {
                        return Err(ServiceError::DependencyError(format!(
                            "Dependency '{}' not found",
                            dep
                        )));
                    },
                }
            }

            // Spawn the service task while still holding the lock
            // tokio::spawn is synchronous and returns immediately
            let shutdown_rx = self.shutdown_tx.subscribe();
            let task = tokio::spawn(async move { service.start(shutdown_rx).await });

            // Update the handle
            let handle = match services.get_mut(name) {
                Some(h) => h,
                None => return Err(ServiceError::NotFound(name.to_string())),
            };
            handle.task = Some(task);
            handle.started_at = Some(Instant::now());
            handle.service.clone()
            // Lock is released here when services goes out of scope
        };

        // Wait briefly for the service to reach Running status
        // This ensures dependencies are actually running before we start dependents
        let start_wait = Instant::now();
        let max_wait = Duration::from_millis(100);
        while start_wait.elapsed() < max_wait {
            if service_ref.status().is_healthy() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        tracing::debug!(service = name, "Service started");
        Ok(())
    }

    /// Stop all services gracefully
    pub async fn shutdown(&self) -> Result<(), ServiceError> {
        tracing::info!("Initiating graceful shutdown");

        // Send shutdown signal
        let _ = self.shutdown_tx.send(());

        // Collect tasks to await (release lock before awaiting)
        let tasks: Vec<(&'static str, JoinHandle<Result<(), ServiceError>>)> = {
            let mut services = self.services.write();
            services
                .iter_mut()
                .filter_map(|(name, handle)| handle.task.take().map(|task| (*name, task)))
                .collect()
        };

        // Wait for services to stop with timeout
        let deadline = Instant::now() + self.config.shutdown_timeout;

        for (name, task) in tasks {
            let remaining = deadline.saturating_duration_since(Instant::now());

            match tokio::time::timeout(remaining, task).await {
                Ok(Ok(Ok(()))) => {
                    tracing::debug!(service = name, "Service stopped gracefully");
                },
                Ok(Ok(Err(e))) => {
                    tracing::warn!(service = name, error = %e, "Service stopped with error");
                },
                Ok(Err(e)) => {
                    tracing::error!(service = name, error = %e, "Service task panicked");
                },
                Err(_) => {
                    tracing::warn!(service = name, "Service shutdown timed out, aborting");
                },
            }
        }

        *self.running.write() = false;
        self.shutdown_notify.notify_waiters();

        tracing::info!("Shutdown complete");
        Ok(())
    }

    /// Wait for shutdown to complete
    pub async fn wait_for_shutdown(&self) {
        self.shutdown_notify.notified().await;
    }

    /// Get the status of all services
    pub fn status(&self) -> HashMap<&'static str, ServiceStatus> {
        let services = self.services.read();
        services
            .iter()
            .map(|(name, handle)| (*name, handle.service.status()))
            .collect()
    }

    /// Get the status of a specific service
    pub fn service_status(&self, name: &str) -> Option<ServiceStatus> {
        let services = self.services.read();
        services.get(name).map(|h| h.service.status())
    }

    /// Get the uptime of a specific service (time since start)
    pub fn service_uptime(&self, name: &str) -> Option<Duration> {
        let services = self.services.read();
        services
            .get(name)
            .and_then(|h| h.started_at.map(|started| started.elapsed()))
    }

    /// Get uptime for all running services
    pub fn uptimes(&self) -> HashMap<&'static str, Duration> {
        let services = self.services.read();
        services
            .iter()
            .filter_map(|(name, handle)| {
                handle.started_at.map(|started| (*name, started.elapsed()))
            })
            .collect()
    }

    /// Check if all services are healthy
    pub fn is_healthy(&self) -> bool {
        let services = self.services.read();
        services.values().all(|h| h.service.status().is_healthy())
    }

    /// Check for failed services and restart them according to their policies
    ///
    /// Returns the names of services that were restarted.
    pub async fn check_and_restart_failed(&self) -> Vec<&'static str> {
        if !self.config.auto_restart {
            return Vec::new();
        }

        let mut restarted = Vec::new();

        // Collect services that need restarting
        let services_to_restart: Vec<(&'static str, RestartPolicy, u32)> = {
            let services = self.services.read();
            services
                .iter()
                .filter_map(|(name, handle)| {
                    if let ServiceStatus::Failed(_) = handle.service.status() {
                        Some((*name, handle.service.restart_policy(), handle.restart_count))
                    } else {
                        None
                    }
                })
                .collect()
        };

        for (name, policy, restart_count) in services_to_restart {
            let should_restart = match &policy {
                RestartPolicy::Never => false,
                RestartPolicy::Always { backoff } => {
                    // Wait for backoff before restarting
                    tokio::time::sleep(*backoff).await;
                    true
                },
                RestartPolicy::OnFailure {
                    max_retries,
                    backoff,
                } => {
                    if restart_count < *max_retries {
                        // Exponential backoff: backoff * (restart_count + 1)
                        let delay = *backoff * (restart_count + 1);
                        tokio::time::sleep(delay).await;
                        true
                    } else {
                        tracing::warn!(
                            service = name,
                            attempts = restart_count,
                            max = max_retries,
                            "Service exceeded max restart attempts"
                        );
                        false
                    }
                },
            };

            if should_restart {
                // Increment restart count
                {
                    let mut services = self.services.write();
                    if let Some(handle) = services.get_mut(name) {
                        handle.restart_count += 1;
                    }
                }

                // Attempt to restart
                match self.start_service(name).await {
                    Ok(()) => {
                        tracing::debug!(service = name, "Service restarted successfully");
                        restarted.push(name);
                    },
                    Err(e) => {
                        tracing::error!(service = name, error = %e, "Failed to restart service");
                    },
                }
            }
        }

        restarted
    }

    /// Reset the restart count for a service (call after successful operation period)
    pub fn reset_restart_count(&self, name: &str) {
        let mut services = self.services.write();
        if let Some(handle) = services.get_mut(name) {
            handle.restart_count = 0;
        }
    }

    /// Topological sort of services based on dependencies
    fn topological_sort(&self) -> Result<Vec<&'static str>, ServiceError> {
        let services = self.services.read();
        let mut result = Vec::new();
        let mut visited = HashMap::new();
        let mut temp_mark = HashMap::new();

        for name in services.keys() {
            if !visited.contains_key(name) {
                Self::visit_for_sort(name, &services, &mut visited, &mut temp_mark, &mut result)?;
            }
        }

        Ok(result)
    }

    /// Helper for topological sort (depth-first search)
    /// This is a static method to avoid clippy's only_used_in_recursion warning
    fn visit_for_sort(
        name: &'static str,
        services: &HashMap<&'static str, ServiceHandle>,
        visited: &mut HashMap<&'static str, bool>,
        temp_mark: &mut HashMap<&'static str, bool>,
        result: &mut Vec<&'static str>,
    ) -> Result<(), ServiceError> {
        if temp_mark.get(&name).copied().unwrap_or(false) {
            return Err(ServiceError::DependencyError(format!(
                "Circular dependency detected at '{}'",
                name
            )));
        }

        if visited.get(&name).copied().unwrap_or(false) {
            return Ok(());
        }

        temp_mark.insert(name, true);

        if let Some(handle) = services.get(name) {
            for dep in handle.service.dependencies() {
                Self::visit_for_sort(dep, services, visited, temp_mark, result)?;
            }
        }

        temp_mark.insert(name, false);
        visited.insert(name, true);
        result.push(name);

        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    /// Test service implementation
    struct TestService {
        name: &'static str,
        status: RwLock<ServiceStatus>,
        started: AtomicBool,
        stopped: AtomicBool,
        run_count: AtomicU32,
    }

    impl TestService {
        fn new(name: &'static str) -> Self {
            Self {
                name,
                status: RwLock::new(ServiceStatus::Stopped),
                started: AtomicBool::new(false),
                stopped: AtomicBool::new(false),
                run_count: AtomicU32::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl Service for TestService {
        async fn start(&self, mut shutdown: broadcast::Receiver<()>) -> Result<(), ServiceError> {
            *self.status.write() = ServiceStatus::Running;
            self.started.store(true, Ordering::SeqCst);
            self.run_count.fetch_add(1, Ordering::SeqCst);

            // Wait for shutdown
            let _ = shutdown.recv().await;

            *self.status.write() = ServiceStatus::Stopped;
            self.stopped.store(true, Ordering::SeqCst);
            Ok(())
        }

        fn name(&self) -> &'static str {
            self.name
        }

        fn status(&self) -> ServiceStatus {
            self.status.read().clone()
        }
    }

    #[tokio::test]
    async fn test_service_manager_lifecycle() {
        let manager = ServiceManager::with_defaults();

        let service = Arc::new(TestService::new("test"));
        manager.register(service.clone()).unwrap();

        // Start services
        manager.start_all().await.unwrap();

        // Give service time to start
        tokio::time::sleep(Duration::from_millis(10)).await;

        assert!(service.started.load(Ordering::SeqCst));
        assert!(matches!(service.status(), ServiceStatus::Running));

        // Shutdown
        manager.shutdown().await.unwrap();

        assert!(service.stopped.load(Ordering::SeqCst));
    }

    #[test]
    fn test_service_status() {
        assert!(ServiceStatus::Running.is_healthy());
        assert!(!ServiceStatus::Starting.is_healthy());
        assert!(!ServiceStatus::Stopped.is_healthy());

        assert!(ServiceStatus::Stopped.is_stopped());
        assert!(ServiceStatus::Failed("error".to_string()).is_stopped());
        assert!(!ServiceStatus::Running.is_stopped());
    }

    #[test]
    fn test_service_config_default() {
        let config = ServiceConfig::default();
        assert_eq!(config.shutdown_timeout, Duration::from_secs(30));
        assert!(config.auto_restart);
        assert_eq!(config.health_check_interval, Duration::from_secs(10));
    }

    #[tokio::test]
    async fn test_service_registration() {
        let manager = ServiceManager::with_defaults();

        let service1 = Arc::new(TestService::new("service1"));
        let service2 = Arc::new(TestService::new("service2"));

        manager.register(service1).unwrap();
        manager.register(service2).unwrap();

        let status = manager.status();
        assert_eq!(status.len(), 2);
    }

    #[tokio::test]
    async fn test_duplicate_registration() {
        let manager = ServiceManager::with_defaults();

        let service = Arc::new(TestService::new("test"));
        manager.register(service.clone()).unwrap();

        let result = manager.register(service);
        assert!(matches!(result, Err(ServiceError::AlreadyRunning)));
    }

    /// Test service with dependencies
    struct DependentService {
        name: &'static str,
        deps: Vec<&'static str>,
        status: RwLock<ServiceStatus>,
    }

    impl DependentService {
        fn new(name: &'static str, deps: Vec<&'static str>) -> Self {
            Self {
                name,
                deps,
                status: RwLock::new(ServiceStatus::Stopped),
            }
        }
    }

    #[async_trait::async_trait]
    impl Service for DependentService {
        async fn start(&self, mut shutdown: broadcast::Receiver<()>) -> Result<(), ServiceError> {
            *self.status.write() = ServiceStatus::Running;
            let _ = shutdown.recv().await;
            *self.status.write() = ServiceStatus::Stopped;
            Ok(())
        }

        fn name(&self) -> &'static str {
            self.name
        }

        fn status(&self) -> ServiceStatus {
            self.status.read().clone()
        }

        fn dependencies(&self) -> Vec<&'static str> {
            self.deps.clone()
        }
    }

    #[test]
    fn test_validate_dependencies_success() {
        let manager = ServiceManager::with_defaults();

        // Register services in order - parent first
        let parent = Arc::new(TestService::new("parent"));
        let child = Arc::new(DependentService::new("child", vec!["parent"]));

        manager.register(parent).unwrap();
        manager.register(child).unwrap();

        // Validation should pass
        assert!(manager.validate_dependencies().is_ok());
    }

    #[test]
    fn test_validate_dependencies_missing() {
        let manager = ServiceManager::with_defaults();

        // Register only the child, not the parent it depends on
        let child = Arc::new(DependentService::new("child", vec!["parent"]));
        manager.register(child).unwrap();

        // Validation should fail
        let result = manager.validate_dependencies();
        assert!(result.is_err());
        if let Err(ServiceError::DependencyError(msg)) = result {
            assert!(msg.contains("parent"));
            assert!(msg.contains("child"));
        } else {
            panic!("Expected DependencyError");
        }
    }

    #[tokio::test]
    async fn test_start_all_validates_dependencies() {
        let manager = ServiceManager::with_defaults();

        // Register service with missing dependency
        let child = Arc::new(DependentService::new("child", vec!["missing_parent"]));
        manager.register(child).unwrap();

        // start_all should fail with dependency error
        let result = manager.start_all().await;
        assert!(matches!(result, Err(ServiceError::DependencyError(_))));
    }

    #[tokio::test]
    async fn test_service_uptime() {
        let manager = ServiceManager::with_defaults();

        let service = Arc::new(TestService::new("test"));
        manager.register(service).unwrap();

        // Before start, uptime should be None
        assert!(manager.service_uptime("test").is_none());

        // Start service
        manager.start_all().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // After start, uptime should be Some
        let uptime = manager.service_uptime("test");
        assert!(uptime.is_some());
        assert!(uptime.unwrap() >= Duration::from_millis(50));

        // Cleanup
        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_multiple_services_with_dependencies() {
        let manager = ServiceManager::with_defaults();

        // Create parent and child services
        let parent = Arc::new(TestService::new("parent"));
        let child = Arc::new(DependentService::new("child", vec!["parent"]));

        // Register in reverse order (child first) - should still work
        manager.register(child).unwrap();
        manager.register(parent).unwrap();

        // Start all - should start parent first, then child
        manager.start_all().await.unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;

        // Both should be running
        assert!(matches!(
            manager.service_status("parent"),
            Some(ServiceStatus::Running)
        ));
        assert!(matches!(
            manager.service_status("child"),
            Some(ServiceStatus::Running)
        ));

        // Cleanup
        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_uptimes_returns_all_services() {
        let manager = ServiceManager::with_defaults();

        let service1 = Arc::new(TestService::new("svc1"));
        let service2 = Arc::new(TestService::new("svc2"));

        manager.register(service1).unwrap();
        manager.register(service2).unwrap();
        manager.start_all().await.unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;

        let uptimes = manager.uptimes();
        assert_eq!(uptimes.len(), 2);
        assert!(uptimes.contains_key("svc1"));
        assert!(uptimes.contains_key("svc2"));

        manager.shutdown().await.unwrap();
    }
}
