//! Tiered Storage Integration for Automatic Data Migration
//!
//! This module provides tiered storage functionality for automatic migration of cold
//! chunks to cheaper storage tiers with transparent read-through capabilities.
//!
//! # Architecture
//!
//! The tiered storage system uses three tiers:
//! - **Hot**: Local SSD storage for recent, frequently accessed data
//! - **Warm**: Local HDD storage for older, less frequently accessed data
//! - **Cold**: Remote object storage (S3, GCS) for archival data
//!
//! # Example
//!
//! ```rust,no_run
//! use kuba_tsdb::storage::tiered::{TierConfig, TierManager, StorageTier};
//!
//! // Create tiered storage configuration
//! let config = TierConfig::default();
//! let manager = TierManager::new(config, "/data".into()).expect("Failed to create manager");
//!
//! // Data is automatically migrated based on age and access patterns
//! ```

use crate::types::ChunkId;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Read, Write as IoWrite};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

// ============================================================================
// Constants
// ============================================================================

/// Default age threshold for hot to warm migration (7 days)
const DEFAULT_HOT_TO_WARM_AGE_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// Default age threshold for warm to cold migration (30 days)
const DEFAULT_WARM_TO_COLD_AGE_MS: u64 = 30 * 24 * 60 * 60 * 1000;

/// Default access count threshold for keeping data hot
const DEFAULT_ACCESS_THRESHOLD: u64 = 10;

/// Default cache size for cold tier data (100 MB)
const DEFAULT_COLD_CACHE_SIZE: usize = 100 * 1024 * 1024;

/// Default migration batch size
const DEFAULT_MIGRATION_BATCH_SIZE: usize = 10;

/// Default migration interval (5 minutes)
const DEFAULT_MIGRATION_INTERVAL_MS: u64 = 5 * 60 * 1000;

// ============================================================================
// Error Types
// ============================================================================

/// Errors that can occur during tiered storage operations
#[derive(Debug)]
pub enum TieredStorageError {
    /// I/O error during file operations
    Io(io::Error),
    /// Chunk not found in any tier
    ChunkNotFound {
        /// The chunk ID that was not found
        chunk_id: ChunkId,
    },
    /// Migration failed
    MigrationFailed {
        /// The chunk ID that failed to migrate
        chunk_id: ChunkId,
        /// Reason for the failure
        reason: String,
    },
    /// Remote storage error
    RemoteStorageError {
        /// Description of the remote storage error
        message: String,
    },
    /// Configuration error
    ConfigError {
        /// Description of the configuration error
        message: String,
    },
    /// Tier not available
    TierNotAvailable {
        /// The tier that is not available
        tier: StorageTier,
    },
    /// Cache error
    CacheError {
        /// Description of the cache error
        message: String,
    },
}

impl std::fmt::Display for TieredStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TieredStorageError::Io(e) => write!(f, "I/O error: {}", e),
            TieredStorageError::ChunkNotFound { chunk_id } => {
                write!(f, "Chunk not found: {:?}", chunk_id)
            },
            TieredStorageError::MigrationFailed { chunk_id, reason } => {
                write!(f, "Migration failed for {:?}: {}", chunk_id, reason)
            },
            TieredStorageError::RemoteStorageError { message } => {
                write!(f, "Remote storage error: {}", message)
            },
            TieredStorageError::ConfigError { message } => {
                write!(f, "Configuration error: {}", message)
            },
            TieredStorageError::TierNotAvailable { tier } => {
                write!(f, "Tier not available: {:?}", tier)
            },
            TieredStorageError::CacheError { message } => {
                write!(f, "Cache error: {}", message)
            },
        }
    }
}

impl std::error::Error for TieredStorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TieredStorageError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for TieredStorageError {
    fn from(err: io::Error) -> Self {
        TieredStorageError::Io(err)
    }
}

// ============================================================================
// Storage Tiers
// ============================================================================

/// Storage tier classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum StorageTier {
    /// Hot tier: Local SSD storage for recent, frequently accessed data
    /// Fastest access but most expensive
    #[default]
    Hot,

    /// Warm tier: Local HDD storage for older, less frequently accessed data
    /// Medium access speed and cost
    Warm,

    /// Cold tier: Remote object storage (S3, GCS) for archival data
    /// Slowest access but cheapest storage
    Cold,
}

impl StorageTier {
    /// Get the relative cost of this tier (higher = more expensive)
    pub fn relative_cost(&self) -> u32 {
        match self {
            StorageTier::Hot => 100,
            StorageTier::Warm => 30,
            StorageTier::Cold => 5,
        }
    }

    /// Get the relative latency of this tier (higher = slower)
    pub fn relative_latency(&self) -> u32 {
        match self {
            StorageTier::Hot => 1,
            StorageTier::Warm => 5,
            StorageTier::Cold => 50,
        }
    }

    /// Check if this tier is local (vs remote)
    pub fn is_local(&self) -> bool {
        matches!(self, StorageTier::Hot | StorageTier::Warm)
    }

    /// Get the next colder tier (for downward migration)
    pub fn colder(&self) -> Option<StorageTier> {
        match self {
            StorageTier::Hot => Some(StorageTier::Warm),
            StorageTier::Warm => Some(StorageTier::Cold),
            StorageTier::Cold => None,
        }
    }

    /// Get the next warmer tier (for upward migration)
    pub fn warmer(&self) -> Option<StorageTier> {
        match self {
            StorageTier::Hot => None,
            StorageTier::Warm => Some(StorageTier::Hot),
            StorageTier::Cold => Some(StorageTier::Warm),
        }
    }
}

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for tiered storage
#[derive(Debug, Clone)]
pub struct TierConfig {
    /// Age threshold for hot to warm migration (milliseconds)
    pub hot_to_warm_age_ms: u64,

    /// Age threshold for warm to cold migration (milliseconds)
    pub warm_to_cold_age_ms: u64,

    /// Access count threshold for keeping data in hot tier
    /// Data accessed more than this many times stays hot regardless of age
    pub access_threshold: u64,

    /// Maximum size of the cold tier cache (bytes)
    pub cold_cache_size: usize,

    /// Number of chunks to migrate per batch
    pub migration_batch_size: usize,

    /// Interval between migration cycles (milliseconds)
    pub migration_interval_ms: u64,

    /// Whether to enable automatic migration
    pub auto_migration_enabled: bool,

    /// Path to hot tier storage
    pub hot_path: Option<PathBuf>,

    /// Path to warm tier storage
    pub warm_path: Option<PathBuf>,

    /// Remote storage configuration for cold tier
    pub remote_config: Option<RemoteStorageConfig>,
}

impl Default for TierConfig {
    fn default() -> Self {
        Self {
            hot_to_warm_age_ms: DEFAULT_HOT_TO_WARM_AGE_MS,
            warm_to_cold_age_ms: DEFAULT_WARM_TO_COLD_AGE_MS,
            access_threshold: DEFAULT_ACCESS_THRESHOLD,
            cold_cache_size: DEFAULT_COLD_CACHE_SIZE,
            migration_batch_size: DEFAULT_MIGRATION_BATCH_SIZE,
            migration_interval_ms: DEFAULT_MIGRATION_INTERVAL_MS,
            auto_migration_enabled: true,
            hot_path: None,
            warm_path: None,
            remote_config: None,
        }
    }
}

impl TierConfig {
    /// Create a configuration optimized for cost (aggressive migration to cold)
    pub fn optimize_cost() -> Self {
        Self {
            hot_to_warm_age_ms: 24 * 60 * 60 * 1000,      // 1 day
            warm_to_cold_age_ms: 7 * 24 * 60 * 60 * 1000, // 7 days
            access_threshold: 50, // Higher threshold to prevent frequent promotion
            cold_cache_size: 50 * 1024 * 1024, // Smaller cache
            migration_batch_size: 20, // Larger batches for efficiency
            migration_interval_ms: 60 * 1000, // More frequent migration (1 minute)
            auto_migration_enabled: true,
            hot_path: None,
            warm_path: None,
            remote_config: None,
        }
    }

    /// Create a configuration optimized for performance (keep data hot longer)
    pub fn optimize_performance() -> Self {
        Self {
            hot_to_warm_age_ms: 30 * 24 * 60 * 60 * 1000,  // 30 days
            warm_to_cold_age_ms: 90 * 24 * 60 * 60 * 1000, // 90 days
            access_threshold: 5, // Lower threshold for easier hot retention
            cold_cache_size: 500 * 1024 * 1024, // Larger cache
            migration_batch_size: 5, // Smaller batches
            migration_interval_ms: 30 * 60 * 1000, // Less frequent (30 minutes)
            auto_migration_enabled: true,
            hot_path: None,
            warm_path: None,
            remote_config: None,
        }
    }

    /// Create a testing configuration with short intervals
    pub fn testing() -> Self {
        Self {
            hot_to_warm_age_ms: 1000,  // 1 second
            warm_to_cold_age_ms: 2000, // 2 seconds
            access_threshold: 3,
            cold_cache_size: 1024 * 1024, // 1 MB
            migration_batch_size: 2,
            migration_interval_ms: 100,    // 100ms
            auto_migration_enabled: false, // Manual control for tests
            hot_path: None,
            warm_path: None,
            remote_config: None,
        }
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), TieredStorageError> {
        if self.hot_to_warm_age_ms == 0 {
            return Err(TieredStorageError::ConfigError {
                message: "hot_to_warm_age_ms must be greater than 0".to_string(),
            });
        }

        if self.warm_to_cold_age_ms == 0 {
            return Err(TieredStorageError::ConfigError {
                message: "warm_to_cold_age_ms must be greater than 0".to_string(),
            });
        }

        if self.hot_to_warm_age_ms >= self.warm_to_cold_age_ms {
            return Err(TieredStorageError::ConfigError {
                message: "hot_to_warm_age_ms must be less than warm_to_cold_age_ms".to_string(),
            });
        }

        if self.migration_batch_size == 0 {
            return Err(TieredStorageError::ConfigError {
                message: "migration_batch_size must be greater than 0".to_string(),
            });
        }

        Ok(())
    }

    /// Set the hot tier storage path
    pub fn with_hot_path(mut self, path: PathBuf) -> Self {
        self.hot_path = Some(path);
        self
    }

    /// Set the warm tier storage path
    pub fn with_warm_path(mut self, path: PathBuf) -> Self {
        self.warm_path = Some(path);
        self
    }

    /// Set the remote storage configuration
    pub fn with_remote_config(mut self, config: RemoteStorageConfig) -> Self {
        self.remote_config = Some(config);
        self
    }
}

/// Configuration for remote storage (S3, GCS, etc.)
#[derive(Debug, Clone)]
pub struct RemoteStorageConfig {
    /// Storage backend type
    pub backend: RemoteBackend,

    /// Bucket or container name
    pub bucket: String,

    /// Prefix for all objects
    pub prefix: String,

    /// Region (for AWS S3)
    pub region: Option<String>,

    /// Endpoint override (for S3-compatible storage)
    pub endpoint: Option<String>,

    /// Connection timeout (milliseconds)
    pub timeout_ms: u64,

    /// Maximum retries
    pub max_retries: u32,
}

impl Default for RemoteStorageConfig {
    fn default() -> Self {
        Self {
            backend: RemoteBackend::LocalSimulated,
            bucket: "tsdb-cold-storage".to_string(),
            prefix: "chunks/".to_string(),
            region: None,
            endpoint: None,
            timeout_ms: 30000,
            max_retries: 3,
        }
    }
}

/// Remote storage backend types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RemoteBackend {
    /// Amazon S3
    S3,

    /// Google Cloud Storage
    Gcs,

    /// Azure Blob Storage
    AzureBlob,

    /// Local filesystem simulating remote storage (for testing)
    #[default]
    LocalSimulated,
}

// ============================================================================
// Chunk Tier Information
// ============================================================================

/// Information about a chunk's tier placement
#[derive(Debug, Clone)]
pub struct ChunkTierInfo {
    /// The chunk identifier
    pub chunk_id: ChunkId,

    /// Current storage tier
    pub tier: StorageTier,

    /// Size in bytes
    pub size_bytes: u64,

    /// Creation timestamp (milliseconds since epoch)
    pub created_at: u64,

    /// Last access timestamp (milliseconds since epoch)
    pub last_accessed_at: u64,

    /// Total access count
    pub access_count: u64,

    /// Last migration timestamp (if migrated)
    pub last_migrated_at: Option<u64>,

    /// Path to the chunk data
    pub path: PathBuf,
}

impl ChunkTierInfo {
    /// Create new chunk tier info
    pub fn new(chunk_id: ChunkId, tier: StorageTier, size_bytes: u64, path: PathBuf) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            chunk_id,
            tier,
            size_bytes,
            created_at: now,
            last_accessed_at: now,
            access_count: 0,
            last_migrated_at: None,
            path,
        }
    }

    /// Get the age of this chunk in milliseconds
    pub fn age_ms(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        now.saturating_sub(self.created_at)
    }

    /// Get the time since last access in milliseconds
    pub fn idle_time_ms(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        now.saturating_sub(self.last_accessed_at)
    }

    /// Record an access to this chunk
    pub fn record_access(&mut self) {
        self.access_count += 1;
        self.last_accessed_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
    }

    /// Update the tier after migration
    pub fn update_tier(&mut self, new_tier: StorageTier, new_path: PathBuf) {
        self.tier = new_tier;
        self.path = new_path;
        self.last_migrated_at = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        );
    }
}

// ============================================================================
// Tier Policy
// ============================================================================

/// Policy for determining tier placement and migration
#[derive(Debug, Clone)]
pub struct TierPolicy {
    /// Configuration settings
    config: TierConfig,
}

impl TierPolicy {
    /// Create a new tier policy with the given configuration
    pub fn new(config: TierConfig) -> Self {
        Self { config }
    }

    /// Determine if a chunk should be migrated
    pub fn should_migrate(&self, info: &ChunkTierInfo) -> bool {
        match info.tier {
            StorageTier::Hot => self.should_migrate_to_warm(info),
            StorageTier::Warm => self.should_migrate_to_cold(info),
            StorageTier::Cold => false, // Cold is the final tier
        }
    }

    /// Determine the target tier for a chunk
    pub fn target_tier(&self, info: &ChunkTierInfo) -> StorageTier {
        // If frequently accessed, keep hot
        if info.access_count >= self.config.access_threshold {
            return StorageTier::Hot;
        }

        let age = info.age_ms();

        if age >= self.config.warm_to_cold_age_ms {
            StorageTier::Cold
        } else if age >= self.config.hot_to_warm_age_ms {
            StorageTier::Warm
        } else {
            StorageTier::Hot
        }
    }

    /// Check if a chunk should be promoted (moved to a warmer tier)
    pub fn should_promote(&self, info: &ChunkTierInfo) -> bool {
        // Only promote if frequently accessed
        info.access_count >= self.config.access_threshold && info.tier != StorageTier::Hot
    }

    /// Get the promotion target tier
    pub fn promotion_target(&self, info: &ChunkTierInfo) -> Option<StorageTier> {
        if self.should_promote(info) {
            info.tier.warmer()
        } else {
            None
        }
    }

    /// Check if a hot chunk should migrate to warm
    fn should_migrate_to_warm(&self, info: &ChunkTierInfo) -> bool {
        // Don't migrate if frequently accessed
        if info.access_count >= self.config.access_threshold {
            return false;
        }

        info.age_ms() >= self.config.hot_to_warm_age_ms
    }

    /// Check if a warm chunk should migrate to cold
    fn should_migrate_to_cold(&self, info: &ChunkTierInfo) -> bool {
        // Don't migrate if frequently accessed (promote instead)
        if info.access_count >= self.config.access_threshold {
            return false;
        }

        info.age_ms() >= self.config.warm_to_cold_age_ms
    }
}

// ============================================================================
// Remote Storage Trait
// ============================================================================

/// Trait for remote storage backends
pub trait RemoteStorage: Send + Sync {
    /// Upload data to remote storage
    fn upload(&self, key: &str, data: &[u8]) -> Result<(), TieredStorageError>;

    /// Download data from remote storage
    fn download(&self, key: &str) -> Result<Vec<u8>, TieredStorageError>;

    /// Delete data from remote storage
    fn delete(&self, key: &str) -> Result<(), TieredStorageError>;

    /// Check if a key exists
    fn exists(&self, key: &str) -> Result<bool, TieredStorageError>;

    /// List keys with a given prefix
    fn list(&self, prefix: &str) -> Result<Vec<String>, TieredStorageError>;
}

// ============================================================================
// Local Simulated Remote Storage
// ============================================================================

/// Local filesystem implementation of remote storage (for testing)
pub struct LocalSimulatedStorage {
    /// Base path for simulated remote storage
    base_path: PathBuf,

    /// Simulated latency (milliseconds)
    latency_ms: u64,
}

impl LocalSimulatedStorage {
    /// Create a new local simulated storage
    pub fn new(base_path: PathBuf) -> Result<Self, TieredStorageError> {
        fs::create_dir_all(&base_path)?;
        Ok(Self {
            base_path,
            latency_ms: 0,
        })
    }

    /// Create with simulated latency
    pub fn with_latency(mut self, latency_ms: u64) -> Self {
        self.latency_ms = latency_ms;
        self
    }

    /// Get the full path for a key
    fn key_path(&self, key: &str) -> PathBuf {
        self.base_path.join(key.replace('/', "_"))
    }

    /// Simulate latency
    fn simulate_latency(&self) {
        if self.latency_ms > 0 {
            std::thread::sleep(Duration::from_millis(self.latency_ms));
        }
    }
}

impl RemoteStorage for LocalSimulatedStorage {
    fn upload(&self, key: &str, data: &[u8]) -> Result<(), TieredStorageError> {
        self.simulate_latency();

        let path = self.key_path(key);
        let mut file = File::create(&path)?;
        file.write_all(data)?;
        file.sync_all()?;

        debug!("Uploaded {} bytes to simulated remote: {}", data.len(), key);
        Ok(())
    }

    fn download(&self, key: &str) -> Result<Vec<u8>, TieredStorageError> {
        self.simulate_latency();

        let path = self.key_path(key);
        let mut file = File::open(&path).map_err(|e| {
            if e.kind() == io::ErrorKind::NotFound {
                TieredStorageError::RemoteStorageError {
                    message: format!("Key not found: {}", key),
                }
            } else {
                TieredStorageError::Io(e)
            }
        })?;

        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        debug!(
            "Downloaded {} bytes from simulated remote: {}",
            data.len(),
            key
        );
        Ok(data)
    }

    fn delete(&self, key: &str) -> Result<(), TieredStorageError> {
        self.simulate_latency();

        let path = self.key_path(key);
        if path.exists() {
            fs::remove_file(&path)?;
        }

        debug!("Deleted from simulated remote: {}", key);
        Ok(())
    }

    fn exists(&self, key: &str) -> Result<bool, TieredStorageError> {
        self.simulate_latency();
        Ok(self.key_path(key).exists())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, TieredStorageError> {
        self.simulate_latency();

        let mut keys = Vec::new();
        let prefix_normalized = prefix.replace('/', "_");

        for entry in fs::read_dir(&self.base_path)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with(&prefix_normalized) {
                keys.push(name.replace('_', "/"));
            }
        }

        Ok(keys)
    }
}

// ============================================================================
// Cold Tier Cache
// ============================================================================

/// LRU cache for cold tier data
pub struct ColdTierCache {
    /// Maximum cache size in bytes
    max_size: usize,

    /// Current cache size
    current_size: AtomicUsize,

    /// Cached data: key -> (data, last_access_time)
    cache: RwLock<HashMap<String, CacheEntry>>,

    /// Cache hit counter
    hits: AtomicU64,

    /// Cache miss counter
    misses: AtomicU64,
}

/// Entry in the cold tier cache
#[derive(Clone)]
struct CacheEntry {
    /// Cached data
    data: Vec<u8>,

    /// Last access timestamp
    last_access: Instant,

    /// Size in bytes
    size: usize,
}

impl ColdTierCache {
    /// Create a new cache with the given maximum size
    pub fn new(max_size: usize) -> Self {
        Self {
            max_size,
            current_size: AtomicUsize::new(0),
            cache: RwLock::new(HashMap::new()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Get data from the cache
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let mut cache = self.cache.write().ok()?;

        if let Some(entry) = cache.get_mut(key) {
            entry.last_access = Instant::now();
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.data.clone())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Put data into the cache
    pub fn put(&self, key: String, data: Vec<u8>) {
        let size = data.len();

        // Don't cache if larger than max size
        if size > self.max_size {
            return;
        }

        let mut cache = self.cache.write().unwrap();

        // Evict if necessary
        while self.current_size.load(Ordering::Relaxed) + size > self.max_size {
            if !self.evict_oldest(&mut cache) {
                break;
            }
        }

        // Remove old entry if exists
        if let Some(old) = cache.remove(&key) {
            self.current_size.fetch_sub(old.size, Ordering::Relaxed);
        }

        // Insert new entry
        cache.insert(
            key,
            CacheEntry {
                data,
                last_access: Instant::now(),
                size,
            },
        );
        self.current_size.fetch_add(size, Ordering::Relaxed);
    }

    /// Evict the oldest (least recently accessed) entry
    fn evict_oldest(&self, cache: &mut HashMap<String, CacheEntry>) -> bool {
        let oldest_key = cache
            .iter()
            .min_by_key(|(_, entry)| entry.last_access)
            .map(|(key, _)| key.clone());

        if let Some(key) = oldest_key {
            if let Some(entry) = cache.remove(&key) {
                self.current_size.fetch_sub(entry.size, Ordering::Relaxed);
                return true;
            }
        }

        false
    }

    /// Remove an entry from the cache
    pub fn remove(&self, key: &str) {
        if let Ok(mut cache) = self.cache.write() {
            if let Some(entry) = cache.remove(key) {
                self.current_size.fetch_sub(entry.size, Ordering::Relaxed);
            }
        }
    }

    /// Clear the entire cache
    pub fn clear(&self) {
        if let Ok(mut cache) = self.cache.write() {
            cache.clear();
            self.current_size.store(0, Ordering::Relaxed);
        }
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            size_bytes: self.current_size.load(Ordering::Relaxed),
            max_size_bytes: self.max_size,
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            entry_count: self.cache.read().map(|c| c.len()).unwrap_or(0),
        }
    }
}

/// Cache statistics snapshot
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// Current size in bytes
    pub size_bytes: usize,

    /// Maximum size in bytes
    pub max_size_bytes: usize,

    /// Number of cache hits
    pub hits: u64,

    /// Number of cache misses
    pub misses: u64,

    /// Number of entries in cache
    pub entry_count: usize,
}

impl CacheStats {
    /// Calculate the hit rate (0.0 to 1.0)
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

// ============================================================================
// Tier Manager
// ============================================================================

/// Manages tiered storage operations
pub struct TierManager {
    /// Configuration
    config: TierConfig,

    /// Base data directory
    base_path: PathBuf,

    /// Tier policy
    policy: TierPolicy,

    /// Chunk tier information
    chunk_info: RwLock<HashMap<ChunkId, ChunkTierInfo>>,

    /// Remote storage backend
    remote_storage: Option<Arc<dyn RemoteStorage>>,

    /// Cold tier cache
    cold_cache: ColdTierCache,

    /// Statistics
    stats: TierManagerStats,
}

/// Statistics for the tier manager
struct TierManagerStats {
    /// Total migrations performed
    migrations: AtomicU64,

    /// Bytes migrated
    bytes_migrated: AtomicU64,

    /// Migration errors
    migration_errors: AtomicU64,

    /// Reads from hot tier
    hot_reads: AtomicU64,

    /// Reads from warm tier
    warm_reads: AtomicU64,

    /// Reads from cold tier
    cold_reads: AtomicU64,
}

impl Default for TierManagerStats {
    fn default() -> Self {
        Self {
            migrations: AtomicU64::new(0),
            bytes_migrated: AtomicU64::new(0),
            migration_errors: AtomicU64::new(0),
            hot_reads: AtomicU64::new(0),
            warm_reads: AtomicU64::new(0),
            cold_reads: AtomicU64::new(0),
        }
    }
}

impl TierManager {
    /// Create a new tier manager
    pub fn new(config: TierConfig, base_path: PathBuf) -> Result<Self, TieredStorageError> {
        config.validate()?;

        // Create tier directories
        let hot_path = config
            .hot_path
            .clone()
            .unwrap_or_else(|| base_path.join("hot"));
        let warm_path = config
            .warm_path
            .clone()
            .unwrap_or_else(|| base_path.join("warm"));
        let cold_path = base_path.join("cold");

        fs::create_dir_all(&hot_path)?;
        fs::create_dir_all(&warm_path)?;
        fs::create_dir_all(&cold_path)?;

        // Create remote storage if configured
        let remote_storage: Option<Arc<dyn RemoteStorage>> =
            if let Some(ref remote_config) = config.remote_config {
                match remote_config.backend {
                    RemoteBackend::LocalSimulated => {
                        Some(Arc::new(LocalSimulatedStorage::new(cold_path)?))
                    },
                    _ => {
                        // For S3, GCS, Azure - would need actual SDK integration
                        // For now, fall back to local simulated
                        warn!(
                            "Remote backend {:?} not implemented, using local simulated",
                            remote_config.backend
                        );
                        Some(Arc::new(LocalSimulatedStorage::new(cold_path)?))
                    },
                }
            } else {
                Some(Arc::new(LocalSimulatedStorage::new(cold_path)?))
            };

        let policy = TierPolicy::new(config.clone());
        let cold_cache = ColdTierCache::new(config.cold_cache_size);

        Ok(Self {
            config,
            base_path,
            policy,
            chunk_info: RwLock::new(HashMap::new()),
            remote_storage,
            cold_cache,
            stats: TierManagerStats::default(),
        })
    }

    /// Register a new chunk in the hot tier
    pub fn register_chunk(
        &self,
        chunk_id: &ChunkId,
        data: &[u8],
    ) -> Result<PathBuf, TieredStorageError> {
        let hot_path = self
            .config
            .hot_path
            .clone()
            .unwrap_or_else(|| self.base_path.join("hot"));

        let chunk_path = hot_path.join(format!("{}.chunk", chunk_id));

        // Write data to hot tier
        let mut file = File::create(&chunk_path)?;
        file.write_all(data)?;
        file.sync_all()?;

        // Register chunk info
        let info = ChunkTierInfo::new(
            chunk_id.clone(),
            StorageTier::Hot,
            data.len() as u64,
            chunk_path.clone(),
        );

        let mut chunk_info = self.chunk_info.write().unwrap();
        chunk_info.insert(chunk_id.clone(), info);

        debug!(
            "Registered chunk {:?} in hot tier ({} bytes)",
            chunk_id,
            data.len()
        );
        Ok(chunk_path)
    }

    /// Get the current tier for a chunk
    pub fn get_tier(&self, chunk_id: &ChunkId) -> Option<StorageTier> {
        self.chunk_info
            .read()
            .ok()
            .and_then(|info| info.get(chunk_id).map(|i| i.tier))
    }

    /// Get chunk tier information
    pub fn get_chunk_info(&self, chunk_id: &ChunkId) -> Option<ChunkTierInfo> {
        self.chunk_info
            .read()
            .ok()
            .and_then(|info| info.get(chunk_id).cloned())
    }

    /// Read a chunk from any tier
    pub fn read_chunk(&self, chunk_id: &ChunkId) -> Result<Vec<u8>, TieredStorageError> {
        let info = self
            .chunk_info
            .read()
            .unwrap()
            .get(chunk_id)
            .cloned()
            .ok_or_else(|| TieredStorageError::ChunkNotFound {
                chunk_id: chunk_id.clone(),
            })?;

        let data = match info.tier {
            StorageTier::Hot => {
                self.stats.hot_reads.fetch_add(1, Ordering::Relaxed);
                self.read_local(&info.path)?
            },
            StorageTier::Warm => {
                self.stats.warm_reads.fetch_add(1, Ordering::Relaxed);
                self.read_local(&info.path)?
            },
            StorageTier::Cold => {
                self.stats.cold_reads.fetch_add(1, Ordering::Relaxed);
                self.read_cold(chunk_id)?
            },
        };

        // Update access info
        if let Ok(mut chunk_info) = self.chunk_info.write() {
            if let Some(info) = chunk_info.get_mut(chunk_id) {
                info.record_access();
            }
        }

        Ok(data)
    }

    /// Read from local storage
    fn read_local(&self, path: &Path) -> Result<Vec<u8>, TieredStorageError> {
        let mut file = File::open(path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        Ok(data)
    }

    /// Read from cold tier (with caching)
    fn read_cold(&self, chunk_id: &ChunkId) -> Result<Vec<u8>, TieredStorageError> {
        let key = format!("{}.chunk", chunk_id);

        // Check cache first
        if let Some(data) = self.cold_cache.get(&key) {
            debug!("Cold cache hit for chunk {:?}", chunk_id);
            return Ok(data);
        }

        // Fetch from remote storage
        let remote = self
            .remote_storage
            .as_ref()
            .ok_or(TieredStorageError::TierNotAvailable {
                tier: StorageTier::Cold,
            })?;

        let data = remote.download(&key)?;

        // Cache the data
        self.cold_cache.put(key, data.clone());

        Ok(data)
    }

    /// Migrate a chunk to a target tier
    pub fn migrate(
        &self,
        chunk_id: &ChunkId,
        target_tier: StorageTier,
    ) -> Result<(), TieredStorageError> {
        let info = self
            .chunk_info
            .read()
            .unwrap()
            .get(chunk_id)
            .cloned()
            .ok_or_else(|| TieredStorageError::ChunkNotFound {
                chunk_id: chunk_id.clone(),
            })?;

        if info.tier == target_tier {
            return Ok(()); // Already in target tier
        }

        // Read current data
        let data = self.read_chunk(chunk_id)?;

        // Write to target tier
        let new_path = match target_tier {
            StorageTier::Hot => {
                let hot_path = self
                    .config
                    .hot_path
                    .clone()
                    .unwrap_or_else(|| self.base_path.join("hot"));
                let path = hot_path.join(format!("{}.chunk", chunk_id));
                let mut file = File::create(&path)?;
                file.write_all(&data)?;
                file.sync_all()?;
                path
            },
            StorageTier::Warm => {
                let warm_path = self
                    .config
                    .warm_path
                    .clone()
                    .unwrap_or_else(|| self.base_path.join("warm"));
                let path = warm_path.join(format!("{}.chunk", chunk_id));
                let mut file = File::create(&path)?;
                file.write_all(&data)?;
                file.sync_all()?;
                path
            },
            StorageTier::Cold => {
                let key = format!("{}.chunk", chunk_id);
                let remote =
                    self.remote_storage
                        .as_ref()
                        .ok_or(TieredStorageError::TierNotAvailable {
                            tier: StorageTier::Cold,
                        })?;
                remote.upload(&key, &data)?;
                PathBuf::from(format!("cold:{}", key))
            },
        };

        // Remove from old location
        if info.tier.is_local() && info.path.exists() {
            fs::remove_file(&info.path)?;
        } else if info.tier == StorageTier::Cold {
            let key = format!("{}.chunk", chunk_id);
            if let Some(remote) = &self.remote_storage {
                let _ = remote.delete(&key); // Best effort deletion
            }
            self.cold_cache.remove(&key);
        }

        // Update chunk info
        if let Ok(mut chunk_info) = self.chunk_info.write() {
            if let Some(info) = chunk_info.get_mut(chunk_id) {
                info.update_tier(target_tier, new_path);
            }
        }

        // Update stats
        self.stats.migrations.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_migrated
            .fetch_add(data.len() as u64, Ordering::Relaxed);

        info!(
            "Migrated chunk {:?} from {:?} to {:?} ({} bytes)",
            chunk_id,
            info.tier,
            target_tier,
            data.len()
        );

        Ok(())
    }

    /// Find chunks that should be migrated
    pub fn find_migration_candidates(&self) -> Vec<(ChunkId, StorageTier)> {
        let chunk_info = self.chunk_info.read().unwrap();
        let mut candidates = Vec::new();

        for (chunk_id, info) in chunk_info.iter() {
            if self.policy.should_migrate(info) {
                if let Some(target) = info.tier.colder() {
                    candidates.push((chunk_id.clone(), target));
                }
            }
        }

        // Sort by age (oldest first)
        candidates.sort_by(|a, b| {
            let info_a = chunk_info.get(&a.0);
            let info_b = chunk_info.get(&b.0);
            match (info_a, info_b) {
                (Some(a), Some(b)) => b.age_ms().cmp(&a.age_ms()), // Descending
                _ => std::cmp::Ordering::Equal,
            }
        });

        candidates
    }

    /// Find chunks that should be promoted (moved to warmer tier)
    pub fn find_promotion_candidates(&self) -> Vec<(ChunkId, StorageTier)> {
        let chunk_info = self.chunk_info.read().unwrap();
        let mut candidates = Vec::new();

        for (chunk_id, info) in chunk_info.iter() {
            if let Some(target) = self.policy.promotion_target(info) {
                candidates.push((chunk_id.clone(), target));
            }
        }

        candidates
    }

    /// Get tier statistics
    pub fn stats(&self) -> TierManagerStatsSnapshot {
        let chunk_info = self.chunk_info.read().unwrap();

        let mut hot_chunks = 0u64;
        let mut warm_chunks = 0u64;
        let mut cold_chunks = 0u64;
        let mut hot_bytes = 0u64;
        let mut warm_bytes = 0u64;
        let mut cold_bytes = 0u64;

        for info in chunk_info.values() {
            match info.tier {
                StorageTier::Hot => {
                    hot_chunks += 1;
                    hot_bytes += info.size_bytes;
                },
                StorageTier::Warm => {
                    warm_chunks += 1;
                    warm_bytes += info.size_bytes;
                },
                StorageTier::Cold => {
                    cold_chunks += 1;
                    cold_bytes += info.size_bytes;
                },
            }
        }

        TierManagerStatsSnapshot {
            hot_chunks,
            warm_chunks,
            cold_chunks,
            hot_bytes,
            warm_bytes,
            cold_bytes,
            total_migrations: self.stats.migrations.load(Ordering::Relaxed),
            bytes_migrated: self.stats.bytes_migrated.load(Ordering::Relaxed),
            migration_errors: self.stats.migration_errors.load(Ordering::Relaxed),
            hot_reads: self.stats.hot_reads.load(Ordering::Relaxed),
            warm_reads: self.stats.warm_reads.load(Ordering::Relaxed),
            cold_reads: self.stats.cold_reads.load(Ordering::Relaxed),
            cache_stats: self.cold_cache.stats(),
        }
    }

    /// Remove a chunk from all tiers
    pub fn remove_chunk(&self, chunk_id: &ChunkId) -> Result<(), TieredStorageError> {
        let info = self
            .chunk_info
            .write()
            .unwrap()
            .remove(chunk_id)
            .ok_or_else(|| TieredStorageError::ChunkNotFound {
                chunk_id: chunk_id.clone(),
            })?;

        // Remove from storage
        if info.tier.is_local() && info.path.exists() {
            fs::remove_file(&info.path)?;
        } else if info.tier == StorageTier::Cold {
            let key = format!("{}.chunk", chunk_id);
            if let Some(remote) = &self.remote_storage {
                remote.delete(&key)?;
            }
            self.cold_cache.remove(&key);
        }

        debug!("Removed chunk {:?} from {:?} tier", chunk_id, info.tier);
        Ok(())
    }

    /// Get the policy being used
    pub fn policy(&self) -> &TierPolicy {
        &self.policy
    }
}

/// Snapshot of tier manager statistics
#[derive(Debug, Clone, Default)]
pub struct TierManagerStatsSnapshot {
    /// Number of chunks in hot tier
    pub hot_chunks: u64,

    /// Number of chunks in warm tier
    pub warm_chunks: u64,

    /// Number of chunks in cold tier
    pub cold_chunks: u64,

    /// Total bytes in hot tier
    pub hot_bytes: u64,

    /// Total bytes in warm tier
    pub warm_bytes: u64,

    /// Total bytes in cold tier
    pub cold_bytes: u64,

    /// Total number of migrations performed
    pub total_migrations: u64,

    /// Total bytes migrated
    pub bytes_migrated: u64,

    /// Number of migration errors
    pub migration_errors: u64,

    /// Number of reads from hot tier
    pub hot_reads: u64,

    /// Number of reads from warm tier
    pub warm_reads: u64,

    /// Number of reads from cold tier
    pub cold_reads: u64,

    /// Cold tier cache statistics
    pub cache_stats: CacheStats,
}

impl TierManagerStatsSnapshot {
    /// Get the total number of chunks across all tiers
    pub fn total_chunks(&self) -> u64 {
        self.hot_chunks + self.warm_chunks + self.cold_chunks
    }

    /// Get the total bytes across all tiers
    pub fn total_bytes(&self) -> u64 {
        self.hot_bytes + self.warm_bytes + self.cold_bytes
    }
}

// ============================================================================
// Tier Migrator
// ============================================================================

/// Background service for automatic tier migration
pub struct TierMigrator {
    /// Tier manager reference
    manager: Arc<TierManager>,

    /// Whether the migrator is running
    running: Arc<std::sync::atomic::AtomicBool>,

    /// Migration statistics
    stats: TierMigratorStats,
}

/// Statistics for the tier migrator
struct TierMigratorStats {
    /// Successful migrations
    successful_migrations: AtomicU64,

    /// Failed migrations
    failed_migrations: AtomicU64,

    /// Successful promotions
    successful_promotions: AtomicU64,

    /// Migration cycles run
    cycles: AtomicU64,
}

impl Default for TierMigratorStats {
    fn default() -> Self {
        Self {
            successful_migrations: AtomicU64::new(0),
            failed_migrations: AtomicU64::new(0),
            successful_promotions: AtomicU64::new(0),
            cycles: AtomicU64::new(0),
        }
    }
}

impl TierMigrator {
    /// Create a new tier migrator
    pub fn new(manager: Arc<TierManager>) -> Self {
        Self {
            manager,
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            stats: TierMigratorStats::default(),
        }
    }

    /// Check if the migrator is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Run a single migration cycle
    pub fn run_cycle(&self) -> MigrationCycleResult {
        self.stats.cycles.fetch_add(1, Ordering::Relaxed);

        let mut result = MigrationCycleResult::default();

        // Find and execute migrations (hot/warm to colder)
        let migration_candidates = self.manager.find_migration_candidates();
        let batch_size = self.manager.config.migration_batch_size;

        for (chunk_id, target_tier) in migration_candidates.into_iter().take(batch_size) {
            match self.manager.migrate(&chunk_id, target_tier) {
                Ok(_) => {
                    result.successful_migrations += 1;
                    self.stats
                        .successful_migrations
                        .fetch_add(1, Ordering::Relaxed);
                },
                Err(e) => {
                    result.failed_migrations += 1;
                    self.stats.failed_migrations.fetch_add(1, Ordering::Relaxed);
                    error!("Migration failed for chunk {:?}: {}", chunk_id, e);
                },
            }
        }

        // Find and execute promotions (cold/warm to warmer)
        let promotion_candidates = self.manager.find_promotion_candidates();

        for (chunk_id, target_tier) in promotion_candidates.into_iter().take(batch_size) {
            match self.manager.migrate(&chunk_id, target_tier) {
                Ok(_) => {
                    result.successful_promotions += 1;
                    self.stats
                        .successful_promotions
                        .fetch_add(1, Ordering::Relaxed);
                },
                Err(e) => {
                    result.failed_promotions += 1;
                    error!("Promotion failed for chunk {:?}: {}", chunk_id, e);
                },
            }
        }

        result
    }

    /// Start the background migration service
    pub fn start(&self) -> MigrationHandle {
        self.running.store(true, Ordering::Relaxed);

        let running = self.running.clone();
        let manager = self.manager.clone();
        let interval_ms = self.manager.config.migration_interval_ms;
        let stats = Arc::new(Mutex::new(MigrationCycleResult::default()));
        let stats_clone = stats.clone();

        let handle = std::thread::spawn(move || {
            info!("Tier migration service started");

            while running.load(Ordering::Relaxed) {
                // Run migration cycle
                let migrator = TierMigrator::new(manager.clone());
                let result = migrator.run_cycle();

                // Update stats
                if let Ok(mut s) = stats_clone.lock() {
                    s.successful_migrations += result.successful_migrations;
                    s.failed_migrations += result.failed_migrations;
                    s.successful_promotions += result.successful_promotions;
                    s.failed_promotions += result.failed_promotions;
                }

                // Sleep until next cycle
                std::thread::sleep(Duration::from_millis(interval_ms));
            }

            info!("Tier migration service stopped");
        });

        MigrationHandle {
            running: self.running.clone(),
            stats,
            _thread: Some(handle),
        }
    }

    /// Get migrator statistics
    pub fn stats(&self) -> TierMigratorStatsSnapshot {
        TierMigratorStatsSnapshot {
            successful_migrations: self.stats.successful_migrations.load(Ordering::Relaxed),
            failed_migrations: self.stats.failed_migrations.load(Ordering::Relaxed),
            successful_promotions: self.stats.successful_promotions.load(Ordering::Relaxed),
            cycles: self.stats.cycles.load(Ordering::Relaxed),
        }
    }
}

/// Handle for the migration background service
pub struct MigrationHandle {
    /// Running flag
    running: Arc<std::sync::atomic::AtomicBool>,

    /// Accumulated statistics
    stats: Arc<Mutex<MigrationCycleResult>>,

    /// Background thread (for cleanup on drop)
    _thread: Option<std::thread::JoinHandle<()>>,
}

impl MigrationHandle {
    /// Stop the migration service
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    /// Check if the service is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Get accumulated statistics
    pub fn stats(&self) -> MigrationCycleResult {
        self.stats.lock().map(|s| s.clone()).unwrap_or_default()
    }
}

/// Result of a migration cycle
#[derive(Debug, Clone, Default)]
pub struct MigrationCycleResult {
    /// Number of successful migrations (to colder tier)
    pub successful_migrations: u64,

    /// Number of failed migrations
    pub failed_migrations: u64,

    /// Number of successful promotions (to warmer tier)
    pub successful_promotions: u64,

    /// Number of failed promotions
    pub failed_promotions: u64,
}

/// Snapshot of migrator statistics
#[derive(Debug, Clone, Default)]
pub struct TierMigratorStatsSnapshot {
    /// Total successful migrations
    pub successful_migrations: u64,

    /// Total failed migrations
    pub failed_migrations: u64,

    /// Total successful promotions
    pub successful_promotions: u64,

    /// Total migration cycles run
    pub cycles: u64,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // --- Storage Tier Tests ---

    #[test]
    fn test_storage_tier_default() {
        let tier = StorageTier::default();
        assert_eq!(tier, StorageTier::Hot);
    }

    #[test]
    fn test_storage_tier_relative_cost() {
        assert!(StorageTier::Hot.relative_cost() > StorageTier::Warm.relative_cost());
        assert!(StorageTier::Warm.relative_cost() > StorageTier::Cold.relative_cost());
    }

    #[test]
    fn test_storage_tier_relative_latency() {
        assert!(StorageTier::Hot.relative_latency() < StorageTier::Warm.relative_latency());
        assert!(StorageTier::Warm.relative_latency() < StorageTier::Cold.relative_latency());
    }

    #[test]
    fn test_storage_tier_is_local() {
        assert!(StorageTier::Hot.is_local());
        assert!(StorageTier::Warm.is_local());
        assert!(!StorageTier::Cold.is_local());
    }

    #[test]
    fn test_storage_tier_colder() {
        assert_eq!(StorageTier::Hot.colder(), Some(StorageTier::Warm));
        assert_eq!(StorageTier::Warm.colder(), Some(StorageTier::Cold));
        assert_eq!(StorageTier::Cold.colder(), None);
    }

    #[test]
    fn test_storage_tier_warmer() {
        assert_eq!(StorageTier::Hot.warmer(), None);
        assert_eq!(StorageTier::Warm.warmer(), Some(StorageTier::Hot));
        assert_eq!(StorageTier::Cold.warmer(), Some(StorageTier::Warm));
    }

    // --- Configuration Tests ---

    #[test]
    fn test_config_default() {
        let config = TierConfig::default();
        assert_eq!(config.hot_to_warm_age_ms, DEFAULT_HOT_TO_WARM_AGE_MS);
        assert_eq!(config.warm_to_cold_age_ms, DEFAULT_WARM_TO_COLD_AGE_MS);
        assert!(config.auto_migration_enabled);
    }

    #[test]
    fn test_config_optimize_cost() {
        let config = TierConfig::optimize_cost();
        assert!(config.hot_to_warm_age_ms < TierConfig::default().hot_to_warm_age_ms);
        assert!(config.warm_to_cold_age_ms < TierConfig::default().warm_to_cold_age_ms);
    }

    #[test]
    fn test_config_optimize_performance() {
        let config = TierConfig::optimize_performance();
        assert!(config.hot_to_warm_age_ms > TierConfig::default().hot_to_warm_age_ms);
        assert!(config.warm_to_cold_age_ms > TierConfig::default().warm_to_cold_age_ms);
    }

    #[test]
    fn test_config_testing() {
        let config = TierConfig::testing();
        assert!(!config.auto_migration_enabled);
        assert!(config.hot_to_warm_age_ms < 10000); // Very short for testing
    }

    #[test]
    fn test_config_validation_valid() {
        let config = TierConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation_invalid_hot_age() {
        let config = TierConfig {
            hot_to_warm_age_ms: 0,
            ..TierConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_invalid_warm_age() {
        let config = TierConfig {
            warm_to_cold_age_ms: 0,
            ..TierConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_invalid_age_order() {
        let config = TierConfig {
            hot_to_warm_age_ms: 1000,
            warm_to_cold_age_ms: 500, // Less than hot_to_warm
            ..TierConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_invalid_batch_size() {
        let config = TierConfig {
            migration_batch_size: 0,
            ..TierConfig::default()
        };
        assert!(config.validate().is_err());
    }

    // --- Chunk Tier Info Tests ---

    #[test]
    fn test_chunk_tier_info_creation() {
        let chunk_id = ChunkId::new();
        let info = ChunkTierInfo::new(
            chunk_id.clone(),
            StorageTier::Hot,
            1000,
            PathBuf::from("/test"),
        );
        assert_eq!(info.chunk_id, chunk_id);
        assert_eq!(info.tier, StorageTier::Hot);
        assert_eq!(info.size_bytes, 1000);
        assert_eq!(info.access_count, 0);
        assert!(info.last_migrated_at.is_none());
    }

    #[test]
    fn test_chunk_tier_info_record_access() {
        let mut info = ChunkTierInfo::new(
            ChunkId::new(),
            StorageTier::Hot,
            1000,
            PathBuf::from("/test"),
        );
        assert_eq!(info.access_count, 0);

        info.record_access();
        assert_eq!(info.access_count, 1);

        info.record_access();
        assert_eq!(info.access_count, 2);
    }

    #[test]
    fn test_chunk_tier_info_update_tier() {
        let mut info = ChunkTierInfo::new(
            ChunkId::new(),
            StorageTier::Hot,
            1000,
            PathBuf::from("/hot/test"),
        );
        info.update_tier(StorageTier::Warm, PathBuf::from("/warm/test"));

        assert_eq!(info.tier, StorageTier::Warm);
        assert_eq!(info.path, PathBuf::from("/warm/test"));
        assert!(info.last_migrated_at.is_some());
    }

    // --- Tier Policy Tests ---

    #[test]
    fn test_policy_creation() {
        let config = TierConfig::testing();
        let policy = TierPolicy::new(config);
        // Verify policy was created by accessing its method
        let test_info = ChunkTierInfo::new(
            ChunkId::new(),
            StorageTier::Hot,
            1000,
            PathBuf::from("/test"),
        );
        let _ = policy.target_tier(&test_info);
    }

    #[test]
    fn test_policy_target_tier_hot() {
        let config = TierConfig::testing();
        let policy = TierPolicy::new(config);
        let info = ChunkTierInfo::new(
            ChunkId::new(),
            StorageTier::Hot,
            1000,
            PathBuf::from("/test"),
        );

        // New chunk should stay hot
        assert_eq!(policy.target_tier(&info), StorageTier::Hot);
    }

    #[test]
    fn test_policy_target_tier_frequent_access() {
        let config = TierConfig {
            access_threshold: 5,
            ..TierConfig::testing()
        };
        let policy = TierPolicy::new(config);
        let mut info = ChunkTierInfo::new(
            ChunkId::new(),
            StorageTier::Hot,
            1000,
            PathBuf::from("/test"),
        );

        // Simulate frequent access
        for _ in 0..10 {
            info.record_access();
        }

        assert_eq!(policy.target_tier(&info), StorageTier::Hot);
    }

    #[test]
    fn test_policy_should_promote() {
        let config = TierConfig {
            access_threshold: 5,
            ..TierConfig::testing()
        };
        let policy = TierPolicy::new(config);
        let mut info = ChunkTierInfo::new(
            ChunkId::new(),
            StorageTier::Cold,
            1000,
            PathBuf::from("/test"),
        );

        // Not enough accesses
        assert!(!policy.should_promote(&info));

        // Simulate frequent access
        for _ in 0..10 {
            info.record_access();
        }

        assert!(policy.should_promote(&info));
    }

    // --- Cold Tier Cache Tests ---

    #[test]
    fn test_cache_creation() {
        let cache = ColdTierCache::new(1024);
        assert_eq!(cache.stats().max_size_bytes, 1024);
        assert_eq!(cache.stats().size_bytes, 0);
    }

    #[test]
    fn test_cache_put_get() {
        let cache = ColdTierCache::new(1024);
        let data = vec![1, 2, 3, 4, 5];

        cache.put("key1".to_string(), data.clone());
        let retrieved = cache.get("key1").expect("Should get cached data");

        assert_eq!(retrieved, data);
    }

    #[test]
    fn test_cache_miss() {
        let cache = ColdTierCache::new(1024);
        assert!(cache.get("nonexistent").is_none());
    }

    #[test]
    fn test_cache_eviction() {
        let cache = ColdTierCache::new(100); // Small cache

        // Fill cache
        cache.put("key1".to_string(), vec![0; 50]);
        cache.put("key2".to_string(), vec![0; 50]);

        // This should trigger eviction
        cache.put("key3".to_string(), vec![0; 50]);

        // One of the first two should be evicted
        let has_key1 = cache.get("key1").is_some();
        let has_key2 = cache.get("key2").is_some();
        let has_key3 = cache.get("key3").is_some();

        assert!(has_key3); // Latest should be present
        assert!(!has_key1 || !has_key2); // One should be evicted
    }

    #[test]
    fn test_cache_remove() {
        let cache = ColdTierCache::new(1024);
        cache.put("key1".to_string(), vec![1, 2, 3]);

        assert!(cache.get("key1").is_some());
        cache.remove("key1");
        assert!(cache.get("key1").is_none());
    }

    #[test]
    fn test_cache_clear() {
        let cache = ColdTierCache::new(1024);
        cache.put("key1".to_string(), vec![1, 2, 3]);
        cache.put("key2".to_string(), vec![4, 5, 6]);

        cache.clear();

        assert!(cache.get("key1").is_none());
        assert!(cache.get("key2").is_none());
        assert_eq!(cache.stats().size_bytes, 0);
    }

    #[test]
    fn test_cache_stats() {
        let cache = ColdTierCache::new(1024);
        cache.put("key1".to_string(), vec![1, 2, 3]);

        let _ = cache.get("key1"); // Hit
        let _ = cache.get("key2"); // Miss

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.entry_count, 1);
    }

    #[test]
    fn test_cache_hit_rate() {
        let stats = CacheStats {
            hits: 75,
            misses: 25,
            ..Default::default()
        };
        assert!((stats.hit_rate() - 0.75).abs() < 0.01);
    }

    // --- Local Simulated Storage Tests ---

    #[test]
    fn test_local_simulated_storage_creation() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let storage = LocalSimulatedStorage::new(temp_dir.path().to_path_buf());
        assert!(storage.is_ok());
    }

    #[test]
    fn test_local_simulated_storage_upload_download() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let storage =
            LocalSimulatedStorage::new(temp_dir.path().to_path_buf()).expect("Failed to create");

        let data = vec![1, 2, 3, 4, 5];
        storage.upload("test_key", &data).expect("Upload failed");

        let downloaded = storage.download("test_key").expect("Download failed");
        assert_eq!(downloaded, data);
    }

    #[test]
    fn test_local_simulated_storage_delete() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let storage =
            LocalSimulatedStorage::new(temp_dir.path().to_path_buf()).expect("Failed to create");

        storage
            .upload("test_key", &[1, 2, 3])
            .expect("Upload failed");
        assert!(storage.exists("test_key").unwrap());

        storage.delete("test_key").expect("Delete failed");
        assert!(!storage.exists("test_key").unwrap());
    }

    #[test]
    fn test_local_simulated_storage_list() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let storage =
            LocalSimulatedStorage::new(temp_dir.path().to_path_buf()).expect("Failed to create");

        storage.upload("prefix/key1", &[1]).expect("Upload failed");
        storage.upload("prefix/key2", &[2]).expect("Upload failed");
        storage.upload("other/key3", &[3]).expect("Upload failed");

        let keys = storage.list("prefix/").expect("List failed");
        assert_eq!(keys.len(), 2);
    }

    // --- Tier Manager Tests ---

    #[test]
    fn test_tier_manager_creation() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = TierConfig::testing();
        let manager = TierManager::new(config, temp_dir.path().to_path_buf());
        assert!(manager.is_ok());
    }

    #[test]
    fn test_tier_manager_register_chunk() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = TierConfig::testing();
        let manager =
            TierManager::new(config, temp_dir.path().to_path_buf()).expect("Failed to create");

        let chunk_id = ChunkId::new();
        let data = vec![1, 2, 3, 4, 5];
        let path = manager
            .register_chunk(&chunk_id, &data)
            .expect("Registration failed");

        assert!(path.exists());
        assert_eq!(manager.get_tier(&chunk_id), Some(StorageTier::Hot));
    }

    #[test]
    fn test_tier_manager_read_chunk() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = TierConfig::testing();
        let manager =
            TierManager::new(config, temp_dir.path().to_path_buf()).expect("Failed to create");

        let chunk_id = ChunkId::new();
        let data = vec![1, 2, 3, 4, 5];
        manager
            .register_chunk(&chunk_id, &data)
            .expect("Registration failed");

        let read_data = manager.read_chunk(&chunk_id).expect("Read failed");
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_tier_manager_migrate_hot_to_warm() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = TierConfig::testing();
        let manager =
            TierManager::new(config, temp_dir.path().to_path_buf()).expect("Failed to create");

        let chunk_id = ChunkId::new();
        let data = vec![1, 2, 3, 4, 5];
        manager
            .register_chunk(&chunk_id, &data)
            .expect("Registration failed");

        assert_eq!(manager.get_tier(&chunk_id), Some(StorageTier::Hot));

        manager
            .migrate(&chunk_id, StorageTier::Warm)
            .expect("Migration failed");

        assert_eq!(manager.get_tier(&chunk_id), Some(StorageTier::Warm));

        // Data should still be readable
        let read_data = manager.read_chunk(&chunk_id).expect("Read failed");
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_tier_manager_migrate_to_cold() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = TierConfig::testing();
        let manager =
            TierManager::new(config, temp_dir.path().to_path_buf()).expect("Failed to create");

        let chunk_id = ChunkId::new();
        let data = vec![1, 2, 3, 4, 5];
        manager
            .register_chunk(&chunk_id, &data)
            .expect("Registration failed");

        manager
            .migrate(&chunk_id, StorageTier::Cold)
            .expect("Migration failed");

        assert_eq!(manager.get_tier(&chunk_id), Some(StorageTier::Cold));

        // Data should still be readable (via cold tier)
        let read_data = manager.read_chunk(&chunk_id).expect("Read failed");
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_tier_manager_remove_chunk() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = TierConfig::testing();
        let manager =
            TierManager::new(config, temp_dir.path().to_path_buf()).expect("Failed to create");

        let chunk_id = ChunkId::new();
        let data = vec![1, 2, 3, 4, 5];
        let path = manager
            .register_chunk(&chunk_id, &data)
            .expect("Registration failed");

        assert!(path.exists());
        manager.remove_chunk(&chunk_id).expect("Remove failed");
        assert!(!path.exists());
        assert!(manager.get_tier(&chunk_id).is_none());
    }

    #[test]
    fn test_tier_manager_stats() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = TierConfig::testing();
        let manager =
            TierManager::new(config, temp_dir.path().to_path_buf()).expect("Failed to create");

        let chunk_id1 = ChunkId::new();
        let chunk_id2 = ChunkId::new();
        manager
            .register_chunk(&chunk_id1, &[1, 2, 3])
            .expect("Registration failed");
        manager
            .register_chunk(&chunk_id2, &[4, 5, 6])
            .expect("Registration failed");

        let stats = manager.stats();
        assert_eq!(stats.hot_chunks, 2);
        assert_eq!(stats.hot_bytes, 6);
        assert_eq!(stats.warm_chunks, 0);
        assert_eq!(stats.cold_chunks, 0);
    }

    // --- Tier Migrator Tests ---

    #[test]
    fn test_tier_migrator_creation() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = TierConfig::testing();
        let manager =
            Arc::new(TierManager::new(config, temp_dir.path().to_path_buf()).expect("Failed"));

        let migrator = TierMigrator::new(manager);
        assert!(!migrator.is_running());
    }

    #[test]
    fn test_tier_migrator_run_cycle() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = TierConfig::testing();
        let manager =
            Arc::new(TierManager::new(config, temp_dir.path().to_path_buf()).expect("Failed"));

        // Register some chunks
        let chunk_id = ChunkId::new();
        manager
            .register_chunk(&chunk_id, &[1, 2, 3])
            .expect("Registration failed");

        let migrator = TierMigrator::new(manager);
        let result = migrator.run_cycle();

        // With testing config and fresh chunks, no migrations expected
        assert_eq!(result.successful_migrations, 0);
    }

    #[test]
    fn test_tier_migrator_stats() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = TierConfig::testing();
        let manager =
            Arc::new(TierManager::new(config, temp_dir.path().to_path_buf()).expect("Failed"));

        let migrator = TierMigrator::new(manager);
        migrator.run_cycle();

        let stats = migrator.stats();
        assert_eq!(stats.cycles, 1);
    }

    #[test]
    fn test_migration_handle_stop() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = TierConfig {
            migration_interval_ms: 10, // Short interval for testing
            ..TierConfig::testing()
        };
        let manager =
            Arc::new(TierManager::new(config, temp_dir.path().to_path_buf()).expect("Failed"));

        let migrator = TierMigrator::new(manager);
        let handle = migrator.start();

        assert!(handle.is_running());
        handle.stop();

        // Give it time to stop
        std::thread::sleep(Duration::from_millis(50));
        assert!(!handle.is_running());
    }

    // --- TierManagerStatsSnapshot Tests ---

    #[test]
    fn test_stats_snapshot_totals() {
        let stats = TierManagerStatsSnapshot {
            hot_chunks: 10,
            warm_chunks: 20,
            cold_chunks: 30,
            hot_bytes: 1000,
            warm_bytes: 2000,
            cold_bytes: 3000,
            ..Default::default()
        };

        assert_eq!(stats.total_chunks(), 60);
        assert_eq!(stats.total_bytes(), 6000);
    }

    // --- Error Display Tests ---

    #[test]
    fn test_error_display() {
        let chunk_id = ChunkId::new();
        let err = TieredStorageError::ChunkNotFound {
            chunk_id: chunk_id.clone(),
        };
        assert!(err.to_string().contains(&chunk_id.to_string()));

        let err = TieredStorageError::MigrationFailed {
            chunk_id: ChunkId::new(),
            reason: "test reason".to_string(),
        };
        assert!(err.to_string().contains("test reason"));
    }
}
