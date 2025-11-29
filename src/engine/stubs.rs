//! Stub implementations for storage engines, compressors, and indexes.
//!
//! These implementations are intended for:
//! - **Unit testing** without external dependencies (Redis, S3, etc.)
//! - **Integration testing** with in-memory storage
//! - **Development and prototyping** new features
//! - **Benchmarking** compression algorithms
//!
//! # Available Stubs
//!
//! ## S3 Storage Engine ([`S3Engine`])
//!
//! The S3 engine stub simulates S3-compatible cloud storage for cold/archival data.
//! In production, this would use the AWS SDK for actual S3 operations.
//!
//! ## Parquet Compressor ([`ParquetCompressor`])
//!
//! The Parquet compressor stub provides columnar storage format optimized for
//! analytical queries. Currently uses a simple binary format; a production
//! implementation would use the `parquet` crate.
//!
//! ## In-Memory Time Index ([`InMemoryTimeIndex`])
//!
//! A lightweight alternative to Redis for testing. Stores all index data in memory
//! with no persistence.
//!
//! # Warning
//!
//! **These stubs are NOT suitable for production use:**
//!
//! - [`InMemoryTimeIndex`] loses all data on restart
//! - [`S3Engine`] does not actually connect to S3 (stores in memory)
//! - [`ParquetCompressor`] uses a simple binary format, not actual Parquet
//!
//! # Security
//!
//! - [`S3Config`] uses [`SecretString`] for credentials to prevent accidental logging
//! - [`ParquetCompressor`] limits input size to prevent memory exhaustion
//!
//! # Example
//!
//! ```rust,ignore
//! use gorilla_tsdb::engine::stubs::{InMemoryTimeIndex, S3Engine, S3Config};
//! use gorilla_tsdb::engine::traits::{TimeIndex, StorageEngine};
//!
//! // Create an in-memory index for testing
//! let index = InMemoryTimeIndex::new();
//!
//! // Create an S3 stub engine
//! let config = S3Config::default();
//! let storage = S3Engine::new(config).unwrap();
//!
//! // Use in tests...
//! ```

use crate::engine::traits::{
    ChunkLocation, ChunkMetadata, ChunkReference, ChunkStatus, CompressedBlock, CompressionStats,
    Compressor, IndexConfig, IndexStats, MaintenanceReport, SeriesMetadata, StorageConfig,
    StorageEngine, StorageStats, TimeIndex,
};
use crate::error::{CompressionError, IndexError, StorageError};
use crate::types::{ChunkId, DataPoint, SeriesId, TagFilter, TimeRange};
use async_trait::async_trait;
use futures::Stream;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};

// =============================================================================
// SecretString - Secure credential handling
// =============================================================================

/// A string wrapper that prevents accidental logging of sensitive data
///
/// This type is used to store credentials like AWS access keys. It implements
/// Debug to print `[REDACTED]` instead of the actual value to prevent
/// credential leakage in logs.
#[derive(Clone)]
pub struct SecretString(String);

impl SecretString {
    /// Create a new SecretString from a regular string
    pub fn new(s: String) -> Self {
        Self(s)
    }

    /// Access the secret value
    ///
    /// Use this method only when you actually need the credential value,
    /// such as when making API calls.
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

/// Debug implementation that redacts the secret value
impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[REDACTED]")
    }
}

/// Display implementation that also redacts the secret value
impl fmt::Display for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[REDACTED]")
    }
}

impl From<String> for SecretString {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl From<&str> for SecretString {
    fn from(s: &str) -> Self {
        Self::new(s.to_string())
    }
}

// =============================================================================
// S3 Storage Engine Stub
// =============================================================================

/// S3-compatible storage engine configuration
///
/// Credentials are stored using SecretString to prevent accidental logging.
#[derive(Clone)]
pub struct S3Config {
    /// S3 bucket name (must not be empty)
    pub bucket: String,
    /// AWS region
    pub region: String,
    /// Optional endpoint URL (for S3-compatible services like MinIO)
    pub endpoint: Option<String>,
    /// Access key ID (redacted in Debug output)
    pub access_key_id: Option<SecretString>,
    /// Secret access key (redacted in Debug output)
    pub secret_access_key: Option<SecretString>,
    /// Key prefix for all objects
    pub prefix: String,
    /// Enable server-side encryption
    pub encryption_enabled: bool,
    /// Storage class (STANDARD, GLACIER, etc.)
    pub storage_class: String,
}

/// Custom Debug implementation that redacts credentials
impl fmt::Debug for S3Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Config")
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field(
                "access_key_id",
                &self.access_key_id.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "secret_access_key",
                &self.secret_access_key.as_ref().map(|_| "[REDACTED]"),
            )
            .field("prefix", &self.prefix)
            .field("encryption_enabled", &self.encryption_enabled)
            .field("storage_class", &self.storage_class)
            .finish()
    }
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            bucket: "gorilla-tsdb".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            prefix: "chunks/".to_string(),
            encryption_enabled: true,
            storage_class: "STANDARD".to_string(),
        }
    }
}

impl S3Config {
    /// Validate the configuration
    ///
    /// Returns an error if:
    /// - Bucket name is empty
    /// - Region is empty
    pub fn validate(&self) -> Result<(), StorageError> {
        if self.bucket.is_empty() {
            return Err(StorageError::ConfigurationError(
                "S3 bucket name cannot be empty".to_string(),
            ));
        }
        if self.region.is_empty() {
            return Err(StorageError::ConfigurationError(
                "S3 region cannot be empty".to_string(),
            ));
        }
        Ok(())
    }
}

/// S3-compatible storage engine for cloud/archival storage
///
/// This is a stub implementation that demonstrates the interface.
/// A full implementation would use the AWS SDK for Rust.
///
/// # Example
///
/// ```rust,ignore
/// use gorilla_tsdb::engine::stubs::{S3Engine, S3Config};
///
/// let config = S3Config {
///     bucket: "my-bucket".to_string(),
///     region: "us-west-2".to_string(),
///     ..Default::default()
/// };
///
/// let engine = S3Engine::new(config);
/// ```
pub struct S3Engine {
    /// S3 configuration
    config: S3Config,
    /// In-memory index of uploaded chunks (would be persisted in production)
    chunk_index: RwLock<HashMap<SeriesId, Vec<ChunkMetadata>>>,
    /// Storage statistics
    stats: S3Stats,
}

struct S3Stats {
    total_chunks: AtomicU64,
    total_bytes: AtomicU64,
    write_ops: AtomicU64,
    read_ops: AtomicU64,
    delete_ops: AtomicU64,
}

impl S3Engine {
    /// Create a new S3 storage engine with the given configuration
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid (empty bucket or region).
    pub fn new(config: S3Config) -> Result<Self, StorageError> {
        // Validate configuration before creating engine
        config.validate()?;

        Ok(Self {
            config,
            chunk_index: RwLock::new(HashMap::new()),
            stats: S3Stats {
                total_chunks: AtomicU64::new(0),
                total_bytes: AtomicU64::new(0),
                write_ops: AtomicU64::new(0),
                read_ops: AtomicU64::new(0),
                delete_ops: AtomicU64::new(0),
            },
        })
    }

    /// Generate S3 object key for a chunk
    fn object_key(&self, series_id: SeriesId, chunk_id: &ChunkId) -> String {
        format!(
            "{}series_{}/chunk_{}.gor",
            self.config.prefix, series_id, chunk_id.0
        )
    }

    /// Get the bucket name
    pub fn bucket(&self) -> &str {
        &self.config.bucket
    }

    /// Get the region
    pub fn region(&self) -> &str {
        &self.config.region
    }
}

#[async_trait]
impl StorageEngine for S3Engine {
    /// Returns the engine identifier
    fn engine_id(&self) -> &str {
        "s3-v1"
    }

    /// Initialize the S3 storage engine
    ///
    /// In a full implementation, this would:
    /// - Verify bucket exists and is accessible
    /// - Check credentials
    /// - Load existing chunk index from S3
    async fn initialize(&self, _config: StorageConfig) -> Result<(), StorageError> {
        // Stub: Would verify S3 connectivity here
        // Example with real SDK:
        // self.client.head_bucket().bucket(&self.config.bucket).send().await?;
        Ok(())
    }

    /// Write a chunk to S3
    ///
    /// In a full implementation, this would:
    /// - Serialize the CompressedBlock
    /// - Upload to S3 with appropriate metadata
    /// - Handle multipart upload for large chunks
    async fn write_chunk(
        &self,
        series_id: SeriesId,
        chunk_id: ChunkId,
        data: &CompressedBlock,
    ) -> Result<ChunkLocation, StorageError> {
        let key = self.object_key(series_id, &chunk_id);

        // Stub: Would upload to S3 here
        // Example with real SDK:
        // self.client.put_object()
        //     .bucket(&self.config.bucket)
        //     .key(&key)
        //     .body(ByteStream::from(data.data.to_vec()))
        //     .storage_class(StorageClass::from(&self.config.storage_class))
        //     .send().await?;

        let location = ChunkLocation {
            engine_id: self.engine_id().to_string(),
            path: format!("s3://{}/{}", self.config.bucket, key),
            offset: None,
            size: Some(data.compressed_size),
        };

        // Update in-memory index
        let metadata = ChunkMetadata {
            chunk_id: chunk_id.clone(),
            location: location.clone(),
            time_range: TimeRange {
                start: data.metadata.start_timestamp,
                end: data.metadata.end_timestamp,
            },
            size_bytes: data.compressed_size,
            point_count: data.metadata.point_count,
            created_at: chrono::Utc::now().timestamp_millis(),
        };

        {
            let mut index = self.chunk_index.write();
            index.entry(series_id).or_default().push(metadata);
        }

        // Update stats
        self.stats.write_ops.fetch_add(1, Ordering::Relaxed);
        self.stats.total_chunks.fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_bytes
            .fetch_add(data.compressed_size as u64, Ordering::Relaxed);

        Ok(location)
    }

    /// Read a chunk from S3
    ///
    /// In a full implementation, this would:
    /// - Download the object from S3
    /// - Deserialize into CompressedBlock
    /// - Handle range requests for partial reads
    async fn read_chunk(&self, location: &ChunkLocation) -> Result<CompressedBlock, StorageError> {
        self.stats.read_ops.fetch_add(1, Ordering::Relaxed);

        // Stub: Would download from S3 here
        // Example with real SDK:
        // let resp = self.client.get_object()
        //     .bucket(&self.config.bucket)
        //     .key(&location.path)
        //     .send().await?;
        // let data = resp.body.collect().await?.into_bytes();

        // Return error for stub - real implementation would return actual data
        Err(StorageError::ChunkNotFound(format!(
            "S3 read not implemented: {}",
            location.path
        )))
    }

    /// Delete a chunk from S3
    ///
    /// In a full implementation, this would:
    /// - Delete the object from S3
    /// - Update the chunk index
    async fn delete_chunk(&self, location: &ChunkLocation) -> Result<(), StorageError> {
        self.stats.delete_ops.fetch_add(1, Ordering::Relaxed);

        // Stub: Would delete from S3 here
        // Example with real SDK:
        // self.client.delete_object()
        //     .bucket(&self.config.bucket)
        //     .key(&location.path)
        //     .send().await?;

        // Remove from in-memory index
        let mut index = self.chunk_index.write();
        for chunks in index.values_mut() {
            chunks.retain(|c| c.location.path != location.path);
        }

        self.stats.total_chunks.fetch_sub(1, Ordering::Relaxed);
        if let Some(size) = location.size {
            self.stats
                .total_bytes
                .fetch_sub(size as u64, Ordering::Relaxed);
        }

        Ok(())
    }

    /// List chunks for a series
    async fn list_chunks(
        &self,
        series_id: SeriesId,
        time_range: Option<TimeRange>,
    ) -> Result<Vec<ChunkMetadata>, StorageError> {
        let index = self.chunk_index.read();
        let chunks = index.get(&series_id).cloned().unwrap_or_default();

        let filtered = if let Some(range) = time_range {
            chunks
                .into_iter()
                .filter(|c| c.time_range.end >= range.start && c.time_range.start <= range.end)
                .collect()
        } else {
            chunks
        };

        Ok(filtered)
    }

    /// Stream chunks from S3 (stub returns empty stream)
    fn stream_chunks(
        &self,
        _series_id: SeriesId,
        _time_range: TimeRange,
    ) -> Pin<Box<dyn Stream<Item = Result<CompressedBlock, StorageError>> + Send>> {
        // Stub: Would implement streaming download here
        Box::pin(futures::stream::empty())
    }

    /// Get storage statistics
    fn stats(&self) -> StorageStats {
        StorageStats {
            total_chunks: self.stats.total_chunks.load(Ordering::Relaxed),
            total_bytes: self.stats.total_bytes.load(Ordering::Relaxed),
            write_ops: self.stats.write_ops.load(Ordering::Relaxed),
            read_ops: self.stats.read_ops.load(Ordering::Relaxed),
            delete_ops: self.stats.delete_ops.load(Ordering::Relaxed),
        }
    }

    /// Perform maintenance on S3 storage
    ///
    /// In a full implementation, this would:
    /// - Transition old data to Glacier storage class
    /// - Delete expired data based on retention policy
    /// - Optimize storage costs
    async fn maintenance(&self) -> Result<MaintenanceReport, StorageError> {
        // Stub: Would implement lifecycle transitions here
        Ok(MaintenanceReport {
            chunks_compacted: 0,
            chunks_deleted: 0,
            bytes_freed: 0,
        })
    }
}

// =============================================================================
// Parquet Compressor Stub
// =============================================================================

/// Parquet compressor configuration
#[derive(Clone, Debug)]
pub struct ParquetConfig {
    /// Row group size (number of rows per group)
    pub row_group_size: usize,
    /// Compression codec (snappy, gzip, lz4, zstd)
    pub compression_codec: String,
    /// Enable dictionary encoding for tags
    pub dictionary_encoding: bool,
    /// Enable statistics for predicate pushdown
    pub statistics_enabled: bool,
}

impl Default for ParquetConfig {
    fn default() -> Self {
        Self {
            row_group_size: 10_000,
            compression_codec: "zstd".to_string(),
            dictionary_encoding: true,
            statistics_enabled: true,
        }
    }
}

/// Parquet-based compressor for columnar storage
///
/// This is a stub implementation that demonstrates the interface.
/// A full implementation would use the `parquet` crate.
///
/// # Benefits
///
/// - Excellent compression for time-series with repeated tag values
/// - Efficient column pruning for queries
/// - Predicate pushdown support
/// - Compatible with data lake ecosystems (Spark, Trino, etc.)
///
/// # Example
///
/// ```rust,ignore
/// use gorilla_tsdb::engine::stubs::{ParquetCompressor, ParquetConfig};
///
/// let config = ParquetConfig {
///     compression_codec: "zstd".to_string(),
///     ..Default::default()
/// };
///
/// let compressor = ParquetCompressor::new(config);
/// ```
pub struct ParquetCompressor {
    /// Parquet configuration
    config: ParquetConfig,
    /// Compression statistics
    stats: RwLock<CompressionStats>,
}

impl ParquetCompressor {
    /// Create a new Parquet compressor with the given configuration
    pub fn new(config: ParquetConfig) -> Self {
        Self {
            config,
            stats: RwLock::new(CompressionStats::default()),
        }
    }

    /// Get the compression codec
    pub fn compression_codec(&self) -> &str {
        &self.config.compression_codec
    }
}

#[async_trait]
impl Compressor for ParquetCompressor {
    /// Returns the algorithm identifier
    fn algorithm_id(&self) -> &str {
        "parquet-v1"
    }

    /// Compress data points to Parquet format
    ///
    /// In a full implementation, this would:
    /// - Create a Parquet schema for time-series data
    /// - Write data points as row groups
    /// - Apply dictionary encoding for tags
    /// - Apply the configured compression codec
    async fn compress(&self, points: &[DataPoint]) -> Result<CompressedBlock, CompressionError> {
        // Fix SEC-005: Limit maximum points to prevent memory exhaustion
        const MAX_POINTS_PER_BLOCK: usize = 1_000_000;

        if points.is_empty() {
            return Err(CompressionError::InvalidData(
                "Cannot compress empty points".to_string(),
            ));
        }

        if points.len() > MAX_POINTS_PER_BLOCK {
            return Err(CompressionError::InvalidData(format!(
                "Too many points: {} exceeds maximum {} per block",
                points.len(),
                MAX_POINTS_PER_BLOCK
            )));
        }

        let start_time = std::time::Instant::now();

        // Stub: Would use parquet crate here
        // Example with real parquet:
        // let schema = Arc::new(Schema::new(vec![
        //     Field::new("timestamp", DataType::Int64, false),
        //     Field::new("value", DataType::Float64, false),
        //     Field::new("series_id", DataType::UInt128, false),
        // ]));
        // let mut writer = ArrowWriter::try_new(buffer, schema, Some(props))?;
        // writer.write(&batch)?;
        // writer.close()?;

        // For stub, manually serialize points (would be Parquet in real impl)
        // Format: [count:u32][series_id:u128, timestamp:i64, value:f64]...
        let mut data = Vec::with_capacity(4 + points.len() * 32);
        data.extend_from_slice(&(points.len() as u32).to_le_bytes());
        for point in points {
            data.extend_from_slice(&point.series_id.to_le_bytes()); // 16 bytes for u128
            data.extend_from_slice(&point.timestamp.to_le_bytes()); // 8 bytes
            data.extend_from_slice(&point.value.to_le_bytes()); // 8 bytes
        }

        let original_size = std::mem::size_of_val(points);
        let compressed_size = data.len();

        // Calculate checksum
        let checksum = crc::Crc::<u64>::new(&crc::CRC_64_ECMA_182).checksum(&data);

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.total_compressed += 1;
            stats.compression_time_ms += start_time.elapsed().as_millis() as u64;
            let ratio = original_size as f64 / compressed_size as f64;
            stats.average_ratio = (stats.average_ratio * (stats.total_compressed - 1) as f64
                + ratio)
                / stats.total_compressed as f64;
        }

        Ok(CompressedBlock {
            algorithm_id: self.algorithm_id().to_string(),
            original_size,
            compressed_size,
            checksum,
            data: bytes::Bytes::from(data),
            metadata: crate::engine::traits::BlockMetadata {
                start_timestamp: points.first().map(|p| p.timestamp).unwrap_or(0),
                end_timestamp: points.last().map(|p| p.timestamp).unwrap_or(0),
                point_count: points.len(),
                series_id: points.first().map(|p| p.series_id).unwrap_or(0),
            },
        })
    }

    /// Decompress Parquet data back to data points
    ///
    /// In a full implementation, this would:
    /// - Read Parquet file from bytes
    /// - Iterate over row groups
    /// - Convert back to DataPoint structs
    async fn decompress(
        &self,
        block: &CompressedBlock,
    ) -> Result<Vec<DataPoint>, CompressionError> {
        let start_time = std::time::Instant::now();

        // Verify checksum
        let computed = crc::Crc::<u64>::new(&crc::CRC_64_ECMA_182).checksum(&block.data);
        if computed != block.checksum {
            return Err(CompressionError::CorruptedData(format!(
                "Checksum mismatch: expected {}, got {}",
                block.checksum, computed
            )));
        }

        // Stub: Manually deserialize points
        // Format: [count:u32][series_id:u128, timestamp:i64, value:f64]...
        if block.data.len() < 4 {
            return Err(CompressionError::CorruptedData(
                "Data too short to contain point count".to_string(),
            ));
        }

        let count = u32::from_le_bytes([block.data[0], block.data[1], block.data[2], block.data[3]])
            as usize;

        let expected_size = 4 + count * 32; // 16 bytes for u128 + 8 for i64 + 8 for f64
        if block.data.len() < expected_size {
            return Err(CompressionError::CorruptedData(format!(
                "Data too short: expected {} bytes, got {}",
                expected_size,
                block.data.len()
            )));
        }

        let mut points = Vec::with_capacity(count);
        let mut offset = 4;

        for _ in 0..count {
            // Read series_id as u128 (16 bytes)
            let series_id = u128::from_le_bytes([
                block.data[offset],
                block.data[offset + 1],
                block.data[offset + 2],
                block.data[offset + 3],
                block.data[offset + 4],
                block.data[offset + 5],
                block.data[offset + 6],
                block.data[offset + 7],
                block.data[offset + 8],
                block.data[offset + 9],
                block.data[offset + 10],
                block.data[offset + 11],
                block.data[offset + 12],
                block.data[offset + 13],
                block.data[offset + 14],
                block.data[offset + 15],
            ]);
            offset += 16;

            let timestamp = i64::from_le_bytes([
                block.data[offset],
                block.data[offset + 1],
                block.data[offset + 2],
                block.data[offset + 3],
                block.data[offset + 4],
                block.data[offset + 5],
                block.data[offset + 6],
                block.data[offset + 7],
            ]);
            offset += 8;

            let value = f64::from_le_bytes([
                block.data[offset],
                block.data[offset + 1],
                block.data[offset + 2],
                block.data[offset + 3],
                block.data[offset + 4],
                block.data[offset + 5],
                block.data[offset + 6],
                block.data[offset + 7],
            ]);
            offset += 8;

            points.push(DataPoint::new(series_id, timestamp, value));
        }

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.total_decompressed += 1;
            stats.decompression_time_ms += start_time.elapsed().as_millis() as u64;
        }

        Ok(points)
    }

    /// Estimate compression ratio
    ///
    /// Parquet typically achieves 5:1 to 20:1 compression for time-series data,
    /// depending on data characteristics and codec.
    fn estimate_ratio(&self, _sample: &[DataPoint]) -> f64 {
        // Parquet with zstd typically achieves 8-15x compression
        match self.config.compression_codec.as_str() {
            "snappy" => 5.0,
            "gzip" => 10.0,
            "lz4" => 4.0,
            "zstd" => 12.0,
            _ => 8.0,
        }
    }

    /// Get compression statistics
    fn stats(&self) -> CompressionStats {
        self.stats.read().clone()
    }
}

// =============================================================================
// In-Memory Index Stub (for testing)
// =============================================================================

/// Simple in-memory time index for testing
///
/// This provides a lightweight alternative to Redis for single-node deployments
/// or testing scenarios.
pub struct InMemoryTimeIndex {
    /// Series registry: series_id -> metadata
    series: RwLock<HashMap<SeriesId, SeriesMetadata>>,
    /// Chunk index: series_id -> chunks
    chunks: RwLock<HashMap<SeriesId, Vec<ChunkReference>>>,
    /// Metric index: metric_name -> series_ids
    metric_index: RwLock<HashMap<String, Vec<SeriesId>>>,
    /// Statistics
    stats: IndexStatsAtomic,
}

struct IndexStatsAtomic {
    total_series: AtomicU64,
    total_chunks: AtomicU64,
    queries_served: AtomicU64,
}

impl InMemoryTimeIndex {
    /// Create a new in-memory time index
    pub fn new() -> Self {
        Self {
            series: RwLock::new(HashMap::new()),
            chunks: RwLock::new(HashMap::new()),
            metric_index: RwLock::new(HashMap::new()),
            stats: IndexStatsAtomic {
                total_series: AtomicU64::new(0),
                total_chunks: AtomicU64::new(0),
                queries_served: AtomicU64::new(0),
            },
        }
    }
}

impl Default for InMemoryTimeIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TimeIndex for InMemoryTimeIndex {
    fn index_id(&self) -> &str {
        "in-memory-v1"
    }

    async fn initialize(&self, _config: IndexConfig) -> Result<(), IndexError> {
        Ok(())
    }

    async fn register_series(
        &self,
        series_id: SeriesId,
        metadata: SeriesMetadata,
    ) -> Result<(), IndexError> {
        // Fix RACE-001: Acquire both locks together to prevent race conditions
        // between series registration and metric index update
        let metric_name = metadata.metric_name.clone();
        let mut series = self.series.write();
        let mut metric_idx = self.metric_index.write();

        let is_new = series.insert(series_id, metadata).is_none();
        if is_new {
            self.stats.total_series.fetch_add(1, Ordering::Relaxed);
            metric_idx.entry(metric_name).or_default().push(series_id);
        }

        Ok(())
    }

    async fn add_chunk(
        &self,
        series_id: SeriesId,
        chunk_id: ChunkId,
        time_range: TimeRange,
        location: ChunkLocation,
    ) -> Result<(), IndexError> {
        let chunk_ref = ChunkReference {
            chunk_id,
            location,
            time_range,
            status: ChunkStatus::Sealed,
        };

        let mut chunks = self.chunks.write();
        chunks.entry(series_id).or_default().push(chunk_ref);
        self.stats.total_chunks.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    async fn query_chunks(
        &self,
        series_id: SeriesId,
        time_range: TimeRange,
    ) -> Result<Vec<ChunkReference>, IndexError> {
        self.stats.queries_served.fetch_add(1, Ordering::Relaxed);

        let chunks = self.chunks.read();
        let result = chunks
            .get(&series_id)
            .map(|c| {
                c.iter()
                    .filter(|chunk| {
                        chunk.time_range.end >= time_range.start
                            && chunk.time_range.start <= time_range.end
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        Ok(result)
    }

    async fn find_series(
        &self,
        metric_name: &str,
        tag_filter: &TagFilter,
    ) -> Result<Vec<SeriesId>, IndexError> {
        self.stats.queries_served.fetch_add(1, Ordering::Relaxed);

        let metric_idx = self.metric_index.read();
        let series_ids = metric_idx.get(metric_name).cloned().unwrap_or_default();

        // Filter by tags
        let series = self.series.read();
        let filtered: Vec<SeriesId> = series_ids
            .into_iter()
            .filter(|id| {
                if let Some(meta) = series.get(id) {
                    tag_filter.matches(&meta.tags)
                } else {
                    false
                }
            })
            .collect();

        Ok(filtered)
    }

    async fn update_chunk_status(
        &self,
        chunk_id: ChunkId,
        status: ChunkStatus,
    ) -> Result<(), IndexError> {
        let mut chunks = self.chunks.write();
        for series_chunks in chunks.values_mut() {
            for chunk in series_chunks.iter_mut() {
                if chunk.chunk_id == chunk_id {
                    chunk.status = status.clone();
                    return Ok(());
                }
            }
        }
        // Fix ERR-003: Return error when chunk not found instead of silent Ok
        Err(IndexError::ChunkNotFound(format!(
            "Chunk {} not found in index",
            chunk_id
        )))
    }

    async fn delete_series(&self, series_id: SeriesId) -> Result<(), IndexError> {
        {
            let mut series = self.series.write();
            if let Some(meta) = series.remove(&series_id) {
                self.stats.total_series.fetch_sub(1, Ordering::Relaxed);

                // Remove from metric index
                let mut metric_idx = self.metric_index.write();
                if let Some(ids) = metric_idx.get_mut(&meta.metric_name) {
                    ids.retain(|id| *id != series_id);
                }
            }
        }

        {
            let mut chunks = self.chunks.write();
            if let Some(removed) = chunks.remove(&series_id) {
                self.stats
                    .total_chunks
                    .fetch_sub(removed.len() as u64, Ordering::Relaxed);
            }
        }

        Ok(())
    }

    fn stats(&self) -> IndexStats {
        IndexStats {
            total_series: self.stats.total_series.load(Ordering::Relaxed),
            total_chunks: self.stats.total_chunks.load(Ordering::Relaxed),
            queries_served: self.stats.queries_served.load(Ordering::Relaxed),
            cache_hits: 0, // In-memory doesn't have cache layers
            cache_misses: 0,
        }
    }

    async fn rebuild(&self) -> Result<(), IndexError> {
        // In-memory index doesn't need rebuilding from storage
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_config_default() {
        let config = S3Config::default();
        assert_eq!(config.bucket, "gorilla-tsdb");
        assert_eq!(config.region, "us-east-1");
        assert!(config.encryption_enabled);
    }

    #[test]
    fn test_s3_engine_object_key() {
        let config = S3Config::default();
        let engine = S3Engine::new(config).unwrap();
        // Use a valid UUID for testing
        let chunk_id = ChunkId::from_string("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let key = engine.object_key(123, &chunk_id);
        assert_eq!(
            key,
            "chunks/series_123/chunk_550e8400-e29b-41d4-a716-446655440000.gor"
        );
    }

    #[test]
    fn test_s3_config_validation() {
        // Empty bucket should fail
        let config = S3Config {
            bucket: "".to_string(),
            ..Default::default()
        };
        assert!(S3Engine::new(config).is_err());

        // Empty region should fail
        let config = S3Config {
            region: "".to_string(),
            ..Default::default()
        };
        assert!(S3Engine::new(config).is_err());

        // Valid config should succeed
        let config = S3Config::default();
        assert!(S3Engine::new(config).is_ok());
    }

    #[test]
    fn test_parquet_config_default() {
        let config = ParquetConfig::default();
        assert_eq!(config.compression_codec, "zstd");
        assert!(config.dictionary_encoding);
    }

    #[tokio::test]
    async fn test_parquet_compressor_roundtrip() {
        let compressor = ParquetCompressor::new(ParquetConfig::default());

        let points = vec![
            DataPoint::new(1, 1000, 42.0),
            DataPoint::new(1, 2000, 43.0),
            DataPoint::new(1, 3000, 44.0),
        ];

        let block = compressor.compress(&points).await.unwrap();
        assert_eq!(block.algorithm_id, "parquet-v1");
        assert_eq!(block.metadata.point_count, 3);

        let decompressed = compressor.decompress(&block).await.unwrap();
        assert_eq!(decompressed.len(), 3);
        assert_eq!(decompressed[0].value, 42.0);
    }

    #[tokio::test]
    async fn test_in_memory_index() {
        let index = InMemoryTimeIndex::new();

        // Register series
        let metadata = SeriesMetadata {
            metric_name: "cpu.usage".to_string(),
            tags: HashMap::from([("host".to_string(), "server1".to_string())]),
            created_at: 1000,
            retention_days: None,
        };
        index.register_series(1, metadata).await.unwrap();

        // Add chunk
        let location = ChunkLocation {
            engine_id: "test".to_string(),
            path: "/test/chunk.gor".to_string(),
            offset: None,
            size: Some(1024),
        };
        // Use valid UUID for chunk ID
        let chunk_id = ChunkId::from_string("550e8400-e29b-41d4-a716-446655440001").unwrap();
        index
            .add_chunk(
                1,
                chunk_id,
                TimeRange {
                    start: 0,
                    end: 1000,
                },
                location,
            )
            .await
            .unwrap();

        // Query chunks
        let chunks = index
            .query_chunks(
                1,
                TimeRange {
                    start: 0,
                    end: 2000,
                },
            )
            .await
            .unwrap();
        assert_eq!(chunks.len(), 1);

        // Find series
        let series = index
            .find_series("cpu.usage", &TagFilter::All)
            .await
            .unwrap();
        assert_eq!(series.len(), 1);
        assert_eq!(series[0], 1);

        // Check stats
        let stats = index.stats();
        assert_eq!(stats.total_series, 1);
        assert_eq!(stats.total_chunks, 1);
    }
}
