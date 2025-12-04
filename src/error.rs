//! Error types for the database

use thiserror::Error;

/// Main error type for the database
#[derive(Error, Debug)]
pub enum Error {
    /// Compression error
    #[error("Compression error: {0}")]
    Compression(#[from] CompressionError),

    /// Storage error
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    /// Index error
    #[error("Index error: {0}")]
    Index(#[from] IndexError),

    /// Ingestion error
    #[error("Ingestion error: {0}")]
    Ingestion(#[from] IngestionError),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// General error
    #[error("{0}")]
    General(String),
}

/// Compression errors
#[derive(Error, Debug)]
pub enum CompressionError {
    /// Compression operation failed
    #[error("Compression failed: {0}")]
    CompressionFailed(String),

    /// Decompression operation failed
    #[error("Decompression failed: {0}")]
    DecompressionFailed(String),

    /// Unsupported compression algorithm
    #[error("Unsupported algorithm: {0}")]
    UnsupportedAlgorithm(String),

    /// Data is corrupted
    #[error("Corrupted data: {0}")]
    CorruptedData(String),

    /// Invalid input data
    #[error("Invalid data: {0}")]
    InvalidData(String),
}

/// Storage errors
///
/// STYLE-001: Comprehensive error type for storage operations
/// Replaces `Result<T, String>` with structured error variants
#[derive(Error, Debug)]
pub enum StorageError {
    /// IO operation failed
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Requested chunk not found
    #[error("Chunk not found: {0}")]
    ChunkNotFound(String),

    /// Storage is full
    #[error("Storage full")]
    StorageFull,

    /// Storage configuration error
    #[error("Configuration error: {0}")]
    ConfigurationError(String),

    /// Data is corrupted
    #[error("Corrupted data: {0}")]
    CorruptedData(String),

    /// Checksum verification failed
    ///
    /// Provides detailed information about the expected vs actual checksum
    /// for debugging data corruption issues.
    #[error("Checksum mismatch: expected {expected:#x}, got {actual:#x}")]
    ChecksumMismatch {
        /// The expected checksum value from the header
        expected: u64,
        /// The actual checksum computed from the data
        actual: u64,
    },

    // STYLE-001: Additional variants for chunk operations
    /// Invalid chunk header (magic number, version, etc.)
    #[error("Invalid chunk header: {0}")]
    InvalidHeader(String),

    /// Chunk is in wrong state for the requested operation
    #[error("Invalid chunk state: expected {expected}, got {actual}")]
    InvalidState {
        /// Expected chunk state
        expected: String,
        /// Actual chunk state
        actual: String,
    },

    /// Series ID mismatch (point doesn't belong to chunk's series)
    #[error("Series ID mismatch: chunk has {chunk_series}, point has {point_series}")]
    SeriesMismatch {
        /// The series ID the chunk belongs to
        chunk_series: u128,
        /// The series ID of the data point
        point_series: u128,
    },

    /// Duplicate timestamp detected
    #[error("Duplicate timestamp {timestamp}: existing value {existing}, new value {new}")]
    DuplicateTimestamp {
        /// The duplicate timestamp
        timestamp: i64,
        /// The existing value at that timestamp
        existing: f64,
        /// The new value being inserted
        new: f64,
    },

    /// Chunk capacity exceeded
    #[error("Chunk capacity exceeded: limit is {limit} points")]
    CapacityExceeded {
        /// Maximum allowed points
        limit: usize,
    },

    /// Empty data (cannot create chunk from empty data)
    #[error("Cannot create chunk from empty data")]
    EmptyData,

    /// Compression operation failed
    #[error("Compression failed: {0}")]
    CompressionFailed(String),

    /// Decompression operation failed
    #[error("Decompression failed: {0}")]
    DecompressionFailed(String),

    /// Path validation failed
    #[error("Invalid path: {0}")]
    InvalidPath(String),

    /// Time range validation failed
    #[error("Invalid time range: start {start} > end {end}")]
    InvalidTimeRange {
        /// Start timestamp
        start: i64,
        /// End timestamp
        end: i64,
    },
}

/// Ingestion errors
#[derive(Error, Debug)]
pub enum IngestionError {
    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Channel closed unexpectedly
    #[error("Channel closed: {0}")]
    ChannelClosed(String),

    /// Backpressure triggered
    #[error("Backpressure: {0}")]
    Backpressure(String),

    /// Write operation failed
    #[error("Write error: {0}")]
    WriteError(String),

    /// Validation failed
    #[error("Validation error: {0}")]
    ValidationError(String),

    /// Shutdown error
    #[error("Shutdown error: {0}")]
    ShutdownError(String),

    /// Storage error
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
}

/// Index errors
#[derive(Error, Debug)]
pub enum IndexError {
    /// Requested series not found
    #[error("Series not found: {0}")]
    SeriesNotFound(String),

    /// Requested chunk not found
    #[error("Chunk not found: {0}")]
    ChunkNotFound(String),

    /// Index data is corrupted
    #[error("Index corrupted: {0}")]
    IndexCorrupted(String),

    /// Connection to index backend failed
    #[error("Connection error: {0}")]
    ConnectionError(String),

    /// Query execution failed
    #[error("Query error: {0}")]
    QueryError(String),

    /// Serialization failed
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Deserialization failed
    #[error("Deserialization error: {0}")]
    DeserializationError(String),

    /// Parsing failed
    #[error("Parse error: {0}")]
    ParseError(String),
}

/// Validation errors
///
/// STYLE-001: Error type for configuration and input validation
#[derive(Error, Debug)]
pub enum ValidationError {
    /// Value is out of allowed range
    #[error("{field} value {value} is out of range [{min}, {max}]")]
    OutOfRange {
        /// Field name being validated
        field: String,
        /// The invalid value
        value: String,
        /// Minimum allowed value
        min: String,
        /// Maximum allowed value
        max: String,
    },

    /// Required field is missing
    #[error("Missing required field: {0}")]
    MissingField(String),

    /// Invalid format
    #[error("Invalid format for {field}: {message}")]
    InvalidFormat {
        /// Field name being validated
        field: String,
        /// Description of the format error
        message: String,
    },

    /// Generic validation failure
    #[error("Validation failed: {0}")]
    Failed(String),
}

impl From<ValidationError> for Error {
    fn from(e: ValidationError) -> Self {
        Error::Configuration(e.to_string())
    }
}

/// Result type alias
pub type Result<T> = std::result::Result<T, Error>;
