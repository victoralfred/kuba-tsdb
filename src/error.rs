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

/// Result type alias
pub type Result<T> = std::result::Result<T, Error>;
