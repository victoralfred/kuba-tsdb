//! Codec trait and implementations for AHPAC
//!
//! This module defines the `Codec` trait that all compression algorithms must implement,
//! along with concrete implementations for various codecs.

mod alp;
mod chimp;
mod delta_lz4;
mod kuba;

pub use alp::AlpCodec;
pub use chimp::ChimpCodec;
pub use delta_lz4::DeltaLz4Codec;
pub use kuba::KubaCodec;

use crate::ahpac::profile::ChunkProfile;
use crate::types::DataPoint;

/// Codec identifier stored in chunk headers
///
/// Each codec has a unique ID that is stored in the compressed chunk header
/// to enable proper decompression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum CodecId {
    /// Kuba XOR compression (Facebook Gorilla variant)
    Kuba = 0,
    /// Chimp improved XOR compression
    Chimp = 1,
    /// ALP (Adaptive Lossless floating-Point) for decimal-scaled data
    Alp = 2,
    /// Delta encoding followed by LZ4 compression
    DeltaLz4 = 3,
    /// Delta encoding followed by Zstd compression
    DeltaZstd = 4,
    /// Uncompressed raw data
    Raw = 255,
}

impl CodecId {
    /// Convert a byte to a CodecId
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(CodecId::Kuba),
            1 => Some(CodecId::Chimp),
            2 => Some(CodecId::Alp),
            3 => Some(CodecId::DeltaLz4),
            4 => Some(CodecId::DeltaZstd),
            255 => Some(CodecId::Raw),
            _ => None,
        }
    }

    /// Get the name of the codec
    pub fn name(&self) -> &'static str {
        match self {
            CodecId::Kuba => "kuba",
            CodecId::Chimp => "chimp",
            CodecId::Alp => "alp",
            CodecId::DeltaLz4 => "delta_lz4",
            CodecId::DeltaZstd => "delta_zstd",
            CodecId::Raw => "raw",
        }
    }
}

/// Errors that can occur during codec operations
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    /// Data is not suitable for this codec
    #[error("Unsupported data: {0}")]
    UnsupportedData(String),

    /// Compression operation failed
    #[error("Compression failed: {0}")]
    CompressionFailed(String),

    /// Decompression operation failed
    #[error("Decompression failed: {0}")]
    DecompressionFailed(String),

    /// Buffer too small for operation
    #[error("Buffer too small: need {needed} bytes, have {have}")]
    BufferTooSmall {
        /// Bytes needed
        needed: usize,
        /// Bytes available
        have: usize,
    },

    /// Invalid data format
    #[error("Invalid format: {0}")]
    InvalidFormat(String),

    /// IO error during codec operations
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Trait for all compression codecs
///
/// Implementations must be thread-safe (`Send + Sync`) to support
/// concurrent compression of multiple chunks.
pub trait Codec: Send + Sync {
    /// Get the unique identifier for this codec
    fn id(&self) -> CodecId;

    /// Compress data points to bytes
    ///
    /// # Arguments
    ///
    /// * `points` - Slice of data points to compress
    ///
    /// # Returns
    ///
    /// Compressed bytes on success, or an error if compression fails.
    fn compress(&self, points: &[DataPoint]) -> Result<Vec<u8>, CodecError>;

    /// Decompress bytes back to data points
    ///
    /// # Arguments
    ///
    /// * `data` - Compressed bytes
    /// * `count` - Expected number of data points
    ///
    /// # Returns
    ///
    /// Vector of decompressed data points.
    fn decompress(&self, data: &[u8], count: usize) -> Result<Vec<DataPoint>, CodecError>;

    /// Estimate bits per sample for this codec on the given data
    ///
    /// This is used for fast codec selection without full compression.
    /// Returns `f64::MAX` if the codec is not applicable to this data.
    ///
    /// # Arguments
    ///
    /// * `profile` - Statistical profile of the data
    /// * `sample` - Sample of data points for estimation
    fn estimate_bits(&self, profile: &ChunkProfile, sample: &[DataPoint]) -> f64;

    /// Get the codec name
    fn name(&self) -> &'static str {
        self.id().name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_codec_id_from_byte() {
        assert_eq!(CodecId::from_byte(0), Some(CodecId::Kuba));
        assert_eq!(CodecId::from_byte(1), Some(CodecId::Chimp));
        assert_eq!(CodecId::from_byte(2), Some(CodecId::Alp));
        assert_eq!(CodecId::from_byte(3), Some(CodecId::DeltaLz4));
        assert_eq!(CodecId::from_byte(255), Some(CodecId::Raw));
        assert_eq!(CodecId::from_byte(100), None);
    }

    #[test]
    fn test_codec_id_name() {
        assert_eq!(CodecId::Kuba.name(), "kuba");
        assert_eq!(CodecId::Chimp.name(), "chimp");
        assert_eq!(CodecId::Alp.name(), "alp");
    }
}
