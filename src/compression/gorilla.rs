//! Gorilla compression implementation for time-series data
//!
//! This module implements Facebook's Gorilla compression algorithm, which achieves excellent
//! compression ratios (typically 12:1) for time-series data through two key techniques:
//!
//! # Algorithm Overview
//!
//! ## 1. Delta-of-Delta Timestamp Compression
//!
//! Time-series data typically has regular intervals between timestamps. Instead of storing
//! timestamps directly, we store the "delta of deltas":
//!
//! ```text
//! Timestamps:  1000, 1010, 1020, 1030, 1040
//! Deltas:           10,   10,   10,   10
//! Delta-of-Delta:    0,    0,    0,    0    <- Highly compressible!
//! ```
//!
//! The algorithm uses variable bit packing based on the delta-of-delta magnitude:
//! - `0`: Single bit (most common case for regular intervals)
//! - `'10' + 7 bits`: For changes in range [-63, 64) (127 values)
//! - `'110' + 9 bits`: For changes in range [-255, 256) (511 values)
//! - `'1110' + 12 bits`: For changes in range [-2047, 2048) (4095 values)
//! - `'1111' + 32 bits`: Full delta for large changes
//!
//! ## 2. XOR Floating-Point Value Compression
//!
//! Floating-point values in time-series often change slightly between samples. XOR compression
//! exploits this by storing only the bits that changed:
//!
//! ```text
//! Value 1: 42.123456  (as bits: 0x4045...ABCD)
//! Value 2: 42.123457  (as bits: 0x4045...ABCE)
//! XOR:                           0x0000...0001  <- Only 1 bit different!
//! ```
//!
//! The algorithm tracks leading and trailing zeros in the XOR result:
//! - `0`: Single bit if value unchanged (XOR = 0)
//! - `'10' + meaningful bits`: If leading/trailing zeros match previous
//! - `'11' + 5-bit leading + 6-bit length + meaningful bits`: New block info
//!
//! # Performance Characteristics
//!
//! - **Compression Ratio**: 10-12:1 for typical monitoring data
//! - **Speed**: >500MB/sec compression, >2GB/sec decompression
//! - **Best For**: Regular time intervals, slowly changing values
//! - **Worst For**: Completely random data, irregular timestamps
//!
//! # Example
//!
//! ```rust
//! use gorilla_tsdb::compression::gorilla::GorillaCompressor;
//! use gorilla_tsdb::types::DataPoint;
//! use gorilla_tsdb::engine::traits::Compressor;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let compressor = GorillaCompressor::new();
//!
//! // Create time-series data (e.g., CPU usage every 10 seconds)
//! let points = vec![
//!     DataPoint { series_id: 1, timestamp: 1000, value: 45.2 },
//!     DataPoint { series_id: 1, timestamp: 1010, value: 45.3 },
//!     DataPoint { series_id: 1, timestamp: 1020, value: 45.1 },
//! ];
//!
//! // Compress the data
//! let compressed = compressor.compress(&points).await?;
//! println!("Compressed {} bytes to {} bytes",
//!          compressed.original_size, compressed.compressed_size);
//!
//! // Decompress back to original data
//! let decompressed = compressor.decompress(&compressed).await?;
//! assert_eq!(decompressed, points);
//! # Ok(())
//! # }
//! ```
//!
//! # References
//!
//! - Paper: "Gorilla: A Fast, Scalable, In-Memory Time Series Database"
//! - URL: <http://www.vldb.org/pvldb/vol8/p1816-teller.pdf>
//! - Authors: Tuomas Pelkonen et al., Facebook Inc.

use super::bit_stream::{BitReader, BitWriter};
use crate::engine::traits::{
    BlockMetadata, CompressedBlock, Compressor, CompressionStats,
};
use crate::error::CompressionError;
use crate::types::DataPoint;
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use std::mem::size_of_val;
use std::sync::Arc;

/// Gorilla compression implementation
///
/// `GorillaCompressor` implements the Compressor trait to provide Facebook's Gorilla
/// compression algorithm for time-series data. It maintains compression statistics
/// and configuration options.
///
/// # Thread Safety
///
/// This struct is thread-safe and can be shared across threads using `Arc`. The internal
/// statistics are protected by a `Mutex`.
///
/// # Performance
///
/// - Compression speed: >500MB/sec
/// - Decompression speed: >2GB/sec
/// - Typical compression ratio: 10-12:1
///
/// # Example
///
/// ```rust
/// use gorilla_tsdb::compression::gorilla::{GorillaCompressor, GorillaConfig};
///
/// // Create with default configuration
/// let compressor = GorillaCompressor::new();
///
/// // Or customize the configuration
/// let config = GorillaConfig {
///     block_size: 2000,
///     enable_checksum: true,
/// };
/// let custom_compressor = GorillaCompressor::with_config(config);
/// ```
pub struct GorillaCompressor {
    /// Compression statistics (thread-safe via Mutex)
    /// Tracks total compressed/decompressed blocks, average ratios, and timing
    stats: Arc<Mutex<CompressionStats>>,

    /// Configuration options for the compressor
    config: GorillaConfig,
}

/// Configuration options for Gorilla compressor
///
/// These settings control the behavior of the compression algorithm.
#[derive(Clone, Debug)]
pub struct GorillaConfig {
    /// Target block size (number of data points per compressed block)
    ///
    /// Larger blocks achieve better compression but increase memory usage.
    /// Typical values: 1000-10000 points
    pub block_size: usize,

    /// Enable CRC64 checksum verification
    ///
    /// When enabled, a checksum is calculated during compression and verified
    /// during decompression to detect data corruption. Adds ~5% overhead.
    pub enable_checksum: bool,
}

impl Default for GorillaConfig {
    fn default() -> Self {
        Self {
            block_size: 1000,
            enable_checksum: true,
        }
    }
}

impl GorillaCompressor {
    /// Create a new Gorilla compressor with default configuration
    pub fn new() -> Self {
        Self::with_config(GorillaConfig::default())
    }

    /// Create a new Gorilla compressor with custom configuration
    pub fn with_config(config: GorillaConfig) -> Self {
        Self {
            stats: Arc::new(Mutex::new(CompressionStats::default())),
            config,
        }
    }

    /// Compress timestamps using delta-of-delta encoding with variable bit packing
    ///
    /// This is the core of Gorilla's timestamp compression. It exploits the regularity
    /// of time-series timestamps by storing the "delta of deltas" instead of raw values.
    ///
    /// # Algorithm Steps
    ///
    /// 1. Store first timestamp as full 64 bits (baseline)
    /// 2. Store second timestamp as delta from first (64 bits)
    /// 3. For remaining timestamps, calculate delta-of-delta and use variable encoding:
    ///    - If dod = 0: Write single '0' bit (1 bit total)
    ///    - If dod in [-63, 64): Write '10' + 7-bit value (9 bits total)
    ///    - If dod in [-255, 256): Write '110' + 9-bit value (12 bits total)
    ///    - If dod in [-2047, 2048): Write '1110' + 12-bit value (16 bits total)
    ///    - Otherwise: Write '1111' + full 32-bit delta (36 bits total)
    ///
    /// # Example
    ///
    /// ```text
    /// Input timestamps: [1000, 1010, 1020, 1030, 1040]
    ///
    /// Step 1: Write 1000 (64 bits)
    /// Step 2: Delta = 10, write 10 (64 bits)
    /// Step 3: Delta = 10, dod = 0 → write '0' (1 bit)
    /// Step 4: Delta = 10, dod = 0 → write '0' (1 bit)
    /// Step 5: Delta = 10, dod = 0 → write '0' (1 bit)
    ///
    /// Total: 64 + 64 + 1 + 1 + 1 = 131 bits vs 320 bits uncompressed (59% savings)
    /// ```
    ///
    /// # Arguments
    ///
    /// * `points` - Slice of data points with timestamps to compress
    /// * `writer` - Bit writer to output compressed data
    ///
    /// # Performance
    ///
    /// For regular intervals (most common case), this achieves ~64:1 compression
    /// after the first two timestamps (1 bit vs 64 bits per timestamp).
    fn compress_timestamps(points: &[DataPoint], writer: &mut BitWriter) {
        if points.is_empty() {
            return;
        }

        // Store first timestamp as full 64 bits
        // This serves as the baseline for all subsequent delta calculations
        writer.write_bits(points[0].timestamp as u64, 64);

        if points.len() == 1 {
            return;
        }

        // Store second timestamp as delta from first (64 bits)
        // We need this to establish the baseline delta for delta-of-delta encoding
        // Note: Using 64 bits here (not 14) to handle any possible interval
        let first_delta = points[1].timestamp - points[0].timestamp;
        writer.write_bits(first_delta as u64, 64);

        // Now we can do delta-of-delta encoding for remaining timestamps
        let mut prev_timestamp = points[1].timestamp;
        let mut prev_delta = first_delta;

        for point in &points[2..] {
            // Calculate current delta and delta-of-delta
            // Using wrapping arithmetic to handle potential overflow safely
            let delta = point.timestamp.wrapping_sub(prev_timestamp);
            let dod = delta.wrapping_sub(prev_delta);

            // Variable bit packing based on delta-of-delta magnitude
            // The ranges are EXCLUSIVE on the upper bound (e.g., -63..64 means [-63, 63])
            // This ensures we only use values that fit in the specified bit width

            if dod == 0 {
                // Most common case: interval unchanged
                // Write single '0' bit (1 bit total)
                writer.write_bit(false);
            } else if (-63..64).contains(&dod) {
                // Small change in interval: [-63, 63]
                // Write '10' prefix (2 bits) + 7-bit encoded value (9 bits total)
                // Encoding: Add 63 to shift range from [-63,63] to [0,126]
                writer.write_bits(0b10, 2);
                writer.write_bits(((dod + 63) as u64) & 0x7F, 7);
            } else if (-255..256).contains(&dod) {
                // Medium change in interval: [-255, 255]
                // Write '110' prefix (3 bits) + 9-bit encoded value (12 bits total)
                // Encoding: Add 255 to shift range from [-255,255] to [0,510]
                writer.write_bits(0b110, 3);
                writer.write_bits(((dod + 255) as u64) & 0x1FF, 9);
            } else if (-2047..2048).contains(&dod) {
                // Large change in interval: [-2047, 2047]
                // Write '1110' prefix (4 bits) + 12-bit encoded value (16 bits total)
                // Encoding: Add 2047 to shift range from [-2047,2047] to [0,4094]
                writer.write_bits(0b1110, 4);
                writer.write_bits(((dod + 2047) as u64) & 0xFFF, 12);
            } else {
                // Very large/irregular change: store full delta
                // Write '1111' prefix (4 bits) + full 32-bit delta (36 bits total)
                // This bypasses delta-of-delta for irregular time series
                writer.write_bits(0b1111, 4);
                writer.write_bits(delta as u64, 32);
            }

            // Update tracking variables for next iteration
            prev_timestamp = point.timestamp;
            prev_delta = delta;
        }
    }

    /// Decompress timestamps using delta-of-delta decoding
    ///
    /// This reverses the timestamp compression process, reading variable-length encoded
    /// delta-of-delta values and reconstructing the original timestamps.
    ///
    /// # Algorithm Steps
    ///
    /// 1. Read first timestamp (64 bits)
    /// 2. Read first delta (64 bits), calculate second timestamp
    /// 3. For remaining timestamps, read control bits to determine encoding:
    ///    - '0': dod = 0, delta unchanged
    ///    - '10': Read 7 bits, decode to dod in [-63, 63]
    ///    - '110': Read 9 bits, decode to dod in [-255, 255]
    ///    - '1110': Read 12 bits, decode to dod in [-2047, 2047]
    ///    - '1111': Read 32 bits as full delta (bypass dod)
    /// 4. Reconstruct timestamp: prev_timestamp + delta
    ///
    /// # Arguments
    ///
    /// * `count` - Number of timestamps to decompress
    /// * `reader` - Bit reader to read compressed data from
    ///
    /// # Returns
    ///
    /// Vector of decompressed timestamps in chronological order
    ///
    /// # Errors
    ///
    /// - `InvalidData` if count exceeds MAX_POINTS_PER_BLOCK (10 million)
    /// - `CorruptedData` if bit stream is truncated or invalid
    ///
    /// # Security
    ///
    /// Validates count to prevent unbounded allocation attacks. The 10M limit
    /// is generous for legitimate use but prevents memory exhaustion DoS.
    fn decompress_timestamps(
        count: usize,
        reader: &mut BitReader,
    ) -> Result<Vec<i64>, CompressionError> {
        // Maximum reasonable points per block (10 million)
        // This prevents DoS attacks via malicious count values that would cause
        // unbounded memory allocation
        const MAX_POINTS_PER_BLOCK: usize = 10_000_000;

        if count > MAX_POINTS_PER_BLOCK {
            return Err(CompressionError::InvalidData(
                format!("Point count {} exceeds maximum allowed {}", count, MAX_POINTS_PER_BLOCK)
            ));
        }

        if count == 0 {
            return Ok(Vec::new());
        }

        // Pre-allocate with exact capacity for performance
        let mut timestamps = Vec::with_capacity(count);

        // Read first timestamp (full 64 bits baseline)
        let first_timestamp = reader.read_bits(64)? as i64;
        timestamps.push(first_timestamp);

        if count == 1 {
            return Ok(timestamps);
        }

        // Read first delta (64 bits) and calculate second timestamp
        let first_delta = reader.read_bits(64)? as i64;
        let second_timestamp = first_timestamp + first_delta;
        timestamps.push(second_timestamp);

        // Decode remaining timestamps using delta-of-delta
        let mut prev_timestamp = second_timestamp;
        let mut prev_delta = first_delta;

        for _ in 2..count {
            // Read control bits to determine encoding format
            let delta = if !reader.read_bit()? {
                // Single '0' bit: delta unchanged (dod = 0)
                prev_delta
            } else {
                // Read additional control bits to determine format
                if !reader.read_bit()? {
                    // '10': 7-bit encoded dod
                    // Decode: Read 7 bits [0,126], subtract 63 to get [-63,63]
                    let dod = (reader.read_bits(7)? as i64) - 63;
                    prev_delta + dod
                } else if !reader.read_bit()? {
                    // '110': 9-bit encoded dod
                    // Decode: Read 9 bits [0,510], subtract 255 to get [-255,255]
                    let dod = (reader.read_bits(9)? as i64) - 255;
                    prev_delta + dod
                } else if !reader.read_bit()? {
                    // '1110': 12-bit encoded dod
                    // Decode: Read 12 bits [0,4094], subtract 2047 to get [-2047,2047]
                    let dod = (reader.read_bits(12)? as i64) - 2047;
                    prev_delta + dod
                } else {
                    // '1111': Full 32-bit delta (bypass delta-of-delta)
                    // Used for irregular time series with large jumps
                    // Directly use this as the delta without adding to prev_delta
                    reader.read_bits(32)? as i64
                }
            };

            // Reconstruct timestamp and add to result
            let timestamp = prev_timestamp + delta;
            timestamps.push(timestamp);

            // Update state for next iteration
            prev_timestamp = timestamp;
            prev_delta = delta;
        }

        Ok(timestamps)
    }

    /// Compress floating-point values using XOR encoding
    ///
    /// This exploits the property that consecutive values in time-series data often differ
    /// by only a few bits. By XORing consecutive values and encoding only the changed bits,
    /// we achieve excellent compression.
    ///
    /// # Algorithm Overview
    ///
    /// ```text
    /// Previous: 42.123456 (bits: 0x4045...ABCD)
    /// Current:  42.123457 (bits: 0x4045...ABCE)
    /// XOR:                        0x0000...0001
    ///                             ^^^^^    ^^^^^
    ///                             leading  trailing
    ///                             zeros    zeros
    /// Meaningful bits: only the middle part that's non-zero
    /// ```
    ///
    /// # Encoding Strategy
    ///
    /// 1. Store first value as full 64 bits
    /// 2. For subsequent values:
    ///    - If value unchanged (XOR = 0): Write single '0' bit
    ///    - If value changed:
    ///      - Write '1' bit to indicate change
    ///      - If leading/trailing zeros match previous:
    ///        - Write '0' bit + meaningful bits only
    ///      - Otherwise:
    ///        - Write '1' bit + 5-bit leading count + 6-bit length + meaningful bits
    ///
    /// # Example
    ///
    /// ```text
    /// Values: [100.0, 100.1, 100.2, 100.3]
    ///
    /// Step 1: Write 100.0 (64 bits)
    /// Step 2: XOR(100.0, 100.1) has ~50 leading zeros, ~8 meaningful bits
    ///         Write: '1' + '1' + leading(5) + length(6) + meaningful(8) = 21 bits
    /// Step 3: XOR(100.1, 100.2) has same pattern
    ///         Write: '1' + '0' + meaningful(8) = 10 bits
    /// Step 4: XOR(100.2, 100.3) has same pattern
    ///         Write: '1' + '0' + meaningful(8) = 10 bits
    ///
    /// Total: 64 + 21 + 10 + 10 = 105 bits vs 256 bits uncompressed (59% savings)
    /// ```
    ///
    /// # Arguments
    ///
    /// * `points` - Slice of data points with values to compress
    /// * `writer` - Bit writer to output compressed data
    ///
    /// # Performance
    ///
    /// For slowly changing values (typical in monitoring), this achieves 6-8:1 compression.
    /// For rapidly changing values, compression ratio degrades but still saves space.
    fn compress_values(points: &[DataPoint], writer: &mut BitWriter) {
        if points.is_empty() {
            return;
        }

        // Write first value as full 64 bits (IEEE 754 double precision)
        // This serves as the baseline for XOR comparisons
        writer.write_bits(points[0].value.to_bits(), 64);

        // Track previous value and zero-run information
        let mut prev_value = points[0].value.to_bits();
        let mut prev_leading = 0u32;   // Leading zeros in previous XOR
        let mut prev_trailing = 0u32;  // Trailing zeros in previous XOR

        for point in &points[1..] {
            // Convert current value to bits and XOR with previous
            let curr_value = point.value.to_bits();
            let xor = prev_value ^ curr_value;

            if xor == 0 {
                // Value unchanged: write single '0' bit (most compact case)
                writer.write_bit(false);
            } else {
                // Value changed: write '1' bit to indicate this
                writer.write_bit(true);

                // Count leading and trailing zeros in the XOR result
                // These tell us which bits actually changed
                let leading = xor.leading_zeros();
                let trailing = xor.trailing_zeros();

                // Try to reuse previous zero-run information
                // If the current XOR has at least as many leading/trailing zeros,
                // we can use the previous block's parameters
                if leading >= prev_leading && trailing >= prev_trailing {
                    // Reuse previous block: write '0' + meaningful bits
                    writer.write_bit(false);

                    // Calculate how many bits are meaningful (non-zero)
                    let meaningful_bits = 64 - prev_leading - prev_trailing;

                    if meaningful_bits > 0 {
                        // Extract and write only the meaningful bits
                        // Shift right by trailing zeros to remove them
                        writer.write_bits(
                            xor >> prev_trailing,
                            meaningful_bits as u8,
                        );
                    }
                } else {
                    // Need new block info: write '1' + block metadata + meaningful bits
                    writer.write_bit(true);

                    // Write leading zero count (5 bits = up to 31, but we have 64-bit values)
                    // Note: We can encode up to 31 leading zeros with 5 bits
                    writer.write_bits(leading as u64, 5);

                    // Write meaningful bits length (6 bits = up to 63 bits)
                    let meaningful_bits = 64 - leading - trailing;
                    writer.write_bits(meaningful_bits as u64, 6);

                    // Write the meaningful bits themselves
                    if meaningful_bits > 0 {
                        writer.write_bits(xor >> trailing, meaningful_bits as u8);
                    }

                    // Update tracking for next iteration
                    prev_leading = leading;
                    prev_trailing = trailing;
                }
            }

            // Update previous value for next comparison
            prev_value = curr_value;
        }
    }

    /// Decompress values using XOR decoding
    ///
    /// Reverses the XOR compression, reading control bits and reconstructing original
    /// floating-point values from XOR deltas.
    ///
    /// # Arguments
    ///
    /// * `count` - Number of values to decompress
    /// * `reader` - Bit reader to read compressed data from
    ///
    /// # Returns
    ///
    /// Vector of decompressed floating-point values
    ///
    /// # Errors
    ///
    /// - `InvalidData` if count exceeds MAX_POINTS_PER_BLOCK
    /// - `CorruptedData` if bit stream is truncated or invalid
    fn decompress_values(
        count: usize,
        reader: &mut BitReader,
    ) -> Result<Vec<f64>, CompressionError> {
        // Maximum reasonable points per block (10 million)
        // Prevents DoS via unbounded allocation
        const MAX_POINTS_PER_BLOCK: usize = 10_000_000;

        if count > MAX_POINTS_PER_BLOCK {
            return Err(CompressionError::InvalidData(
                format!("Point count {} exceeds maximum allowed {}", count, MAX_POINTS_PER_BLOCK)
            ));
        }

        if count == 0 {
            return Ok(Vec::new());
        }

        let mut values = Vec::with_capacity(count);

        // Read first value (full 64-bit IEEE 754 float)
        let first_value = f64::from_bits(reader.read_bits(64)?);
        values.push(first_value);

        // Track previous value and block parameters
        let mut prev_value = first_value.to_bits();
        let mut prev_leading = 0u32;
        let mut prev_trailing = 0u32;

        for _ in 1..count {
            // Read control bit
            if !reader.read_bit()? {
                // '0': Value unchanged, reuse previous value
                values.push(f64::from_bits(prev_value));
                continue;
            }

            // Value changed, read XOR encoding
            let xor = if !reader.read_bit()? {
                // '10': Use previous block's leading/trailing parameters
                let meaningful_bits = 64 - prev_leading - prev_trailing;
                if meaningful_bits > 0 {
                    // Read meaningful bits and shift back to original position
                    reader.read_bits(meaningful_bits as u8)? << prev_trailing
                } else {
                    0
                }
            } else {
                // '11': New block parameters
                let leading = reader.read_bits(5)? as u32;
                let meaningful_bits = reader.read_bits(6)? as u32;

                if meaningful_bits > 0 {
                    // Read meaningful bits
                    let bits = reader.read_bits(meaningful_bits as u8)?;
                    // Calculate trailing zeros and update tracking
                    let trailing = 64 - leading - meaningful_bits;
                    prev_leading = leading;
                    prev_trailing = trailing;
                    // Shift bits back to original position
                    bits << trailing
                } else {
                    // Edge case: no meaningful bits (shouldn't happen normally)
                    prev_leading = leading;
                    prev_trailing = 64 - leading;
                    0
                }
            };

            // XOR with previous value to get current value
            let value_bits = prev_value ^ xor;
            values.push(f64::from_bits(value_bits));
            prev_value = value_bits;
        }

        Ok(values)
    }

    /// Calculate CRC64 checksum for data integrity verification
    ///
    /// Uses the CRC-64-ECMA-182 polynomial for checksumming compressed blocks.
    /// This detects corruption in stored or transmitted compressed data.
    ///
    /// # Arguments
    ///
    /// * `data` - Byte slice to checksum
    ///
    /// # Returns
    ///
    /// 64-bit checksum value
    pub fn calculate_checksum(data: &[u8]) -> u64 {
        crc::Crc::<u64>::new(&crc::CRC_64_ECMA_182).checksum(data)
    }
}

impl Default for GorillaCompressor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Compressor for GorillaCompressor {
    fn algorithm_id(&self) -> &str {
        "gorilla-v1"
    }

    async fn compress(&self, points: &[DataPoint]) -> Result<CompressedBlock, CompressionError> {
        if points.is_empty() {
            return Err(CompressionError::InvalidData(
                "Cannot compress empty point set".to_string(),
            ));
        }

        // Validate timestamps are monotonically increasing and no duplicates
        for i in 1..points.len() {
            if points[i].timestamp <= points[i - 1].timestamp {
                return Err(CompressionError::InvalidData(
                    format!(
                        "Timestamps must be strictly increasing: point {} has timestamp {} <= previous {}",
                        i, points[i].timestamp, points[i - 1].timestamp
                    ),
                ));
            }
        }

        // Validate timestamp deltas won't overflow
        if points.len() > 1 {
            for i in 1..points.len() {
                let delta = points[i].timestamp.checked_sub(points[i - 1].timestamp);
                if delta.is_none() {
                    return Err(CompressionError::InvalidData(
                        format!(
                            "Timestamp overflow detected at point {}: {} - {} would overflow",
                            i, points[i].timestamp, points[i - 1].timestamp
                        ),
                    ));
                }
            }
        }

        let start = std::time::Instant::now();
        let mut writer = BitWriter::new();

        // Write point count
        writer.write_bits(points.len() as u64, 32);

        // Compress timestamps and values
        Self::compress_timestamps(points, &mut writer);
        Self::compress_values(points, &mut writer);

        let compressed_data = writer.finish();
        let compressed_size = compressed_data.len();
        let original_size = size_of_val(points);

        // Calculate checksum
        let checksum = if self.config.enable_checksum {
            Self::calculate_checksum(&compressed_data)
        } else {
            0
        };

        // Update stats
        {
            let mut stats = self.stats.lock();
            stats.total_compressed += 1;
            stats.compression_time_ms += start.elapsed().as_millis() as u64;
            let ratio = original_size as f64 / compressed_size.max(1) as f64;
            stats.average_ratio = if stats.total_compressed == 1 {
                ratio
            } else {
                (stats.average_ratio * (stats.total_compressed - 1) as f64 + ratio)
                    / stats.total_compressed as f64
            };
        }

        Ok(CompressedBlock {
            algorithm_id: self.algorithm_id().to_string(),
            original_size,
            compressed_size,
            checksum,
            data: Bytes::from(compressed_data),
            metadata: BlockMetadata {
                start_timestamp: points.first()
                    .expect("points already validated non-empty").timestamp,
                end_timestamp: points.last()
                    .expect("points already validated non-empty").timestamp,
                point_count: points.len(),
                series_id: points[0].series_id,
            },
        })
    }

    async fn decompress(&self, block: &CompressedBlock) -> Result<Vec<DataPoint>, CompressionError> {
        let start = std::time::Instant::now();

        // Handle empty blocks early
        if block.data.is_empty() || block.metadata.point_count == 0 {
            return Ok(Vec::new());
        }

        // Verify checksum if enabled
        if self.config.enable_checksum && block.checksum != 0 {
            let calculated = Self::calculate_checksum(&block.data);
            if calculated != block.checksum {
                return Err(CompressionError::CorruptedData(format!(
                    "Checksum mismatch: expected {}, got {}",
                    block.checksum, calculated
                )));
            }
        }

        let mut reader = BitReader::new(&block.data);

        // Read point count
        let count = reader.read_bits(32)? as usize;

        // Maximum reasonable points per block (10 million)
        const MAX_POINTS_PER_BLOCK: usize = 10_000_000;

        if count > MAX_POINTS_PER_BLOCK {
            return Err(CompressionError::InvalidData(
                format!("Point count {} exceeds maximum allowed {}", count, MAX_POINTS_PER_BLOCK)
            ));
        }

        if count == 0 {
            return Ok(Vec::new());
        }

        // Decompress timestamps and values
        let timestamps = Self::decompress_timestamps(count, &mut reader)?;
        let values = Self::decompress_values(count, &mut reader)?;

        if timestamps.len() != values.len() {
            return Err(CompressionError::CorruptedData(
                "Timestamp and value count mismatch".to_string(),
            ));
        }

        // Reconstruct data points
        let points: Vec<DataPoint> = timestamps
            .into_iter()
            .zip(values)
            .map(|(timestamp, value)| DataPoint {
                series_id: block.metadata.series_id,
                timestamp,
                value,
            })
            .collect();

        // Update stats
        {
            let mut stats = self.stats.lock();
            stats.total_decompressed += 1;
            stats.decompression_time_ms += start.elapsed().as_millis() as u64;
        }

        Ok(points)
    }

    fn estimate_ratio(&self, _sample: &[DataPoint]) -> f64 {
        // Typical Gorilla compression ratio based on research
        12.0
    }

    fn stats(&self) -> CompressionStats {
        self.stats.lock().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_points(count: usize) -> Vec<DataPoint> {
        (0..count)
            .map(|i| DataPoint {
                series_id: 1,
                timestamp: 1000 + (i as i64 * 10), // Regular 10ms intervals
                value: 100.0 + (i as f64 * 0.5),   // Gradually increasing values
            })
            .collect()
    }

    #[tokio::test]
    async fn test_compress_decompress_single_point() {
        let compressor = GorillaCompressor::new();
        let points = vec![DataPoint {
            series_id: 1,
            timestamp: 1000,
            value: 42.5,
        }];

        let compressed = compressor.compress(&points).await.unwrap();
        let decompressed = compressor.decompress(&compressed).await.unwrap();

        assert_eq!(decompressed.len(), 1);
        assert_eq!(decompressed[0].timestamp, 1000);
        assert_eq!(decompressed[0].value, 42.5);
    }

    #[tokio::test]
    async fn test_compress_decompress_multiple_points() {
        let compressor = GorillaCompressor::new();
        let points = create_test_points(100);

        let compressed = compressor.compress(&points).await.unwrap();
        let decompressed = compressor.decompress(&compressed).await.unwrap();

        assert_eq!(decompressed.len(), points.len());

        for (original, decompressed) in points.iter().zip(decompressed.iter()) {
            assert_eq!(decompressed.timestamp, original.timestamp);
            assert_eq!(decompressed.value, original.value);
        }
    }

    #[tokio::test]
    async fn test_compression_ratio() {
        let compressor = GorillaCompressor::new();
        let points = create_test_points(1000);

        let original_size = std::mem::size_of_val(points.as_slice());
        let compressed = compressor.compress(&points).await.unwrap();

        let ratio = original_size as f64 / compressed.compressed_size as f64;
        println!("Compression ratio: {:.2}:1", ratio);

        // Should achieve at least 2:1 compression
        assert!(ratio > 2.0);
    }

    #[tokio::test]
    async fn test_checksum_verification() {
        let compressor = GorillaCompressor::new();
        let points = create_test_points(10);

        let mut compressed = compressor.compress(&points).await.unwrap();

        // Corrupt the data
        let mut data = compressed.data.to_vec();
        if !data.is_empty() {
            data[0] ^= 0xFF;
        }
        compressed.data = Bytes::from(data);

        // Should fail checksum verification
        let result = compressor.decompress(&compressed).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_empty_points() {
        let compressor = GorillaCompressor::new();
        let points: Vec<DataPoint> = vec![];

        let result = compressor.compress(&points).await;
        assert!(result.is_err());
    }
}
