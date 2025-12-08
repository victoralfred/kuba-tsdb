//! Bit-level I/O primitives for Kuba compression
//!
//! This module provides low-level bit manipulation utilities for reading and writing
//! individual bits and bit sequences. These are essential for the Kuba compression
//! algorithm which requires precise bit-level control.
//!
//! # Overview
//!
//! Bits are stored MSB-first (most significant bit first) within each byte:
//! ```text
//! Byte: [bit0 bit1 bit2 bit3 bit4 bit5 bit6 bit7]
//!        MSB                                    LSB
//! ```
//!
//! # Example
//! ```
//! use kuba_tsdb::compression::bit_stream::{BitWriter, BitReader};
//!
//! // Write some bits
//! let mut writer = BitWriter::new();
//! writer.write_bit(true);
//! writer.write_bits(0b1010, 4);
//! let buffer = writer.finish().unwrap();
//!
//! // Read them back
//! let mut reader = BitReader::new(&buffer);
//! assert!(reader.read_bit().unwrap());
//! assert_eq!(reader.read_bits(4).unwrap(), 0b1010);
//! ```

use crate::error::CompressionError;

/// Writer for bit-level operations
///
/// `BitWriter` accumulates bits into bytes and maintains precise bit-level positioning.
/// Bits are written MSB-first within each byte, matching the Kuba paper specification.
///
/// # Internal State
///
/// - `buffer`: Completed bytes that have been fully written
/// - `current_byte`: The byte currently being assembled (0-7 bits written)
/// - `bit_position`: Position within current_byte (0-7), indicating how many bits are used
/// - `overflow`: Flag indicating if buffer size limit was exceeded (data loss occurred)
///
/// # Example
/// ```
/// use kuba_tsdb::compression::bit_stream::BitWriter;
///
/// let mut writer = BitWriter::new();
/// writer.write_bit(true);   // Writes 1 to bit position 0
/// writer.write_bits(5, 3);  // Writes 101 to bit positions 1-3
/// let data = writer.finish(); // Flushes partial byte if needed
/// ```
pub struct BitWriter {
    /// Buffer of fully completed bytes
    buffer: Vec<u8>,
    /// Current byte being assembled (contains 0-7 bits)
    current_byte: u8,
    /// Number of bits written to current_byte (0-7)
    bit_position: u8,
    /// Flag indicating if buffer overflow occurred (data loss)
    overflow: bool,
}

impl BitWriter {
    /// Create a new bit writer with empty state
    ///
    /// Initializes with:
    /// - Empty buffer
    /// - current_byte = 0
    /// - bit_position = 0
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            current_byte: 0,
            bit_position: 0,
            overflow: false,
        }
    }

    /// Create a new bit writer with capacity hint
    ///
    /// # Security
    /// - Caps capacity to prevent DoS via huge allocations
    pub fn with_capacity(capacity: usize) -> Self {
        // SEC: Cap capacity to prevent DoS
        const MAX_CAPACITY: usize = 100_000_000; // 100MB max
        let capacity = capacity.min(MAX_CAPACITY);
        Self {
            buffer: Vec::with_capacity(capacity),
            current_byte: 0,
            bit_position: 0,
            overflow: false,
        }
    }

    /// Write a single bit to the stream
    ///
    /// Bits are written MSB-first within each byte. When a byte is complete (8 bits),
    /// it's automatically flushed to the buffer and a new byte is started.
    ///
    /// # Arguments
    /// * `bit` - The bit value to write (true = 1, false = 0)
    ///
    /// # Example
    /// ```
    /// use kuba_tsdb::compression::bit_stream::BitWriter;
    ///
    /// let mut writer = BitWriter::new();
    /// writer.write_bit(true);   // Writes bit 1
    /// writer.write_bit(false);  // Writes bit 0
    /// ```
    pub fn write_bit(&mut self, bit: bool) {
        // If bit is true (1), set the appropriate bit in current_byte
        // Position is calculated as (7 - bit_position) for MSB-first ordering
        if bit {
            self.current_byte |= 1 << (7 - self.bit_position);
        }
        // Note: if bit is false, we don't need to do anything since current_byte
        // starts at 0 and false bits remain 0

        // Move to next bit position
        self.bit_position += 1;

        // Safety check: ensure bit_position never exceeds 8
        debug_assert!(self.bit_position <= 8, "bit_position overflow");

        // If we've filled a complete byte (8 bits), flush it to buffer
        if self.bit_position >= 8 {
            // SEC: Limit buffer size to prevent DoS via memory exhaustion
            const MAX_BUFFER_SIZE: usize = 100_000_000; // 100MB max
            if self.buffer.len() >= MAX_BUFFER_SIZE {
                // BUG-002 FIX: Mark overflow and prevent further writes
                // This indicates potential DoS attack or data corruption
                // We mark overflow so finish() can detect and report it
                self.overflow = true;
                // SEC: Don't reset bit_position or current_byte - they contain data that will be lost
                // This allows finish() to detect partial byte loss
                return;
            }
            self.buffer.push(self.current_byte);
            self.current_byte = 0; // Reset for next byte
            self.bit_position = 0; // Reset position counter
        }
    }

    /// Write multiple bits from a u64 value
    ///
    /// Writes bits MSB-first from the value. For example, write_bits(0b1010, 4)
    /// will write the bits 1, 0, 1, 0 in that order.
    ///
    /// # Arguments
    /// * `value` - The value containing the bits to write
    /// * `num_bits` - Number of bits to write (must be <= 64)
    ///
    /// # Panics
    /// In debug mode, panics if num_bits > 64
    ///
    /// # Example
    /// ```
    /// use kuba_tsdb::compression::bit_stream::BitWriter;
    ///
    /// let mut writer = BitWriter::new();
    /// writer.write_bits(0b1010, 4);  // Writes bits: 1, 0, 1, 0
    /// writer.write_bits(0xFF, 8);    // Writes 8 bits: all 1s
    /// ```
    pub fn write_bits(&mut self, value: u64, num_bits: u8) {
        // SEC: Validate num_bits to prevent DoS
        if num_bits > 64 {
            // Invalid input - silently ignore or could return error
            // For now, clamp to 64 to prevent issues
            return;
        }

        // Write bits from MSB to LSB by iterating in reverse order
        // For num_bits=4, value=0b1010:
        //   i=3: writes bit at position 3 (1)
        //   i=2: writes bit at position 2 (0)
        //   i=1: writes bit at position 1 (1)
        //   i=0: writes bit at position 0 (0)
        for i in (0..num_bits).rev() {
            // Extract the i-th bit: shift right by i positions and mask with 1
            let bit = (value >> i) & 1 == 1;
            self.write_bit(bit);
        }
    }

    /// Flush remaining bits and return the completed buffer
    ///
    /// If there are any bits in the current_byte (bit_position > 0), they are
    /// flushed to the buffer. The remaining bits in the byte will be 0-padded.
    ///
    /// This method consumes self and returns the final byte buffer.
    ///
    /// # Returns
    /// Vector of bytes containing all written bits
    ///
    /// # Errors
    /// Returns `CompressionError::ResourceLimit` if buffer overflow occurred during writing
    ///
    /// # Example
    /// ```
    /// use kuba_tsdb::compression::bit_stream::BitWriter;
    ///
    /// let mut writer = BitWriter::new();
    /// writer.write_bits(0b1010, 4);  // Only 4 bits written
    /// let buffer = writer.finish().unwrap();   // Flushes with 4 trailing zero bits
    /// assert_eq!(buffer.len(), 1);    // One byte total
    /// ```
    pub fn finish(mut self) -> Result<Vec<u8>, CompressionError> {
        // BUG-002 FIX: Check for overflow before finishing
        if self.overflow {
            return Err(CompressionError::ResourceLimit(format!(
                "BitWriter buffer overflow: exceeded maximum size of {} bytes. Data loss occurred.",
                100_000_000
            )));
        }

        // If there are any bits written in current_byte, flush it
        // The remaining bits (8 - bit_position) will be 0-padded
        // SEC: Check buffer size before adding final byte
        const MAX_BUFFER_SIZE: usize = 100_000_000; // 100MB max
        if self.bit_position > 0 {
            if self.buffer.len() >= MAX_BUFFER_SIZE {
                return Err(CompressionError::ResourceLimit(format!(
                    "BitWriter buffer overflow: cannot flush final byte ({} bits) - buffer already at maximum size of {} bytes",
                    self.bit_position,
                    MAX_BUFFER_SIZE
                )));
            }
            self.buffer.push(self.current_byte);
        }

        Ok(self.buffer)
    }

    /// Get current buffer size in bytes
    ///
    /// Returns the number of bytes that will be in the final buffer.
    /// If there are any bits in current_byte, they count as a full byte.
    ///
    /// # Returns
    /// Total number of bytes (including partial byte if present)
    pub fn len(&self) -> usize {
        let mut len = self.buffer.len();
        // If we have any bits in current_byte, count it as one more byte
        if self.bit_position > 0 {
            len += 1;
        }
        len
    }

    /// Check if buffer is empty (no bits written yet)
    ///
    /// # Returns
    /// true if no bits have been written, false otherwise
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty() && self.bit_position == 0
    }
}

impl Default for BitWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// Reader for bit-level operations
///
/// `BitReader` reads bits sequentially from a byte buffer, maintaining precise
/// bit-level positioning. Reads are performed MSB-first within each byte.
///
/// # Internal State
///
/// - `buffer`: The source byte slice to read from
/// - `byte_position`: Current byte being read (index into buffer)
/// - `bit_position`: Position within current byte (0-7)
///
/// # Error Handling
///
/// Returns `CompressionError::CorruptedData` if attempting to read past the end
/// of the buffer, which indicates corrupted or truncated compressed data.
///
/// # Example
/// ```
/// use kuba_tsdb::compression::bit_stream::BitReader;
///
/// let data = vec![0b10101100];
/// let mut reader = BitReader::new(&data);
/// assert!(reader.read_bit().unwrap());   // Reads 1
/// assert!(!reader.read_bit().unwrap());  // Reads 0
/// ```
pub struct BitReader<'a> {
    /// Source byte buffer to read from (borrowed)
    buffer: &'a [u8],
    /// Current byte index in buffer
    byte_position: usize,
    /// Current bit position within the current byte (0-7)
    bit_position: u8,
}

impl<'a> BitReader<'a> {
    /// Create a new bit reader from a byte slice
    ///
    /// Initializes reading from the beginning of the buffer (position 0, bit 0).
    ///
    /// # Arguments
    /// * `buffer` - Byte slice to read from
    ///
    /// # Example
    /// ```
    /// use kuba_tsdb::compression::bit_stream::BitReader;
    ///
    /// let data = vec![0xFF, 0x00];
    /// let reader = BitReader::new(&data);
    /// ```
    pub fn new(buffer: &'a [u8]) -> Self {
        Self {
            buffer,
            byte_position: 0,
            bit_position: 0,
        }
    }

    /// Read a single bit from the stream
    ///
    /// Reads the next bit in MSB-first order from the current position.
    /// Automatically advances to the next byte when a byte boundary is crossed.
    ///
    /// # Returns
    /// - `Ok(bool)` - The bit value (true = 1, false = 0)
    /// - `Err(CompressionError)` - If attempting to read past end of buffer
    ///
    /// # Errors
    /// Returns `CorruptedData` error if we've reached the end of the buffer,
    /// indicating the compressed data is truncated or corrupted.
    ///
    /// # Example
    /// ```
    /// use kuba_tsdb::compression::bit_stream::BitReader;
    ///
    /// let data = vec![0b10101010];
    /// let mut reader = BitReader::new(&data);
    /// assert!(reader.read_bit().unwrap());   // 1
    /// assert!(!reader.read_bit().unwrap());  // 0
    /// ```
    pub fn read_bit(&mut self) -> Result<bool, CompressionError> {
        // Check if we've reached the end of the buffer
        if self.byte_position >= self.buffer.len() {
            return Err(CompressionError::CorruptedData(
                "Unexpected end of buffer".to_string(),
            ));
        }

        // Get the current byte we're reading from
        let byte = self.buffer[self.byte_position];

        // Extract the bit at position (7 - bit_position) for MSB-first ordering
        // Example: for bit_position=0, we read bit 7 (MSB)
        //          for bit_position=7, we read bit 0 (LSB)
        let bit = (byte >> (7 - self.bit_position)) & 1 == 1;

        // Move to next bit position
        self.bit_position += 1;

        // If we've read all 8 bits of current byte, move to next byte
        if self.bit_position >= 8 {
            self.byte_position += 1; // Move to next byte
            self.bit_position = 0; // Reset to first bit of new byte
        }

        Ok(bit)
    }

    /// Read multiple bits into a u64 value
    ///
    /// Reads the specified number of bits and assembles them into a u64 value.
    /// Bits are read MSB-first and accumulated left-to-right.
    ///
    /// # Arguments
    /// * `num_bits` - Number of bits to read (0-64 inclusive)
    ///
    /// # Returns
    /// - `Ok(u64)` - The assembled value from the bits read
    /// - `Err(CompressionError)` - If num_bits > 64 or buffer ends prematurely
    ///
    /// # Edge Cases
    /// - `num_bits = 0`: Returns `Ok(0)` without advancing the read position.
    ///   This is intentional for variable-width encoding schemes.
    ///
    /// # Errors
    /// - `InvalidData` if num_bits > 64
    /// - `CorruptedData` if we run out of data while reading
    ///
    /// # Example
    /// ```
    /// use kuba_tsdb::compression::bit_stream::BitReader;
    ///
    /// let data = vec![0b10101100];
    /// let mut reader = BitReader::new(&data);
    /// let value = reader.read_bits(4).unwrap();  // Reads 0b1010
    /// assert_eq!(value, 0b1010);
    /// ```
    pub fn read_bits(&mut self, num_bits: u8) -> Result<u64, CompressionError> {
        // EDGE-003: Handle zero bits case explicitly
        // Reading 0 bits returns 0 without advancing the position.
        // This is intentional and useful for variable-width encoding schemes
        // where the bit count may be computed dynamically.
        if num_bits == 0 {
            return Ok(0);
        }

        // Validate input: can't read more than 64 bits into a u64
        if num_bits > 64 {
            return Err(CompressionError::InvalidData(format!(
                "Cannot read more than 64 bits (requested: {})",
                num_bits
            )));
        }

        let mut value: u64 = 0;

        // SEC: Limit number of bit reads to prevent DoS
        // This is already limited by num_bits <= 64, but add explicit check
        const MAX_BIT_READS: u8 = 64;
        let bits_to_read = num_bits.min(MAX_BIT_READS);

        // Read bits one at a time and accumulate them
        // Each iteration: shift value left 1 bit and add the new bit
        // Example: Reading 4 bits (1,0,1,0):
        //   Iteration 1: value = 0b0     -> read 1 -> value = 0b1
        //   Iteration 2: value = 0b1     -> read 0 -> value = 0b10
        //   Iteration 3: value = 0b10    -> read 1 -> value = 0b101
        //   Iteration 4: value = 0b101   -> read 0 -> value = 0b1010
        for _ in 0..bits_to_read {
            value = (value << 1) | (self.read_bit()? as u64);
        }

        Ok(value)
    }

    /// Check if we've reached the end of the buffer
    ///
    /// Returns true if the current byte_position is at or past the end of buffer.
    /// This doesn't account for partial byte reads.
    ///
    /// # Returns
    /// true if at end, false if more data available
    pub fn is_at_end(&self) -> bool {
        self.byte_position >= self.buffer.len()
    }

    /// Get current read position (for debugging)
    ///
    /// Returns the current position as (byte_index, bit_index_within_byte).
    ///
    /// # Returns
    /// Tuple of (byte_position, bit_position)
    ///
    /// # Example
    /// ```
    /// use kuba_tsdb::compression::bit_stream::BitReader;
    ///
    /// let mut reader = BitReader::new(&[0xFF]);
    /// reader.read_bits(5).unwrap();
    /// let (byte_pos, bit_pos) = reader.position();
    /// assert_eq!(byte_pos, 0);
    /// assert_eq!(bit_pos, 5);
    /// ```
    pub fn position(&self) -> (usize, u8) {
        (self.byte_position, self.bit_position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_read_single_bit() {
        let mut writer = BitWriter::new();
        writer.write_bit(true);
        writer.write_bit(false);
        writer.write_bit(true);
        writer.write_bit(true);

        let buffer = writer.finish().unwrap();
        let mut reader = BitReader::new(&buffer);

        assert!(reader.read_bit().unwrap());
        assert!(!reader.read_bit().unwrap());
        assert!(reader.read_bit().unwrap());
        assert!(reader.read_bit().unwrap());
    }

    #[test]
    fn test_write_read_multiple_bits() {
        let mut writer = BitWriter::new();
        writer.write_bits(0b1010, 4);
        writer.write_bits(0b110011, 6);

        let buffer = writer.finish().unwrap();
        let mut reader = BitReader::new(&buffer);

        assert_eq!(reader.read_bits(4).unwrap(), 0b1010);
        assert_eq!(reader.read_bits(6).unwrap(), 0b110011);
    }

    #[test]
    fn test_write_64_bits() {
        let mut writer = BitWriter::new();
        let value: u64 = 0x123456789ABCDEF0;
        writer.write_bits(value, 64);

        let buffer = writer.finish().unwrap();
        let mut reader = BitReader::new(&buffer);

        assert_eq!(reader.read_bits(64).unwrap(), value);
    }

    #[test]
    fn test_mixed_operations() {
        let mut writer = BitWriter::new();

        // Write various bit patterns
        writer.write_bit(true);
        writer.write_bits(0b101, 3);
        writer.write_bits(0xFF, 8);
        writer.write_bit(false);

        let buffer = writer.finish().unwrap();
        let mut reader = BitReader::new(&buffer);

        // Read them back
        assert!(reader.read_bit().unwrap());
        assert_eq!(reader.read_bits(3).unwrap(), 0b101);
        assert_eq!(reader.read_bits(8).unwrap(), 0xFF);
        assert!(!reader.read_bit().unwrap());
    }

    #[test]
    fn test_byte_alignment() {
        let mut writer = BitWriter::new();

        // Write exactly one byte
        writer.write_bits(0b10101010, 8);

        let buffer = writer.finish().unwrap();
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer[0], 0b10101010);
    }

    #[test]
    fn test_partial_byte() {
        let mut writer = BitWriter::new();

        // Write less than one byte
        writer.write_bits(0b1010, 4);

        let buffer = writer.finish().unwrap();
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer[0] >> 4, 0b1010);
    }

    #[test]
    fn test_read_past_end() {
        let buffer = vec![0b10101010];
        let mut reader = BitReader::new(&buffer);

        // Read all 8 bits
        reader.read_bits(8).unwrap();

        // Try to read more - should fail
        assert!(reader.read_bit().is_err());
    }
    #[test]
    fn test_write_read_64_bits_boundary_patterns() {
        let patterns = [
            0xFFFFFFFFFFFFFFFFu64,
            0x0000000000000000u64,
            0xAAAAAAAAAAAAAAAAu64, // 1010...
            0x5555555555555555u64, // 0101...
            0x8000000000000001u64, // MSB + LSB
        ];

        for &value in &patterns {
            let mut writer = BitWriter::new();
            writer.write_bits(value, 64);

            let buffer = writer.finish().unwrap();
            let mut reader = BitReader::new(&buffer);

            assert_eq!(reader.read_bits(64).unwrap(), value);
        }
    }
    #[test]
    fn test_cross_byte_boundaries() {
        let mut writer = BitWriter::new();

        writer.write_bits(0b1, 1); // uses bit 0
        writer.write_bits(0b1010101, 7); // fills byte exactly
        writer.write_bits(0b11, 2); // crosses into next byte

        let buffer = writer.finish().unwrap();
        let mut reader = BitReader::new(&buffer);

        assert_eq!(reader.read_bits(1).unwrap(), 0b1);
        assert_eq!(reader.read_bits(7).unwrap(), 0b1010101);
        assert_eq!(reader.read_bits(2).unwrap(), 0b11);
    }
    #[test]
    fn test_is_at_end_bit_precision() {
        let mut writer = BitWriter::new();
        writer.write_bits(0b10110000, 8);
        let buffer = writer.finish().unwrap();

        let mut reader = BitReader::new(&buffer);
        assert!(!reader.is_at_end());

        // read 4 bits (still in the same byte)
        reader.read_bits(4).unwrap();
        assert!(
            !reader.is_at_end(),
            "reader incorrectly reports end when bits remain"
        );

        // read remaining bits
        reader.read_bits(4).unwrap();
        assert!(reader.is_at_end());
    }
    #[test]
    fn test_partial_byte_padding_accuracy() {
        let mut writer = BitWriter::new();
        writer.write_bits(0b1011, 4); // expect top 4 bits = 1011

        let buffer = writer.finish().unwrap();
        assert_eq!(buffer.len(), 1);

        let byte = buffer[0];
        assert_eq!(byte >> 4, 0b1011, "high nibble is wrong");
        assert_eq!(byte & 0b1111, 0, "low nibble should be zero padding");
    }
    #[test]
    fn test_reversed_bit_patterns() {
        let mut writer = BitWriter::new();
        writer.write_bits(0b11010000, 8); // known MSB pattern

        let buffer = writer.finish().unwrap();
        let mut reader = BitReader::new(&buffer);

        assert_eq!(reader.read_bits(1).unwrap(), 1);
        assert_eq!(reader.read_bits(1).unwrap(), 1);
        assert_eq!(reader.read_bits(1).unwrap(), 0);
        assert_eq!(reader.read_bits(1).unwrap(), 1);
    }
    #[test]
    fn fuzzy_random_bit_sequences() {
        use rand::Rng;

        let mut rng = rand::rng();

        for _ in 0..10_000 {
            let bit_len: u8 = rng.random_range(1..=64);
            let value: u64 = rng.random();

            let mut writer = BitWriter::new();
            writer.write_bits(value, bit_len);
            let buffer = writer.finish().unwrap();

            let mut reader = BitReader::new(&buffer);
            let read_val = reader.read_bits(bit_len).unwrap();

            let mask = if bit_len == 64 {
                u64::MAX
            } else {
                (1u64 << bit_len) - 1
            };

            assert_eq!(read_val, value & mask);
        }
    }
}
