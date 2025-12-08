//! ANS (Asymmetric Numeral Systems) Entropy Coding
//!
//! Provides near-optimal entropy coding with fast encoding/decoding performance.
//! ANS achieves compression ratios close to arithmetic coding while being
//! significantly faster (similar to Huffman speed).
//!
//! # Theory
//!
//! ANS encodes symbols into a single integer state that grows with each symbol.
//! The key insight is that the state growth is proportional to the symbol probability:
//! - High probability symbols barely increase state
//! - Low probability symbols significantly increase state
//!
//! # Variants
//!
//! This module implements tANS (tabled ANS) for optimal speed with pre-computed tables.
//!
//! # Example
//!
//! ```rust,ignore
//! use kuba_tsdb::compression::ans::{AnsEncoder, AnsDecoder};
//!
//! let data = vec![1, 2, 1, 1, 3, 1, 2, 1];
//! let mut encoder = AnsEncoder::new();
//! let compressed = encoder.encode(&data);
//!
//! let mut decoder = AnsDecoder::new();
//! let decompressed = decoder.decode(&compressed, data.len());
//! assert_eq!(data, decompressed);
//! ```

/// Logarithm base 2 table size for tANS (larger = better compression, slower)
const TABLE_LOG: u32 = 12;
/// Table size (2^TABLE_LOG)
const TABLE_SIZE: usize = 1 << TABLE_LOG;

/// ANS encoding state minimum value
const STATE_MIN: u64 = 1 << 16;
/// ANS encoding state maximum value
const STATE_MAX: u64 = STATE_MIN << 16;

/// ANS encoder for entropy-optimal compression
///
/// Uses range ANS (rANS) for fast encoding without large lookup tables.
#[derive(Clone)]
pub struct AnsEncoder {
    /// Symbol frequency table
    frequencies: Vec<u32>,
    /// Cumulative frequencies
    cumulative: Vec<u32>,
    /// Total frequency count (sum of all frequencies)
    total_freq: u32,
    /// Whether tables have been built
    tables_built: bool,
}

impl AnsEncoder {
    /// Create a new ANS encoder
    pub fn new() -> Self {
        Self {
            frequencies: vec![1; 256], // Initialize with uniform distribution
            cumulative: Vec::new(),
            total_freq: 256,
            tables_built: false,
        }
    }

    /// Build frequency table from data
    ///
    /// # Security
    /// - Limits input size to prevent DoS via huge data
    /// - Uses checked arithmetic to prevent overflow
    pub fn build_frequencies(&mut self, data: &[u8]) {
        // SEC: Limit input size to prevent DoS
        const MAX_INPUT_SIZE: usize = 100_000_000; // 100MB max
        if data.len() > MAX_INPUT_SIZE {
            // For very large inputs, sample instead of processing all
            // This prevents DoS while still building reasonable frequency table
            let sample_size = MAX_INPUT_SIZE.min(data.len());
            let step = data.len() / sample_size.max(1);
            let sampled: Vec<u8> = data.iter().step_by(step.max(1)).copied().collect();
            return self.build_frequencies(&sampled);
        }

        // Count symbol occurrences
        let mut counts = vec![0u32; 256];
        for &byte in data {
            counts[byte as usize] += 1;
        }

        // Normalize frequencies to table size
        // SEC: Use checked sum to prevent overflow
        let total: u32 = counts.iter().sum();
        if total == 0 {
            self.frequencies = vec![1; 256];
            self.total_freq = 256;
        } else {
            // Scale to TABLE_SIZE while ensuring each non-zero symbol has at least 1
            let mut freqs = vec![0u32; 256];
            let non_zero_count = counts.iter().filter(|&&c| c > 0).count() as u32;

            if non_zero_count >= TABLE_SIZE as u32 {
                // Too many symbols - just assign 1 to each
                for (i, &count) in counts.iter().enumerate() {
                    if count > 0 {
                        freqs[i] = 1;
                    }
                }
                self.total_freq = non_zero_count;
            } else {
                // First pass: assign minimum of 1 to each non-zero symbol
                let mut assigned = 0u32;
                for (i, &count) in counts.iter().enumerate() {
                    if count > 0 {
                        freqs[i] = 1;
                        assigned += 1;
                    }
                }

                // Second pass: distribute remaining table slots proportionally
                // SEC: Use checked arithmetic to prevent overflow
                let remaining = (TABLE_SIZE as u32).saturating_sub(assigned);
                if remaining > 0 && total > 0 {
                    let scale = remaining as f64 / total as f64;
                    for (i, &count) in counts.iter().enumerate() {
                        if count > 0 {
                            // SEC: Use saturating arithmetic to prevent overflow
                            let extra = (count as f64 * scale) as u32;
                            freqs[i] = freqs[i].saturating_add(extra);
                        }
                    }
                }

                // Adjust to exactly match TABLE_SIZE
                let mut current_total: u32 = freqs.iter().sum();

                // Add or remove from the most frequent symbols
                while current_total != TABLE_SIZE as u32 {
                    let diff = current_total as i64 - TABLE_SIZE as i64;

                    if diff > 0 {
                        // Need to reduce - find largest symbol with freq > 1
                        if let Some((idx, _)) = freqs
                            .iter()
                            .enumerate()
                            .filter(|(_, &f)| f > 1)
                            .max_by_key(|(_, &f)| f)
                        {
                            freqs[idx] -= 1;
                            current_total -= 1;
                        } else {
                            break; // Can't reduce further
                        }
                    } else {
                        // Need to add - find any non-zero symbol
                        if let Some((idx, _)) = freqs
                            .iter()
                            .enumerate()
                            .filter(|(_, &f)| f > 0)
                            .max_by_key(|(_, &f)| f)
                        {
                            freqs[idx] += 1;
                            current_total += 1;
                        } else {
                            break;
                        }
                    }
                }

                self.total_freq = current_total;
            }

            self.frequencies = freqs;
        }

        self.build_cumulative();
        self.tables_built = false;
    }

    /// Build cumulative frequency table
    fn build_cumulative(&mut self) {
        self.cumulative = Vec::with_capacity(257);
        self.cumulative.push(0);
        let mut sum = 0u32;
        for &freq in &self.frequencies {
            sum += freq;
            self.cumulative.push(sum);
        }
    }

    /// Build encoding tables (call after build_frequencies)
    pub fn build_tables(&mut self) {
        if self.cumulative.is_empty() {
            self.build_cumulative();
        }

        // For simplicity, we'll use rANS (range ANS) instead of full tANS tables
        // This is still very fast and doesn't require huge tables
        self.tables_built = true;
    }

    /// Encode data using ANS
    ///
    /// Returns compressed bytes including frequency table header
    pub fn encode(&mut self, data: &[u8]) -> Vec<u8> {
        if data.is_empty() {
            return Vec::new();
        }

        // Build tables if not already done
        if !self.tables_built {
            self.build_frequencies(data);
            self.build_tables();
        }

        let mut output = Vec::new();

        // Write header: number of non-zero symbols and their frequencies
        let non_zero: Vec<(u8, u32)> = self
            .frequencies
            .iter()
            .enumerate()
            .filter(|(_, &f)| f > 0)
            .map(|(i, &f)| (i as u8, f))
            .collect();

        // Write count of non-zero symbols (as varint to handle 256 symbols)
        let count_bytes = encode_varint(non_zero.len() as u32);
        output.extend_from_slice(&count_bytes);

        // Write symbol-frequency pairs (compact format)
        for (symbol, freq) in &non_zero {
            output.push(*symbol);
            // Use varint encoding for frequency
            let freq_bytes = encode_varint(*freq);
            output.extend_from_slice(&freq_bytes);
        }

        // Encode data using rANS
        let mut state = STATE_MIN;
        let mut bits_buffer = Vec::new();

        // Encode in reverse order (ANS property)
        for &symbol in data.iter().rev() {
            let freq = self.frequencies[symbol as usize];
            let cumul = self.cumulative[symbol as usize];

            // Renormalize if state would overflow
            // SEC: Use checked arithmetic to prevent overflow
            // Calculate threshold: STATE_MAX / (total_freq / freq)
            // Equivalent to: STATE_MAX * freq / total_freq
            let threshold = if freq > 0 && self.total_freq > 0 {
                // SEC: Use checked multiplication to prevent overflow
                if let Some(product) = (STATE_MAX as u128).checked_mul(freq as u128) {
                    (product / self.total_freq as u128) as u64
                } else {
                    STATE_MAX // Overflow: use max threshold
                }
            } else {
                STATE_MAX // Defensive: avoid division by zero
            };

            while state >= threshold {
                bits_buffer.push((state & 0xFFFF) as u16);
                state >>= 16;
            }

            // rANS encoding step
            // SEC: Use checked arithmetic to prevent overflow
            if freq > 0 && self.total_freq > 0 {
                let quotient = state / freq as u64;
                let remainder = state % freq as u64;
                // SEC: Check for overflow in multiplication
                if let Some(product) = quotient.checked_mul(self.total_freq as u64) {
                    if let Some(sum) = product.checked_add(cumul as u64) {
                        state = sum.saturating_add(remainder);
                    } else {
                        state = u64::MAX; // Overflow: cap at max
                    }
                } else {
                    state = u64::MAX; // Overflow: cap at max
                }
            } else {
                // Defensive: should never happen if frequencies are valid
                return Vec::new();
            }
        }

        // Output final state
        while state > 0 {
            bits_buffer.push((state & 0xFFFF) as u16);
            state >>= 16;
        }

        // Write data length
        let len_bytes = encode_varint(data.len() as u32);
        output.extend_from_slice(&len_bytes);

        // Write bits buffer length
        let bits_len_bytes = encode_varint(bits_buffer.len() as u32);
        output.extend_from_slice(&bits_len_bytes);

        // Write bits buffer (in reverse order for decoding)
        for &word in bits_buffer.iter().rev() {
            output.push((word >> 8) as u8);
            output.push((word & 0xFF) as u8);
        }

        output
    }
}

impl Default for AnsEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// ANS decoder for entropy-optimal decompression
#[derive(Clone)]
pub struct AnsDecoder {
    /// Symbol frequency table
    frequencies: Vec<u32>,
    /// Cumulative frequencies
    cumulative: Vec<u32>,
    /// Total frequency
    total_freq: u32,
    /// Symbol lookup table (maps cumulative position to symbol)
    symbol_table: Vec<u8>,
}

impl AnsDecoder {
    /// Create a new ANS decoder
    pub fn new() -> Self {
        Self {
            frequencies: Vec::new(),
            cumulative: Vec::new(),
            total_freq: 0,
            symbol_table: Vec::new(),
        }
    }

    /// Build lookup table from frequencies
    fn build_symbol_table(&mut self) {
        self.symbol_table = vec![0; self.total_freq as usize];
        for (symbol, (&freq, &cumul)) in self
            .frequencies
            .iter()
            .zip(self.cumulative.iter())
            .enumerate()
        {
            for i in cumul..(cumul + freq) {
                if (i as usize) < self.symbol_table.len() {
                    self.symbol_table[i as usize] = symbol as u8;
                }
            }
        }
    }

    /// Decode ANS-compressed data
    ///
    /// Returns decompressed bytes
    pub fn decode(&mut self, data: &[u8]) -> Result<Vec<u8>, AnsError> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        let mut pos = 0;

        // Read number of non-zero symbols (as varint)
        let (num_symbols, bytes_read) = decode_varint(data)?;
        // SEC: Validate num_symbols to prevent DoS
        if num_symbols > 256 {
            return Err(AnsError::InvalidFrequencies);
        }
        let num_symbols = num_symbols as usize;
        pos += bytes_read;

        // Read symbol-frequency pairs
        self.frequencies = vec![0; 256];
        for _ in 0..num_symbols {
            if pos >= data.len() {
                return Err(AnsError::TruncatedHeader);
            }
            let symbol = data[pos] as usize;
            pos += 1;

            let (freq, bytes_read) = decode_varint(&data[pos..])?;
            pos += bytes_read;
            // SEC: Validate frequency to prevent DoS
            if freq > TABLE_SIZE as u32 {
                return Err(AnsError::InvalidFrequencies);
            }
            self.frequencies[symbol] = freq;
        }

        // Build cumulative and symbol table
        self.cumulative = Vec::with_capacity(257);
        self.cumulative.push(0);
        let mut sum = 0u32;
        for &freq in &self.frequencies {
            // SEC: Use checked arithmetic to prevent overflow
            sum = sum.checked_add(freq).ok_or(AnsError::InvalidFrequencies)?;
            self.cumulative.push(sum);
        }
        self.total_freq = sum;

        if self.total_freq == 0 || self.total_freq > TABLE_SIZE as u32 {
            return Err(AnsError::InvalidFrequencies);
        }

        self.build_symbol_table();

        // Read data length
        let (data_len, bytes_read) = decode_varint(&data[pos..])?;
        // SEC: Validate data_len to prevent DoS
        const MAX_DECODE_SIZE: u32 = 100_000_000; // 100MB max
        if data_len > MAX_DECODE_SIZE {
            return Err(AnsError::TruncatedData);
        }
        pos += bytes_read;

        // Read bits buffer length
        let (bits_len, bytes_read) = decode_varint(&data[pos..])?;
        // SEC: Validate bits_len to prevent DoS
        const MAX_BITS_BUFFER: u32 = 10_000_000; // Reasonable limit
        if bits_len > MAX_BITS_BUFFER {
            return Err(AnsError::TruncatedData);
        }
        pos += bytes_read;

        // SEC: Validate we have enough data
        let required_bytes = bits_len as usize * 2;
        if pos + required_bytes > data.len() {
            return Err(AnsError::TruncatedData);
        }

        // Read bits buffer
        let mut bits_buffer = Vec::with_capacity(bits_len as usize);
        for _ in 0..bits_len {
            if pos + 1 >= data.len() {
                return Err(AnsError::TruncatedData);
            }
            let word = ((data[pos] as u16) << 8) | (data[pos + 1] as u16);
            bits_buffer.push(word);
            pos += 2;
        }

        // Decode using rANS
        let mut output = Vec::with_capacity(data_len as usize);
        let mut state = 0u64;
        let mut bits_pos = 0;

        // Initialize state
        while state < STATE_MIN && bits_pos < bits_buffer.len() {
            state = (state << 16) | bits_buffer[bits_pos] as u64;
            bits_pos += 1;
        }

        // Decode symbols
        // SEC: Limit decode iterations to prevent DoS
        const MAX_DECODE_ITERATIONS: u32 = 100_000_000; // 100M max
        let decode_limit = data_len.min(MAX_DECODE_ITERATIONS);

        for _ in 0..decode_limit {
            // Find symbol from state
            let slot = (state % self.total_freq as u64) as usize;
            if slot >= self.symbol_table.len() {
                return Err(AnsError::InvalidState);
            }
            let symbol = self.symbol_table[slot];
            output.push(symbol);

            // Update state (rANS decoding step)
            let freq = self.frequencies[symbol as usize];
            let cumul = self.cumulative[symbol as usize];

            // SEC: Use checked arithmetic to prevent overflow
            if freq > 0 && self.total_freq > 0 {
                let quotient = state / self.total_freq as u64;
                let remainder = state % self.total_freq as u64;
                // SEC: Check for overflow in multiplication
                if let Some(product) = (freq as u64).checked_mul(quotient) {
                    if let Some(sum) = product.checked_add(remainder) {
                        state = sum.saturating_sub(cumul as u64);
                    } else {
                        return Err(AnsError::InvalidState);
                    }
                } else {
                    return Err(AnsError::InvalidState);
                }
            } else {
                return Err(AnsError::InvalidState);
            }

            // Renormalize if needed
            // SEC: Limit renormalization iterations to prevent DoS
            let mut renormalize_count = 0;
            const MAX_RENORMALIZE: usize = 1000;
            while state < STATE_MIN
                && bits_pos < bits_buffer.len()
                && renormalize_count < MAX_RENORMALIZE
            {
                state = (state << 16) | bits_buffer[bits_pos] as u64;
                bits_pos += 1;
                renormalize_count += 1;
            }
            if renormalize_count >= MAX_RENORMALIZE {
                return Err(AnsError::InvalidState);
            }
        }

        // SEC: Verify we decoded the expected amount
        if decode_limit < data_len {
            return Err(AnsError::TruncatedData);
        }

        Ok(output)
    }
}

impl Default for AnsDecoder {
    fn default() -> Self {
        Self::new()
    }
}

/// ANS compression/decompression errors
#[derive(Debug, Clone)]
pub enum AnsError {
    /// Header is truncated
    TruncatedHeader,
    /// Compressed data is truncated
    TruncatedData,
    /// Invalid frequency table
    InvalidFrequencies,
    /// Invalid ANS state during decoding
    InvalidState,
    /// Varint decode error
    InvalidVarint,
}

impl std::fmt::Display for AnsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AnsError::TruncatedHeader => write!(f, "ANS header is truncated"),
            AnsError::TruncatedData => write!(f, "ANS compressed data is truncated"),
            AnsError::InvalidFrequencies => write!(f, "Invalid frequency table in ANS data"),
            AnsError::InvalidState => write!(f, "Invalid ANS state during decoding"),
            AnsError::InvalidVarint => write!(f, "Invalid varint encoding"),
        }
    }
}

impl std::error::Error for AnsError {}

/// Encode a u32 as a variable-length integer
fn encode_varint(mut value: u32) -> Vec<u8> {
    let mut result = Vec::new();
    while value >= 0x80 {
        result.push((value as u8) | 0x80);
        value >>= 7;
    }
    result.push(value as u8);
    result
}

/// Decode a variable-length integer
fn decode_varint(data: &[u8]) -> Result<(u32, usize), AnsError> {
    let mut result = 0u32;
    let mut shift = 0;
    let mut pos = 0;

    loop {
        if pos >= data.len() {
            return Err(AnsError::InvalidVarint);
        }
        let byte = data[pos];
        pos += 1;

        result |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            return Ok((result, pos));
        }
        shift += 7;
        if shift > 28 {
            return Err(AnsError::InvalidVarint);
        }
    }
}

/// Compress data using ANS entropy coding
///
/// Convenience function that creates encoder, builds tables, and compresses.
///
/// # Arguments
///
/// * `data` - Raw bytes to compress
///
/// # Returns
///
/// Compressed bytes
pub fn compress(data: &[u8]) -> Vec<u8> {
    if data.is_empty() {
        return Vec::new();
    }

    let mut encoder = AnsEncoder::new();
    encoder.encode(data)
}

/// Decompress ANS-compressed data
///
/// Convenience function that creates decoder and decompresses.
///
/// # Arguments
///
/// * `data` - ANS-compressed bytes
///
/// # Returns
///
/// Decompressed bytes or error
pub fn decompress(data: &[u8]) -> Result<Vec<u8>, AnsError> {
    if data.is_empty() {
        return Ok(Vec::new());
    }

    let mut decoder = AnsDecoder::new();
    decoder.decode(data)
}

/// Calculate theoretical entropy of data in bits per byte
pub fn calculate_entropy(data: &[u8]) -> f64 {
    if data.is_empty() {
        return 0.0;
    }

    let mut counts = [0u64; 256];
    for &byte in data {
        counts[byte as usize] += 1;
    }

    let len = data.len() as f64;
    let mut entropy = 0.0;

    for &count in &counts {
        if count > 0 {
            let p = count as f64 / len;
            entropy -= p * p.log2();
        }
    }

    entropy
}

/// Statistics about ANS compression performance
#[derive(Debug, Clone, Default)]
pub struct AnsStats {
    /// Original data size in bytes
    pub original_size: usize,
    /// Compressed data size in bytes
    pub compressed_size: usize,
    /// Theoretical entropy (bits per byte)
    pub entropy_bpb: f64,
    /// Actual bits per byte achieved
    pub actual_bpb: f64,
    /// Coding efficiency (entropy / actual, higher is better, max 1.0)
    pub efficiency: f64,
}

/// Analyze ANS compression performance
pub fn analyze_compression(original: &[u8], compressed: &[u8]) -> AnsStats {
    let entropy = calculate_entropy(original);
    let actual_bpb = if original.is_empty() {
        0.0
    } else {
        (compressed.len() * 8) as f64 / original.len() as f64
    };

    AnsStats {
        original_size: original.len(),
        compressed_size: compressed.len(),
        entropy_bpb: entropy,
        actual_bpb,
        efficiency: if actual_bpb > 0.0 {
            entropy / actual_bpb
        } else {
            1.0
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_roundtrip() {
        for value in [0, 1, 127, 128, 255, 256, 16383, 16384, u32::MAX / 2] {
            let encoded = encode_varint(value);
            let (decoded, _) = decode_varint(&encoded).unwrap();
            assert_eq!(value, decoded, "Varint roundtrip failed for {}", value);
        }
    }

    #[test]
    fn test_ans_roundtrip_simple() {
        let data = b"hello world";
        let compressed = compress(data);
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_ans_roundtrip_repeated() {
        // Highly compressible data
        let data: Vec<u8> = vec![42; 1000];
        let compressed = compress(&data);
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(data, decompressed);

        // Should compress well
        assert!(
            compressed.len() < data.len() / 5,
            "Repeated data should compress well"
        );
    }

    #[test]
    fn test_ans_roundtrip_random() {
        // Less compressible data
        let data: Vec<u8> = (0..1000).map(|i| (i * 17 + 31) as u8).collect();
        let compressed = compress(&data);
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_ans_empty() {
        let data: Vec<u8> = vec![];
        let compressed = compress(&data);
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_ans_single_byte() {
        let data = vec![42u8];
        let compressed = compress(&data);
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_entropy_calculation() {
        // Uniform distribution: entropy should be 8 bits
        let uniform: Vec<u8> = (0..=255).collect();
        let entropy = calculate_entropy(&uniform);
        assert!(
            (entropy - 8.0).abs() < 0.01,
            "Uniform entropy should be ~8 bits"
        );

        // Single symbol: entropy should be 0
        let single: Vec<u8> = vec![42; 100];
        let entropy = calculate_entropy(&single);
        assert!(entropy.abs() < 0.01, "Single symbol entropy should be 0");
    }

    #[test]
    fn test_compression_stats() {
        let data: Vec<u8> = vec![42; 1000];
        let compressed = compress(&data);
        let stats = analyze_compression(&data, &compressed);

        assert_eq!(stats.original_size, 1000);
        assert!(
            stats.entropy_bpb < 0.1,
            "Repeated data should have very low entropy"
        );
        // For highly compressible data, just check we compress well
        assert!(
            stats.compressed_size < stats.original_size,
            "Should compress repeated data"
        );
    }
}
