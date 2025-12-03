//! Protocol Auto-Detection Module
//!
//! Automatically detects the format of incoming data based on content analysis.
//! Supports detection of:
//! - InfluxDB Line Protocol
//! - JSON (single object or array)
//! - Prometheus Remote Write (Protobuf)
//!
//! # Detection Strategy
//!
//! The detector analyzes the first few bytes of the input to determine format:
//!
//! 1. **Protobuf**: Starts with protobuf wire format markers (field tags)
//! 2. **JSON**: Starts with `{` or `[` after optional whitespace
//! 3. **Line Protocol**: Default fallback, text-based format
//!
//! # Example
//!
//! ```rust
//! use gorilla_tsdb::ingestion::protocol::{detect_protocol, Protocol};
//!
//! let line_data = b"cpu,host=server01 value=42.0";
//! assert_eq!(detect_protocol(line_data), Protocol::LineProtocol);
//!
//! let json_data = b"{\"measurement\": \"cpu\", \"value\": 42.0}";
//! assert_eq!(detect_protocol(json_data), Protocol::Json);
//! ```

use super::Protocol;

/// Minimum bytes needed for reliable detection
const MIN_DETECTION_BYTES: usize = 2;

/// Maximum bytes to scan for detection (avoid scanning entire payload)
const MAX_DETECTION_SCAN: usize = 64;

/// Detect the protocol format of input data
///
/// Analyzes the first bytes of the input to determine the most likely format.
/// Returns `Protocol::LineProtocol` as the default if detection is inconclusive.
///
/// # Arguments
///
/// * `data` - Raw bytes to analyze
///
/// # Returns
///
/// The detected `Protocol` variant
///
/// # Performance
///
/// This function is O(1) - it only examines the first few bytes regardless
/// of input size. Typical detection completes in under 100ns.
#[inline]
pub fn detect_protocol(data: &[u8]) -> Protocol {
    if data.len() < MIN_DETECTION_BYTES {
        // Too short to detect reliably, default to line protocol
        return Protocol::LineProtocol;
    }

    // First check for binary protobuf format (before whitespace handling)
    // Protobuf detection: Check for wire format field tags
    // Field 1 (TimeSeries) with wire type 2 (length-delimited) = 0x0A
    // This is the expected start of a Prometheus WriteRequest
    if data[0] == 0x0A && is_likely_protobuf(data) {
        return Protocol::Protobuf;
    }

    // Skip leading whitespace for text-based formats (up to MAX_DETECTION_SCAN bytes)
    let scan_limit = data.len().min(MAX_DETECTION_SCAN);
    let trimmed = skip_whitespace(&data[..scan_limit]);

    if trimmed.is_empty() {
        return Protocol::LineProtocol;
    }

    // Check first non-whitespace byte for text formats
    match trimmed[0] {
        // JSON object or array
        b'{' | b'[' => Protocol::Json,

        // Any other printable ASCII is likely line protocol
        _ => Protocol::LineProtocol,
    }
}

/// Detect protocol from Content-Type header with data fallback
///
/// First tries to determine protocol from Content-Type header.
/// Falls back to content-based detection if header is ambiguous.
///
/// # Arguments
///
/// * `content_type` - Optional Content-Type header value
/// * `data` - Raw data bytes for fallback detection
///
/// # Returns
///
/// The detected `Protocol` variant
pub fn detect_protocol_with_hint(content_type: Option<&str>, data: &[u8]) -> Protocol {
    // Try Content-Type first
    if let Some(ct) = content_type {
        let ct_lower = ct.to_lowercase();

        if ct_lower.contains("application/json") {
            return Protocol::Json;
        }

        if ct_lower.contains("application/x-protobuf") || ct_lower.contains("application/protobuf")
        {
            return Protocol::Protobuf;
        }

        // text/plain could be line protocol or JSON, fall through to detection
        if ct_lower.contains("text/plain") {
            return detect_protocol(data);
        }

        // Explicit line protocol content type
        if ct_lower.contains("application/x-influxdb-line-protocol") {
            return Protocol::LineProtocol;
        }
    }

    // Fall back to content-based detection
    detect_protocol(data)
}

/// Skip leading ASCII whitespace bytes
#[inline]
fn skip_whitespace(data: &[u8]) -> &[u8] {
    let mut start = 0;
    for &byte in data {
        if byte == b' ' || byte == b'\t' || byte == b'\n' || byte == b'\r' {
            start += 1;
        } else {
            break;
        }
    }
    &data[start..]
}

/// Check if data looks like protobuf format
///
/// Performs additional validation beyond just the first byte to reduce
/// false positives. Checks for valid varint length encoding and ensures
/// the data doesn't look like text.
#[inline]
fn is_likely_protobuf(data: &[u8]) -> bool {
    if data.len() < 3 {
        return false;
    }

    // After field tag 0x0A, expect a varint length
    let second_byte = data[1];

    // If second byte is printable ASCII letter (a-z, A-Z), this is likely text
    // This catches cases like "\ncpu..." (line protocol with leading newline)
    if second_byte.is_ascii_alphabetic() {
        return false;
    }

    // If second byte is '{' or '[', this is JSON with leading newline
    if second_byte == b'{' || second_byte == b'[' {
        return false;
    }

    // Check if it looks like a valid protobuf structure:
    // After tag 0x0A and length byte, we should see another field tag
    // Common field tags in Prometheus WriteRequest:
    // - 0x0A (field 1, length-delimited) - repeated TimeSeries
    // - 0x08 (field 1, varint) - common in nested messages
    // - 0x10 (field 2, varint)
    // - 0x12 (field 2, length-delimited)

    // Get the length (if it's a simple single-byte varint)
    if second_byte & 0x80 == 0 {
        // Single byte length, check if third byte looks like a field tag
        let third_byte = data[2];
        // Field tags have wire type in lower 3 bits (0-5 are valid)
        // and field number in upper bits
        let wire_type = third_byte & 0x07;
        if wire_type <= 5 {
            return true;
        }
        return false;
    }

    // Multi-byte varint length - check if it terminates properly
    for byte in data.iter().take(7).skip(2) {
        if byte & 0x80 == 0 {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_line_protocol() {
        // Standard line protocol
        assert_eq!(
            detect_protocol(b"cpu,host=server01 value=42.0 1234567890"),
            Protocol::LineProtocol
        );

        // Without timestamp
        assert_eq!(
            detect_protocol(b"cpu,host=server01 value=42.0"),
            Protocol::LineProtocol
        );

        // Without tags
        assert_eq!(detect_protocol(b"cpu value=42.0"), Protocol::LineProtocol);

        // Multiple fields
        assert_eq!(
            detect_protocol(b"cpu,host=server01 value=42.0,count=100i"),
            Protocol::LineProtocol
        );
    }

    #[test]
    fn test_detect_json_object() {
        // Simple JSON object
        assert_eq!(
            detect_protocol(b"{\"measurement\": \"cpu\"}"),
            Protocol::Json
        );

        // With leading whitespace
        assert_eq!(
            detect_protocol(b"  {\"measurement\": \"cpu\"}"),
            Protocol::Json
        );

        // With leading newline
        assert_eq!(
            detect_protocol(b"\n{\"measurement\": \"cpu\"}"),
            Protocol::Json
        );
    }

    #[test]
    fn test_detect_json_array() {
        // JSON array
        assert_eq!(
            detect_protocol(b"[{\"measurement\": \"cpu\"}]"),
            Protocol::Json
        );

        // With whitespace
        assert_eq!(
            detect_protocol(b"  [{\"measurement\": \"cpu\"}]"),
            Protocol::Json
        );
    }

    #[test]
    fn test_detect_protobuf() {
        // Simulated protobuf wire format:
        // Field 1, wire type 2 (length-delimited), length 10
        let proto_data = [0x0A, 0x0A, 0x08, 0x01, 0x12, 0x04, 0x74, 0x65, 0x73, 0x74];
        assert_eq!(detect_protocol(&proto_data), Protocol::Protobuf);

        // Larger length (varint)
        let proto_data_large = [0x0A, 0x80, 0x01]; // length 128
        assert_eq!(detect_protocol(&proto_data_large), Protocol::Protobuf);
    }

    #[test]
    fn test_detect_empty_and_short() {
        // Empty input defaults to line protocol
        assert_eq!(detect_protocol(b""), Protocol::LineProtocol);

        // Single byte defaults to line protocol
        assert_eq!(detect_protocol(b"c"), Protocol::LineProtocol);

        // Whitespace only defaults to line protocol
        assert_eq!(detect_protocol(b"   "), Protocol::LineProtocol);
    }

    #[test]
    fn test_detect_with_content_type() {
        let data = b"cpu value=42.0";

        // JSON content type overrides content detection
        assert_eq!(
            detect_protocol_with_hint(Some("application/json"), data),
            Protocol::Json
        );

        // Protobuf content type
        assert_eq!(
            detect_protocol_with_hint(Some("application/x-protobuf"), data),
            Protocol::Protobuf
        );

        // text/plain falls back to content detection
        assert_eq!(
            detect_protocol_with_hint(Some("text/plain"), data),
            Protocol::LineProtocol
        );

        // No content type uses detection
        assert_eq!(
            detect_protocol_with_hint(None, data),
            Protocol::LineProtocol
        );

        // Explicit line protocol content type
        assert_eq!(
            detect_protocol_with_hint(Some("application/x-influxdb-line-protocol"), data),
            Protocol::LineProtocol
        );
    }

    #[test]
    fn test_skip_whitespace() {
        assert_eq!(skip_whitespace(b"  hello"), b"hello");
        assert_eq!(skip_whitespace(b"\t\nhello"), b"hello");
        assert_eq!(skip_whitespace(b"hello"), b"hello");
        assert_eq!(skip_whitespace(b"   "), b"");
    }

    #[test]
    fn test_ambiguous_cases() {
        // 0x0A that's not protobuf (it's a newline in line protocol)
        // This is tricky - a line starting with newline followed by text
        let newline_start = b"\ncpu value=42.0";
        // After skipping whitespace, first char is 'c', so line protocol
        assert_eq!(detect_protocol(newline_start), Protocol::LineProtocol);
    }
}
