//! Tag and value sanitization
//!
//! Provides sanitization functions to prevent injection attacks
//! and ensure data integrity. Handles:
//! - Escaping special characters
//! - Normalizing Unicode
//! - Removing control characters
//! - Truncating overly long values

/// Sanitization configuration
#[derive(Debug, Clone)]
pub struct SanitizeConfig {
    /// Strip control characters (U+0000-U+001F, U+007F-U+009F)
    pub strip_control_chars: bool,
    /// Normalize Unicode to NFC form
    pub normalize_unicode: bool,
    /// Trim whitespace from start/end
    pub trim_whitespace: bool,
    /// Replace null bytes with replacement character
    pub replace_null_bytes: bool,
    /// Maximum length for measurement names
    pub max_measurement_length: usize,
    /// Maximum length for tag keys
    pub max_tag_key_length: usize,
    /// Maximum length for tag values
    pub max_tag_value_length: usize,
    /// Maximum length for field keys
    pub max_field_key_length: usize,
    /// Maximum length for field string values
    pub max_field_string_length: usize,
}

impl Default for SanitizeConfig {
    fn default() -> Self {
        Self {
            strip_control_chars: true,
            normalize_unicode: false, // Off by default for performance
            trim_whitespace: true,
            replace_null_bytes: true,
            max_measurement_length: super::MAX_MEASUREMENT_LENGTH,
            max_tag_key_length: super::MAX_TAG_KEY_LENGTH,
            max_tag_value_length: super::MAX_TAG_VALUE_LENGTH,
            max_field_key_length: super::MAX_FIELD_KEY_LENGTH,
            max_field_string_length: super::MAX_FIELD_STRING_LENGTH,
        }
    }
}

/// Sanitizer for incoming data
///
/// Applies configured sanitization rules to clean and normalize
/// measurement names, tag keys/values, and field keys/values.
pub struct Sanitizer {
    config: SanitizeConfig,
}

impl Sanitizer {
    /// Create a new sanitizer with default configuration
    pub fn new() -> Self {
        Self {
            config: SanitizeConfig::default(),
        }
    }

    /// Create a sanitizer with custom configuration
    pub fn with_config(config: SanitizeConfig) -> Self {
        Self { config }
    }

    /// Sanitize a measurement name
    ///
    /// Returns the sanitized string, truncated if necessary.
    pub fn sanitize_measurement(&self, value: &str) -> String {
        let mut result = self.sanitize_string(value);
        self.truncate(&mut result, self.config.max_measurement_length);
        result
    }

    /// Sanitize a tag key
    pub fn sanitize_tag_key(&self, value: &str) -> String {
        let mut result = self.sanitize_identifier(value);
        self.truncate(&mut result, self.config.max_tag_key_length);
        result
    }

    /// Sanitize a tag value
    pub fn sanitize_tag_value(&self, value: &str) -> String {
        let mut result = self.sanitize_string(value);
        self.truncate(&mut result, self.config.max_tag_value_length);
        result
    }

    /// Sanitize a field key
    pub fn sanitize_field_key(&self, value: &str) -> String {
        let mut result = self.sanitize_identifier(value);
        self.truncate(&mut result, self.config.max_field_key_length);
        result
    }

    /// Sanitize a field string value
    pub fn sanitize_field_string(&self, value: &str) -> String {
        let mut result = self.sanitize_string(value);
        self.truncate(&mut result, self.config.max_field_string_length);
        result
    }

    /// Core sanitization for strings
    fn sanitize_string(&self, value: &str) -> String {
        let mut result = String::with_capacity(value.len());

        for c in value.chars() {
            if self.should_keep_char(c) {
                result.push(c);
            } else if self.config.replace_null_bytes && c == '\0' {
                result.push('\u{FFFD}'); // Replacement character
            }
            // Otherwise skip the character
        }

        // Trim whitespace if configured
        if self.config.trim_whitespace {
            result = result.trim().to_string();
        }

        result
    }

    /// Sanitize an identifier (tag/field key)
    ///
    /// Identifiers are more restrictive - they should be alphanumeric
    /// with underscores and hyphens.
    fn sanitize_identifier(&self, value: &str) -> String {
        let mut result = String::with_capacity(value.len());

        for c in value.chars() {
            // Allow alphanumeric, underscore, hyphen, and dot
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.' {
                result.push(c);
            } else if c == ' ' {
                // Replace spaces with underscores in identifiers
                result.push('_');
            }
            // Skip other characters
        }

        // Trim and ensure not empty
        result = result
            .trim_matches(|c| c == '_' || c == '-' || c == '.')
            .to_string();

        if result.is_empty() {
            result = "_unknown_".to_string();
        }

        result
    }

    /// Check if a character should be kept
    fn should_keep_char(&self, c: char) -> bool {
        // Always reject null bytes unless configured to replace
        if c == '\0' && !self.config.replace_null_bytes {
            return false;
        }

        // Strip control characters if configured
        if self.config.strip_control_chars
            && is_control_char(c)
            && c != '\n'
            && c != '\r'
            && c != '\t'
        {
            return false;
        }

        true
    }

    /// Truncate string to max length at char boundary
    fn truncate(&self, s: &mut String, max_len: usize) {
        if s.len() > max_len {
            // Find last valid char boundary
            let mut end = max_len;
            while end > 0 && !s.is_char_boundary(end) {
                end -= 1;
            }
            s.truncate(end);
        }
    }

    /// Escape special characters for line protocol
    ///
    /// Escapes characters that have special meaning in line protocol:
    /// - Comma (,) - tag/field separator
    /// - Space ( ) - field/timestamp separator
    /// - Equals (=) - key/value separator
    /// - Backslash (\) - escape character
    pub fn escape_line_protocol(&self, value: &str, context: EscapeContext) -> String {
        let mut result = String::with_capacity(value.len() + 10);

        for c in value.chars() {
            match context {
                EscapeContext::Measurement => match c {
                    ',' | ' ' | '\\' => {
                        result.push('\\');
                        result.push(c);
                    }
                    _ => result.push(c),
                },
                EscapeContext::TagKey | EscapeContext::TagValue | EscapeContext::FieldKey => {
                    match c {
                        ',' | '=' | ' ' | '\\' => {
                            result.push('\\');
                            result.push(c);
                        }
                        _ => result.push(c),
                    }
                }
                EscapeContext::FieldString => match c {
                    '"' | '\\' => {
                        result.push('\\');
                        result.push(c);
                    }
                    _ => result.push(c),
                },
            }
        }

        result
    }

    /// Unescape a line protocol value
    pub fn unescape_line_protocol(&self, value: &str, context: EscapeContext) -> String {
        let mut result = String::with_capacity(value.len());
        let mut chars = value.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '\\' {
                if let Some(&next) = chars.peek() {
                    let should_unescape = match context {
                        EscapeContext::Measurement => {
                            matches!(next, ',' | ' ' | '\\')
                        }
                        EscapeContext::TagKey
                        | EscapeContext::TagValue
                        | EscapeContext::FieldKey => {
                            matches!(next, ',' | '=' | ' ' | '\\')
                        }
                        EscapeContext::FieldString => {
                            matches!(next, '"' | '\\')
                        }
                    };

                    if should_unescape {
                        result.push(chars.next().unwrap());
                        continue;
                    }
                }
            }
            result.push(c);
        }

        result
    }
}

impl Default for Sanitizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Context for escaping/unescaping
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EscapeContext {
    /// Measurement name
    Measurement,
    /// Tag key
    TagKey,
    /// Tag value
    TagValue,
    /// Field key
    FieldKey,
    /// Field string value
    FieldString,
}

/// Check if a character is a control character
fn is_control_char(c: char) -> bool {
    let code = c as u32;
    // C0 controls (0x00-0x1F) or DEL (0x7F) or C1 controls (0x80-0x9F)
    code <= 0x1F || code == 0x7F || (0x80..=0x9F).contains(&code)
}

// =============================================================================
// Redis Key Sanitization
// =============================================================================
//
// These functions provide sanitization specifically for values that will be
// used in Redis key construction. They are more restrictive than general
// sanitization because Redis keys use colons as separators.

/// Characters that are NOT allowed in Redis key components.
/// These characters could cause key injection or parsing issues:
/// - Colon (:) - Redis key separator
/// - Newline (\n) - Could break protocol
/// - Carriage return (\r) - Could break protocol
/// - Null (\0) - Could terminate strings early
/// - Space ( ) - Could cause parsing ambiguity
const REDIS_KEY_FORBIDDEN_CHARS: &[char] = &[':', '\n', '\r', '\0', ' '];

/// Maximum length for Redis key components (tag keys and values)
/// This prevents overly long keys that could cause memory issues
pub const MAX_REDIS_KEY_COMPONENT_LENGTH: usize = 128;

/// Sanitize a string for use in Redis key construction.
///
/// This function removes or replaces characters that could cause
/// Redis key injection attacks when tags are used to construct
/// secondary index keys like `ts:tag:{key}:{value}:series`.
///
/// # Security
///
/// This prevents attackers from:
/// - Creating keys in unintended namespaces via colon injection
/// - Breaking Redis protocol via newline injection
/// - Creating excessively long keys via unbounded input
///
/// # Arguments
///
/// * `value` - The string to sanitize
///
/// # Returns
///
/// A sanitized string safe for use in Redis keys, with:
/// - Forbidden characters replaced with underscores
/// - Length truncated to `MAX_REDIS_KEY_COMPONENT_LENGTH`
/// - Leading/trailing whitespace removed
///
/// # Examples
///
/// ```
/// use gorilla_tsdb::ingestion::schema::sanitize::sanitize_for_redis_key;
///
/// // Normal values pass through (with trimming)
/// assert_eq!(sanitize_for_redis_key("server1"), "server1");
///
/// // Colons are replaced to prevent key injection
/// assert_eq!(sanitize_for_redis_key("host:port"), "host_port");
///
/// // Newlines are replaced to prevent protocol issues
/// assert_eq!(sanitize_for_redis_key("value\nmalicious"), "value_malicious");
/// ```
pub fn sanitize_for_redis_key(value: &str) -> String {
    // First trim the input to remove leading/trailing whitespace
    let trimmed_input = value.trim();

    if trimmed_input.is_empty() {
        return "_empty_".to_string();
    }

    let mut result = String::with_capacity(trimmed_input.len().min(MAX_REDIS_KEY_COMPONENT_LENGTH));

    for c in trimmed_input.chars() {
        if result.len() >= MAX_REDIS_KEY_COMPONENT_LENGTH {
            break;
        }

        if REDIS_KEY_FORBIDDEN_CHARS.contains(&c) {
            // Replace forbidden characters with underscore
            result.push('_');
        } else if c.is_control() {
            // Skip other control characters entirely
            continue;
        } else {
            result.push(c);
        }
    }

    // Handle case where all characters were control characters
    if result.is_empty() {
        "_empty_".to_string()
    } else {
        result
    }
}

/// Sanitize a HashMap of tags for safe use in Redis key construction.
///
/// This is a convenience function that sanitizes both keys and values
/// of a tag map, preparing them for use in Redis secondary indexes.
///
/// # Arguments
///
/// * `tags` - The tag key-value pairs to sanitize
///
/// # Returns
///
/// A new HashMap with sanitized keys and values
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use gorilla_tsdb::ingestion::schema::sanitize::sanitize_tags_for_redis;
///
/// let mut tags = HashMap::new();
/// tags.insert("host:name".to_string(), "server:1".to_string());
///
/// let sanitized = sanitize_tags_for_redis(&tags);
/// assert_eq!(sanitized.get("host_name"), Some(&"server_1".to_string()));
/// ```
pub fn sanitize_tags_for_redis(
    tags: &std::collections::HashMap<String, String>,
) -> std::collections::HashMap<String, String> {
    tags.iter()
        .map(|(k, v)| (sanitize_for_redis_key(k), sanitize_for_redis_key(v)))
        .collect()
}

/// Validate that a string contains only safe characters
///
/// Returns true if the string is safe, false if it contains
/// potentially dangerous characters.
pub fn is_safe_string(value: &str) -> bool {
    for c in value.chars() {
        // Reject null bytes
        if c == '\0' {
            return false;
        }
        // Reject most control characters
        if is_control_char(c) && c != '\n' && c != '\r' && c != '\t' {
            return false;
        }
    }
    true
}

/// Validate that a string is a valid identifier
///
/// Identifiers must start with a letter or underscore and contain
/// only alphanumeric characters, underscores, hyphens, and dots.
pub fn is_valid_identifier(value: &str) -> bool {
    if value.is_empty() {
        return false;
    }

    let mut chars = value.chars();

    // First character must be letter or underscore
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }

    // Rest must be alphanumeric, underscore, hyphen, or dot
    for c in chars {
        if !c.is_ascii_alphanumeric() && c != '_' && c != '-' && c != '.' {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_string_basic() {
        let sanitizer = Sanitizer::new();

        assert_eq!(sanitizer.sanitize_string("hello"), "hello");
        assert_eq!(sanitizer.sanitize_string("  hello  "), "hello");
        assert_eq!(
            sanitizer.sanitize_string("hello\0world"),
            "hello\u{FFFD}world"
        );
    }

    #[test]
    fn test_sanitize_identifier() {
        let sanitizer = Sanitizer::new();

        assert_eq!(sanitizer.sanitize_tag_key("host"), "host");
        assert_eq!(sanitizer.sanitize_tag_key("host name"), "host_name");
        assert_eq!(sanitizer.sanitize_tag_key("host@name"), "hostname");
        assert_eq!(sanitizer.sanitize_tag_key("___host___"), "host");
        assert_eq!(sanitizer.sanitize_tag_key(""), "_unknown_");
    }

    #[test]
    fn test_sanitize_measurement() {
        let sanitizer = Sanitizer::new();

        assert_eq!(sanitizer.sanitize_measurement("cpu"), "cpu");
        assert_eq!(sanitizer.sanitize_measurement("  cpu  "), "cpu");
    }

    #[test]
    fn test_truncation() {
        let config = SanitizeConfig {
            max_tag_value_length: 10,
            ..Default::default()
        };
        let sanitizer = Sanitizer::with_config(config);

        assert_eq!(sanitizer.sanitize_tag_value("short"), "short");
        assert_eq!(sanitizer.sanitize_tag_value("verylongvalue"), "verylongva");
    }

    #[test]
    fn test_escape_line_protocol() {
        let sanitizer = Sanitizer::new();

        // Measurement
        assert_eq!(
            sanitizer.escape_line_protocol("my measurement", EscapeContext::Measurement),
            r"my\ measurement"
        );

        // Tag value with special chars
        assert_eq!(
            sanitizer.escape_line_protocol("value=1, value=2", EscapeContext::TagValue),
            r"value\=1\,\ value\=2"
        );

        // Field string
        assert_eq!(
            sanitizer.escape_line_protocol(r#"say "hello""#, EscapeContext::FieldString),
            r#"say \"hello\""#
        );
    }

    #[test]
    fn test_unescape_line_protocol() {
        let sanitizer = Sanitizer::new();

        assert_eq!(
            sanitizer.unescape_line_protocol(r"my\ measurement", EscapeContext::Measurement),
            "my measurement"
        );

        assert_eq!(
            sanitizer.unescape_line_protocol(r"value\=1\,\ value\=2", EscapeContext::TagValue),
            "value=1, value=2"
        );
    }

    #[test]
    fn test_is_safe_string() {
        assert!(is_safe_string("hello world"));
        assert!(is_safe_string("hello\nworld"));
        assert!(!is_safe_string("hello\0world"));
        assert!(!is_safe_string("hello\x01world"));
    }

    #[test]
    fn test_is_valid_identifier() {
        assert!(is_valid_identifier("host"));
        assert!(is_valid_identifier("_host"));
        assert!(is_valid_identifier("host_name"));
        assert!(is_valid_identifier("host-name"));
        assert!(is_valid_identifier("host.name"));
        assert!(!is_valid_identifier(""));
        assert!(!is_valid_identifier("123host"));
        assert!(!is_valid_identifier("host name"));
    }

    #[test]
    fn test_is_control_char() {
        assert!(is_control_char('\0'));
        assert!(is_control_char('\x01'));
        assert!(is_control_char('\x7F'));
        assert!(!is_control_char('a'));
        assert!(!is_control_char(' '));
    }

    // =========================================================================
    // Redis Key Sanitization Tests
    // =========================================================================

    #[test]
    fn test_sanitize_for_redis_key_basic() {
        // Normal values pass through
        assert_eq!(sanitize_for_redis_key("server1"), "server1");
        assert_eq!(sanitize_for_redis_key("us-east-1"), "us-east-1");
        assert_eq!(sanitize_for_redis_key("my_tag"), "my_tag");
    }

    #[test]
    fn test_sanitize_for_redis_key_colon_injection() {
        // Colons are replaced to prevent key injection
        assert_eq!(sanitize_for_redis_key("host:port"), "host_port");
        assert_eq!(sanitize_for_redis_key("ts:admin:root"), "ts_admin_root");
        assert_eq!(
            sanitize_for_redis_key("::multiple::colons::"),
            "__multiple__colons__"
        );
    }

    #[test]
    fn test_sanitize_for_redis_key_newline_injection() {
        // Newlines are replaced to prevent protocol issues
        assert_eq!(
            sanitize_for_redis_key("value\nmalicious"),
            "value_malicious"
        );
        assert_eq!(sanitize_for_redis_key("value\r\nwindows"), "value__windows");
    }

    #[test]
    fn test_sanitize_for_redis_key_null_bytes() {
        // Null bytes are replaced
        assert_eq!(sanitize_for_redis_key("value\0null"), "value_null");
    }

    #[test]
    fn test_sanitize_for_redis_key_spaces() {
        // Spaces are replaced
        assert_eq!(sanitize_for_redis_key("hello world"), "hello_world");
        assert_eq!(sanitize_for_redis_key("  trimmed  "), "trimmed");
    }

    #[test]
    fn test_sanitize_for_redis_key_empty() {
        // Empty or whitespace-only strings return placeholder
        assert_eq!(sanitize_for_redis_key(""), "_empty_");
        assert_eq!(sanitize_for_redis_key("   "), "_empty_");
        assert_eq!(sanitize_for_redis_key("\n\r"), "_empty_");
    }

    #[test]
    fn test_sanitize_for_redis_key_length_limit() {
        // Long strings are truncated
        let long_value = "a".repeat(200);
        let result = sanitize_for_redis_key(&long_value);
        assert_eq!(result.len(), MAX_REDIS_KEY_COMPONENT_LENGTH);
    }

    #[test]
    fn test_sanitize_for_redis_key_unicode() {
        // Unicode characters are preserved (only ASCII control chars are special)
        assert_eq!(sanitize_for_redis_key("日本語"), "日本語");
        assert_eq!(sanitize_for_redis_key("émojis🎉"), "émojis🎉");
    }

    #[test]
    fn test_sanitize_for_redis_key_complex_attack() {
        // Complex attack patterns are sanitized
        let attack = "admin:root\nSET password hacked\r\n";
        let result = sanitize_for_redis_key(attack);
        assert!(!result.contains(':'));
        assert!(!result.contains('\n'));
        assert!(!result.contains('\r'));
    }

    #[test]
    fn test_sanitize_tags_for_redis() {
        use std::collections::HashMap;

        let mut tags = HashMap::new();
        tags.insert("host:name".to_string(), "server:1".to_string());
        tags.insert("region".to_string(), "us-east\n1".to_string());

        let sanitized = sanitize_tags_for_redis(&tags);

        // Keys are sanitized
        assert!(sanitized.contains_key("host_name"));
        assert!(!sanitized.contains_key("host:name"));

        // Values are sanitized
        assert_eq!(sanitized.get("host_name"), Some(&"server_1".to_string()));
        assert_eq!(sanitized.get("region"), Some(&"us-east_1".to_string()));
    }
}
