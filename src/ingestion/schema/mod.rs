//! Schema validation and registry module
//!
//! Provides schema validation, tag/value sanitization, and scalable bloom filter
//! deduplication for ingested data points.
//!
//! # Features
//!
//! - **Schema Registry**: Manages measurement schemas with field types, required tags
//! - **Validation**: Type checking, value bounds, cardinality limits
//! - **Sanitization**: Escape/encode values to prevent injection attacks
//! - **Deduplication**: Scalable bloom filter for detecting duplicate points
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │                   Schema Registry                        │
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
//! │  │ Measurement │  │ Measurement │  │ Measurement │     │
//! │  │   Schema    │  │   Schema    │  │   Schema    │     │
//! │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘     │
//! └─────────┼────────────────┼────────────────┼─────────────┘
//!           │                │                │
//!           └────────────────┴────────────────┘
//!                            │
//!                            v
//!              ┌─────────────────────────┐
//!              │   SchemaValidator       │
//!              │   - Type checking       │
//!              │   - Value bounds        │
//!              │   - Tag validation      │
//!              └────────────┬────────────┘
//!                           │
//!                           v
//!              ┌─────────────────────────┐
//!              │   Sanitizer             │
//!              │   - Escape special chars│
//!              │   - Normalize Unicode   │
//!              └────────────┬────────────┘
//!                           │
//!                           v
//!              ┌─────────────────────────┐
//!              │   Bloom Filter          │
//!              │   - Deduplication       │
//!              │   - Series counting     │
//!              └─────────────────────────┘
//! ```

pub mod bloom;
pub mod registry;
pub mod sanitize;
pub mod validation;

pub use bloom::{BloomFilter, ScalableBloomFilter};
pub use registry::{FieldSchema, MeasurementSchema, SchemaRegistry};
pub use sanitize::{SanitizeConfig, Sanitizer};
pub use validation::{ValidationConfig, ValidationError, ValidationResult, Validator};

use std::collections::HashSet;

/// Maximum measurement name length (default: 256 bytes)
pub const MAX_MEASUREMENT_LENGTH: usize = 256;

/// Maximum tag key length (default: 256 bytes)
pub const MAX_TAG_KEY_LENGTH: usize = 256;

/// Maximum tag value length (default: 256 bytes)
pub const MAX_TAG_VALUE_LENGTH: usize = 256;

/// Maximum field key length (default: 256 bytes)
pub const MAX_FIELD_KEY_LENGTH: usize = 256;

/// Maximum field string value length (default: 64KB)
pub const MAX_FIELD_STRING_LENGTH: usize = 64 * 1024;

/// Maximum number of tags per point (default: 256)
pub const MAX_TAGS_PER_POINT: usize = 256;

/// Maximum number of fields per point (default: 256)
pub const MAX_FIELDS_PER_POINT: usize = 256;

/// Reserved tag/field names that cannot be used
pub fn reserved_names() -> HashSet<&'static str> {
    let mut names = HashSet::new();
    names.insert("time");
    names.insert("_measurement");
    names.insert("_field");
    names.insert("_value");
    names.insert("_start");
    names.insert("_stop");
    names
}

/// Check if a name is reserved
pub fn is_reserved_name(name: &str) -> bool {
    reserved_names().contains(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reserved_names() {
        assert!(is_reserved_name("time"));
        assert!(is_reserved_name("_measurement"));
        assert!(!is_reserved_name("host"));
        assert!(!is_reserved_name("value"));
    }
}
