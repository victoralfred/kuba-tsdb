//! Schema validation for parsed data points
//!
//! Validates parsed points against registered schemas and
//! enforces constraints like type matching, value bounds, and
//! cardinality limits.

use std::fmt;

use crate::ingestion::protocol::ParsedPoint;

use super::registry::{MeasurementSchema, SchemaRegistry};
use super::{
    is_reserved_name, MAX_FIELDS_PER_POINT, MAX_FIELD_KEY_LENGTH, MAX_FIELD_STRING_LENGTH,
    MAX_MEASUREMENT_LENGTH, MAX_TAGS_PER_POINT, MAX_TAG_KEY_LENGTH, MAX_TAG_VALUE_LENGTH,
};

/// Validation configuration
#[derive(Debug, Clone)]
pub struct ValidationConfig {
    /// Maximum measurement name length
    pub max_measurement_length: usize,
    /// Maximum tag key length
    pub max_tag_key_length: usize,
    /// Maximum tag value length
    pub max_tag_value_length: usize,
    /// Maximum field key length
    pub max_field_key_length: usize,
    /// Maximum field string value length
    pub max_field_string_length: usize,
    /// Maximum tags per point
    pub max_tags: usize,
    /// Maximum fields per point
    pub max_fields: usize,
    /// Reject reserved names
    pub reject_reserved_names: bool,
    /// Require at least one field
    pub require_field: bool,
    /// Maximum timestamp in the future (nanoseconds, 0 = no limit)
    pub max_future_timestamp_ns: i64,
    /// Minimum timestamp allowed (nanoseconds, 0 = no limit)
    pub min_timestamp_ns: i64,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            max_measurement_length: MAX_MEASUREMENT_LENGTH,
            max_tag_key_length: MAX_TAG_KEY_LENGTH,
            max_tag_value_length: MAX_TAG_VALUE_LENGTH,
            max_field_key_length: MAX_FIELD_KEY_LENGTH,
            max_field_string_length: MAX_FIELD_STRING_LENGTH,
            max_tags: MAX_TAGS_PER_POINT,
            max_fields: MAX_FIELDS_PER_POINT,
            reject_reserved_names: true,
            require_field: true,
            max_future_timestamp_ns: 0, // No limit by default
            min_timestamp_ns: 0,        // No limit by default
        }
    }
}

/// Validation error types
#[derive(Debug, Clone)]
pub enum ValidationError {
    /// Measurement name too long
    MeasurementTooLong {
        /// Actual length
        length: usize,
        /// Maximum allowed length
        max: usize,
    },

    /// Empty measurement name
    EmptyMeasurement,

    /// Tag key too long
    TagKeyTooLong {
        /// Tag key
        key: String,
        /// Actual length
        length: usize,
        /// Maximum allowed
        max: usize,
    },

    /// Tag value too long
    TagValueTooLong {
        /// Tag key
        key: String,
        /// Actual length
        length: usize,
        /// Maximum allowed
        max: usize,
    },

    /// Field key too long
    FieldKeyTooLong {
        /// Field key
        key: String,
        /// Actual length
        length: usize,
        /// Maximum allowed
        max: usize,
    },

    /// Too many tags
    TooManyTags {
        /// Actual count
        count: usize,
        /// Maximum allowed
        max: usize,
    },

    /// Too many fields
    TooManyFields {
        /// Actual count
        count: usize,
        /// Maximum allowed
        max: usize,
    },

    /// No fields provided
    NoFields,

    /// Reserved name used
    ReservedName {
        /// The reserved name
        name: String,
        /// Where it was used
        location: String,
    },

    /// Type mismatch with schema
    TypeMismatch {
        /// Field name
        field: String,
        /// Expected type
        expected: String,
        /// Actual type
        actual: String,
    },

    /// Value out of range
    ValueOutOfRange {
        /// Field name
        field: String,
        /// Actual value
        value: f64,
        /// Minimum allowed
        min: Option<f64>,
        /// Maximum allowed
        max: Option<f64>,
    },

    /// String too long
    StringTooLong {
        /// Field name
        field: String,
        /// Actual length
        length: usize,
        /// Maximum allowed
        max: usize,
    },

    /// Invalid tag value
    InvalidTagValue {
        /// Tag name
        tag: String,
        /// Actual value
        value: String,
        /// Allowed values
        allowed: Vec<String>,
    },

    /// Missing required tag
    MissingRequiredTag {
        /// Tag name
        tag: String,
    },

    /// Missing required field
    MissingRequiredField {
        /// Field name
        field: String,
    },

    /// Extra tag not allowed
    ExtraTagNotAllowed {
        /// Tag name
        tag: String,
    },

    /// Extra field not allowed
    ExtraFieldNotAllowed {
        /// Field name
        field: String,
    },

    /// Unknown measurement in strict mode
    UnknownMeasurement {
        /// Measurement name
        measurement: String,
    },

    /// Timestamp too far in the future
    TimestampTooFuture {
        /// Provided timestamp
        timestamp: i64,
        /// Maximum allowed
        max: i64,
    },

    /// Timestamp too far in the past
    TimestampTooOld {
        /// Provided timestamp
        timestamp: i64,
        /// Minimum allowed
        min: i64,
    },
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationError::MeasurementTooLong { length, max } => {
                write!(f, "Measurement name too long: {} > {}", length, max)
            },
            ValidationError::EmptyMeasurement => {
                write!(f, "Empty measurement name")
            },
            ValidationError::TagKeyTooLong { key, length, max } => {
                write!(f, "Tag key '{}' too long: {} > {}", key, length, max)
            },
            ValidationError::TagValueTooLong { key, length, max } => {
                write!(f, "Tag value for '{}' too long: {} > {}", key, length, max)
            },
            ValidationError::FieldKeyTooLong { key, length, max } => {
                write!(f, "Field key '{}' too long: {} > {}", key, length, max)
            },
            ValidationError::TooManyTags { count, max } => {
                write!(f, "Too many tags: {} > {}", count, max)
            },
            ValidationError::TooManyFields { count, max } => {
                write!(f, "Too many fields: {} > {}", count, max)
            },
            ValidationError::NoFields => {
                write!(f, "No fields provided")
            },
            ValidationError::ReservedName { name, location } => {
                write!(f, "Reserved name '{}' used in {}", name, location)
            },
            ValidationError::TypeMismatch {
                field,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "Type mismatch for field '{}': expected {}, got {}",
                    field, expected, actual
                )
            },
            ValidationError::ValueOutOfRange {
                field,
                value,
                min,
                max,
            } => {
                write!(
                    f,
                    "Value {} for field '{}' out of range [{:?}, {:?}]",
                    value, field, min, max
                )
            },
            ValidationError::StringTooLong { field, length, max } => {
                write!(
                    f,
                    "String value for field '{}' too long: {} > {}",
                    field, length, max
                )
            },
            ValidationError::InvalidTagValue {
                tag,
                value,
                allowed,
            } => {
                write!(
                    f,
                    "Invalid value '{}' for tag '{}', allowed: {:?}",
                    value, tag, allowed
                )
            },
            ValidationError::MissingRequiredTag { tag } => {
                write!(f, "Missing required tag: {}", tag)
            },
            ValidationError::MissingRequiredField { field } => {
                write!(f, "Missing required field: {}", field)
            },
            ValidationError::ExtraTagNotAllowed { tag } => {
                write!(f, "Extra tag not allowed: {}", tag)
            },
            ValidationError::ExtraFieldNotAllowed { field } => {
                write!(f, "Extra field not allowed: {}", field)
            },
            ValidationError::UnknownMeasurement { measurement } => {
                write!(f, "Unknown measurement: {}", measurement)
            },
            ValidationError::TimestampTooFuture { timestamp, max } => {
                write!(
                    f,
                    "Timestamp {} too far in future (max: {})",
                    timestamp, max
                )
            },
            ValidationError::TimestampTooOld { timestamp, min } => {
                write!(f, "Timestamp {} too far in past (min: {})", timestamp, min)
            },
        }
    }
}

impl std::error::Error for ValidationError {}

/// Validation result with multiple possible errors
#[derive(Debug, Clone, Default)]
pub struct ValidationResult {
    /// List of validation errors (empty if valid)
    pub errors: Vec<ValidationError>,
}

impl ValidationResult {
    /// Create a successful result
    pub fn ok() -> Self {
        Self { errors: Vec::new() }
    }

    /// Create a result with one error
    pub fn err(error: ValidationError) -> Self {
        Self {
            errors: vec![error],
        }
    }

    /// Add an error to the result
    pub fn add_error(&mut self, error: ValidationError) {
        self.errors.push(error);
    }

    /// Check if validation passed
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }

    /// Check if validation failed
    pub fn is_err(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Get first error
    pub fn first_error(&self) -> Option<&ValidationError> {
        self.errors.first()
    }

    /// Get all errors
    pub fn all_errors(&self) -> &[ValidationError] {
        &self.errors
    }

    /// Convert to Result
    pub fn into_result(self) -> Result<(), ValidationError> {
        if self.errors.is_empty() {
            Ok(())
        } else {
            Err(self.errors.into_iter().next().unwrap())
        }
    }
}

/// Data point validator
///
/// Validates parsed points against configuration and optional schema.
pub struct Validator {
    config: ValidationConfig,
    registry: Option<SchemaRegistry>,
}

impl Validator {
    /// Create a new validator with default configuration
    pub fn new() -> Self {
        Self {
            config: ValidationConfig::default(),
            registry: None,
        }
    }

    /// Create a validator with custom configuration
    pub fn with_config(config: ValidationConfig) -> Self {
        Self {
            config,
            registry: None,
        }
    }

    /// Set schema registry for schema-based validation
    pub fn with_registry(mut self, registry: SchemaRegistry) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Validate a parsed point
    ///
    /// Performs validation against configuration limits and optional schema.
    /// Returns a result with all validation errors found.
    pub fn validate<'a>(&self, point: &ParsedPoint<'a>) -> ValidationResult {
        let mut result = ValidationResult::ok();

        // Validate measurement name
        if point.measurement.is_empty() {
            result.add_error(ValidationError::EmptyMeasurement);
        } else if point.measurement.len() > self.config.max_measurement_length {
            result.add_error(ValidationError::MeasurementTooLong {
                length: point.measurement.len(),
                max: self.config.max_measurement_length,
            });
        }

        // Check for reserved measurement name
        if self.config.reject_reserved_names && is_reserved_name(&point.measurement) {
            result.add_error(ValidationError::ReservedName {
                name: point.measurement.to_string(),
                location: "measurement".to_string(),
            });
        }

        // Validate tag count
        if point.tags.len() > self.config.max_tags {
            result.add_error(ValidationError::TooManyTags {
                count: point.tags.len(),
                max: self.config.max_tags,
            });
        }

        // Validate each tag
        for (key, value) in &point.tags {
            // Check key length
            if key.len() > self.config.max_tag_key_length {
                result.add_error(ValidationError::TagKeyTooLong {
                    key: key.to_string(),
                    length: key.len(),
                    max: self.config.max_tag_key_length,
                });
            }

            // Check value length
            if value.len() > self.config.max_tag_value_length {
                result.add_error(ValidationError::TagValueTooLong {
                    key: key.to_string(),
                    length: value.len(),
                    max: self.config.max_tag_value_length,
                });
            }

            // Check for reserved tag names
            if self.config.reject_reserved_names && is_reserved_name(key) {
                result.add_error(ValidationError::ReservedName {
                    name: key.to_string(),
                    location: "tag key".to_string(),
                });
            }
        }

        // Validate field count
        if point.fields.len() > self.config.max_fields {
            result.add_error(ValidationError::TooManyFields {
                count: point.fields.len(),
                max: self.config.max_fields,
            });
        }

        // Check for at least one field
        if self.config.require_field && point.fields.is_empty() {
            result.add_error(ValidationError::NoFields);
        }

        // Validate each field
        for (key, value) in &point.fields {
            // Check key length
            if key.len() > self.config.max_field_key_length {
                result.add_error(ValidationError::FieldKeyTooLong {
                    key: key.to_string(),
                    length: key.len(),
                    max: self.config.max_field_key_length,
                });
            }

            // Check string value length
            if let crate::ingestion::protocol::FieldValue::String(s) = value {
                if s.len() > self.config.max_field_string_length {
                    result.add_error(ValidationError::StringTooLong {
                        field: key.to_string(),
                        length: s.len(),
                        max: self.config.max_field_string_length,
                    });
                }
            }

            // Check for reserved field names
            if self.config.reject_reserved_names && is_reserved_name(key) {
                result.add_error(ValidationError::ReservedName {
                    name: key.to_string(),
                    location: "field key".to_string(),
                });
            }
        }

        // Validate timestamp if present
        if let Some(ts) = point.timestamp {
            if self.config.max_future_timestamp_ns > 0 && ts > self.config.max_future_timestamp_ns {
                result.add_error(ValidationError::TimestampTooFuture {
                    timestamp: ts,
                    max: self.config.max_future_timestamp_ns,
                });
            }
            if self.config.min_timestamp_ns > 0 && ts < self.config.min_timestamp_ns {
                result.add_error(ValidationError::TimestampTooOld {
                    timestamp: ts,
                    min: self.config.min_timestamp_ns,
                });
            }
        }

        // Schema-based validation
        if let Some(ref registry) = self.registry {
            self.validate_against_schema(point, registry, &mut result);
        }

        result
    }

    /// Validate against schema registry
    fn validate_against_schema<'a>(
        &self,
        point: &ParsedPoint<'a>,
        registry: &SchemaRegistry,
        result: &mut ValidationResult,
    ) {
        let measurement = point.measurement.as_ref();

        // Check if schema exists
        let schema = match registry.get(measurement) {
            Some(s) => s,
            None => {
                if registry.is_strict() {
                    result.add_error(ValidationError::UnknownMeasurement {
                        measurement: measurement.to_string(),
                    });
                }
                return;
            },
        };

        // Validate against schema
        self.validate_against_measurement_schema(point, &schema, result);
    }

    /// Validate against a specific measurement schema
    fn validate_against_measurement_schema<'a>(
        &self,
        point: &ParsedPoint<'a>,
        schema: &MeasurementSchema,
        result: &mut ValidationResult,
    ) {
        // Check required tags
        for tag_name in schema.required_tags() {
            if !point.tags.contains_key(tag_name) {
                result.add_error(ValidationError::MissingRequiredTag {
                    tag: tag_name.to_string(),
                });
            }
        }

        // Check required fields
        for field_name in schema.required_fields() {
            if !point.fields.contains_key(field_name) {
                result.add_error(ValidationError::MissingRequiredField {
                    field: field_name.to_string(),
                });
            }
        }

        // Validate tag values
        for (key, value) in &point.tags {
            if let Some(tag_schema) = schema.tags.get(key.as_ref()) {
                if let Err(e) = tag_schema.validate(value) {
                    result.add_error(e);
                }
            } else if !schema.allow_extra_tags {
                result.add_error(ValidationError::ExtraTagNotAllowed {
                    tag: key.to_string(),
                });
            }
        }

        // Validate field values
        for (key, value) in &point.fields {
            if let Some(field_schema) = schema.fields.get(key.as_ref()) {
                if let Err(e) = field_schema.validate(value) {
                    result.add_error(e);
                }
            } else if !schema.allow_extra_fields {
                result.add_error(ValidationError::ExtraFieldNotAllowed {
                    field: key.to_string(),
                });
            }
        }
    }

    /// Quick validation - returns on first error
    pub fn is_valid<'a>(&self, point: &ParsedPoint<'a>) -> bool {
        self.validate(point).is_ok()
    }
}

impl Default for Validator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingestion::protocol::{FieldValue, ParsedPoint};
    use std::borrow::Cow;
    use std::collections::HashMap;

    fn make_point<'a>(
        measurement: &'a str,
        tags: Vec<(&'a str, &'a str)>,
        fields: Vec<(&'a str, FieldValue)>,
    ) -> ParsedPoint<'a> {
        let mut tag_map = HashMap::new();
        for (k, v) in tags {
            tag_map.insert(Cow::Borrowed(k), Cow::Borrowed(v));
        }

        let mut field_map = HashMap::new();
        for (k, v) in fields {
            field_map.insert(Cow::Borrowed(k), v);
        }

        ParsedPoint {
            measurement: Cow::Borrowed(measurement),
            tags: tag_map,
            fields: field_map,
            timestamp: Some(1234567890),
        }
    }

    #[test]
    fn test_validation_valid_point() {
        let validator = Validator::new();
        let point = make_point(
            "cpu",
            vec![("host", "server01")],
            vec![("usage", FieldValue::Float(0.5))],
        );

        let result = validator.validate(&point);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validation_empty_measurement() {
        let validator = Validator::new();
        let point = make_point("", vec![], vec![("value", FieldValue::Float(1.0))]);

        let result = validator.validate(&point);
        assert!(result.is_err());
        assert!(matches!(
            result.first_error(),
            Some(ValidationError::EmptyMeasurement)
        ));
    }

    #[test]
    fn test_validation_no_fields() {
        let validator = Validator::new();
        let point = make_point("cpu", vec![], vec![]);

        let result = validator.validate(&point);
        assert!(result.is_err());
        assert!(matches!(
            result.first_error(),
            Some(ValidationError::NoFields)
        ));
    }

    #[test]
    fn test_validation_reserved_name() {
        let validator = Validator::new();
        let point = make_point("time", vec![], vec![("value", FieldValue::Float(1.0))]);

        let result = validator.validate(&point);
        assert!(result.is_err());
        assert!(matches!(
            result.first_error(),
            Some(ValidationError::ReservedName { .. })
        ));
    }

    #[test]
    fn test_validation_too_many_tags() {
        let config = ValidationConfig {
            max_tags: 2,
            ..Default::default()
        };
        let validator = Validator::with_config(config);

        let point = make_point(
            "cpu",
            vec![("host", "a"), ("region", "b"), ("env", "c")],
            vec![("value", FieldValue::Float(1.0))],
        );

        let result = validator.validate(&point);
        assert!(result.is_err());
        assert!(matches!(
            result.first_error(),
            Some(ValidationError::TooManyTags { .. })
        ));
    }

    #[test]
    fn test_validation_with_schema() {
        use super::super::registry::{FieldSchema, FieldType, MeasurementSchema, TagSchema};

        let registry = SchemaRegistry::new();
        registry.register(
            MeasurementSchema::new("cpu")
                .with_tag(TagSchema::required("host"))
                .with_field(FieldSchema::required("usage", FieldType::Float).with_range(0.0, 1.0)),
        );

        let validator = Validator::new().with_registry(registry);

        // Valid point
        let valid_point = make_point(
            "cpu",
            vec![("host", "server01")],
            vec![("usage", FieldValue::Float(0.5))],
        );
        assert!(validator.validate(&valid_point).is_ok());

        // Missing required tag
        let missing_tag = make_point("cpu", vec![], vec![("usage", FieldValue::Float(0.5))]);
        let result = validator.validate(&missing_tag);
        assert!(result.is_err());
        assert!(matches!(
            result.first_error(),
            Some(ValidationError::MissingRequiredTag { .. })
        ));

        // Value out of range
        let out_of_range = make_point(
            "cpu",
            vec![("host", "server01")],
            vec![("usage", FieldValue::Float(1.5))],
        );
        let result = validator.validate(&out_of_range);
        assert!(result.is_err());
        assert!(matches!(
            result.first_error(),
            Some(ValidationError::ValueOutOfRange { .. })
        ));
    }

    #[test]
    fn test_validation_result() {
        let mut result = ValidationResult::ok();
        assert!(result.is_ok());

        result.add_error(ValidationError::NoFields);
        assert!(result.is_err());
        assert_eq!(result.all_errors().len(), 1);
    }
}
