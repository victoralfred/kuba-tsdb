//! Schema registry for measurement definitions
//!
//! Manages schemas that define the expected structure of measurements,
//! including required/optional tags, field types, and constraints.

use std::collections::HashMap;
use std::sync::RwLock;

use super::validation::ValidationError;
use crate::ingestion::protocol::FieldValue;

/// Field data type for schema definition
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FieldType {
    /// 64-bit floating point
    Float,
    /// 64-bit signed integer
    Integer,
    /// 64-bit unsigned integer
    UInteger,
    /// UTF-8 string
    String,
    /// Boolean
    Boolean,
    /// Any type (no type constraint)
    Any,
}

impl FieldType {
    /// Check if a value matches this type
    pub fn matches(&self, value: &FieldValue) -> bool {
        match (self, value) {
            (FieldType::Any, _) => true,
            (FieldType::Float, FieldValue::Float(_)) => true,
            (FieldType::Integer, FieldValue::Integer(_)) => true,
            (FieldType::UInteger, FieldValue::UInteger(_)) => true,
            (FieldType::String, FieldValue::String(_)) => true,
            (FieldType::Boolean, FieldValue::Boolean(_)) => true,
            // Allow integer to float coercion
            (FieldType::Float, FieldValue::Integer(_)) => true,
            (FieldType::Float, FieldValue::UInteger(_)) => true,
            _ => false,
        }
    }

    /// Get the type name as a string
    pub fn name(&self) -> &'static str {
        match self {
            FieldType::Float => "float",
            FieldType::Integer => "integer",
            FieldType::UInteger => "uinteger",
            FieldType::String => "string",
            FieldType::Boolean => "boolean",
            FieldType::Any => "any",
        }
    }

    /// Parse from string
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "float" | "f64" | "double" => Some(FieldType::Float),
            "integer" | "int" | "i64" => Some(FieldType::Integer),
            "uinteger" | "uint" | "u64" => Some(FieldType::UInteger),
            "string" | "str" => Some(FieldType::String),
            "boolean" | "bool" => Some(FieldType::Boolean),
            "any" | "*" => Some(FieldType::Any),
            _ => None,
        }
    }
}

/// Field schema definition
#[derive(Debug, Clone)]
pub struct FieldSchema {
    /// Field name
    pub name: String,
    /// Expected field type
    pub field_type: FieldType,
    /// Is this field required?
    pub required: bool,
    /// Minimum value (for numeric types)
    pub min_value: Option<f64>,
    /// Maximum value (for numeric types)
    pub max_value: Option<f64>,
    /// Maximum string length (for string type)
    pub max_length: Option<usize>,
    /// Description for documentation
    pub description: Option<String>,
}

impl FieldSchema {
    /// Create a new required field schema
    pub fn required(name: impl Into<String>, field_type: FieldType) -> Self {
        Self {
            name: name.into(),
            field_type,
            required: true,
            min_value: None,
            max_value: None,
            max_length: None,
            description: None,
        }
    }

    /// Create a new optional field schema
    pub fn optional(name: impl Into<String>, field_type: FieldType) -> Self {
        Self {
            name: name.into(),
            field_type,
            required: false,
            min_value: None,
            max_value: None,
            max_length: None,
            description: None,
        }
    }

    /// Set value range for numeric fields
    pub fn with_range(mut self, min: f64, max: f64) -> Self {
        self.min_value = Some(min);
        self.max_value = Some(max);
        self
    }

    /// Set maximum length for string fields
    pub fn with_max_length(mut self, len: usize) -> Self {
        self.max_length = Some(len);
        self
    }

    /// Set description
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Validate a field value against this schema
    pub fn validate(&self, value: &FieldValue) -> Result<(), ValidationError> {
        // Check type
        if !self.field_type.matches(value) {
            return Err(ValidationError::TypeMismatch {
                field: self.name.clone(),
                expected: self.field_type.name().to_string(),
                actual: value.type_name().to_string(),
            });
        }

        // Check numeric range
        if let Some(min) = self.min_value {
            if let Some(v) = value.as_f64() {
                if v < min {
                    return Err(ValidationError::ValueOutOfRange {
                        field: self.name.clone(),
                        value: v,
                        min: Some(min),
                        max: self.max_value,
                    });
                }
            }
        }

        if let Some(max) = self.max_value {
            if let Some(v) = value.as_f64() {
                if v > max {
                    return Err(ValidationError::ValueOutOfRange {
                        field: self.name.clone(),
                        value: v,
                        min: self.min_value,
                        max: Some(max),
                    });
                }
            }
        }

        // Check string length
        if let Some(max_len) = self.max_length {
            if let FieldValue::String(s) = value {
                if s.len() > max_len {
                    return Err(ValidationError::StringTooLong {
                        field: self.name.clone(),
                        length: s.len(),
                        max: max_len,
                    });
                }
            }
        }

        Ok(())
    }
}

/// Tag schema definition
#[derive(Debug, Clone)]
pub struct TagSchema {
    /// Tag name
    pub name: String,
    /// Is this tag required?
    pub required: bool,
    /// Allowed values (empty = any value allowed)
    pub allowed_values: Vec<String>,
    /// Maximum value length
    pub max_length: Option<usize>,
    /// Description for documentation
    pub description: Option<String>,
}

impl TagSchema {
    /// Create a required tag schema
    pub fn required(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            required: true,
            allowed_values: Vec::new(),
            max_length: None,
            description: None,
        }
    }

    /// Create an optional tag schema
    pub fn optional(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            required: false,
            allowed_values: Vec::new(),
            max_length: None,
            description: None,
        }
    }

    /// Restrict to specific allowed values
    pub fn with_allowed_values(mut self, values: Vec<String>) -> Self {
        self.allowed_values = values;
        self
    }

    /// Set maximum length
    pub fn with_max_length(mut self, len: usize) -> Self {
        self.max_length = Some(len);
        self
    }

    /// Set description
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Validate a tag value
    pub fn validate(&self, value: &str) -> Result<(), ValidationError> {
        // Check length
        if let Some(max) = self.max_length {
            if value.len() > max {
                return Err(ValidationError::StringTooLong {
                    field: self.name.clone(),
                    length: value.len(),
                    max,
                });
            }
        }

        // Check allowed values
        if !self.allowed_values.is_empty() && !self.allowed_values.iter().any(|v| v == value) {
            return Err(ValidationError::InvalidTagValue {
                tag: self.name.clone(),
                value: value.to_string(),
                allowed: self.allowed_values.clone(),
            });
        }

        Ok(())
    }
}

/// Measurement schema definition
///
/// Defines the expected structure of a measurement including
/// required/optional tags and fields.
#[derive(Debug, Clone)]
pub struct MeasurementSchema {
    /// Measurement name
    pub name: String,
    /// Tag schemas (indexed by name)
    pub tags: HashMap<String, TagSchema>,
    /// Field schemas (indexed by name)
    pub fields: HashMap<String, FieldSchema>,
    /// Allow extra tags not defined in schema
    pub allow_extra_tags: bool,
    /// Allow extra fields not defined in schema
    pub allow_extra_fields: bool,
    /// Maximum total tags allowed
    pub max_tags: usize,
    /// Maximum total fields allowed
    pub max_fields: usize,
    /// Description for documentation
    pub description: Option<String>,
}

impl MeasurementSchema {
    /// Create a new measurement schema
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            tags: HashMap::new(),
            fields: HashMap::new(),
            allow_extra_tags: true,
            allow_extra_fields: true,
            max_tags: super::MAX_TAGS_PER_POINT,
            max_fields: super::MAX_FIELDS_PER_POINT,
            description: None,
        }
    }

    /// Add a tag schema
    pub fn with_tag(mut self, tag: TagSchema) -> Self {
        self.tags.insert(tag.name.clone(), tag);
        self
    }

    /// Add a field schema
    pub fn with_field(mut self, field: FieldSchema) -> Self {
        self.fields.insert(field.name.clone(), field);
        self
    }

    /// Disallow extra tags
    pub fn strict_tags(mut self) -> Self {
        self.allow_extra_tags = false;
        self
    }

    /// Disallow extra fields
    pub fn strict_fields(mut self) -> Self {
        self.allow_extra_fields = false;
        self
    }

    /// Set description
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Get required tag names
    pub fn required_tags(&self) -> Vec<&str> {
        self.tags
            .values()
            .filter(|t| t.required)
            .map(|t| t.name.as_str())
            .collect()
    }

    /// Get required field names
    pub fn required_fields(&self) -> Vec<&str> {
        self.fields
            .values()
            .filter(|f| f.required)
            .map(|f| f.name.as_str())
            .collect()
    }
}

/// Thread-safe schema registry
///
/// Manages measurement schemas with thread-safe access for
/// concurrent validation.
pub struct SchemaRegistry {
    /// Schemas by measurement name
    schemas: RwLock<HashMap<String, MeasurementSchema>>,
    /// Default behavior when no schema exists
    strict_mode: bool,
}

impl SchemaRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
            strict_mode: false,
        }
    }

    /// Create a strict registry that rejects unknown measurements
    pub fn strict() -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
            strict_mode: true,
        }
    }

    /// Register a measurement schema
    pub fn register(&self, schema: MeasurementSchema) {
        let mut schemas = self.schemas.write().unwrap();
        schemas.insert(schema.name.clone(), schema);
    }

    /// Unregister a measurement schema
    pub fn unregister(&self, measurement: &str) {
        let mut schemas = self.schemas.write().unwrap();
        schemas.remove(measurement);
    }

    /// Get a schema by measurement name
    pub fn get(&self, measurement: &str) -> Option<MeasurementSchema> {
        let schemas = self.schemas.read().unwrap();
        schemas.get(measurement).cloned()
    }

    /// Check if a measurement has a schema
    pub fn has_schema(&self, measurement: &str) -> bool {
        let schemas = self.schemas.read().unwrap();
        schemas.contains_key(measurement)
    }

    /// Get all registered measurement names
    pub fn measurements(&self) -> Vec<String> {
        let schemas = self.schemas.read().unwrap();
        schemas.keys().cloned().collect()
    }

    /// Check if registry is in strict mode
    pub fn is_strict(&self) -> bool {
        self.strict_mode
    }

    /// Get number of registered schemas
    pub fn len(&self) -> usize {
        let schemas = self.schemas.read().unwrap();
        schemas.len()
    }

    /// Check if registry is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all schemas
    pub fn clear(&self) {
        let mut schemas = self.schemas.write().unwrap();
        schemas.clear();
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_type_matches() {
        assert!(FieldType::Float.matches(&FieldValue::Float(1.0)));
        assert!(FieldType::Float.matches(&FieldValue::Integer(1))); // coercion allowed
        assert!(FieldType::Integer.matches(&FieldValue::Integer(1)));
        assert!(!FieldType::Integer.matches(&FieldValue::Float(1.0)));
        assert!(FieldType::Any.matches(&FieldValue::Float(1.0)));
        assert!(FieldType::Any.matches(&FieldValue::String("test".to_string())));
    }

    #[test]
    fn test_field_type_from_str() {
        assert_eq!(FieldType::parse("float"), Some(FieldType::Float));
        assert_eq!(FieldType::parse("integer"), Some(FieldType::Integer));
        assert_eq!(FieldType::parse("string"), Some(FieldType::String));
        assert_eq!(FieldType::parse("boolean"), Some(FieldType::Boolean));
        assert_eq!(FieldType::parse("any"), Some(FieldType::Any));
        assert_eq!(FieldType::parse("unknown"), None);
    }

    #[test]
    fn test_field_schema_validate() {
        let schema = FieldSchema::required("value", FieldType::Float).with_range(0.0, 100.0);

        assert!(schema.validate(&FieldValue::Float(50.0)).is_ok());
        assert!(schema.validate(&FieldValue::Float(-1.0)).is_err());
        assert!(schema.validate(&FieldValue::Float(101.0)).is_err());
        assert!(schema
            .validate(&FieldValue::String("test".to_string()))
            .is_err());
    }

    #[test]
    fn test_tag_schema_validate() {
        let schema = TagSchema::required("env").with_allowed_values(vec![
            "prod".to_string(),
            "staging".to_string(),
            "dev".to_string(),
        ]);

        assert!(schema.validate("prod").is_ok());
        assert!(schema.validate("staging").is_ok());
        assert!(schema.validate("unknown").is_err());
    }

    #[test]
    fn test_measurement_schema() {
        let schema = MeasurementSchema::new("cpu")
            .with_tag(TagSchema::required("host"))
            .with_field(FieldSchema::required("usage", FieldType::Float))
            .with_description("CPU usage metrics");

        assert_eq!(schema.name, "cpu");
        assert_eq!(schema.required_tags(), vec!["host"]);
        assert_eq!(schema.required_fields(), vec!["usage"]);
    }

    #[test]
    fn test_schema_registry() {
        let registry = SchemaRegistry::new();

        let schema = MeasurementSchema::new("cpu")
            .with_field(FieldSchema::required("usage", FieldType::Float));

        registry.register(schema);

        assert!(registry.has_schema("cpu"));
        assert!(!registry.has_schema("memory"));
        assert_eq!(registry.len(), 1);

        let retrieved = registry.get("cpu").unwrap();
        assert_eq!(retrieved.name, "cpu");

        registry.unregister("cpu");
        assert!(!registry.has_schema("cpu"));
    }

    #[test]
    fn test_schema_registry_strict() {
        let registry = SchemaRegistry::strict();
        assert!(registry.is_strict());
    }
}
