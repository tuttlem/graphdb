//! Shared attribute types and helpers for graph primitives.

use std::collections::HashMap;

/// Encapsulates the set of supported attribute value types.
///
/// `Null` is included so callers can distinguish between “missing key” and
/// “explicit null” when the data model requires it.
#[derive(Debug, Clone, PartialEq)]
pub enum AttributeValue {
    Null,
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
}

/// Provides read-only access to an entity's attributes.
pub trait AttributeContainer {
    /// Returns the backing attribute map keyed by attribute name.
    fn attributes(&self) -> &HashMap<String, AttributeValue>;

    /// Fetches a single attribute by name if present.
    fn attribute(&self, name: &str) -> Option<&AttributeValue> {
        self.attributes().get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestContainer {
        attributes: HashMap<String, AttributeValue>,
    }

    impl AttributeContainer for TestContainer {
        fn attributes(&self) -> &HashMap<String, AttributeValue> {
            &self.attributes
        }
    }

    fn fixture_container() -> TestContainer {
        let mut attributes = HashMap::new();
        attributes.insert("name".to_string(), AttributeValue::String("Ada".into()));
        attributes.insert("active".to_string(), AttributeValue::Boolean(true));

        TestContainer { attributes }
    }

    #[test]
    fn attribute_returns_value_when_present() {
        let container = fixture_container();

        let value = container.attribute("name");

        match value {
            Some(AttributeValue::String(name)) => assert_eq!(name, "Ada"),
            other => panic!("unexpected attribute value: {:?}", other),
        }
    }

    #[test]
    fn attribute_returns_none_when_missing() {
        let container = fixture_container();

        assert!(container.attribute("missing").is_none());
    }
}
