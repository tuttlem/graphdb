use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::attr::{AttributeContainer, AttributeValue};

pub type NodeId = uuid::Uuid;

/// Alias representing a node label
pub type Label = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    id: NodeId,
    labels: Vec<Label>,
    attributes: HashMap<String, AttributeValue>,
}

impl Node {
    pub fn new(
        id: NodeId,
        labels: Vec<Label>,
        attributes: HashMap<String, AttributeValue>,
    ) -> Self {
        Self {
            id,
            labels,
            attributes,
        }
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn labels(&self) -> &[Label] {
        &self.labels
    }
}

impl AttributeContainer for Node {
    fn attributes(&self) -> &HashMap<String, AttributeValue> {
        &self.attributes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn new_preserves_id_and_labels() {
        let id = NodeId::from_u128(1);
        let labels = vec!["Person".to_string(), "Employee".to_string()];
        let node = Node::new(id, labels.clone(), HashMap::new());

        assert_eq!(node.id(), id);
        assert_eq!(node.labels(), labels.as_slice());
    }

    #[test]
    fn attribute_container_exposes_properties() {
        let id = NodeId::from_u128(2);
        let mut attributes = HashMap::new();
        attributes.insert(
            "first_name".to_string(),
            AttributeValue::String("Ada".to_string()),
        );
        let node = Node::new(id, vec![], attributes);

        let value = node.attribute("first_name");

        match value {
            Some(AttributeValue::String(name)) => assert_eq!(name, "Ada"),
            other => panic!("unexpected attribute: {:?}", other),
        }
    }
}
