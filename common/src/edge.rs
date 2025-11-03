use std::collections::HashMap;
use crate::attr::{AttributeContainer, AttributeValue};
use crate::node::NodeId;

pub type EdgeId = uuid::Uuid;


#[derive(Debug, Clone)]
pub struct Edge {
    id: EdgeId,
    source: NodeId,
    target: NodeId,
    attributes: HashMap<String, AttributeValue>,
}

impl Edge {
    pub fn new(id: EdgeId, source: NodeId, target: NodeId, attributes: HashMap<String, AttributeValue>) -> Self {
        Self { id, source, target, attributes }
    }

    pub fn id(&self) -> EdgeId {
        self.id
    }

    pub fn source(&self) -> NodeId {
        self.source
    }

    pub fn target(&self) -> NodeId {
        self.target
    }
}

impl AttributeContainer for Edge {
    fn attributes(&self) -> &HashMap<String, AttributeValue> {
        &self.attributes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::NodeId;
    use std::collections::HashMap;

    #[test]
    fn new_preserves_id_and_endpoints() {
        let id = EdgeId::from_u128(1);
        let source = NodeId::from_u128(2);
        let target = NodeId::from_u128(3);
        let edge = Edge::new(id, source, target, HashMap::new());

        assert_eq!(edge.id(), id);
        assert_eq!(edge.source(), source);
        assert_eq!(edge.target(), target);
    }

    #[test]
    fn attribute_container_exposes_edge_properties() {
        let id = EdgeId::from_u128(4);
        let source = NodeId::from_u128(5);
        let target = NodeId::from_u128(6);
        let mut attributes = HashMap::new();
        attributes.insert("weight".to_string(), AttributeValue::Float(1.5));
        let edge = Edge::new(id, source, target, attributes);

        let attribute = edge.attribute("weight");

        match attribute {
            Some(AttributeValue::Float(weight)) => assert!((weight - 1.5).abs() < f64::EPSILON),
            other => panic!("unexpected attribute: {:?}", other),
        }
    }
}
