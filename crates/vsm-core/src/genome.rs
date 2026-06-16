use crate::{ChannelConfig, GenomeId, NodeId, ParentChildChannelBundle, ViableNode};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrganizationalGenome {
    pub id: GenomeId,
    pub root_node_id: NodeId,
    pub nodes: BTreeMap<NodeId, ViableNode>,
    pub channels: Vec<ChannelConfig>,
    pub lineage: GenomeLineage,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct GenomeLineage {
    pub parent_genome_ids: Vec<GenomeId>,
    pub mutation_ids: Vec<String>,
    pub created_at_unix_ms: Option<i64>,
}

#[derive(Debug, Error)]
pub enum GenomeError {
    #[error("node not found: {0}")]
    NodeNotFound(NodeId),

    #[error("node already exists: {0}")]
    NodeAlreadyExists(NodeId),

    #[error("root node cannot have a parent")]
    RootCannotHaveParent,

    #[error("parent-child cycle detected")]
    CycleDetected,

    #[error("cannot remove root node")]
    CannotRemoveRoot,
}

impl OrganizationalGenome {
    pub fn new(root: ViableNode) -> Self {
        let root_id = root.id.clone();
        let mut nodes = BTreeMap::new();
        nodes.insert(root_id.clone(), root);
        Self {
            id: GenomeId::new(),
            root_node_id: root_id,
            nodes,
            channels: vec![],
            lineage: GenomeLineage::default(),
            metadata: BTreeMap::new(),
        }
    }

    pub fn get_node(&self, id: &NodeId) -> Result<&ViableNode, GenomeError> {
        self.nodes.get(id).ok_or_else(|| GenomeError::NodeNotFound(id.clone()))
    }

    pub fn get_node_mut(&mut self, id: &NodeId) -> Result<&mut ViableNode, GenomeError> {
        self.nodes.get_mut(id).ok_or_else(|| GenomeError::NodeNotFound(id.clone()))
    }

    pub fn add_child(&mut self, parent_id: &NodeId, mut child: ViableNode) -> Result<NodeId, GenomeError> {
        if !self.nodes.contains_key(parent_id) {
            return Err(GenomeError::NodeNotFound(parent_id.clone()));
        }
        if self.nodes.contains_key(&child.id) {
            return Err(GenomeError::NodeAlreadyExists(child.id.clone()));
        }

        let child_id = child.id.clone();
        child.parent_id = Some(parent_id.clone());
        self.nodes.insert(child_id.clone(), child);
        self.get_node_mut(parent_id)?.children.push(child_id.clone());

        let bundle = ParentChildChannelBundle::standard(parent_id.clone(), child_id.clone());
        for channel in [
            bundle.resource_bargaining,
            bundle.command,
            bundle.coordination_via_system2,
            bundle.audit_via_system3_star,
            bundle.algedonic,
        ]
        .into_iter()
        .flatten()
        {
            self.channels.push(channel);
        }

        Ok(child_id)
    }

    pub fn remove_subtree(&mut self, node_id: &NodeId) -> Result<Vec<NodeId>, GenomeError> {
        if node_id == &self.root_node_id {
            return Err(GenomeError::CannotRemoveRoot);
        }
        if !self.nodes.contains_key(node_id) {
            return Err(GenomeError::NodeNotFound(node_id.clone()));
        }

        let mut removed = vec![];
        self.collect_subtree(node_id, &mut removed)?;

        if let Some(parent_id) = self.nodes.get(node_id).and_then(|n| n.parent_id.clone()) {
            if let Some(parent) = self.nodes.get_mut(&parent_id) {
                parent.children.retain(|id| id != node_id);
            }
        }

        for id in &removed {
            self.nodes.remove(id);
        }

        self.channels.retain(|ch| {
            let from_removed = ch.from.as_ref().map(|id| removed.contains(id)).unwrap_or(false);
            let to_removed = ch.to.as_ref().map(|id| removed.contains(id)).unwrap_or(false);
            !from_removed && !to_removed
        });

        Ok(removed)
    }

    pub fn collect_subtree(&self, node_id: &NodeId, out: &mut Vec<NodeId>) -> Result<(), GenomeError> {
        let node = self.get_node(node_id)?;
        out.push(node_id.clone());
        for child_id in &node.children {
            self.collect_subtree(child_id, out)?;
        }
        Ok(())
    }

    pub fn subtree_ids(&self, node_id: &NodeId) -> Result<Vec<NodeId>, GenomeError> {
        let mut ids = vec![];
        self.collect_subtree(node_id, &mut ids)?;
        Ok(ids)
    }

    pub fn ancestor_ids(&self, node_id: &NodeId) -> Result<Vec<NodeId>, GenomeError> {
        let mut ancestors = vec![];
        let mut current = self.get_node(node_id)?.parent_id.clone();

        while let Some(parent_id) = current {
            let parent = self.get_node(&parent_id)?;
            ancestors.push(parent_id.clone());
            current = parent.parent_id.clone();
        }

        Ok(ancestors)
    }
}
