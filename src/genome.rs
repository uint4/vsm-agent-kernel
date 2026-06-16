use crate::channel::{ChannelGene, RelationGene};
use crate::error::{KernelError, Result};
use crate::ids::{GenomeId, MutationId, NodeId};
use crate::node::{LeafOperation, ViableNode};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrganizationalGenome {
    pub id: GenomeId,
    pub root_node_id: NodeId,
    pub nodes: BTreeMap<NodeId, ViableNode>,
    pub relations: Vec<RelationGene>,
    pub channels: Vec<ChannelGene>,
    pub lineage: GenomeLineage,
}

impl OrganizationalGenome {
    pub fn from_root(root: ViableNode) -> Self {
        let root_node_id = root.id.clone();
        let mut nodes = BTreeMap::new();
        nodes.insert(root.id.clone(), root);
        Self {
            id: GenomeId::new(),
            root_node_id,
            nodes,
            relations: Vec::new(),
            channels: Vec::new(),
            lineage: GenomeLineage::default(),
        }
    }

    pub fn get_node(&self, id: &NodeId) -> Result<&ViableNode> {
        self.nodes.get(id).ok_or_else(|| KernelError::NodeNotFound(id.clone()))
    }

    pub fn get_node_mut(&mut self, id: &NodeId) -> Result<&mut ViableNode> {
        self.nodes.get_mut(id).ok_or_else(|| KernelError::NodeNotFound(id.clone()))
    }

    pub fn children_of(&self, id: &NodeId) -> Result<Vec<&ViableNode>> {
        let node = self.get_node(id)?;
        node.children.iter().map(|child| self.get_node(child)).collect()
    }

    pub fn apply_patch(&mut self, patch: GenomePatch) -> Result<()> {
        match patch {
            GenomePatch::AddNode { parent_id, mut node } => {
                let parent = self.get_node(&parent_id)?.clone();
                if parent.operation.is_some() && parent.children.is_empty() {
                    return Err(KernelError::AddChildToOperationalLeaf(parent_id));
                }

                if self.nodes.contains_key(&node.id) {
                    return Err(KernelError::InvalidPatch(format!(
                        "node {} already exists",
                        node.id
                    )));
                }

                node.parent_id = Some(parent_id.clone());
                node.validate_invariants()?;
                self.nodes.insert(node.id.clone(), node.clone());
                self.get_node_mut(&parent_id)?.children.push(node.id);
            }
            GenomePatch::PromoteLeafToMetasystem { node_id, extracted_child_name } => {
                let original_operation = {
                    let node = self.get_node(&node_id)?;
                    if !node.children.is_empty() {
                        return Err(KernelError::AlreadyMetasystem(node_id));
                    }
                    node.operation.clone()
                };

                let Some(operation) = original_operation else {
                    return Err(KernelError::InvalidPatch(format!(
                        "node {} has no leaf operation to extract",
                        node_id
                    )));
                };

                let mut child = ViableNode::new_leaf(extracted_child_name, operation);
                child.parent_id = Some(node_id.clone());

                let child_id = child.id.clone();
                self.nodes.insert(child_id.clone(), child);

                let node = self.get_node_mut(&node_id)?;
                node.operation = None;
                node.children.push(child_id);
                node.validate_invariants()?;
            }
            GenomePatch::CollapseMetasystemToLeaf { node_id, operation } => {
                let child_ids = self.get_node(&node_id)?.children.clone();
                if child_ids.is_empty() {
                    return Err(KernelError::AlreadyLeaf(node_id));
                }

                for child_id in child_ids {
                    self.remove_subtree(&child_id)?;
                }

                let node = self.get_node_mut(&node_id)?;
                node.children.clear();
                node.operation = Some(operation);
                node.validate_invariants()?;
            }
            GenomePatch::RemoveSubtree { node_id } => {
                if node_id == self.root_node_id {
                    return Err(KernelError::InvalidPatch(
                        "cannot remove root subtree".to_string(),
                    ));
                }
                self.remove_subtree(&node_id)?;
            }
            GenomePatch::AddRelation(relation) => {
                self.ensure_node_exists(&relation.from_node_id)?;
                self.ensure_node_exists(&relation.to_node_id)?;
                self.relations.push(relation);
            }
            GenomePatch::RemoveRelation { relation_id } => {
                self.relations.retain(|r| r.id != relation_id);
            }
            GenomePatch::AddChannel(channel) => {
                self.ensure_node_exists(&channel.from_node_id)?;
                self.ensure_node_exists(&channel.to_node_id)?;
                self.channels.push(channel);
            }
            GenomePatch::RemoveChannel { channel_id } => {
                self.channels.retain(|c| c.id != channel_id);
            }
        }

        self.validate()?;
        Ok(())
    }

    fn ensure_node_exists(&self, id: &NodeId) -> Result<()> {
        self.nodes
            .contains_key(id)
            .then_some(())
            .ok_or_else(|| KernelError::NodeNotFound(id.clone()))
    }

    fn remove_subtree(&mut self, node_id: &NodeId) -> Result<()> {
        let node = self.get_node(node_id)?.clone();
        for child_id in node.children.clone() {
            self.remove_subtree(&child_id)?;
        }

        if let Some(parent_id) = node.parent_id {
            if let Some(parent) = self.nodes.get_mut(&parent_id) {
                parent.children.retain(|id| id != node_id);
            }
        }

        self.relations.retain(|r| &r.from_node_id != node_id && &r.to_node_id != node_id);
        self.channels.retain(|c| &c.from_node_id != node_id && &c.to_node_id != node_id);
        self.nodes.remove(node_id);
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        if !self.nodes.contains_key(&self.root_node_id) {
            return Err(KernelError::RootNotFound(self.root_node_id.clone()));
        }

        for node in self.nodes.values() {
            node.validate_invariants()?;
            for child_id in &node.children {
                let child = self.get_node(child_id)?;
                if child.parent_id.as_ref() != Some(&node.id) {
                    return Err(KernelError::InvalidPatch(format!(
                        "child {} does not point back to parent {}",
                        child_id, node.id
                    )));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GenomePatch {
    AddNode { parent_id: NodeId, node: ViableNode },
    RemoveSubtree { node_id: NodeId },

    /// Converts an operational leaf into a metasystem by moving its former leaf
    /// operation into a new child node. The promoted parent loses direct code
    /// writing / review / testing operation authority.
    PromoteLeafToMetasystem {
        node_id: NodeId,
        extracted_child_name: String,
    },

    /// Collapses a whole subtree into one leaf. This is destructive and should
    /// usually be used only as a selection/pruning result.
    CollapseMetasystemToLeaf {
        node_id: NodeId,
        operation: LeafOperation,
    },

    AddRelation(RelationGene),
    RemoveRelation { relation_id: crate::ids::RelationId },
    AddChannel(ChannelGene),
    RemoveChannel { channel_id: crate::ids::ChannelId },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenomeLineage {
    pub parent_genome_ids: Vec<GenomeId>,
    pub mutation_ids: Vec<MutationId>,
    pub created_at: DateTime<Utc>,
}

impl Default for GenomeLineage {
    fn default() -> Self {
        Self {
            parent_genome_ids: Vec::new(),
            mutation_ids: Vec::new(),
            created_at: Utc::now(),
        }
    }
}
