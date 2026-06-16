use crate::{
    ChannelConfig, ContextPolicy, GenomeError, LeafOperationSpec, ModelSpec, MutationId, NodeId,
    OrganizationalGenome, PermissionSpec, PromptComponent, PromptGenome, ToolSpec, ViableNode,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrganizationalGenomePatch {
    AddChild {
        parent_id: NodeId,
        child: ViableNode,
    },
    /// Converts an operational leaf into a metasystem by extracting its former
    /// operational identity into a new child. After this patch, the original
    /// node has children and therefore loses code-writing authority by state.
    PromoteLeafToMetasystem {
        node_id: NodeId,
        extracted_child_name: String,
    },
    RemoveSubtree {
        node_id: NodeId,
    },
    AddChannel {
        channel: ChannelConfig,
    },
    RemoveChannel {
        channel_id: String,
    },
    AddPromptComponent {
        node_id: NodeId,
        section: PromptSection,
        component: PromptComponent,
    },
    RemovePromptComponent {
        node_id: NodeId,
        component_id: String,
    },
    AddTool {
        node_id: NodeId,
        tool: ToolSpec,
    },
    RemoveTool {
        node_id: NodeId,
        tool_name: String,
    },
    SetNodeStatus {
        node_id: NodeId,
        status: crate::NodeLifecycleStatus,
    },
    Batch {
        patches: Vec<OrganizationalGenomePatch>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PromptSection {
    BehaviorRules,
    DomainHints,
    CodebaseConventions,
    NegativeConstraints,
    OutputContract,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationRecord {
    pub id: MutationId,
    pub patch: OrganizationalGenomePatch,
    pub source: String,
    pub hypothesis: String,
}

#[derive(Debug, Error)]
pub enum PatchError {
    #[error(transparent)]
    Genome(#[from] GenomeError),

    #[error("prompt component not found: {0}")]
    PromptComponentNotFound(String),

    #[error("node is already a metasystem and cannot be promoted as a leaf: {0}")]
    NodeIsNotLeaf(NodeId),
}

impl OrganizationalGenomePatch {
    pub fn apply(&self, genome: &mut OrganizationalGenome) -> Result<(), PatchError> {
        match self {
            Self::AddChild { parent_id, child } => {
                genome.add_child(parent_id, child.clone())?;
            }
            Self::PromoteLeafToMetasystem {
                node_id,
                extracted_child_name,
            } => {
                let (
                    leaf_operation,
                    model,
                    prompt,
                    tools,
                    context_policy,
                    permissions,
                    system_5,
                    metadata,
                ) = {
                    let node = genome.get_node(node_id)?;
                    if !node.children.is_empty() {
                        return Err(PatchError::NodeIsNotLeaf(node_id.clone()));
                    }
                    (
                        node.leaf_operation.clone(),
                        node.model.clone(),
                        node.prompt.clone(),
                        node.tools.clone(),
                        node.context_policy.clone(),
                        node.permissions.clone(),
                        node.system_5.clone(),
                        node.metadata.clone(),
                    )
                };

                let mut child = ViableNode::new_leaf(extracted_child_name.clone(), leaf_operation);
                child.system_5 = system_5;
                child.model = model;
                child.prompt = prompt;
                child.tools = tools;
                child.context_policy = context_policy;
                child.permissions = permissions;
                child.metadata = metadata;
                child
                    .metadata
                    .insert("extracted_from_node_id".to_string(), node_id.to_string());

                genome.add_child(node_id, child)?;

                let parent = genome.get_node_mut(node_id)?;
                parent.leaf_operation = LeafOperationSpec::default();
                parent.model = ModelSpec::default();
                parent.prompt = PromptGenome::default();
                parent.tools.clear();
                parent.context_policy = ContextPolicy::default();
                parent.permissions = PermissionSpec::default();
                parent
                    .metadata
                    .insert("promoted_to_metasystem".to_string(), "true".to_string());
            }
            Self::RemoveSubtree { node_id } => {
                genome.remove_subtree(node_id)?;
            }
            Self::AddChannel { channel } => {
                genome.channels.push(channel.clone());
            }
            Self::RemoveChannel { channel_id } => {
                genome
                    .channels
                    .retain(|ch| ch.id.as_str() != channel_id.as_str());
            }
            Self::AddPromptComponent {
                node_id,
                section,
                component,
            } => {
                let node = genome.get_node_mut(node_id)?;
                match section {
                    PromptSection::BehaviorRules => {
                        node.prompt.behavior_rules.push(component.clone())
                    }
                    PromptSection::DomainHints => node.prompt.domain_hints.push(component.clone()),
                    PromptSection::CodebaseConventions => {
                        node.prompt.codebase_conventions.push(component.clone())
                    }
                    PromptSection::NegativeConstraints => {
                        node.prompt.negative_constraints.push(component.clone())
                    }
                    PromptSection::OutputContract => {
                        node.prompt.output_contract = Some(component.clone())
                    }
                }
            }
            Self::RemovePromptComponent {
                node_id,
                component_id,
            } => {
                let node = genome.get_node_mut(node_id)?;
                let mut removed = false;
                for section in [
                    &mut node.prompt.behavior_rules,
                    &mut node.prompt.domain_hints,
                    &mut node.prompt.codebase_conventions,
                    &mut node.prompt.negative_constraints,
                ] {
                    let len_before = section.len();
                    section.retain(|c| c.id != *component_id);
                    removed |= section.len() != len_before;
                }
                if node.prompt.output_contract.as_ref().map(|c| &c.id) == Some(component_id) {
                    node.prompt.output_contract = None;
                    removed = true;
                }
                if !removed {
                    return Err(PatchError::PromptComponentNotFound(component_id.clone()));
                }
            }
            Self::AddTool { node_id, tool } => {
                genome.get_node_mut(node_id)?.tools.push(tool.clone());
            }
            Self::RemoveTool { node_id, tool_name } => {
                genome
                    .get_node_mut(node_id)?
                    .tools
                    .retain(|t| t.name != *tool_name);
            }
            Self::SetNodeStatus { node_id, status } => {
                genome.get_node_mut(node_id)?.status = status.clone();
            }
            Self::Batch { patches } => {
                for patch in patches {
                    patch.apply(genome)?;
                }
            }
        }
        Ok(())
    }
}
