use crate::{CapabilitySet, ChannelConfig, LeafOperationSpec, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct System5Policy {
    pub identity: String,
    pub values: Vec<String>,
    pub non_negotiable_constraints: Vec<String>,
    pub denied_capabilities: Vec<String>,
}

impl Default for System5Policy {
    fn default() -> Self {
        Self {
            identity: "unclassified viable node".to_string(),
            values: vec![],
            non_negotiable_constraints: vec![],
            denied_capabilities: vec![],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct System4Config {
    pub future_probe_enabled: bool,
    pub probe_interval_seconds: Option<u64>,
    pub watched_environments: Vec<String>,
    pub horizon: Option<String>,
}

impl Default for System4Config {
    fn default() -> Self {
        Self {
            future_probe_enabled: true,
            probe_interval_seconds: None,
            watched_environments: vec![],
            horizon: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct System3Config {
    pub can_allocate_budget: bool,
    pub can_decompose_tasks: bool,
    pub can_issue_commands: bool,
    pub can_integrate_child_outputs: bool,
    pub max_parallel_children: Option<u32>,
    pub default_task_budget_tokens: Option<u64>,
}

impl Default for System3Config {
    fn default() -> Self {
        Self {
            can_allocate_budget: true,
            can_decompose_tasks: true,
            can_issue_commands: true,
            can_integrate_child_outputs: true,
            max_parallel_children: None,
            default_task_budget_tokens: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct System3StarConfig {
    pub audit_enabled: bool,
    pub audit_interval_seconds: Option<u64>,
    pub audit_window_tasks: Option<u64>,
    pub gene_suggestion_enabled: bool,
}

impl Default for System3StarConfig {
    fn default() -> Self {
        Self {
            audit_enabled: true,
            audit_interval_seconds: None,
            audit_window_tasks: Some(50),
            gene_suggestion_enabled: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptComponent {
    pub id: String,
    pub text: String,
    pub tags: Vec<String>,
    pub origin: PromptOrigin,
    pub active: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PromptOrigin {
    Manual,
    Mutation,
    SummarizedMemory,
    TaskCluster,
    SystemPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PromptGenome {
    pub base_identity: String,
    pub behavior_rules: Vec<PromptComponent>,
    pub domain_hints: Vec<PromptComponent>,
    pub codebase_conventions: Vec<PromptComponent>,
    pub negative_constraints: Vec<PromptComponent>,
    pub output_contract: Option<PromptComponent>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelSpec {
    pub provider: String,
    pub model: String,
    pub effort: Option<String>,
    pub max_context_tokens: Option<u64>,
}

impl Default for ModelSpec {
    fn default() -> Self {
        Self {
            provider: "abstract".to_string(),
            model: "unbound".to_string(),
            effort: None,
            max_context_tokens: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: Option<String>,
    pub permissions: Vec<String>,
    pub enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ContextPolicy {
    pub fixed_context_refs: Vec<String>,
    pub retrievable_context_refs: Vec<String>,
    pub max_retrieval_tokens: Option<u64>,
    pub max_total_task_tokens: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PermissionSpec {
    pub allowed_paths: Vec<String>,
    pub denied_paths: Vec<String>,
    pub allowed_tools: Vec<String>,
    pub denied_tools: Vec<String>,
    pub requires_human_approval: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeLifecycleStatus {
    Shadow,
    Probation,
    Active,
    Retired,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViableNode {
    pub id: NodeId,
    pub parent_id: Option<NodeId>,
    pub name: String,

    pub system_5: System5Policy,
    pub system_4: System4Config,
    pub system_3: System3Config,
    pub system_3_star: System3StarConfig,

    /// Children are System 1 units from this node's perspective. Because VSM is
    /// recursive, each child is itself a full viable node.
    pub children: Vec<NodeId>,

    /// If children is empty, this node may operate as a leaf according to this
    /// spec. If children is non-empty, this spec is ignored for code-writing
    /// authority and the node is treated as a metasystem.
    pub leaf_operation: LeafOperationSpec,

    pub model: ModelSpec,
    pub prompt: PromptGenome,
    pub tools: Vec<ToolSpec>,
    pub context_policy: ContextPolicy,
    pub permissions: PermissionSpec,
    pub channels: Vec<ChannelConfig>,

    pub age_epochs: u64,
    pub status: NodeLifecycleStatus,
    pub metadata: BTreeMap<String, String>,
}

impl ViableNode {
    pub fn new_leaf(name: impl Into<String>, operation: LeafOperationSpec) -> Self {
        Self {
            id: NodeId::new(),
            parent_id: None,
            name: name.into(),
            system_5: System5Policy::default(),
            system_4: System4Config::default(),
            system_3: System3Config::default(),
            system_3_star: System3StarConfig::default(),
            children: vec![],
            leaf_operation: operation,
            model: ModelSpec::default(),
            prompt: PromptGenome::default(),
            tools: vec![],
            context_policy: ContextPolicy::default(),
            permissions: PermissionSpec::default(),
            channels: vec![],
            age_epochs: 0,
            status: NodeLifecycleStatus::Active,
            metadata: BTreeMap::new(),
        }
    }

    pub fn new_metasystem(name: impl Into<String>) -> Self {
        Self {
            id: NodeId::new(),
            parent_id: None,
            name: name.into(),
            system_5: System5Policy::default(),
            system_4: System4Config::default(),
            system_3: System3Config::default(),
            system_3_star: System3StarConfig::default(),
            children: vec![],
            leaf_operation: LeafOperationSpec::default(),
            model: ModelSpec::default(),
            prompt: PromptGenome::default(),
            tools: vec![],
            context_policy: ContextPolicy::default(),
            permissions: PermissionSpec::default(),
            channels: vec![],
            age_epochs: 0,
            status: NodeLifecycleStatus::Active,
            metadata: BTreeMap::new(),
        }
    }

    pub fn is_leaf(&self) -> bool {
        self.children.is_empty()
    }

    pub fn is_metasystem(&self) -> bool {
        !self.children.is_empty()
    }

    pub fn capabilities(&self) -> CapabilitySet {
        if self.is_metasystem() {
            CapabilitySet::for_metasystem()
        } else {
            CapabilitySet::for_leaf(&self.leaf_operation)
        }
    }
}
