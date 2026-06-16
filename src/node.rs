use crate::error::{KernelError, Result};
use crate::ids::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViableNode {
    pub id: NodeId,
    pub name: String,
    pub parent_id: Option<NodeId>,

    /// Child viable systems. If non-empty, this node is a metasystem.
    pub children: Vec<NodeId>,

    pub system5: System5,
    pub system4: System4,
    pub system3: System3,
    pub system3_star: System3Star,
    pub system2: System2,

    /// Present only when this node is a leaf System 1 operation.
    /// A node with children must have `operation == None`.
    pub operation: Option<LeafOperation>,

    pub prompt_genome: PromptGenome,
    pub model: ModelSpec,
    pub context_policy: ContextPolicy,
    pub effort_budget: EffortBudget,
    pub status: NodeStatus,
}

impl ViableNode {
    pub fn new_root(name: impl Into<String>) -> Self {
        Self {
            id: NodeId::new(),
            name: name.into(),
            parent_id: None,
            children: Vec::new(),
            system5: System5::default(),
            system4: System4::default(),
            system3: System3::default(),
            system3_star: System3Star::default(),
            system2: System2::default(),
            operation: None,
            prompt_genome: PromptGenome::default(),
            model: ModelSpec::default(),
            context_policy: ContextPolicy::default(),
            effort_budget: EffortBudget::default(),
            status: NodeStatus::Active,
        }
    }

    pub fn new_leaf(name: impl Into<String>, operation: LeafOperation) -> Self {
        Self {
            id: NodeId::new(),
            name: name.into(),
            parent_id: None,
            children: Vec::new(),
            system5: System5::default(),
            system4: System4::default(),
            system3: System3::default(),
            system3_star: System3Star::default(),
            system2: System2::default(),
            operation: Some(operation),
            prompt_genome: PromptGenome::default(),
            model: ModelSpec::default(),
            context_policy: ContextPolicy::default(),
            effort_budget: EffortBudget::default(),
            status: NodeStatus::Active,
        }
    }

    pub fn mode(&self) -> NodeMode {
        if self.children.is_empty() {
            NodeMode::Leaf
        } else {
            NodeMode::Metasystem
        }
    }

    pub fn is_leaf(&self) -> bool {
        self.children.is_empty()
    }

    pub fn is_metasystem(&self) -> bool {
        !self.children.is_empty()
    }

    pub fn capabilities(&self) -> CapabilitySet {
        match self.mode() {
            NodeMode::Metasystem => CapabilitySet {
                can_write_code: false,
                can_run_tests: false,
                can_review: false,
                can_research: false,
                can_integrate: true,
                can_decompose: true,
                can_delegate: true,
                can_audit_children: true,
                can_mutate_child_topology: true,
                can_issue_command: true,
                can_allocate_resources: true,
                can_coordinate_children: true,
                can_emit_algedonic: true,
            },
            NodeMode::Leaf => {
                let Some(op) = &self.operation else {
                    return CapabilitySet::empty_leaf();
                };
                CapabilitySet {
                    can_write_code: matches!(op, LeafOperation::Coding { .. }),
                    can_run_tests: matches!(op, LeafOperation::Testing { .. } | LeafOperation::Coding { .. }),
                    can_review: matches!(op, LeafOperation::Review { .. }),
                    can_research: matches!(op, LeafOperation::Research { .. }),
                    can_integrate: matches!(op, LeafOperation::Integration { .. }),
                    can_decompose: false,
                    can_delegate: false,
                    can_audit_children: false,
                    can_mutate_child_topology: false,
                    can_issue_command: false,
                    can_allocate_resources: false,
                    can_coordinate_children: false,
                    can_emit_algedonic: true,
                }
            }
        }
    }

    pub fn assert_can_write_code(&self) -> Result<()> {
        if self.capabilities().can_write_code {
            Ok(())
        } else {
            Err(KernelError::CodeWriteNotAllowed(self.id.clone()))
        }
    }

    pub fn validate_invariants(&self) -> Result<()> {
        if !self.children.is_empty() && self.operation.is_some() {
            return Err(KernelError::InvalidPatch(format!(
                "node {} has children and a leaf operation",
                self.id
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeMode {
    Leaf,
    Metasystem,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    Shadow,
    Probation,
    Active,
    Retired,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LeafOperation {
    Coding {
        languages: Vec<String>,
        domains: Vec<String>,
    },
    Testing {
        frameworks: Vec<String>,
    },
    Review {
        review_types: Vec<String>,
    },
    Research {
        domains: Vec<String>,
    },
    Integration {
        domains: Vec<String>,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CapabilitySet {
    pub can_write_code: bool,
    pub can_run_tests: bool,
    pub can_review: bool,
    pub can_research: bool,
    pub can_integrate: bool,
    pub can_decompose: bool,
    pub can_delegate: bool,
    pub can_audit_children: bool,
    pub can_mutate_child_topology: bool,
    pub can_issue_command: bool,
    pub can_allocate_resources: bool,
    pub can_coordinate_children: bool,
    pub can_emit_algedonic: bool,
}

impl CapabilitySet {
    pub fn empty_leaf() -> Self {
        Self {
            can_emit_algedonic: true,
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct System5 {
    pub identity: String,
    pub policy: Vec<String>,
    pub non_negotiables: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct System4 {
    pub future_probes: Vec<FutureProbe>,
    pub architectural_hypotheses: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct System3 {
    pub resource_policy: ResourcePolicy,
    pub command_policy: CommandPolicy,
    pub decomposition_policy: DecompositionPolicy,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct System3Star {
    pub audit_policy: AuditPolicy,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct System2 {
    pub coordination_policy: CoordinationPolicy,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FutureProbe {
    pub name: String,
    pub query: String,
    pub cadence_seconds: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourcePolicy {
    pub max_tokens_per_task: Option<u64>,
    pub max_parallel_children: Option<u32>,
    pub max_trial_budget_tokens: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CommandPolicy {
    pub command_channel_is_sparse: bool,
    pub non_debatable_rules: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DecompositionPolicy {
    pub max_depth: Option<u32>,
    pub prefer_leaf_delegation: bool,
    pub require_acceptance_predicate: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AuditPolicy {
    pub cadence_seconds: Option<u64>,
    pub min_tasks_before_audit: Option<u32>,
    pub audit_children_for_gene_suggestions: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CoordinationPolicy {
    pub damp_oscillation: bool,
    pub sibling_conflict_strategy: SiblingConflictStrategy,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub enum SiblingConflictStrategy {
    #[default]
    ParentArbitrates,
    FirstWriterWins,
    RequireInterfaceContract,
    TournamentSelection,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PromptGenome {
    pub base_identity: String,
    pub behavior_rules: Vec<PromptComponent>,
    pub domain_hints: Vec<PromptComponent>,
    pub codebase_conventions: Vec<PromptComponent>,
    pub negative_constraints: Vec<PromptComponent>,
    pub output_contract: Option<PromptComponent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptComponent {
    pub id: String,
    pub text: String,
    pub tags: Vec<String>,
    pub origin: PromptComponentOrigin,
    pub active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PromptComponentOrigin {
    Manual,
    Mutation,
    SummarizedMemory,
    TaskCluster,
    AuditFinding,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelSpec {
    pub provider: String,
    pub model: String,
}

impl Default for ModelSpec {
    fn default() -> Self {
        Self {
            provider: "unassigned".to_string(),
            model: "unassigned".to_string(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ContextPolicy {
    pub max_context_tokens: Option<u64>,
    pub rag_sources: Vec<String>,
    pub fixed_context_budget_tokens: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EffortBudget {
    pub max_reasoning_steps: Option<u32>,
    pub max_wall_time_seconds: Option<u64>,
    pub max_tool_calls: Option<u32>,
}

pub type NodeMap = BTreeMap<NodeId, ViableNode>;
