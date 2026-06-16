use crate::ids::{ChannelId, NodeId, RelationId};
use crate::task::TaskPredicate;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ChannelType {
    ResourceBargaining,
    Command,
    System2Coordination,
    System3StarAudit,
    System3FourHomeostat,
    ManagementToOperation,
    OperationToOperation,
    OperationToEnvironment,
    FutureProbeToEnvironment,
    EnvironmentToEnvironment,
    Algedonic,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelGene {
    pub id: ChannelId,
    pub channel_type: ChannelType,
    pub from_node_id: NodeId,
    pub to_node_id: NodeId,
    pub config: ChannelConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChannelConfig {
    pub enabled: bool,
    pub priority: ChannelPriority,
    pub max_tokens_per_message: Option<u64>,
    pub max_messages_per_task: Option<u32>,
    pub requires_parent_visibility: bool,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ChannelPriority {
    Low,
    #[default]
    Normal,
    High,
    Interrupt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationGene {
    pub id: RelationId,
    pub from_node_id: NodeId,
    pub to_node_id: NodeId,
    pub relation: RelationType,
    pub activation_predicate: Option<TaskPredicate>,
    pub traffic_share: Option<f32>,
    pub required: bool,
    pub max_budget_tokens: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RelationType {
    RequestsReview,
    RequestsTests,
    RequestsResearch,
    HandsOffWork,
    CoordinatesWith,
    ReportsAlgedonicSignal,
    RequestsResourceBargain,
    ReceivesCommand,
    RequiresApprovalBeforeMerge,
    RequiresInterfaceContract,
}

impl ChannelGene {
    pub fn new(
        channel_type: ChannelType,
        from_node_id: NodeId,
        to_node_id: NodeId,
        config: ChannelConfig,
    ) -> Self {
        Self {
            id: ChannelId::new(),
            channel_type,
            from_node_id,
            to_node_id,
            config,
        }
    }
}

impl RelationGene {
    pub fn new(from_node_id: NodeId, to_node_id: NodeId, relation: RelationType) -> Self {
        Self {
            id: RelationId::new(),
            from_node_id,
            to_node_id,
            relation,
            activation_predicate: None,
            traffic_share: None,
            required: false,
            max_budget_tokens: None,
        }
    }
}
