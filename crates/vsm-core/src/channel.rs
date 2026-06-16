use crate::{ChannelId, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum VsmChannelType {
    /// System 3 <-> System 1: continuous negotiation about resources, requests,
    /// constraints, and responses. This should carry most direct communication.
    ResourceBargaining,

    /// System 3 -> System 1: non-negotiable requirements and decisions. Use
    /// sparingly except for legal, regulatory, or hard policy constraints.
    Command,

    /// System 3 -> System 2 -> System 1: damp oscillations, conflicts,
    /// duplicated work, and scheduling friction among primary activities.
    System2Coordination,

    /// System 3* -> System 1: intermittent operational reality checks and
    /// audits. This is also a natural source of gene suggestions.
    Audit,

    /// System 3 <-> System 4: homeostat balancing present operations and future
    /// adaptation.
    ThreeFourHomeostat,

    /// System 1 management -> System 1 operation.
    ManagementToOperation,

    /// System 1 operation <-> System 1 operation. May carry work-in-progress,
    /// artifacts, handoffs, supplies, or peer-level information.
    OperationToOperation,

    /// System 1 operation <-> its external environment: repository, CI, package
    /// ecosystem, user feedback, deployed service, or local runtime.
    OperationToEnvironment,

    /// System 4 future probe <-> external environment.
    FutureProbeToEnvironment,

    /// Environment segment <-> environment segment when their interaction
    /// impinges on the viable system.
    EnvironmentToEnvironment,

    /// Bottom-to-top interrupt channel for pain/pleasure signals that can
    /// override normal hierarchy.
    Algedonic,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChannelPriority {
    Low,
    Normal,
    High,
    Critical,
    Interrupt,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChannelDirection {
    ParentToChild,
    ChildToParent,
    SiblingToSibling,
    NodeToEnvironment,
    EnvironmentToNode,
    Bidirectional,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelConfig {
    pub id: ChannelId,
    pub channel_type: VsmChannelType,
    pub from: Option<NodeId>,
    pub to: Option<NodeId>,
    pub direction: ChannelDirection,
    pub enabled: bool,
    pub required: bool,
    pub priority: ChannelPriority,
    pub max_messages_per_epoch: Option<u64>,
    pub max_token_budget_per_epoch: Option<u64>,
    pub activation_predicate: Option<String>,
    pub metadata: BTreeMap<String, String>,
}

impl ChannelConfig {
    pub fn new(channel_type: VsmChannelType, direction: ChannelDirection) -> Self {
        Self {
            id: ChannelId::new(),
            channel_type,
            from: None,
            to: None,
            direction,
            enabled: true,
            required: false,
            priority: ChannelPriority::Normal,
            max_messages_per_epoch: None,
            max_token_budget_per_epoch: None,
            activation_predicate: None,
            metadata: BTreeMap::new(),
        }
    }

    pub fn between(mut self, from: impl Into<NodeId>, to: impl Into<NodeId>) -> Self {
        self.from = Some(from.into());
        self.to = Some(to.into());
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ParentChildChannelBundle {
    pub resource_bargaining: Option<ChannelConfig>,
    pub command: Option<ChannelConfig>,
    pub coordination_via_system2: Option<ChannelConfig>,
    pub audit_via_system3_star: Option<ChannelConfig>,
    pub algedonic: Option<ChannelConfig>,
}

impl ParentChildChannelBundle {
    pub fn standard(parent: NodeId, child: NodeId) -> Self {
        Self {
            resource_bargaining: Some(
                ChannelConfig::new(VsmChannelType::ResourceBargaining, ChannelDirection::Bidirectional)
                    .between(parent.clone(), child.clone()),
            ),
            command: Some(
                ChannelConfig::new(VsmChannelType::Command, ChannelDirection::ParentToChild)
                    .between(parent.clone(), child.clone()),
            ),
            coordination_via_system2: Some(
                ChannelConfig::new(VsmChannelType::System2Coordination, ChannelDirection::ParentToChild)
                    .between(parent.clone(), child.clone()),
            ),
            audit_via_system3_star: Some(
                ChannelConfig::new(VsmChannelType::Audit, ChannelDirection::Bidirectional)
                    .between(parent.clone(), child.clone()),
            ),
            algedonic: Some(
                ChannelConfig::new(VsmChannelType::Algedonic, ChannelDirection::ChildToParent)
                    .between(child, parent),
            ),
        }
    }
}
