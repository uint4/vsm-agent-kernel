use crate::{
    ChannelPriority, Directive, MessageId, NodeId, OrganizationalGenomePatch, RiskClass,
    SuggestionId, TaskArtifact, TaskId, TaskPacket, TaskResult, VsmChannelType,
};
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageEnvelope {
    pub id: MessageId,
    pub channel_type: VsmChannelType,
    pub source_node_id: Option<NodeId>,
    pub target_node_id: Option<NodeId>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<MessageId>,
    pub priority: ChannelPriority,
    pub created_at: DateTime<Utc>,
    pub payload_type: String,
    pub payload: Value,
    pub trace: Vec<NodeId>,
    pub metadata: BTreeMap<String, String>,
}

impl MessageEnvelope {
    pub fn new<T: Serialize>(
        channel_type: VsmChannelType,
        payload_type: impl Into<String>,
        payload: &T,
    ) -> Result<Self, serde_json::Error> {
        Ok(Self {
            id: MessageId::new(),
            channel_type,
            source_node_id: None,
            target_node_id: None,
            correlation_id: None,
            causation_id: None,
            priority: ChannelPriority::Normal,
            created_at: Utc::now(),
            payload_type: payload_type.into(),
            payload: serde_json::to_value(payload)?,
            trace: vec![],
            metadata: BTreeMap::new(),
        })
    }

    pub fn with_route(mut self, source: Option<NodeId>, target: Option<NodeId>) -> Self {
        self.source_node_id = source;
        self.target_node_id = target;
        self
    }

    pub fn with_priority(mut self, priority: ChannelPriority) -> Self {
        self.priority = priority;
        self
    }

    pub fn payload_as<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_value(self.payload.clone())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlgedonicValence {
    Pain,
    Pleasure,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlgedonicSource {
    User,
    Ci,
    Production,
    SecurityScanner,
    CostMonitor,
    ChildNode,
    Reviewer,
    ExternalEnvironment,
    Other(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AlgedonicOverridePolicy {
    pub pause_subtree: bool,
    #[serde(default)]
    pub resume_subtree: bool,
    pub escalate_to_root: bool,
    pub freeze_mutation: bool,
    pub require_human_confirmation: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlgedonicSignal {
    pub source: AlgedonicSource,
    pub valence: AlgedonicValence,
    pub severity: u8,
    pub target_node_id: Option<NodeId>,
    pub related_task_id: Option<TaskId>,
    pub related_suggestion_id: Option<SuggestionId>,
    pub message: String,
    pub override_policy: Option<AlgedonicOverridePolicy>,
    pub created_at: DateTime<Utc>,
}

impl AlgedonicSignal {
    pub fn pain(source: AlgedonicSource, severity: u8, message: impl Into<String>) -> Self {
        Self {
            source,
            valence: AlgedonicValence::Pain,
            severity,
            target_node_id: None,
            related_task_id: None,
            related_suggestion_id: None,
            message: message.into(),
            override_policy: None,
            created_at: Utc::now(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceBargain {
    pub requested_by: NodeId,
    pub task_id: Option<TaskId>,
    #[serde(default)]
    pub proposed_task: Option<TaskPacket>,
    pub requested_tokens: Option<u64>,
    pub requested_tool_permissions: Vec<String>,
    pub requested_context_refs: Vec<String>,
    pub justification: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceAllocationStatus {
    Approved,
    PartiallyApproved,
    Denied,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceAllocationDecision {
    pub requested_by: NodeId,
    pub task_id: Option<TaskId>,
    pub status: ResourceAllocationStatus,
    pub approved_tokens: Option<u64>,
    pub approved_tool_permissions: Vec<String>,
    pub denied_tool_permissions: Vec<String>,
    pub approved_context_refs: Vec<String>,
    pub denied_context_refs: Vec<String>,
    pub reasons: Vec<String>,
    pub allocation_policy: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum System2CoordinationKind {
    DependencyBlocked,
    DependencyReady,
    Contention,
    Oscillation,
    HandoffNotice,
    Other(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct System2CoordinationSignal {
    pub coordinator_node_id: NodeId,
    pub source_node_id: Option<NodeId>,
    pub target_node_id: Option<NodeId>,
    pub affected_node_ids: Vec<NodeId>,
    pub affected_task_ids: Vec<TaskId>,
    pub kind: System2CoordinationKind,
    pub summary: String,
    pub evidence: Vec<String>,
    pub severity: Option<u8>,
    pub metadata: BTreeMap<String, String>,
    pub created_at: DateTime<Utc>,
}

impl System2CoordinationSignal {
    pub fn new(
        coordinator_node_id: NodeId,
        kind: System2CoordinationKind,
        summary: impl Into<String>,
    ) -> Self {
        Self {
            coordinator_node_id,
            source_node_id: None,
            target_node_id: None,
            affected_node_ids: vec![],
            affected_task_ids: vec![],
            kind,
            summary: summary.into(),
            evidence: vec![],
            severity: None,
            metadata: BTreeMap::new(),
            created_at: Utc::now(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationHandoffKind {
    WorkInProgress,
    DependencyReady,
    DependencyBlocked,
    ArtifactTransfer,
    ReviewRequest,
    Other(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationHandoff {
    pub source_node_id: NodeId,
    pub target_node_id: NodeId,
    pub related_task_id: Option<TaskId>,
    pub dependency_task_ids: Vec<TaskId>,
    pub kind: OperationHandoffKind,
    pub title: String,
    pub summary: String,
    pub artifacts: Vec<TaskArtifact>,
    pub evidence: Vec<String>,
    pub metadata: BTreeMap<String, String>,
    pub created_at: DateTime<Utc>,
}

impl OperationHandoff {
    pub fn new(
        source_node_id: NodeId,
        target_node_id: NodeId,
        kind: OperationHandoffKind,
        title: impl Into<String>,
        summary: impl Into<String>,
    ) -> Self {
        Self {
            source_node_id,
            target_node_id,
            related_task_id: None,
            dependency_task_ids: vec![],
            kind,
            title: title.into(),
            summary: summary.into(),
            artifacts: vec![],
            evidence: vec![],
            metadata: BTreeMap::new(),
            created_at: Utc::now(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManagementOperationKind {
    AssignWork,
    Reprioritize,
    IntegrateResult,
    RequestStatus,
    LocalPolicy,
    Other(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagementOperationDirective {
    pub manager_node_id: NodeId,
    pub operation_node_id: NodeId,
    pub related_task_id: Option<TaskId>,
    pub dependency_task_ids: Vec<TaskId>,
    pub kind: ManagementOperationKind,
    pub title: String,
    pub body: String,
    pub target_state: Option<String>,
    pub constraints: Vec<String>,
    pub context_refs: Vec<String>,
    pub authority_refs: Vec<String>,
    pub risk: RiskClass,
    pub metadata: BTreeMap<String, String>,
    pub created_at: DateTime<Utc>,
}

impl ManagementOperationDirective {
    pub fn new(
        manager_node_id: NodeId,
        operation_node_id: NodeId,
        kind: ManagementOperationKind,
        title: impl Into<String>,
        body: impl Into<String>,
    ) -> Self {
        Self {
            manager_node_id,
            operation_node_id,
            related_task_id: None,
            dependency_task_ids: vec![],
            kind,
            title: title.into(),
            body: body.into(),
            target_state: None,
            constraints: vec![],
            context_refs: vec![],
            authority_refs: vec![],
            risk: RiskClass::Medium,
            metadata: BTreeMap::new(),
            created_at: Utc::now(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EnvironmentSignalKind {
    Observation,
    UserFeedback,
    Artifact,
    DependencyChange,
    CapabilityChange,
    Risk,
    Opportunity,
    Other(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentSignal {
    pub observed_by_node_id: Option<NodeId>,
    pub target_node_id: Option<NodeId>,
    pub related_task_id: Option<TaskId>,
    pub related_suggestion_id: Option<SuggestionId>,
    pub source_environment: String,
    pub target_environment: Option<String>,
    pub kind: EnvironmentSignalKind,
    pub summary: String,
    pub evidence: Vec<String>,
    pub severity: Option<u8>,
    pub metadata: BTreeMap<String, String>,
    pub created_at: DateTime<Utc>,
}

impl EnvironmentSignal {
    pub fn new(
        kind: EnvironmentSignalKind,
        source_environment: impl Into<String>,
        summary: impl Into<String>,
    ) -> Self {
        Self {
            observed_by_node_id: None,
            target_node_id: None,
            related_task_id: None,
            related_suggestion_id: None,
            source_environment: source_environment.into(),
            target_environment: None,
            kind,
            summary: summary.into(),
            evidence: vec![],
            severity: None,
            metadata: BTreeMap::new(),
            created_at: Utc::now(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ThreeFourHomeostatKind {
    PresentConstraint,
    FutureRisk,
    FutureOpportunity,
    CapabilityGap,
    ResourceImbalance,
    CoordinationDebt,
    Other(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ThreeFourHomeostatBalance {
    PresentDominant,
    FutureDominant,
    Balanced,
    Conflict,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreeFourHomeostatSignal {
    pub system_3_node_id: NodeId,
    pub system_4_node_id: NodeId,
    pub target_node_id: NodeId,
    pub related_task_id: Option<TaskId>,
    pub related_suggestion_id: Option<SuggestionId>,
    pub kind: ThreeFourHomeostatKind,
    pub balance: ThreeFourHomeostatBalance,
    pub present_summary: String,
    pub future_summary: String,
    pub recommendation: String,
    pub evidence: Vec<String>,
    pub suggested_patches: Vec<OrganizationalGenomePatch>,
    pub severity: Option<u8>,
    pub metadata: BTreeMap<String, String>,
    pub created_at: DateTime<Utc>,
}

impl ThreeFourHomeostatSignal {
    pub fn new(
        system_3_node_id: NodeId,
        system_4_node_id: NodeId,
        target_node_id: NodeId,
        kind: ThreeFourHomeostatKind,
        balance: ThreeFourHomeostatBalance,
        recommendation: impl Into<String>,
    ) -> Self {
        Self {
            system_3_node_id,
            system_4_node_id,
            target_node_id,
            related_task_id: None,
            related_suggestion_id: None,
            kind,
            balance,
            present_summary: String::new(),
            future_summary: String::new(),
            recommendation: recommendation.into(),
            evidence: vec![],
            suggested_patches: vec![],
            severity: None,
            metadata: BTreeMap::new(),
            created_at: Utc::now(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Command {
    pub issued_by: NodeId,
    pub target: NodeId,
    pub title: String,
    pub body: String,
    pub non_negotiable: bool,
    pub legal_or_policy_basis: Option<String>,
    #[serde(default)]
    pub system5_identity: Option<String>,
    #[serde(default)]
    pub policy_values: Vec<String>,
    #[serde(default)]
    pub non_negotiable_constraints: Vec<String>,
    #[serde(default)]
    pub denied_capabilities: Vec<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditRequest {
    pub requested_by: NodeId,
    pub target_node_id: NodeId,
    pub window_tasks: Option<u64>,
    pub include_gene_suggestions: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditFinding {
    pub title: String,
    pub evidence: Vec<String>,
    pub severity: u8,
    pub related_nodes: Vec<NodeId>,
    pub related_tasks: Vec<TaskId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditReport {
    pub target_node_id: NodeId,
    pub findings: Vec<AuditFinding>,
    pub suggested_patches: Vec<OrganizationalGenomePatch>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuiltinPayloadType {
    Directive,
    TaskPacket,
    TaskResult,
    ResourceBargain,
    ResourceAllocationDecision,
    System2CoordinationSignal,
    OperationHandoff,
    ManagementOperationDirective,
    EnvironmentSignal,
    Command,
    AuditRequest,
    AuditReport,
    AlgedonicSignal,
    ThreeFourHomeostatSignal,
    GeneSuggestion,
}

impl BuiltinPayloadType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Directive => "vsm.directive",
            Self::TaskPacket => "vsm.task_packet",
            Self::TaskResult => "vsm.task_result",
            Self::ResourceBargain => "vsm.resource_bargain",
            Self::ResourceAllocationDecision => "vsm.resource_allocation_decision",
            Self::System2CoordinationSignal => "vsm.system2_coordination_signal",
            Self::OperationHandoff => "vsm.operation_handoff",
            Self::ManagementOperationDirective => "vsm.management_operation_directive",
            Self::EnvironmentSignal => "vsm.environment_signal",
            Self::Command => "vsm.command",
            Self::AuditRequest => "vsm.audit_request",
            Self::AuditReport => "vsm.audit_report",
            Self::AlgedonicSignal => "vsm.algedonic_signal",
            Self::ThreeFourHomeostatSignal => "vsm.three_four_homeostat_signal",
            Self::GeneSuggestion => "vsm.gene_suggestion",
        }
    }
}

pub fn envelope_for_directive(directive: &Directive) -> Result<MessageEnvelope, serde_json::Error> {
    MessageEnvelope::new(
        VsmChannelType::OperationToEnvironment,
        BuiltinPayloadType::Directive.as_str(),
        directive,
    )
}

pub fn envelope_for_task(task: &TaskPacket) -> Result<MessageEnvelope, serde_json::Error> {
    MessageEnvelope::new(
        VsmChannelType::ResourceBargaining,
        BuiltinPayloadType::TaskPacket.as_str(),
        task,
    )
}

pub fn envelope_for_task_result(result: &TaskResult) -> Result<MessageEnvelope, serde_json::Error> {
    MessageEnvelope::new(
        VsmChannelType::ManagementToOperation,
        BuiltinPayloadType::TaskResult.as_str(),
        result,
    )
}
