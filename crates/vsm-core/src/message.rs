use crate::{
    ChannelPriority, Directive, MessageId, NodeId, OrganizationalGenomePatch, SuggestionId, TaskId,
    TaskPacket, TaskResult, VsmChannelType,
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
    pub requested_tokens: Option<u64>,
    pub requested_tool_permissions: Vec<String>,
    pub requested_context_refs: Vec<String>,
    pub justification: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Command {
    pub issued_by: NodeId,
    pub target: NodeId,
    pub title: String,
    pub body: String,
    pub non_negotiable: bool,
    pub legal_or_policy_basis: Option<String>,
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
    Command,
    AuditRequest,
    AuditReport,
    AlgedonicSignal,
    GeneSuggestion,
}

impl BuiltinPayloadType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Directive => "vsm.directive",
            Self::TaskPacket => "vsm.task_packet",
            Self::TaskResult => "vsm.task_result",
            Self::ResourceBargain => "vsm.resource_bargain",
            Self::Command => "vsm.command",
            Self::AuditRequest => "vsm.audit_request",
            Self::AuditReport => "vsm.audit_report",
            Self::AlgedonicSignal => "vsm.algedonic_signal",
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
