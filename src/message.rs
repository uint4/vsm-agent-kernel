use crate::channel::ChannelType;
use crate::ids::{DirectiveId, MessageId, NodeId, TaskId};
use crate::task::{Directive, TaskPacket};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub id: MessageId,
    pub correlation_id: Option<String>,
    pub channel: ChannelType,
    pub source_node_id: Option<NodeId>,
    pub target_node_id: Option<NodeId>,
    pub priority: MessagePriority,
    pub created_at: DateTime<Utc>,
    pub payload: MessagePayload,
}

impl Envelope {
    pub fn new(channel: ChannelType, payload: MessagePayload) -> Self {
        Self {
            id: MessageId::new(),
            correlation_id: None,
            channel,
            source_node_id: None,
            target_node_id: None,
            priority: MessagePriority::Normal,
            created_at: Utc::now(),
            payload,
        }
    }

    pub fn addressed_to(mut self, node_id: NodeId) -> Self {
        self.target_node_id = Some(node_id);
        self
    }

    pub fn from(mut self, node_id: NodeId) -> Self {
        self.source_node_id = Some(node_id);
        self
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub enum MessagePriority {
    Low,
    #[default]
    Normal,
    High,
    Interrupt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessagePayload {
    Directive(Directive),
    TaskPacket(TaskPacket),
    TaskResult(TaskResultMessage),
    ResourceBargain(ResourceBargainMessage),
    Command(CommandMessage),
    AuditRequest(AuditRequestMessage),
    AuditReport(AuditReportMessage),
    Algedonic(AlgedonicSignal),
    Raw(Value),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResultMessage {
    pub task_id: TaskId,
    pub success: bool,
    pub summary: String,
    pub artifacts: Vec<String>,
    pub metrics: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceBargainMessage {
    pub task_id: Option<TaskId>,
    pub request: String,
    pub requested_tokens: Option<u64>,
    pub requested_tools: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandMessage {
    pub directive_id: Option<DirectiveId>,
    pub command: String,
    pub non_debatable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditRequestMessage {
    pub target_node_id: NodeId,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditReportMessage {
    pub target_node_id: NodeId,
    pub summary: String,
    pub findings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlgedonicSignal {
    pub source: AlgedonicSource,
    pub valence: AlgedonicValence,
    pub severity: u8,
    pub target_node_id: Option<NodeId>,
    pub related_task_id: Option<TaskId>,
    pub message: String,
    pub override_policy: Option<AlgedonicOverridePolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlgedonicSource {
    User,
    Ci,
    Production,
    SecurityScanner,
    CostMonitor,
    ChildNode,
    Reviewer,
    ExternalEnvironment,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlgedonicValence {
    Pain,
    Pleasure,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AlgedonicOverridePolicy {
    pub pause_subtree: bool,
    pub escalate_to_root: bool,
    pub freeze_mutation: bool,
    pub require_human_confirmation: bool,
}
