use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use uuid::Uuid;
use vsm_core::{DirectiveId, GenomeId, MessageEnvelope, NodeId, SuggestionId, TaskId, TaskTrace};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LedgerEventKind {
    ControllerStarted,
    WorkerStarted,
    MessageReceived,
    MessagePublished,
    DirectiveAccepted,
    TaskMapped,
    TaskRouted,
    TaskResultReceived,
    TraceWritten,
    AlgedonicSignalReceived,
    AuditStarted,
    AuditCompleted,
    GeneSuggestionCreated,
    GenomePatchApplied,
    TrialQueued,
    TrialStarted,
    TrialTaskRouted,
    TrialTraceRecorded,
    TrialDecisionRecorded,
    TrialPromoted,
    TrialPruned,
    TrialRejected,
    Other(String),
}

impl LedgerEventKind {
    pub fn as_storage_key(&self) -> String {
        match self {
            Self::ControllerStarted => "controller_started".to_string(),
            Self::WorkerStarted => "worker_started".to_string(),
            Self::MessageReceived => "message_received".to_string(),
            Self::MessagePublished => "message_published".to_string(),
            Self::DirectiveAccepted => "directive_accepted".to_string(),
            Self::TaskMapped => "task_mapped".to_string(),
            Self::TaskRouted => "task_routed".to_string(),
            Self::TaskResultReceived => "task_result_received".to_string(),
            Self::TraceWritten => "trace_written".to_string(),
            Self::AlgedonicSignalReceived => "algedonic_signal_received".to_string(),
            Self::AuditStarted => "audit_started".to_string(),
            Self::AuditCompleted => "audit_completed".to_string(),
            Self::GeneSuggestionCreated => "gene_suggestion_created".to_string(),
            Self::GenomePatchApplied => "genome_patch_applied".to_string(),
            Self::TrialQueued => "trial_queued".to_string(),
            Self::TrialStarted => "trial_started".to_string(),
            Self::TrialTaskRouted => "trial_task_routed".to_string(),
            Self::TrialTraceRecorded => "trial_trace_recorded".to_string(),
            Self::TrialDecisionRecorded => "trial_decision_recorded".to_string(),
            Self::TrialPromoted => "trial_promoted".to_string(),
            Self::TrialPruned => "trial_pruned".to_string(),
            Self::TrialRejected => "trial_rejected".to_string(),
            Self::Other(value) => format!("other:{value}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LedgerEvent {
    pub id: String,
    pub kind: LedgerEventKind,
    pub created_at: DateTime<Utc>,
    pub genome_id: Option<GenomeId>,
    pub node_id: Option<NodeId>,
    pub task_id: Option<TaskId>,
    pub directive_id: Option<DirectiveId>,
    pub suggestion_id: Option<SuggestionId>,
    pub correlation_id: Option<String>,
    pub payload: Value,
    pub metadata: BTreeMap<String, String>,
}

impl LedgerEvent {
    pub fn new(kind: LedgerEventKind, payload: impl Serialize) -> Result<Self, serde_json::Error> {
        Ok(Self {
            id: Uuid::new_v4().to_string(),
            kind,
            created_at: Utc::now(),
            genome_id: None,
            node_id: None,
            task_id: None,
            directive_id: None,
            suggestion_id: None,
            correlation_id: None,
            payload: serde_json::to_value(payload)?,
            metadata: BTreeMap::new(),
        })
    }

    pub fn for_message(
        kind: LedgerEventKind,
        envelope: &MessageEnvelope,
    ) -> Result<Self, serde_json::Error> {
        let mut event = Self::new(kind, envelope)?;
        event.node_id = envelope
            .target_node_id
            .clone()
            .or_else(|| envelope.source_node_id.clone());
        event.correlation_id = envelope.correlation_id.clone();
        event
            .metadata
            .insert("payload_type".to_string(), envelope.payload_type.clone());
        event.metadata.insert(
            "channel_type".to_string(),
            format!("{:?}", envelope.channel_type),
        );
        Ok(event)
    }

    pub fn for_trace(trace: &TaskTrace) -> Result<Self, serde_json::Error> {
        let mut event = Self::new(LedgerEventKind::TraceWritten, trace)?;
        event.genome_id = Some(trace.genome_id.clone());
        event.node_id = Some(trace.assigned_node_id.clone());
        event.task_id = Some(trace.task_id.clone());
        event
            .metadata
            .insert("trace_id".to_string(), trace.id.to_string());
        Ok(event)
    }

    pub fn with_node(mut self, node_id: NodeId) -> Self {
        self.node_id = Some(node_id);
        self
    }

    pub fn with_genome(mut self, genome_id: GenomeId) -> Self {
        self.genome_id = Some(genome_id);
        self
    }

    pub fn with_task(mut self, task_id: TaskId) -> Self {
        self.task_id = Some(task_id);
        self
    }

    pub fn with_directive(mut self, directive_id: DirectiveId) -> Self {
        self.directive_id = Some(directive_id);
        self
    }

    pub fn with_correlation(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = Some(correlation_id.into());
        self
    }

    pub fn with_suggestion(mut self, suggestion_id: SuggestionId) -> Self {
        self.suggestion_id = Some(suggestion_id);
        self
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventFilter {
    pub kinds: Vec<LedgerEventKind>,
    pub node_id: Option<NodeId>,
    pub task_id: Option<TaskId>,
    pub directive_id: Option<DirectiveId>,
    pub correlation_id: Option<String>,
    pub since: Option<DateTime<Utc>>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceWindow {
    pub since: Option<DateTime<Utc>>,
    pub limit: Option<usize>,
}

impl Default for TraceWindow {
    fn default() -> Self {
        Self {
            since: None,
            limit: Some(100),
        }
    }
}
