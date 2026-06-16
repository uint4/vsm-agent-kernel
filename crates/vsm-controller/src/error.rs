use thiserror::Error;
use vsm_core::{GenomeError, NodeId, PatchError, SuggestionId, TransportError};
use vsm_ledger::LedgerError;

#[derive(Debug, Error)]
pub enum ControllerError {
    #[error(transparent)]
    Transport(#[from] TransportError),

    #[error(transparent)]
    Ledger(#[from] LedgerError),

    #[error(transparent)]
    Serialization(#[from] serde_json::Error),

    #[error(transparent)]
    Genome(#[from] GenomeError),

    #[error(transparent)]
    Patch(#[from] PatchError),

    #[error("controller node is not a metasystem: {0}")]
    NotMetasystem(NodeId),

    #[error("no routeable child found for node {node_id} and task {task_title}")]
    NoRouteableChild { node_id: NodeId, task_title: String },

    #[error("message payload type is unsupported by controller: {0}")]
    UnsupportedPayload(String),

    #[error("a mutation trial is already active: {0}")]
    TrialAlreadyActive(SuggestionId),

    #[error("no active mutation trial")]
    NoActiveTrial,
}
