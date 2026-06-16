use thiserror::Error;
use vsm_core::{GenomeError, NodeId, TransportError};

#[derive(Debug, Error)]
pub enum ModelProviderError {
    #[error("missing configuration: {0}")]
    MissingConfig(String),

    #[error("provider request failed: {0}")]
    Request(String),

    #[error("provider response was invalid: {0}")]
    InvalidResponse(String),
}

#[derive(Debug, Error)]
pub enum TraceSinkError {
    #[error("trace sink failed: {0}")]
    Operation(String),
}

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error(transparent)]
    Transport(#[from] TransportError),

    #[error(transparent)]
    Model(#[from] ModelProviderError),

    #[error(transparent)]
    TraceSink(#[from] TraceSinkError),

    #[error(transparent)]
    Serialization(#[from] serde_json::Error),

    #[error(transparent)]
    Genome(#[from] GenomeError),

    #[error("node is not executable as a leaf: {0}")]
    NotExecutableLeaf(NodeId),

    #[error("node lacks required capability: {node_id}: {capability}")]
    CapabilityDenied { node_id: NodeId, capability: String },
}
