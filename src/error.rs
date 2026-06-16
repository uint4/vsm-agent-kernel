use crate::ids::NodeId;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum KernelError {
    #[error("node {0} was not found")]
    NodeNotFound(NodeId),

    #[error("genome root {0} was not found")]
    RootNotFound(NodeId),

    #[error("node {0} is a metasystem and cannot perform leaf operation `{1}`")]
    MetasystemCannotOperate(NodeId, String),

    #[error("node {0} is a leaf and cannot perform metasystem operation `{1}`")]
    LeafCannotGovern(NodeId, String),

    #[error("node {0} cannot write code")]
    CodeWriteNotAllowed(NodeId),

    #[error("cannot add child to operational leaf {0}; promote it to a metasystem first")]
    AddChildToOperationalLeaf(NodeId),

    #[error("cannot promote node {0}; it is already a metasystem")]
    AlreadyMetasystem(NodeId),

    #[error("cannot collapse node {0}; it is already a leaf")]
    AlreadyLeaf(NodeId),

    #[error("invalid genome patch: {0}")]
    InvalidPatch(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("transport error: {0}")]
    Transport(String),
}

pub type Result<T> = std::result::Result<T, KernelError>;
