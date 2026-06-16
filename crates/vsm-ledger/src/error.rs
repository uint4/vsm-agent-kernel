use thiserror::Error;

#[derive(Debug, Error)]
pub enum LedgerError {
    #[error("ledger storage error: {0}")]
    Storage(String),

    #[error("ledger serialization error: {0}")]
    Serialization(String),

    #[error("ledger event not found: {0}")]
    EventNotFound(String),

    #[error("ledger trace not found: {0}")]
    TraceNotFound(String),
}

impl From<serde_json::Error> for LedgerError {
    fn from(value: serde_json::Error) -> Self {
        Self::Serialization(value.to_string())
    }
}

impl From<rusqlite::Error> for LedgerError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Storage(value.to_string())
    }
}
