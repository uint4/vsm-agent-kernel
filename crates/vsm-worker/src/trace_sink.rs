use crate::TraceSinkError;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use vsm_core::TaskTrace;
use vsm_ledger::{Ledger, LedgerEvent};

#[async_trait]
pub trait TraceSink: Send + Sync {
    async fn record(&self, trace: TaskTrace) -> Result<(), TraceSinkError>;
}

#[derive(Clone, Default)]
pub struct InMemoryTraceSink {
    inner: Arc<Mutex<Vec<TaskTrace>>>,
}

impl InMemoryTraceSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn traces(&self) -> Vec<TaskTrace> {
        self.inner.lock().await.clone()
    }
}

#[async_trait]
impl TraceSink for InMemoryTraceSink {
    async fn record(&self, trace: TaskTrace) -> Result<(), TraceSinkError> {
        self.inner.lock().await.push(trace);
        Ok(())
    }
}

/// Adapter from the worker harness's narrow TraceSink trait into the durable
/// system ledger. This keeps the worker independent of controller internals
/// while still making leaf execution observable for System 3* audit and later
/// genetic selection.
#[derive(Clone)]
pub struct LedgerTraceSink {
    ledger: Arc<dyn Ledger>,
    append_trace_event: bool,
}

impl LedgerTraceSink {
    pub fn new(ledger: Arc<dyn Ledger>) -> Self {
        Self {
            ledger,
            append_trace_event: true,
        }
    }

    pub fn without_trace_event(mut self) -> Self {
        self.append_trace_event = false;
        self
    }
}

#[async_trait]
impl TraceSink for LedgerTraceSink {
    async fn record(&self, trace: TaskTrace) -> Result<(), TraceSinkError> {
        self.ledger
            .write_task_trace(trace.clone())
            .await
            .map_err(|err| TraceSinkError::Operation(err.to_string()))?;

        if self.append_trace_event {
            let event = LedgerEvent::for_trace(&trace)
                .map_err(|err| TraceSinkError::Operation(err.to_string()))?;
            self.ledger
                .append_event(event)
                .await
                .map_err(|err| TraceSinkError::Operation(err.to_string()))?;
        }

        Ok(())
    }
}
