use crate::TraceSinkError;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use vsm_core::TaskTrace;

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
