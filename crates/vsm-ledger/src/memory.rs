use crate::{EventFilter, Ledger, LedgerError, LedgerEvent, TraceWindow};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use vsm_core::{TaskTrace, TraceId};

#[derive(Clone, Default)]
pub struct InMemoryLedger {
    events: Arc<Mutex<Vec<LedgerEvent>>>,
    traces: Arc<Mutex<Vec<TaskTrace>>>,
}

impl InMemoryLedger {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Ledger for InMemoryLedger {
    async fn append_event(&self, event: LedgerEvent) -> Result<(), LedgerError> {
        self.events.lock().await.push(event);
        Ok(())
    }

    async fn write_task_trace(&self, trace: TaskTrace) -> Result<(), LedgerError> {
        self.traces.lock().await.push(trace);
        Ok(())
    }

    async fn get_trace(&self, trace_id: &TraceId) -> Result<Option<TaskTrace>, LedgerError> {
        Ok(self
            .traces
            .lock()
            .await
            .iter()
            .find(|trace| &trace.id == trace_id)
            .cloned())
    }

    async fn recent_events(&self, filter: EventFilter) -> Result<Vec<LedgerEvent>, LedgerError> {
        let mut events: Vec<LedgerEvent> = self
            .events
            .lock()
            .await
            .iter()
            .filter(|event| filter_event(event, &filter))
            .cloned()
            .collect();
        events.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        if let Some(limit) = filter.limit {
            let start = events.len().saturating_sub(limit);
            events = events.split_off(start);
        }
        Ok(events)
    }

    async fn recent_task_traces(&self, window: TraceWindow) -> Result<Vec<TaskTrace>, LedgerError> {
        let mut traces: Vec<TaskTrace> = self
            .traces
            .lock()
            .await
            .iter()
            .filter(|trace| {
                window
                    .since
                    .as_ref()
                    .map(|since| &trace.started_at >= since)
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        traces.sort_by(|a, b| a.started_at.cmp(&b.started_at));
        if let Some(limit) = window.limit {
            let start = traces.len().saturating_sub(limit);
            traces = traces.split_off(start);
        }
        Ok(traces)
    }
}

fn filter_event(event: &LedgerEvent, filter: &EventFilter) -> bool {
    if !filter.kinds.is_empty() && !filter.kinds.contains(&event.kind) {
        return false;
    }
    if let Some(node_id) = &filter.node_id {
        if event.node_id.as_ref() != Some(node_id) {
            return false;
        }
    }
    if let Some(task_id) = &filter.task_id {
        if event.task_id.as_ref() != Some(task_id) {
            return false;
        }
    }
    if let Some(directive_id) = &filter.directive_id {
        if event.directive_id.as_ref() != Some(directive_id) {
            return false;
        }
    }
    if let Some(correlation_id) = &filter.correlation_id {
        if event.correlation_id.as_ref() != Some(correlation_id) {
            return false;
        }
    }
    if let Some(since) = filter.since.as_ref() {
        if &event.created_at < since {
            return false;
        }
    }
    true
}
