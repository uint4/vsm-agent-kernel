use crate::{EventFilter, LedgerError, LedgerEvent, TraceWindow};
use async_trait::async_trait;
use vsm_core::{NodeId, TaskTrace, TraceId};

#[async_trait]
pub trait Ledger: Send + Sync {
    async fn append_event(&self, event: LedgerEvent) -> Result<(), LedgerError>;

    async fn write_task_trace(&self, trace: TaskTrace) -> Result<(), LedgerError>;

    async fn get_trace(&self, trace_id: &TraceId) -> Result<Option<TaskTrace>, LedgerError>;

    async fn recent_events(&self, filter: EventFilter) -> Result<Vec<LedgerEvent>, LedgerError>;

    async fn recent_task_traces(&self, window: TraceWindow) -> Result<Vec<TaskTrace>, LedgerError>;

    async fn subtree_task_traces(
        &self,
        node_id: &NodeId,
        window: TraceWindow,
    ) -> Result<Vec<TaskTrace>, LedgerError> {
        let traces = self.recent_task_traces(window).await?;
        Ok(traces
            .into_iter()
            .filter(|trace| {
                &trace.assigned_node_id == node_id
                    || trace
                        .responsible_ancestor_ids
                        .iter()
                        .any(|ancestor| ancestor == node_id)
            })
            .collect())
    }
}
