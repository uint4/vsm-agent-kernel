use crate::ids::{DirectiveId, NodeId, TaskId, TraceId};
use crate::task::StaticTaskPredicates;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskTrace {
    pub id: TraceId,
    pub task_id: TaskId,
    pub directive_id: DirectiveId,
    pub assigned_node_id: NodeId,
    pub responsible_parent_ids: Vec<NodeId>,
    pub static_predicates: StaticTaskPredicates,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub tool_calls: u32,
    pub latency_ms: u64,
    pub files_touched: Vec<String>,
    pub lines_added: i64,
    pub lines_deleted: i64,
    pub tests_run: Vec<String>,
    pub tests_passed: Option<bool>,
    pub review_passed: Option<bool>,
    pub merged: bool,
    pub reverted: bool,
    pub post_merge_regression: bool,
    pub human_override: bool,
    pub immediate_outcome: OutcomeScore,
    pub delayed_outcome: Option<OutcomeScore>,
    pub raw_metrics: Value,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct OutcomeScore {
    pub task_value: f64,
    pub token_cost: f64,
    pub latency_penalty: f64,
    pub failed_tests_penalty: f64,
    pub review_failure_penalty: f64,
    pub revert_penalty: f64,
    pub regression_penalty: f64,
    pub human_override_penalty: f64,
    pub diff_size_penalty: f64,
    pub coordination_penalty: f64,
}

impl OutcomeScore {
    pub fn total(&self) -> f64 {
        self.task_value
            - self.token_cost
            - self.latency_penalty
            - self.failed_tests_penalty
            - self.review_failure_penalty
            - self.revert_penalty
            - self.regression_penalty
            - self.human_override_penalty
            - self.diff_size_penalty
            - self.coordination_penalty
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TraceLedger {
    pub traces: Vec<TaskTrace>,
}

impl TraceLedger {
    pub fn push(&mut self, trace: TaskTrace) {
        self.traces.push(trace);
    }

    pub fn for_node<'a>(&'a self, node_id: &'a NodeId) -> impl Iterator<Item = &'a TaskTrace> {
        self.traces.iter().filter(move |t| &t.assigned_node_id == node_id)
    }

    pub fn for_subtree<'a>(
        &'a self,
        subtree_node_ids: &'a [NodeId],
    ) -> impl Iterator<Item = &'a TaskTrace> {
        self.traces.iter().filter(move |t| subtree_node_ids.contains(&t.assigned_node_id))
    }
}
