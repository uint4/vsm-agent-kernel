use crate::{GenomeId, NodeId, SuggestionId, TaskId, TraceId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskTrace {
    pub id: TraceId,
    pub task_id: TaskId,
    pub genome_id: GenomeId,

    /// Leaf or metasystem node that accepted responsibility for this task.
    pub assigned_node_id: NodeId,

    /// Ancestors that receive discounted credit/blame.
    pub responsible_ancestor_ids: Vec<NodeId>,

    pub related_suggestion_ids: Vec<SuggestionId>,

    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,

    pub input_tokens: u64,
    pub output_tokens: u64,
    pub tool_calls: u64,
    pub latency_ms: u64,

    pub files_touched: Vec<String>,
    pub lines_added: i64,
    pub lines_deleted: i64,

    pub tests_run: Vec<String>,
    pub tests_passed: Option<bool>,
    pub review_passed: Option<bool>,
    pub merged: Option<bool>,
    pub reverted: Option<bool>,
    pub post_merge_regression: Option<bool>,
    pub human_override: Option<bool>,

    /// Positive for good outcomes, negative for bad. This can be recomputed
    /// later from the raw fields when scoring changes.
    pub outcome_score: f64,

    pub delayed_adjustments: Vec<DelayedTraceAdjustment>,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DelayedTraceAdjustment {
    pub created_at: DateTime<Utc>,
    pub source: String,
    pub reason: String,
    pub delta_score: f64,
}

impl TaskTrace {
    pub fn started(task_id: TaskId, genome_id: GenomeId, assigned_node_id: NodeId) -> Self {
        Self {
            id: TraceId::new(),
            task_id,
            genome_id,
            assigned_node_id,
            responsible_ancestor_ids: vec![],
            related_suggestion_ids: vec![],
            started_at: Utc::now(),
            completed_at: None,
            input_tokens: 0,
            output_tokens: 0,
            tool_calls: 0,
            latency_ms: 0,
            files_touched: vec![],
            lines_added: 0,
            lines_deleted: 0,
            tests_run: vec![],
            tests_passed: None,
            review_passed: None,
            merged: None,
            reverted: None,
            post_merge_regression: None,
            human_override: None,
            outcome_score: 0.0,
            delayed_adjustments: vec![],
            metadata: BTreeMap::new(),
        }
    }

    pub fn total_score(&self) -> f64 {
        self.outcome_score
            + self
                .delayed_adjustments
                .iter()
                .map(|a| a.delta_score)
                .sum::<f64>()
    }

    pub fn token_total(&self) -> u64 {
        self.input_tokens + self.output_tokens
    }
}
