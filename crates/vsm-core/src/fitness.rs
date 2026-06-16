use crate::{NodeId, TaskTrace};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FitnessWeights {
    pub accepted_merge_value: f64,
    pub failed_task_penalty: f64,
    pub reverted_penalty: f64,
    pub regression_penalty: f64,
    pub human_override_penalty: f64,
    pub token_cost_weight: f64,
    pub latency_ms_weight: f64,
    pub line_delta_weight: f64,
    pub coordination_message_weight: f64,
    pub org_complexity_weight: f64,
}

impl Default for FitnessWeights {
    fn default() -> Self {
        Self {
            accepted_merge_value: 10.0,
            failed_task_penalty: 5.0,
            reverted_penalty: 30.0,
            regression_penalty: 25.0,
            human_override_penalty: 20.0,
            token_cost_weight: 0.00001,
            latency_ms_weight: 0.000001,
            line_delta_weight: 0.001,
            coordination_message_weight: 0.05,
            org_complexity_weight: 0.25,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FitnessSummary {
    pub node_id: NodeId,
    pub task_count: u64,
    pub merged_count: u64,
    pub reverted_count: u64,
    pub regression_count: u64,
    pub human_override_count: u64,
    pub tokens: u64,
    pub latency_ms: u64,
    pub raw_score: f64,
    pub complexity_penalty: f64,
    pub final_score: f64,
}

pub fn score_trace(trace: &TaskTrace, weights: &FitnessWeights) -> f64 {
    let mut score = trace.total_score();

    match trace.merged {
        Some(true) => score += weights.accepted_merge_value,
        Some(false) => score -= weights.failed_task_penalty,
        None => {}
    }

    if trace.reverted == Some(true) {
        score -= weights.reverted_penalty;
    }
    if trace.post_merge_regression == Some(true) {
        score -= weights.regression_penalty;
    }
    if trace.human_override == Some(true) {
        score -= weights.human_override_penalty;
    }

    score -= trace.token_total() as f64 * weights.token_cost_weight;
    score -= trace.latency_ms as f64 * weights.latency_ms_weight;
    score -= (trace.lines_added.unsigned_abs() + trace.lines_deleted.unsigned_abs()) as f64
        * weights.line_delta_weight;

    score
}

/// Computes direct fitness per assigned node. Ancestor/subtree credit assignment
/// should be layered on by the runtime because it depends on the current genome.
pub fn summarize_direct_fitness<'a>(
    traces: impl IntoIterator<Item = &'a TaskTrace>,
    weights: &FitnessWeights,
    complexity_by_node: &BTreeMap<NodeId, f64>,
) -> BTreeMap<NodeId, FitnessSummary> {
    let mut summaries: BTreeMap<NodeId, FitnessSummary> = BTreeMap::new();

    for trace in traces {
        let entry = summaries
            .entry(trace.assigned_node_id.clone())
            .or_insert_with(|| FitnessSummary {
                node_id: trace.assigned_node_id.clone(),
                task_count: 0,
                merged_count: 0,
                reverted_count: 0,
                regression_count: 0,
                human_override_count: 0,
                tokens: 0,
                latency_ms: 0,
                raw_score: 0.0,
                complexity_penalty: 0.0,
                final_score: 0.0,
            });

        entry.task_count += 1;
        entry.merged_count += if trace.merged == Some(true) { 1 } else { 0 };
        entry.reverted_count += if trace.reverted == Some(true) { 1 } else { 0 };
        entry.regression_count += if trace.post_merge_regression == Some(true) {
            1
        } else {
            0
        };
        entry.human_override_count += if trace.human_override == Some(true) {
            1
        } else {
            0
        };
        entry.tokens += trace.token_total();
        entry.latency_ms += trace.latency_ms;
        entry.raw_score += score_trace(trace, weights);
    }

    for (node_id, summary) in summaries.iter_mut() {
        let complexity = complexity_by_node.get(node_id).copied().unwrap_or(0.0);
        summary.complexity_penalty = complexity * weights.org_complexity_weight;
        summary.final_score = summary.raw_score - summary.complexity_penalty;
    }

    summaries
}
