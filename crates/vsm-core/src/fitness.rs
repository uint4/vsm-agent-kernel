use crate::{NodeId, TaskTrace};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

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
pub struct AttributionWeights {
    pub direct_weight: f64,
    pub ancestor_initial_weight: f64,
    pub ancestor_decay: f64,
}

impl Default for AttributionWeights {
    fn default() -> Self {
        Self {
            direct_weight: 1.0,
            ancestor_initial_weight: 1.0,
            ancestor_decay: 0.5,
        }
    }
}

impl AttributionWeights {
    pub fn ancestor_weight(&self, depth: usize) -> f64 {
        self.ancestor_initial_weight * self.ancestor_decay.powi(depth as i32)
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AttributedFitnessSummary {
    pub node_id: NodeId,
    pub direct_task_count: u64,
    pub descendant_task_count: u64,
    pub subtree_task_count: u64,
    pub direct_tokens: u64,
    pub subtree_tokens: u64,
    pub direct_latency_ms: u64,
    pub subtree_latency_ms: u64,
    pub attribution_weight_total: f64,
    pub direct_score: f64,
    pub descendant_score: f64,
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
    score -= trace_coordination_units(trace) * weights.coordination_message_weight;

    score
}

/// Computes direct fitness per assigned node.
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

/// Computes fitness for directly assigned work plus credited descendant work.
///
/// `TaskTrace::responsible_ancestor_ids` is expected to be ordered from
/// immediate parent outward. The immediate parent receives
/// `ancestor_initial_weight`; each higher ancestor is decayed by
/// `ancestor_decay`.
pub fn summarize_attributed_fitness<'a>(
    traces: impl IntoIterator<Item = &'a TaskTrace>,
    weights: &FitnessWeights,
    attribution: &AttributionWeights,
    complexity_by_node: &BTreeMap<NodeId, f64>,
) -> BTreeMap<NodeId, AttributedFitnessSummary> {
    let mut summaries: BTreeMap<NodeId, AttributedFitnessSummary> = BTreeMap::new();

    for trace in traces {
        let trace_score = score_trace(trace, weights);
        let direct_score = trace_score * attribution.direct_weight;
        let token_total = trace.token_total();

        let direct_entry = summaries
            .entry(trace.assigned_node_id.clone())
            .or_insert_with(|| empty_attributed_summary(trace.assigned_node_id.clone()));
        direct_entry.direct_task_count += 1;
        direct_entry.subtree_task_count += 1;
        direct_entry.direct_tokens += token_total;
        direct_entry.subtree_tokens += token_total;
        direct_entry.direct_latency_ms += trace.latency_ms;
        direct_entry.subtree_latency_ms += trace.latency_ms;
        direct_entry.attribution_weight_total += attribution.direct_weight;
        direct_entry.direct_score += direct_score;

        let mut seen_ancestors = BTreeSet::new();
        for (depth, ancestor_id) in trace.responsible_ancestor_ids.iter().enumerate() {
            if ancestor_id == &trace.assigned_node_id || !seen_ancestors.insert(ancestor_id) {
                continue;
            }

            let ancestor_weight = attribution.ancestor_weight(depth);
            if ancestor_weight == 0.0 {
                continue;
            }

            let ancestor_entry = summaries
                .entry(ancestor_id.clone())
                .or_insert_with(|| empty_attributed_summary(ancestor_id.clone()));
            ancestor_entry.descendant_task_count += 1;
            ancestor_entry.subtree_task_count += 1;
            ancestor_entry.subtree_tokens += token_total;
            ancestor_entry.subtree_latency_ms += trace.latency_ms;
            ancestor_entry.attribution_weight_total += ancestor_weight;
            ancestor_entry.descendant_score += trace_score * ancestor_weight;
        }
    }

    for (node_id, summary) in summaries.iter_mut() {
        summary.raw_score = summary.direct_score + summary.descendant_score;
        let complexity = complexity_by_node.get(node_id).copied().unwrap_or(0.0);
        summary.complexity_penalty = complexity * weights.org_complexity_weight;
        summary.final_score = summary.raw_score - summary.complexity_penalty;
    }

    summaries
}

fn empty_attributed_summary(node_id: NodeId) -> AttributedFitnessSummary {
    AttributedFitnessSummary {
        node_id,
        direct_task_count: 0,
        descendant_task_count: 0,
        subtree_task_count: 0,
        direct_tokens: 0,
        subtree_tokens: 0,
        direct_latency_ms: 0,
        subtree_latency_ms: 0,
        attribution_weight_total: 0.0,
        direct_score: 0.0,
        descendant_score: 0.0,
        raw_score: 0.0,
        complexity_penalty: 0.0,
        final_score: 0.0,
    }
}

fn trace_coordination_units(trace: &TaskTrace) -> f64 {
    let mut units = metadata_number(trace, "coordination_message_count").unwrap_or(0.0);

    if let Some(channel) = metadata_value(trace, "vsm_source_channel")
        .or_else(|| metadata_value(trace, "source_channel"))
    {
        units += channel_coordination_units(channel);
    }

    if let Some(outbound_channel) = metadata_value(trace, "vsm_outbound_channel") {
        units += channel_coordination_units(outbound_channel) * 0.5;
    }

    units += count_metadata_list(trace, "dependency_task_ids") as f64;

    if metadata_value(trace, "handoff_kind").is_some() {
        units += 1.0;
    }
    if metadata_value(trace, "management_kind").is_some() {
        units += 1.0;
    }
    if metadata_value(trace, "decomposition_revision_depth")
        .and_then(|value| value.parse::<u32>().ok())
        .is_some_and(|depth| depth > 0)
    {
        units += 1.0;
    }
    if metadata_value(trace, "decomposition_role").is_some_and(|role| role != "implementation") {
        units += 0.5;
    }
    if metadata_value(trace, "trial_shadow").is_some_and(is_truthy) {
        units += 1.0;
    }

    units.max(0.0)
}

fn channel_coordination_units(channel: &str) -> f64 {
    match channel {
        "ResourceBargaining" | "OperationToEnvironment" => 0.0,
        "Command" | "ManagementToOperation" => 1.0,
        "System2Coordination" | "OperationToOperation" | "Audit" => 1.5,
        "ThreeFourHomeostat" => 1.0,
        "FutureProbeToEnvironment" | "EnvironmentToEnvironment" => 0.5,
        "Algedonic" => 2.0,
        _ => 0.0,
    }
}

fn metadata_value<'a>(trace: &'a TaskTrace, key: &str) -> Option<&'a str> {
    trace
        .metadata
        .get(key)
        .or_else(|| trace.metadata.get(&format!("task_metadata.{key}")))
        .map(String::as_str)
}

fn metadata_number(trace: &TaskTrace, key: &str) -> Option<f64> {
    metadata_value(trace, key).and_then(|value| value.parse::<f64>().ok())
}

fn count_metadata_list(trace: &TaskTrace, key: &str) -> usize {
    metadata_value(trace, key)
        .map(|value| {
            value
                .split([',', '|'])
                .filter(|part| !part.trim().is_empty())
                .count()
        })
        .unwrap_or(0)
}

fn is_truthy(value: &str) -> bool {
    value == "true" || value == "1" || value.eq_ignore_ascii_case("yes")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DelayedTraceAdjustment, GenomeId, TaskId};
    use chrono::Utc;

    fn trace_with_score(
        assigned_node_id: NodeId,
        responsible_ancestor_ids: Vec<NodeId>,
        outcome_score: f64,
    ) -> TaskTrace {
        let mut trace = TaskTrace::started(
            TaskId::from("task"),
            GenomeId::from("genome"),
            assigned_node_id,
        );
        trace.responsible_ancestor_ids = responsible_ancestor_ids;
        trace.outcome_score = outcome_score;
        trace
    }

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < f64::EPSILON,
            "expected {expected}, got {actual}"
        );
    }

    #[test]
    fn attributed_fitness_credits_immediate_parent_for_subtree_work() {
        let leaf = NodeId::from("leaf");
        let parent = NodeId::from("parent");
        let trace = trace_with_score(leaf.clone(), vec![parent.clone()], 10.0);

        let summaries = summarize_attributed_fitness(
            [&trace],
            &FitnessWeights::default(),
            &AttributionWeights::default(),
            &BTreeMap::new(),
        );

        let leaf_summary = summaries.get(&leaf).expect("leaf summary");
        assert_eq!(leaf_summary.direct_task_count, 1);
        assert_eq!(leaf_summary.subtree_task_count, 1);
        assert_close(leaf_summary.direct_score, 10.0);
        assert_close(leaf_summary.final_score, 10.0);

        let parent_summary = summaries.get(&parent).expect("parent summary");
        assert_eq!(parent_summary.direct_task_count, 0);
        assert_eq!(parent_summary.descendant_task_count, 1);
        assert_eq!(parent_summary.subtree_task_count, 1);
        assert_close(parent_summary.descendant_score, 10.0);
        assert_close(parent_summary.final_score, 10.0);
    }

    #[test]
    fn attributed_fitness_decays_credit_for_higher_ancestors() {
        let leaf = NodeId::from("leaf");
        let parent = NodeId::from("parent");
        let root = NodeId::from("root");
        let trace = trace_with_score(leaf, vec![parent.clone(), root.clone()], 8.0);

        let summaries = summarize_attributed_fitness(
            [&trace],
            &FitnessWeights::default(),
            &AttributionWeights::default(),
            &BTreeMap::new(),
        );

        let parent_summary = summaries.get(&parent).expect("parent summary");
        assert_close(parent_summary.descendant_score, 8.0);
        assert_close(parent_summary.attribution_weight_total, 1.0);

        let root_summary = summaries.get(&root).expect("root summary");
        assert_close(root_summary.descendant_score, 4.0);
        assert_close(root_summary.attribution_weight_total, 0.5);
    }

    #[test]
    fn attributed_fitness_propagates_delayed_adjustments() {
        let leaf = NodeId::from("leaf");
        let parent = NodeId::from("parent");
        let mut trace = trace_with_score(leaf.clone(), vec![parent.clone()], 10.0);
        trace.delayed_adjustments.push(DelayedTraceAdjustment {
            created_at: Utc::now(),
            source: "post-merge-check".to_string(),
            reason: "regression found later".to_string(),
            delta_score: -3.0,
        });

        let summaries = summarize_attributed_fitness(
            [&trace],
            &FitnessWeights::default(),
            &AttributionWeights::default(),
            &BTreeMap::new(),
        );

        let leaf_summary = summaries.get(&leaf).expect("leaf summary");
        assert_close(leaf_summary.direct_score, 7.0);

        let parent_summary = summaries.get(&parent).expect("parent summary");
        assert_close(parent_summary.descendant_score, 7.0);
    }

    #[test]
    fn attributed_fitness_applies_complexity_penalty_per_node() {
        let leaf = NodeId::from("leaf");
        let trace = trace_with_score(leaf.clone(), vec![], 10.0);
        let mut complexity_by_node = BTreeMap::new();
        complexity_by_node.insert(leaf.clone(), 4.0);

        let summaries = summarize_attributed_fitness(
            [&trace],
            &FitnessWeights::default(),
            &AttributionWeights::default(),
            &complexity_by_node,
        );

        let summary = summaries.get(&leaf).expect("leaf summary");
        assert_close(summary.raw_score, 10.0);
        assert_close(summary.complexity_penalty, 1.0);
        assert_close(summary.final_score, 9.0);
    }

    #[test]
    fn score_trace_penalizes_coordination_evidence_from_channel_metadata() {
        let leaf = NodeId::from("leaf");
        let mut trace = trace_with_score(leaf, vec![], 10.0);
        trace.metadata.insert(
            "task_metadata.vsm_source_channel".to_string(),
            "System2Coordination".to_string(),
        );
        trace.metadata.insert(
            "vsm_outbound_channel".to_string(),
            "System2Coordination".to_string(),
        );
        trace.metadata.insert(
            "task_metadata.dependency_task_ids".to_string(),
            "task-a,task-b".to_string(),
        );
        trace.metadata.insert(
            "task_metadata.handoff_kind".to_string(),
            "operation_to_operation".to_string(),
        );
        trace
            .metadata
            .insert("decomposition_role".to_string(), "review".to_string());
        trace
            .metadata
            .insert("trial_shadow".to_string(), "true".to_string());
        let mut weights = FitnessWeights::default();
        weights.coordination_message_weight = 1.0;

        assert_close(score_trace(&trace, &weights), 3.25);
    }
}
