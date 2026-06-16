use crate::genome::OrganizationalGenome;
use crate::ids::NodeId;
use crate::trace::{TaskTrace, TraceLedger};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FitnessScore {
    pub node_id: NodeId,
    pub direct_task_count: usize,
    pub subtree_task_count: usize,
    pub direct_total: f64,
    pub subtree_total: f64,
    pub org_complexity_penalty: f64,
    pub total: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectionPolicy {
    pub org_complexity_penalty_per_node: f64,
    pub min_probation_tasks: usize,
    pub prune_threshold: f64,
}

impl Default for SelectionPolicy {
    fn default() -> Self {
        Self {
            org_complexity_penalty_per_node: 0.25,
            min_probation_tasks: 5,
            prune_threshold: -10.0,
        }
    }
}

pub fn compute_node_fitness(
    genome: &OrganizationalGenome,
    ledger: &TraceLedger,
    node_id: &NodeId,
    policy: &SelectionPolicy,
) -> FitnessScore {
    let subtree_ids = collect_subtree_ids(genome, node_id);
    let direct: Vec<&TaskTrace> = ledger.for_node(node_id).collect();
    let subtree: Vec<&TaskTrace> = ledger.for_subtree(&subtree_ids).collect();

    let direct_total = direct.iter().map(|t| trace_total(t)).sum::<f64>();
    let subtree_total = subtree.iter().map(|t| trace_total(t)).sum::<f64>();
    let org_complexity_penalty = subtree_ids.len() as f64 * policy.org_complexity_penalty_per_node;

    FitnessScore {
        node_id: node_id.clone(),
        direct_task_count: direct.len(),
        subtree_task_count: subtree.len(),
        direct_total,
        subtree_total,
        org_complexity_penalty,
        total: subtree_total - org_complexity_penalty,
    }
}

pub fn trace_total(trace: &TaskTrace) -> f64 {
    let mut total = trace.immediate_outcome.total();
    if let Some(delayed) = trace.delayed_outcome {
        total += delayed.total();
    }
    total
}

pub fn collect_subtree_ids(genome: &OrganizationalGenome, node_id: &NodeId) -> Vec<NodeId> {
    let mut out = Vec::new();
    collect_subtree_ids_inner(genome, node_id, &mut out);
    out
}

fn collect_subtree_ids_inner(genome: &OrganizationalGenome, node_id: &NodeId, out: &mut Vec<NodeId>) {
    out.push(node_id.clone());
    if let Ok(node) = genome.get_node(node_id) {
        for child_id in &node.children {
            collect_subtree_ids_inner(genome, child_id, out);
        }
    }
}
