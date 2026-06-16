use crate::genome::GenomePatch;
use crate::ids::{NodeId, SuggestionId};
use crate::selection::{compute_node_fitness, SelectionPolicy};
use crate::trace::TraceLedger;
use crate::{OrganizationalGenome, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeneSuggestion {
    pub id: SuggestionId,
    pub suggested_by_node_id: NodeId,
    pub target_node_id: NodeId,
    pub source: GeneSuggestionSource,
    pub patch: GenomePatch,
    pub evidence: Vec<AuditEvidence>,
    pub hypothesis: String,
    pub trial_mode: TrialMode,
    pub safety_limits: TrialSafetyLimits,
    pub measurement_plan: MeasurementPlan,
    pub rollback_plan: RollbackPlan,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GeneSuggestionSource {
    System3StarAudit,
    System4FutureProbe,
    System1ResourceBargain,
    System2CoordinationSignal,
    AlgedonicSignal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvidence {
    pub kind: String,
    pub summary: String,
    pub related_node_ids: Vec<NodeId>,
    pub observed_count: u64,
    pub severity: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TrialMode {
    Shadow,
    Probation,
    Canary,
    Direct,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TrialSafetyLimits {
    pub max_token_budget: Option<u64>,
    pub max_tasks: Option<u32>,
    pub max_traffic_share: Option<f32>,
    pub max_files_touched: Option<u32>,
    pub allowed_task_classes: Vec<String>,
    pub requires_approval: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeasurementPlan {
    pub window_tasks: Option<u32>,
    pub window_days: Option<u32>,
    pub compare_against: ComparisonBaseline,
    pub success_metrics: Vec<String>,
    pub failure_metrics: Vec<String>,
}

impl Default for MeasurementPlan {
    fn default() -> Self {
        Self {
            window_tasks: Some(10),
            window_days: None,
            compare_against: ComparisonBaseline::CurrentParent,
            success_metrics: vec!["subtree_fitness".to_string(), "accepted_patch_rate".to_string()],
            failure_metrics: vec!["token_cost".to_string(), "regression_rate".to_string()],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ComparisonBaseline {
    CurrentParent,
    Sibling,
    ChampionGenome,
    HistoricalBaseline,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RollbackPlan {
    pub remove_nodes: Vec<NodeId>,
    pub restore_genome_id: Option<String>,
    pub notes: Vec<String>,
}

#[async_trait]
pub trait Auditor: Send + Sync {
    async fn audit_children(
        &self,
        genome: &OrganizationalGenome,
        parent_node_id: &NodeId,
        ledger: &TraceLedger,
    ) -> Result<Vec<GeneSuggestion>>;
}

/// Minimal heuristic auditor. It intentionally does not claim to predict benefit.
/// It only identifies mutation pressure from observed performance.
#[derive(Debug, Clone)]
pub struct HeuristicAuditor {
    pub selection_policy: SelectionPolicy,
}

impl Default for HeuristicAuditor {
    fn default() -> Self {
        Self {
            selection_policy: SelectionPolicy::default(),
        }
    }
}

#[async_trait]
impl Auditor for HeuristicAuditor {
    async fn audit_children(
        &self,
        genome: &OrganizationalGenome,
        parent_node_id: &NodeId,
        ledger: &TraceLedger,
    ) -> Result<Vec<GeneSuggestion>> {
        let parent = genome.get_node(parent_node_id)?;
        if parent.children.is_empty() {
            return Ok(Vec::new());
        }

        let mut suggestions = Vec::new();
        for child_id in &parent.children {
            let score = compute_node_fitness(genome, ledger, child_id, &self.selection_policy);
            if score.subtree_task_count >= self.selection_policy.min_probation_tasks
                && score.total < self.selection_policy.prune_threshold
            {
                suggestions.push(GeneSuggestion {
                    id: SuggestionId::new(),
                    suggested_by_node_id: parent_node_id.clone(),
                    target_node_id: child_id.clone(),
                    source: GeneSuggestionSource::System3StarAudit,
                    patch: GenomePatch::RemoveSubtree { node_id: child_id.clone() },
                    evidence: vec![AuditEvidence {
                        kind: "low_subtree_fitness".to_string(),
                        summary: format!(
                            "child subtree fitness {} is below prune threshold {}",
                            score.total, self.selection_policy.prune_threshold
                        ),
                        related_node_ids: vec![child_id.clone()],
                        observed_count: score.subtree_task_count as u64,
                        severity: 6,
                    }],
                    hypothesis: "Removing this child may reduce organizational bloat and coordination cost.".to_string(),
                    trial_mode: TrialMode::Canary,
                    safety_limits: TrialSafetyLimits {
                        requires_approval: true,
                        ..TrialSafetyLimits::default()
                    },
                    measurement_plan: MeasurementPlan::default(),
                    rollback_plan: RollbackPlan {
                        restore_genome_id: Some(genome.id.to_string()),
                        ..RollbackPlan::default()
                    },
                });
            }
        }

        Ok(suggestions)
    }
}
