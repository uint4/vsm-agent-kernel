use crate::{NodeId, OrganizationalGenomePatch, SuggestionId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GeneSuggestionSource {
    System3StarAudit,
    System4FutureProbe,
    System1ResourceBargain,
    System2CoordinationSignal,
    AlgedonicSignal,
    Other(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrialMode {
    Shadow,
    Probation,
    Canary,
    Direct,
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TrialSafetyLimits {
    pub max_token_budget: Option<u64>,
    pub max_tasks: Option<u64>,
    pub max_traffic_share_basis_points: Option<u16>,
    pub max_files_touched: Option<u32>,
    pub allowed_task_classes: Vec<String>,
    pub requires_approval: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompareAgainst {
    CurrentParent,
    Sibling(NodeId),
    ChampionGenome,
    Baseline(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeasurementPlan {
    pub window_tasks: Option<u64>,
    pub window_days: Option<u64>,
    pub compare_against: CompareAgainst,
    pub success_metrics: Vec<String>,
    pub failure_metrics: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RollbackPlan {
    pub remove_nodes: Vec<NodeId>,
    pub restore_genome_id: Option<String>,
    pub notes: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GeneSuggestion {
    pub id: SuggestionId,
    pub suggested_by_node_id: NodeId,
    pub target_node_id: NodeId,
    pub source: GeneSuggestionSource,
    pub patch: OrganizationalGenomePatch,
    pub evidence: Vec<String>,
    pub hypothesis: String,
    pub trial_mode: TrialMode,
    pub safety_limits: TrialSafetyLimits,
    pub measurement_plan: MeasurementPlan,
    pub rollback_plan: RollbackPlan,
}

impl GeneSuggestion {
    pub fn new(
        suggested_by_node_id: NodeId,
        target_node_id: NodeId,
        source: GeneSuggestionSource,
        patch: OrganizationalGenomePatch,
        hypothesis: impl Into<String>,
    ) -> Self {
        Self {
            id: SuggestionId::new(),
            suggested_by_node_id,
            target_node_id,
            source,
            patch,
            evidence: vec![],
            hypothesis: hypothesis.into(),
            trial_mode: TrialMode::Probation,
            safety_limits: TrialSafetyLimits::default(),
            measurement_plan: MeasurementPlan {
                window_tasks: Some(25),
                window_days: None,
                compare_against: CompareAgainst::CurrentParent,
                success_metrics: vec![
                    "accepted_task_value".to_string(),
                    "regression_rate".to_string(),
                ],
                failure_metrics: vec!["token_cost".to_string(), "revert_rate".to_string()],
            },
            rollback_plan: RollbackPlan {
                remove_nodes: vec![],
                restore_genome_id: None,
                notes: None,
            },
        }
    }
}
