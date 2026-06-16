use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use vsm_core::{
    score_trace, FitnessWeights, GeneSuggestion, GeneSuggestionSource, GenomeId, NodeId,
    OrganizationalGenome, RiskClass, TaskId, TaskPacket, TaskTrace, TraceId, TrialMode,
};
use vsm_ledger::{StoredTrialDecision, StoredTrialRecord, StoredTrialStatus};
use vsm_runtime::{MutationTrial, TrialConfig, TrialDecision, TrialEvaluation};

use crate::ControllerError;

const TRIAL_ID_METADATA_KEY: &str = "trial_id";
const TRIAL_APPROVED_METADATA_KEY: &str = "trial_approved";
const TRIAL_EXPOSURE_BASIS_POINTS_METADATA_KEY: &str = "trial_exposure_basis_points";
const TRIAL_EXPOSURE_BUCKET_METADATA_KEY: &str = "trial_exposure_bucket";
const TRIAL_MODE_METADATA_KEY: &str = "trial_mode";
const TRIAL_ROUTE_ROLE_METADATA_KEY: &str = "trial_route_role";
const TRIAL_SHADOW_METADATA_KEY: &str = "trial_shadow";
const TRIAL_TASK_CLASS_METADATA_KEY: &str = "task_class";
const TRIAL_SUGGESTION_METADATA_KEY: &str = "related_suggestion_id";
pub const OFFLINE_REPLAY_VERSION: &str = "route_counterfactual_v2";
const MAX_REPLAY_TRACE_EVALUATIONS: usize = 16;

#[derive(Clone, Debug)]
pub struct TrialRouteDecision {
    pub child_id: NodeId,
    pub reason: String,
    pub genome_id: GenomeId,
    pub suggestion_id: vsm_core::SuggestionId,
    pub trial_mode: TrialMode,
    pub exposure_basis_points: Option<u16>,
    pub exposure_bucket: Option<u16>,
    pub is_shadow: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueuedCandidateScore {
    pub total_score: f64,
    pub reasons: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CandidateObjectives {
    pub expected_value: f64,
    pub safety: f64,
    pub historical_fit: f64,
    pub replay_fit: f64,
    pub complexity_cost: f64,
    pub exposure_cost: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueuedCandidateEvaluation {
    pub score: QueuedCandidateScore,
    pub objectives: CandidateObjectives,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum OfflineReplayTraceStatus {
    BaseGenomeMismatch,
    SafetyRejected,
    CandidateUnroutable,
    UnchangedRoute,
    ChangedUnaffectedRoute,
    AffectedRoute,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OfflineReplayTraceEvaluation {
    pub trace_id: TraceId,
    pub task_id: TaskId,
    pub status: OfflineReplayTraceStatus,
    pub champion_child_id: Option<NodeId>,
    pub candidate_child_id: Option<NodeId>,
    pub route_changed: bool,
    pub candidate_node_affected: bool,
    pub route_impacted: bool,
    pub baseline_score: f64,
    pub estimated_delta_score: f64,
    pub reason: String,
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CandidateReplaySummary {
    pub trace_count: u64,
    pub base_genome_mismatch_count: u64,
    pub eligible_trace_count: u64,
    pub safety_rejected_count: u64,
    pub champion_route_count: u64,
    pub candidate_route_count: u64,
    pub candidate_no_route_count: u64,
    pub changed_route_count: u64,
    pub affected_route_count: u64,
    pub baseline_score: f64,
    pub estimated_delta_score: f64,
    pub replay_score: f64,
    pub reasons: Vec<String>,
    pub trace_evaluations: Vec<OfflineReplayTraceEvaluation>,
}

#[derive(Clone, Debug)]
pub struct StartedTrial {
    pub suggestion_id: vsm_core::SuggestionId,
    pub base_genome_id: GenomeId,
    pub candidate_genome_id: GenomeId,
    pub record: StoredTrialRecord,
}

#[derive(Clone, Debug)]
pub struct QueuedTrial {
    pub suggestion_id: vsm_core::SuggestionId,
    pub base_genome_id: GenomeId,
    pub candidate_genome_id: GenomeId,
    pub candidate_genome: OrganizationalGenome,
    pub record: StoredTrialRecord,
}

#[derive(Clone, Debug)]
pub struct CompletedTrial {
    pub suggestion_id: vsm_core::SuggestionId,
    pub base_genome_id: GenomeId,
    pub candidate_genome_id: GenomeId,
    pub candidate_genome: OrganizationalGenome,
    pub evaluation: TrialEvaluation,
    pub record: StoredTrialRecord,
}

#[derive(Clone, Debug)]
pub struct TrialManager {
    config: TrialConfig,
    weights: FitnessWeights,
    active: Option<MutationTrial>,
    archived: BTreeMap<String, MutationTrial>,
    registered_candidate_workers: BTreeSet<NodeId>,
    routed_tasks: u64,
    consumed_tokens: u64,
    active_started_at: Option<DateTime<Utc>>,
    active_metadata: BTreeMap<String, String>,
}

impl Default for TrialManager {
    fn default() -> Self {
        Self {
            config: TrialConfig::default(),
            weights: FitnessWeights::default(),
            active: None,
            archived: BTreeMap::new(),
            registered_candidate_workers: BTreeSet::new(),
            routed_tasks: 0,
            consumed_tokens: 0,
            active_started_at: None,
            active_metadata: BTreeMap::new(),
        }
    }
}

impl TrialManager {
    pub fn with_config(config: TrialConfig, weights: FitnessWeights) -> Self {
        Self {
            config,
            weights,
            ..Self::default()
        }
    }

    pub fn start_trial(
        &mut self,
        controller_node_id: NodeId,
        champion: &OrganizationalGenome,
        suggestion: GeneSuggestion,
    ) -> Result<StartedTrial, ControllerError> {
        if let Some(active) = &self.active {
            return Err(ControllerError::TrialAlreadyActive(
                active.suggestion.id.clone(),
            ));
        }

        let trial = MutationTrial::from_suggestion(champion, suggestion)?;
        let record =
            self.record_for_trial(&trial, controller_node_id, StoredTrialStatus::Active, None);
        let started = StartedTrial {
            suggestion_id: trial.suggestion.id.clone(),
            base_genome_id: trial.base_genome_id.clone(),
            candidate_genome_id: trial.candidate_genome.id.clone(),
            record,
        };

        self.routed_tasks = 0;
        self.consumed_tokens = 0;
        self.active_started_at = Some(Utc::now());
        self.active_metadata = BTreeMap::new();
        self.active = Some(trial);

        Ok(started)
    }

    pub fn queue_candidate(
        &self,
        controller_node_id: NodeId,
        champion: &OrganizationalGenome,
        suggestion: GeneSuggestion,
    ) -> Result<QueuedTrial, ControllerError> {
        let trial = MutationTrial::from_suggestion(champion, suggestion)?;
        let record = StoredTrialRecord::queued(
            controller_node_id,
            trial.base_genome_id.clone(),
            trial.candidate_genome.id.clone(),
            trial.suggestion.clone(),
        );
        Ok(QueuedTrial {
            suggestion_id: trial.suggestion.id.clone(),
            base_genome_id: trial.base_genome_id,
            candidate_genome_id: trial.candidate_genome.id.clone(),
            candidate_genome: trial.candidate_genome,
            record,
        })
    }

    pub fn activate_queued_trial(
        &mut self,
        record: StoredTrialRecord,
        candidate_genome: OrganizationalGenome,
    ) -> Result<StartedTrial, ControllerError> {
        if let Some(active) = &self.active {
            return Err(ControllerError::TrialAlreadyActive(
                active.suggestion.id.clone(),
            ));
        }
        if record.status != StoredTrialStatus::Queued {
            return Err(ControllerError::TrialNotQueued(record.trial_id));
        }

        let trial = MutationTrial::restore(
            record.suggestion.clone(),
            record.base_genome_id.clone(),
            candidate_genome,
            vec![],
        );
        let mut active_record = record.clone();
        active_record.mark_active();
        active_record.routed_tasks = 0;
        active_record.consumed_tokens = 0;
        active_record.trace_count = 0;
        active_record.total_score = 0.0;

        let started = StartedTrial {
            suggestion_id: active_record.trial_id.clone(),
            base_genome_id: active_record.base_genome_id.clone(),
            candidate_genome_id: active_record.candidate_genome_id.clone(),
            record: active_record.clone(),
        };

        self.routed_tasks = 0;
        self.consumed_tokens = 0;
        self.active_started_at = Some(active_record.started_at);
        self.active_metadata = active_record.metadata.clone();
        self.registered_candidate_workers = active_record
            .registered_candidate_workers
            .iter()
            .cloned()
            .collect();
        self.active = Some(trial);

        Ok(started)
    }

    pub fn restore_active_trial(
        &mut self,
        record: StoredTrialRecord,
        candidate_genome: OrganizationalGenome,
        traces: Vec<TaskTrace>,
    ) -> Result<StartedTrial, ControllerError> {
        if let Some(active) = &self.active {
            return Err(ControllerError::TrialAlreadyActive(
                active.suggestion.id.clone(),
            ));
        }

        let trial = MutationTrial::restore(
            record.suggestion.clone(),
            record.base_genome_id.clone(),
            candidate_genome,
            traces,
        );
        let started = StartedTrial {
            suggestion_id: record.trial_id.clone(),
            base_genome_id: record.base_genome_id.clone(),
            candidate_genome_id: record.candidate_genome_id.clone(),
            record: record.clone(),
        };
        self.routed_tasks = record.routed_tasks;
        self.consumed_tokens = record.consumed_tokens;
        self.active_started_at = Some(record.started_at);
        self.active_metadata = record.metadata.clone();
        self.registered_candidate_workers = record
            .registered_candidate_workers
            .iter()
            .cloned()
            .collect();
        self.active = Some(trial);
        Ok(started)
    }

    pub fn active_trial(&self) -> Option<&MutationTrial> {
        self.active.as_ref()
    }

    pub fn active_candidate_genome(&self) -> Option<OrganizationalGenome> {
        self.active
            .as_ref()
            .map(|trial| trial.candidate_genome.clone())
    }

    pub fn active_suggestion_id(&self) -> Option<vsm_core::SuggestionId> {
        self.active
            .as_ref()
            .map(|trial| trial.suggestion.id.clone())
    }

    pub fn fitness_weights(&self) -> FitnessWeights {
        self.weights.clone()
    }

    pub fn register_candidate_worker(&mut self, node_id: NodeId) {
        self.registered_candidate_workers.insert(node_id);
    }

    pub fn registered_candidate_workers(&self) -> Vec<NodeId> {
        self.registered_candidate_workers.iter().cloned().collect()
    }

    pub fn choose_trial_route(
        &self,
        router: &crate::TaskRouter,
        controller_node_id: &NodeId,
        task: &TaskPacket,
    ) -> Result<Option<TrialRouteDecision>, ControllerError> {
        let Some(trial) = self.active.as_ref() else {
            return Ok(None);
        };

        if !self.task_passes_safety_limits(task, trial) {
            return Ok(None);
        }

        let parent = trial.candidate_genome.get_node(controller_node_id)?.clone();
        if parent.children.is_empty() {
            return Ok(None);
        }

        let decision = router.choose_child(&trial.candidate_genome, &parent, task)?;
        if !self
            .registered_candidate_workers
            .contains(&decision.child_id)
        {
            return Ok(None);
        }

        Ok(Some(TrialRouteDecision {
            child_id: decision.child_id,
            reason: format!("trial {}: {}", trial.suggestion.id, decision.reason),
            genome_id: trial.candidate_genome.id.clone(),
            suggestion_id: trial.suggestion.id.clone(),
            trial_mode: trial.suggestion.trial_mode.clone(),
            exposure_basis_points: trial
                .suggestion
                .safety_limits
                .max_traffic_share_basis_points,
            exposure_bucket: trial
                .suggestion
                .safety_limits
                .max_traffic_share_basis_points
                .map(|_| deterministic_exposure_bucket(task, &trial.suggestion.id)),
            is_shadow: false,
        }))
    }

    pub fn choose_shadow_route(
        &self,
        router: &crate::TaskRouter,
        controller_node_id: &NodeId,
        task: &TaskPacket,
    ) -> Result<Option<TrialRouteDecision>, ControllerError> {
        let Some(trial) = self.active.as_ref() else {
            return Ok(None);
        };
        if !matches!(trial.suggestion.trial_mode, TrialMode::Shadow) {
            return Ok(None);
        }
        if !self.task_passes_shadow_safety_limits(task, trial) {
            return Ok(None);
        }

        let parent = trial.candidate_genome.get_node(controller_node_id)?.clone();
        if parent.children.is_empty() {
            return Ok(None);
        }

        let decision = router.choose_child(&trial.candidate_genome, &parent, task)?;
        if !self
            .registered_candidate_workers
            .contains(&decision.child_id)
        {
            return Ok(None);
        }

        Ok(Some(TrialRouteDecision {
            child_id: decision.child_id,
            reason: format!("shadow trial {}: {}", trial.suggestion.id, decision.reason),
            genome_id: trial.candidate_genome.id.clone(),
            suggestion_id: trial.suggestion.id.clone(),
            trial_mode: TrialMode::Shadow,
            exposure_basis_points: trial
                .suggestion
                .safety_limits
                .max_traffic_share_basis_points,
            exposure_bucket: trial
                .suggestion
                .safety_limits
                .max_traffic_share_basis_points
                .map(|_| deterministic_exposure_bucket(task, &trial.suggestion.id)),
            is_shadow: true,
        }))
    }

    pub fn mark_task_routed(&mut self) {
        self.routed_tasks += 1;
    }

    pub fn record_trace(&mut self, trace: TaskTrace) -> Option<TrialEvaluation> {
        let trial = self.active.as_mut()?;
        if !trace
            .related_suggestion_ids
            .iter()
            .any(|suggestion_id| suggestion_id == &trial.suggestion.id)
        {
            return None;
        }

        self.consumed_tokens = self.consumed_tokens.saturating_add(trace.token_total());
        trial.record_trace(trace);
        Some(trial.evaluate(&self.config, &self.weights))
    }

    pub fn active_record(&self, controller_node_id: NodeId) -> Option<StoredTrialRecord> {
        let trial = self.active.as_ref()?;
        Some(self.record_for_trial(trial, controller_node_id, StoredTrialStatus::Active, None))
    }

    pub fn evaluate(&self) -> Result<TrialEvaluation, ControllerError> {
        let Some(trial) = self.active.as_ref() else {
            return Err(ControllerError::NoActiveTrial);
        };
        Ok(trial.evaluate(&self.config, &self.weights))
    }

    pub fn promote_active(
        &mut self,
        controller_node_id: NodeId,
    ) -> Result<CompletedTrial, ControllerError> {
        let Some(trial) = self.active.take() else {
            return Err(ControllerError::NoActiveTrial);
        };
        let evaluation = trial.evaluate(&self.config, &self.weights);
        let record = self.record_for_trial(
            &trial,
            controller_node_id,
            StoredTrialStatus::Promoted,
            Some(StoredTrialDecision::Promote),
        );
        let completed = completed_trial(&trial, evaluation, record);
        self.archive(trial);
        Ok(completed)
    }

    pub fn prune_active(
        &mut self,
        controller_node_id: NodeId,
    ) -> Result<CompletedTrial, ControllerError> {
        let Some(trial) = self.active.take() else {
            return Err(ControllerError::NoActiveTrial);
        };
        let evaluation = trial.evaluate(&self.config, &self.weights);
        let record = self.record_for_trial(
            &trial,
            controller_node_id,
            StoredTrialStatus::Pruned,
            Some(StoredTrialDecision::Prune),
        );
        let completed = completed_trial(&trial, evaluation, record);
        self.archive(trial);
        Ok(completed)
    }

    fn archive(&mut self, trial: MutationTrial) {
        let suggestion_id = trial.suggestion.id.to_string();
        self.archived.insert(suggestion_id, trial);
        self.routed_tasks = 0;
        self.consumed_tokens = 0;
        self.registered_candidate_workers.clear();
        self.active_started_at = None;
        self.active_metadata.clear();
    }

    fn task_passes_safety_limits(&self, task: &TaskPacket, trial: &MutationTrial) -> bool {
        let limits = &trial.suggestion.safety_limits;
        let approved = task_has_trial_approval(task, &trial.suggestion.id);

        if limits.requires_approval && !approved {
            return false;
        }

        if let Some(max_tasks) = limits.max_tasks {
            if self.routed_tasks >= max_tasks {
                return false;
            }
        }

        if let Some(max_token_budget) = limits.max_token_budget {
            if self.consumed_tokens >= max_token_budget {
                return false;
            }
        }

        if let Some(max_files_touched) = limits.max_files_touched {
            if let Some(estimated_files) = task
                .metadata
                .get("estimated_files_touched")
                .and_then(|value| value.parse::<u32>().ok())
            {
                if estimated_files > max_files_touched {
                    return false;
                }
            }
        }

        if !limits.allowed_task_classes.is_empty() {
            let Some(task_class) = task.metadata.get(TRIAL_TASK_CLASS_METADATA_KEY) else {
                return false;
            };
            if !limits
                .allowed_task_classes
                .iter()
                .any(|allowed| allowed == task_class)
            {
                return false;
            }
        }

        task_passes_exposure_policy(task, trial, approved)
    }

    fn task_passes_shadow_safety_limits(&self, task: &TaskPacket, trial: &MutationTrial) -> bool {
        let limits = &trial.suggestion.safety_limits;
        let approved = task_has_trial_approval(task, &trial.suggestion.id);

        if limits.requires_approval && !approved {
            return false;
        }

        if let Some(max_tasks) = limits.max_tasks {
            if self.routed_tasks >= max_tasks {
                return false;
            }
        }

        if let Some(max_token_budget) = limits.max_token_budget {
            if self.consumed_tokens >= max_token_budget {
                return false;
            }
        }

        if let Some(max_files_touched) = limits.max_files_touched {
            if let Some(estimated_files) = task
                .metadata
                .get("estimated_files_touched")
                .and_then(|value| value.parse::<u32>().ok())
            {
                if estimated_files > max_files_touched {
                    return false;
                }
            }
        }

        if !limits.allowed_task_classes.is_empty() {
            let Some(task_class) = task.metadata.get(TRIAL_TASK_CLASS_METADATA_KEY) else {
                return false;
            };
            if !limits
                .allowed_task_classes
                .iter()
                .any(|allowed| allowed == task_class)
            {
                return false;
            }
        }

        task_passes_shadow_exposure_policy(task, trial, approved)
    }

    fn record_for_trial(
        &self,
        trial: &MutationTrial,
        controller_node_id: NodeId,
        status: StoredTrialStatus,
        decision: Option<StoredTrialDecision>,
    ) -> StoredTrialRecord {
        let evaluation = trial.evaluate(&self.config, &self.weights);
        let mut record = StoredTrialRecord::active(
            controller_node_id,
            trial.base_genome_id.clone(),
            trial.candidate_genome.id.clone(),
            trial.suggestion.clone(),
        );
        if let Some(started_at) = self.active_started_at {
            record.started_at = started_at;
        }
        record.status = status;
        record.routed_tasks = self.routed_tasks;
        record.consumed_tokens = self.consumed_tokens;
        record.trace_count = evaluation.trace_count as u64;
        record.total_score = evaluation.total_score;
        record.decision = decision;
        record.registered_candidate_workers =
            self.registered_candidate_workers.iter().cloned().collect();
        record.metadata = self.active_metadata.clone();
        record.updated_at = chrono::Utc::now();
        if record.status != StoredTrialStatus::Active {
            record.completed_at = Some(record.updated_at);
        }
        record
    }
}

pub fn tag_task_for_trial(
    task: &mut TaskPacket,
    suggestion_id: &vsm_core::SuggestionId,
    candidate_genome_id: &GenomeId,
) {
    task.metadata
        .insert(TRIAL_ID_METADATA_KEY.to_string(), suggestion_id.to_string());
    task.metadata.insert(
        TRIAL_SUGGESTION_METADATA_KEY.to_string(),
        suggestion_id.to_string(),
    );
    task.metadata.insert(
        "candidate_genome_id".to_string(),
        candidate_genome_id.to_string(),
    );
}

pub fn tag_task_for_trial_route(task: &mut TaskPacket, decision: &TrialRouteDecision) {
    tag_task_for_trial(task, &decision.suggestion_id, &decision.genome_id);
    task.metadata.insert(
        TRIAL_MODE_METADATA_KEY.to_string(),
        trial_mode_key(&decision.trial_mode).to_string(),
    );
    task.metadata.insert(
        TRIAL_ROUTE_ROLE_METADATA_KEY.to_string(),
        if decision.is_shadow {
            "shadow".to_string()
        } else {
            "control".to_string()
        },
    );
    if decision.is_shadow {
        task.metadata
            .insert(TRIAL_SHADOW_METADATA_KEY.to_string(), "true".to_string());
    }
    if let Some(basis_points) = decision.exposure_basis_points {
        task.metadata.insert(
            TRIAL_EXPOSURE_BASIS_POINTS_METADATA_KEY.to_string(),
            basis_points.to_string(),
        );
    }
    if let Some(bucket) = decision.exposure_bucket {
        task.metadata.insert(
            TRIAL_EXPOSURE_BUCKET_METADATA_KEY.to_string(),
            bucket.to_string(),
        );
    }
}

pub fn task_is_shadow_trial(task: &TaskPacket) -> bool {
    task.metadata
        .get(TRIAL_SHADOW_METADATA_KEY)
        .map(|value| value == "true")
        .unwrap_or(false)
        || task
            .metadata
            .get(TRIAL_ROUTE_ROLE_METADATA_KEY)
            .map(|value| value == "shadow")
            .unwrap_or(false)
}

pub fn task_trial_suggestion_id(task: &TaskPacket) -> Option<vsm_core::SuggestionId> {
    task.metadata
        .get(TRIAL_SUGGESTION_METADATA_KEY)
        .or_else(|| task.metadata.get(TRIAL_ID_METADATA_KEY))
        .map(|value| vsm_core::SuggestionId::from_string(value.clone()))
}

fn task_has_trial_approval(task: &TaskPacket, suggestion_id: &vsm_core::SuggestionId) -> bool {
    task.metadata
        .get(TRIAL_APPROVED_METADATA_KEY)
        .map(|value| value == "true" || value == suggestion_id.as_str())
        .unwrap_or(false)
}

fn task_passes_exposure_policy(task: &TaskPacket, trial: &MutationTrial, approved: bool) -> bool {
    if matches!(trial.suggestion.trial_mode, TrialMode::Shadow) {
        return false;
    }

    if matches!(
        task.risk,
        vsm_core::RiskClass::High | vsm_core::RiskClass::Critical
    ) && !approved
    {
        return false;
    }

    if approved {
        return true;
    }

    match trial.suggestion.trial_mode {
        TrialMode::Direct => true,
        TrialMode::Probation | TrialMode::Canary => trial
            .suggestion
            .safety_limits
            .max_traffic_share_basis_points
            .is_some_and(|basis_points| {
                deterministic_exposure_bucket(task, &trial.suggestion.id)
                    < clamped_basis_points(basis_points)
            }),
        TrialMode::Shadow => false,
    }
}

fn task_passes_shadow_exposure_policy(
    task: &TaskPacket,
    trial: &MutationTrial,
    approved: bool,
) -> bool {
    if matches!(
        task.risk,
        vsm_core::RiskClass::High | vsm_core::RiskClass::Critical
    ) && !approved
    {
        return false;
    }

    if approved {
        return true;
    }

    trial
        .suggestion
        .safety_limits
        .max_traffic_share_basis_points
        .is_some_and(|basis_points| {
            deterministic_exposure_bucket(task, &trial.suggestion.id)
                < clamped_basis_points(basis_points)
        })
}

fn clamped_basis_points(value: u16) -> u16 {
    value.min(10_000)
}

fn deterministic_exposure_bucket(task: &TaskPacket, suggestion_id: &vsm_core::SuggestionId) -> u16 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in task.id.as_str().bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash ^= b':' as u64;
    hash = hash.wrapping_mul(0x100000001b3);
    for byte in suggestion_id.as_str().bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash % 10_000) as u16
}

pub fn trial_mode_key(mode: &TrialMode) -> &'static str {
    match mode {
        TrialMode::Shadow => "shadow",
        TrialMode::Probation => "probation",
        TrialMode::Canary => "canary",
        TrialMode::Direct => "direct",
    }
}

fn completed_trial(
    trial: &MutationTrial,
    evaluation: TrialEvaluation,
    record: StoredTrialRecord,
) -> CompletedTrial {
    CompletedTrial {
        suggestion_id: trial.suggestion.id.clone(),
        base_genome_id: trial.base_genome_id.clone(),
        candidate_genome_id: trial.candidate_genome.id.clone(),
        candidate_genome: trial.candidate_genome.clone(),
        evaluation,
        record,
    }
}

pub fn trial_decision_key(decision: &TrialDecision) -> &'static str {
    match decision {
        TrialDecision::Continue => "continue",
        TrialDecision::Promote => "promote",
        TrialDecision::Prune => "prune",
    }
}

pub fn score_queued_candidate(record: &StoredTrialRecord) -> QueuedCandidateScore {
    let mut total_score = 0.0;
    let mut reasons = Vec::new();

    add_score(
        &mut total_score,
        &mut reasons,
        source_score(&record.suggestion.source),
        format!(
            "source={}",
            suggestion_source_key(&record.suggestion.source)
        ),
    );
    add_score(
        &mut total_score,
        &mut reasons,
        trial_mode_selection_score(&record.suggestion.trial_mode),
        format!("mode={}", trial_mode_key(&record.suggestion.trial_mode)),
    );

    let evidence_score = (record.suggestion.evidence.len().min(10) as f64) * 2.0;
    add_score(
        &mut total_score,
        &mut reasons,
        evidence_score,
        format!("evidence_count={}", record.suggestion.evidence.len()),
    );

    let limits = &record.suggestion.safety_limits;
    if limits.requires_approval {
        add_score(&mut total_score, &mut reasons, 5.0, "requires_approval");
    }
    if !limits.allowed_task_classes.is_empty() {
        add_score(
            &mut total_score,
            &mut reasons,
            5.0,
            format!("allowed_task_classes={}", limits.allowed_task_classes.len()),
        );
    }
    if let Some(max_tasks) = limits.max_tasks {
        let score = if max_tasks <= 10 {
            8.0
        } else if max_tasks <= 50 {
            4.0
        } else {
            1.0
        };
        add_score(
            &mut total_score,
            &mut reasons,
            score,
            format!("max_tasks={max_tasks}"),
        );
    }
    if let Some(max_token_budget) = limits.max_token_budget {
        add_score(
            &mut total_score,
            &mut reasons,
            4.0,
            format!("max_token_budget={max_token_budget}"),
        );
    }
    if let Some(max_files_touched) = limits.max_files_touched {
        add_score(
            &mut total_score,
            &mut reasons,
            4.0,
            format!("max_files_touched={max_files_touched}"),
        );
    }
    if let Some(basis_points) = limits.max_traffic_share_basis_points {
        let score = if basis_points <= 1_000 {
            10.0
        } else if basis_points <= 2_500 {
            6.0
        } else if basis_points <= 10_000 {
            2.0
        } else {
            0.0
        };
        add_score(
            &mut total_score,
            &mut reasons,
            score,
            format!("traffic_share_basis_points={basis_points}"),
        );
    }

    if matches!(record.suggestion.trial_mode, TrialMode::Direct)
        && !limits.requires_approval
        && limits.max_tasks.is_none()
        && limits.max_traffic_share_basis_points.is_none()
    {
        add_score(
            &mut total_score,
            &mut reasons,
            -20.0,
            "unbounded_direct_penalty",
        );
    }

    if let Some(priority) = record
        .metadata
        .get("selection_priority")
        .and_then(|value| value.parse::<f64>().ok())
    {
        add_score(
            &mut total_score,
            &mut reasons,
            priority,
            format!("metadata_selection_priority={priority}"),
        );
    }
    if let Some(penalty) = record
        .metadata
        .get("selection_penalty")
        .and_then(|value| value.parse::<f64>().ok())
    {
        add_score(
            &mut total_score,
            &mut reasons,
            -penalty,
            format!("metadata_selection_penalty={penalty}"),
        );
    }

    QueuedCandidateScore {
        total_score,
        reasons,
    }
}

pub fn score_queued_candidate_with_history(
    record: &StoredTrialRecord,
    history: &[StoredTrialRecord],
) -> QueuedCandidateScore {
    score_queued_candidate_with_history_and_replay(
        record,
        history,
        &CandidateReplaySummary::default(),
    )
}

pub fn score_queued_candidate_with_history_and_replay(
    record: &StoredTrialRecord,
    history: &[StoredTrialRecord],
    replay: &CandidateReplaySummary,
) -> QueuedCandidateScore {
    let mut score = score_queued_candidate(record);
    add_history_adjustment(
        &mut score,
        "history_same_source",
        history,
        |completed| completed.suggestion.source == record.suggestion.source,
        1.0,
    );
    add_history_adjustment(
        &mut score,
        "history_same_mode",
        history,
        |completed| completed.suggestion.trial_mode == record.suggestion.trial_mode,
        0.75,
    );
    add_history_adjustment(
        &mut score,
        "history_same_patch",
        history,
        |completed| {
            patch_kind_key(&completed.suggestion.patch) == patch_kind_key(&record.suggestion.patch)
        },
        1.25,
    );
    add_replay_adjustment(&mut score, replay);
    score
}

pub fn evaluate_queued_candidate(
    record: &StoredTrialRecord,
    history: &[StoredTrialRecord],
) -> QueuedCandidateEvaluation {
    evaluate_queued_candidate_with_replay(record, history, &CandidateReplaySummary::default())
}

pub fn evaluate_queued_candidate_with_replay(
    record: &StoredTrialRecord,
    history: &[StoredTrialRecord],
    replay: &CandidateReplaySummary,
) -> QueuedCandidateEvaluation {
    QueuedCandidateEvaluation {
        score: score_queued_candidate_with_history_and_replay(record, history, replay),
        objectives: candidate_objectives_with_replay(record, history, replay),
    }
}

pub fn candidate_objectives(
    record: &StoredTrialRecord,
    history: &[StoredTrialRecord],
) -> CandidateObjectives {
    candidate_objectives_with_replay(record, history, &CandidateReplaySummary::default())
}

pub fn candidate_objectives_with_replay(
    record: &StoredTrialRecord,
    history: &[StoredTrialRecord],
    replay: &CandidateReplaySummary,
) -> CandidateObjectives {
    CandidateObjectives {
        expected_value: expected_value_objective(record, history) + replay_fit_objective(replay),
        safety: safety_objective(record),
        historical_fit: historical_fit_objective(record, history),
        replay_fit: replay_fit_objective(replay),
        complexity_cost: patch_complexity_cost(&record.suggestion.patch),
        exposure_cost: exposure_cost_objective(record),
    }
}

pub fn candidate_dominates(left: &CandidateObjectives, right: &CandidateObjectives) -> bool {
    let no_worse = left.expected_value >= right.expected_value
        && left.safety >= right.safety
        && left.historical_fit >= right.historical_fit
        && left.replay_fit >= right.replay_fit
        && left.complexity_cost <= right.complexity_cost
        && left.exposure_cost <= right.exposure_cost;
    let strictly_better = left.expected_value > right.expected_value
        || left.safety > right.safety
        || left.historical_fit > right.historical_fit
        || left.replay_fit > right.replay_fit
        || left.complexity_cost < right.complexity_cost
        || left.exposure_cost < right.exposure_cost;
    no_worse && strictly_better
}

pub fn pareto_frontier_indices(evaluations: &[QueuedCandidateEvaluation]) -> Vec<usize> {
    evaluations
        .iter()
        .enumerate()
        .filter_map(|(candidate_index, candidate)| {
            let dominated = evaluations.iter().enumerate().any(|(other_index, other)| {
                other_index != candidate_index
                    && candidate_dominates(&other.objectives, &candidate.objectives)
            });
            (!dominated).then_some(candidate_index)
        })
        .collect()
}

pub fn compare_queued_candidates(
    left: &(StoredTrialRecord, QueuedCandidateScore),
    right: &(StoredTrialRecord, QueuedCandidateScore),
) -> std::cmp::Ordering {
    right
        .1
        .total_score
        .partial_cmp(&left.1.total_score)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| left.0.started_at.cmp(&right.0.started_at))
        .then_with(|| left.0.trial_id.cmp(&right.0.trial_id))
}

pub fn compare_queued_candidate_evaluations(
    left: &(StoredTrialRecord, QueuedCandidateEvaluation),
    right: &(StoredTrialRecord, QueuedCandidateEvaluation),
) -> std::cmp::Ordering {
    compare_queued_candidates(
        &(left.0.clone(), left.1.score.clone()),
        &(right.0.clone(), right.1.score.clone()),
    )
}

fn source_score(source: &GeneSuggestionSource) -> f64 {
    match source {
        GeneSuggestionSource::AlgedonicSignal => 30.0,
        GeneSuggestionSource::System3StarAudit => 24.0,
        GeneSuggestionSource::System4FutureProbe => 18.0,
        GeneSuggestionSource::System1ResourceBargain => 14.0,
        GeneSuggestionSource::System2CoordinationSignal => 10.0,
        GeneSuggestionSource::Other(_) => 5.0,
    }
}

fn trial_mode_selection_score(mode: &TrialMode) -> f64 {
    match mode {
        TrialMode::Shadow => 25.0,
        TrialMode::Canary => 20.0,
        TrialMode::Probation => 15.0,
        TrialMode::Direct => 5.0,
    }
}

fn expected_value_objective(record: &StoredTrialRecord, history: &[StoredTrialRecord]) -> f64 {
    source_score(&record.suggestion.source)
        + (record.suggestion.evidence.len().min(10) as f64 * 2.0)
        + historical_fit_objective(record, history)
}

fn safety_objective(record: &StoredTrialRecord) -> f64 {
    let limits = &record.suggestion.safety_limits;
    let mut score = trial_mode_selection_score(&record.suggestion.trial_mode);
    if limits.requires_approval {
        score += 10.0;
    }
    if !limits.allowed_task_classes.is_empty() {
        score += 6.0;
    }
    if let Some(max_tasks) = limits.max_tasks {
        score += if max_tasks <= 10 {
            10.0
        } else if max_tasks <= 50 {
            5.0
        } else {
            1.0
        };
    }
    if limits.max_token_budget.is_some() {
        score += 4.0;
    }
    if limits.max_files_touched.is_some() {
        score += 4.0;
    }
    if let Some(basis_points) = limits.max_traffic_share_basis_points {
        score += ((10_000 - clamped_basis_points(basis_points)) as f64) / 500.0;
    }
    if matches!(record.suggestion.trial_mode, TrialMode::Direct)
        && !limits.requires_approval
        && limits.max_tasks.is_none()
        && limits.max_traffic_share_basis_points.is_none()
    {
        score -= 30.0;
    }
    score
}

fn historical_fit_objective(record: &StoredTrialRecord, history: &[StoredTrialRecord]) -> f64 {
    history_adjustment_value(
        history,
        |completed| completed.suggestion.source == record.suggestion.source,
        1.0,
    ) + history_adjustment_value(
        history,
        |completed| completed.suggestion.trial_mode == record.suggestion.trial_mode,
        0.75,
    ) + history_adjustment_value(
        history,
        |completed| {
            patch_kind_key(&completed.suggestion.patch) == patch_kind_key(&record.suggestion.patch)
        },
        1.25,
    )
}

fn exposure_cost_objective(record: &StoredTrialRecord) -> f64 {
    let limits = &record.suggestion.safety_limits;
    let mut cost = match record.suggestion.trial_mode {
        TrialMode::Shadow => 0.0,
        TrialMode::Canary | TrialMode::Probation => limits
            .max_traffic_share_basis_points
            .map(|basis_points| clamped_basis_points(basis_points) as f64 / 100.0)
            .unwrap_or(100.0),
        TrialMode::Direct => 100.0,
    };
    if limits.max_tasks.is_none() {
        cost += 20.0;
    }
    if limits.max_token_budget.is_none() {
        cost += 10.0;
    }
    if !limits.requires_approval && !matches!(record.suggestion.trial_mode, TrialMode::Shadow) {
        cost += 10.0;
    }
    cost
}

pub fn replay_candidate_against_traces(
    router: &crate::TaskRouter,
    controller_node_id: &NodeId,
    champion: &OrganizationalGenome,
    candidate: &OrganizationalGenome,
    record: &StoredTrialRecord,
    traces: &[TaskTrace],
    weights: &FitnessWeights,
) -> Result<CandidateReplaySummary, ControllerError> {
    let candidate_parent = candidate.get_node(controller_node_id)?;
    let champion_parent = champion.get_node(controller_node_id).ok();
    let affected_nodes = patch_affected_node_ids(&record.suggestion.patch);
    let mut summary = CandidateReplaySummary::default();
    let mut routed_tasks = 0_u64;
    let mut consumed_tokens = 0_u64;

    for trace in traces {
        summary.trace_count += 1;
        if trace.genome_id != record.base_genome_id {
            summary.base_genome_mismatch_count += 1;
            push_replay_trace_evaluation(
                &mut summary,
                OfflineReplayTraceEvaluation {
                    trace_id: trace.id.clone(),
                    task_id: trace.task_id.clone(),
                    status: OfflineReplayTraceStatus::BaseGenomeMismatch,
                    champion_child_id: None,
                    candidate_child_id: None,
                    route_changed: false,
                    candidate_node_affected: false,
                    route_impacted: false,
                    baseline_score: 0.0,
                    estimated_delta_score: 0.0,
                    reason: "trace belongs to a different base genome".to_string(),
                },
            );
            continue;
        }

        let task = replay_task_from_trace(trace);
        let baseline_score = score_trace(trace, weights);
        if !replay_task_passes_trial_limits(record, &task, trace, routed_tasks, consumed_tokens) {
            summary.safety_rejected_count += 1;
            push_replay_trace_evaluation(
                &mut summary,
                OfflineReplayTraceEvaluation {
                    trace_id: trace.id.clone(),
                    task_id: trace.task_id.clone(),
                    status: OfflineReplayTraceStatus::SafetyRejected,
                    champion_child_id: None,
                    candidate_child_id: None,
                    route_changed: false,
                    candidate_node_affected: false,
                    route_impacted: false,
                    baseline_score,
                    estimated_delta_score: 0.0,
                    reason: "trace exceeds candidate trial safety limits".to_string(),
                },
            );
            continue;
        }
        summary.eligible_trace_count += 1;
        summary.baseline_score += baseline_score;

        let champion_route =
            champion_parent.and_then(|parent| router.choose_child(champion, parent, &task).ok());
        if champion_route.is_some() {
            summary.champion_route_count += 1;
        }
        let candidate_route = match router.choose_child(candidate, candidate_parent, &task) {
            Ok(route) => route,
            Err(error) => {
                summary.candidate_no_route_count += 1;
                let estimated_delta_score =
                    trace_replay_unroutable_delta(trace, weights, champion_route.is_some());
                summary.estimated_delta_score += estimated_delta_score;
                summary.replay_score = summary.estimated_delta_score;
                push_replay_trace_evaluation(
                    &mut summary,
                    OfflineReplayTraceEvaluation {
                        trace_id: trace.id.clone(),
                        task_id: trace.task_id.clone(),
                        status: OfflineReplayTraceStatus::CandidateUnroutable,
                        champion_child_id: champion_route.map(|route| route.child_id),
                        candidate_child_id: None,
                        route_changed: false,
                        candidate_node_affected: false,
                        route_impacted: false,
                        baseline_score,
                        estimated_delta_score,
                        reason: format!("candidate could not route replay task: {error}"),
                    },
                );
                continue;
            }
        };

        summary.candidate_route_count += 1;
        routed_tasks += 1;
        consumed_tokens = consumed_tokens.saturating_add(trace.token_total());

        let changed_route = champion_route
            .as_ref()
            .map(|route| route.child_id != candidate_route.child_id)
            .unwrap_or(true);
        let candidate_node_affected = affected_nodes.contains(&candidate_route.child_id);
        let route_impacted = changed_route || candidate_node_affected;
        if changed_route {
            summary.changed_route_count += 1;
        }
        let estimated_delta_score = if route_impacted {
            trace_replay_estimated_delta(trace, weights)
        } else {
            0.0
        };
        if route_impacted {
            summary.affected_route_count += 1;
            summary.estimated_delta_score += estimated_delta_score;
            summary.replay_score = summary.estimated_delta_score;
            if summary.reasons.len() < 8 {
                summary.reasons.push(format!(
                    "trace={} route={} changed={} affected={} delta={:.3}",
                    trace.id,
                    candidate_route.child_id,
                    changed_route,
                    candidate_node_affected,
                    estimated_delta_score
                ));
            }
        }
        let status = if candidate_node_affected {
            OfflineReplayTraceStatus::AffectedRoute
        } else if changed_route {
            OfflineReplayTraceStatus::ChangedUnaffectedRoute
        } else {
            OfflineReplayTraceStatus::UnchangedRoute
        };
        let candidate_child_id = candidate_route.child_id.clone();
        push_replay_trace_evaluation(
            &mut summary,
            OfflineReplayTraceEvaluation {
                trace_id: trace.id.clone(),
                task_id: trace.task_id.clone(),
                status,
                champion_child_id: champion_route.map(|route| route.child_id),
                candidate_child_id: Some(candidate_child_id),
                route_changed: changed_route,
                candidate_node_affected,
                route_impacted,
                baseline_score,
                estimated_delta_score,
                reason: if route_impacted {
                    "candidate route changes or touches the patched subtree".to_string()
                } else {
                    "candidate preserves the champion route".to_string()
                },
            },
        );
    }

    Ok(summary)
}

fn push_replay_trace_evaluation(
    summary: &mut CandidateReplaySummary,
    evaluation: OfflineReplayTraceEvaluation,
) {
    if summary.trace_evaluations.len() < MAX_REPLAY_TRACE_EVALUATIONS {
        summary.trace_evaluations.push(evaluation);
    }
}

fn replay_task_from_trace(trace: &TaskTrace) -> TaskPacket {
    let mut task = TaskPacket::new(
        trace
            .metadata
            .get("task_title")
            .cloned()
            .unwrap_or_else(|| format!("replay trace {}", trace.id)),
        trace
            .metadata
            .get("task_goal")
            .cloned()
            .unwrap_or_else(|| format!("Replay historical trace {}", trace.id)),
    );
    task.id = trace.task_id.clone();
    task.risk = risk_from_trace_metadata(trace);
    task.scope = trace.files_touched.clone();
    task.static_predicates.likely_files = trace.files_touched.clone();
    task.static_predicates.test_targets = trace.tests_run.clone();

    for (key, value) in &trace.metadata {
        if let Some(stripped) = key.strip_prefix("task_metadata.") {
            task.metadata.insert(stripped.to_string(), value.clone());
            continue;
        }
        if matches!(
            key.as_str(),
            "required_capability"
                | "requires_code_write"
                | "target_child"
                | "task_class"
                | "trial_approved"
        ) {
            task.metadata.insert(key.clone(), value.clone());
        }
    }

    if !task.metadata.contains_key("required_capability")
        && (!trace.files_touched.is_empty() || trace.lines_added != 0 || trace.lines_deleted != 0)
    {
        task.metadata
            .entry("requires_code_write".to_string())
            .or_insert_with(|| "true".to_string());
    }

    task
}

fn replay_task_passes_trial_limits(
    record: &StoredTrialRecord,
    task: &TaskPacket,
    trace: &TaskTrace,
    routed_tasks: u64,
    consumed_tokens: u64,
) -> bool {
    let limits = &record.suggestion.safety_limits;

    if let Some(max_tasks) = limits.max_tasks {
        if routed_tasks >= max_tasks {
            return false;
        }
    }
    if let Some(max_token_budget) = limits.max_token_budget {
        if consumed_tokens.saturating_add(trace.token_total()) > max_token_budget {
            return false;
        }
    }
    if let Some(max_files_touched) = limits.max_files_touched {
        if trace.files_touched.len() as u32 > max_files_touched {
            return false;
        }
    }
    if !limits.allowed_task_classes.is_empty() {
        let Some(task_class) = task.metadata.get(TRIAL_TASK_CLASS_METADATA_KEY) else {
            return false;
        };
        if !limits
            .allowed_task_classes
            .iter()
            .any(|allowed| allowed == task_class)
        {
            return false;
        }
    }

    let approved = task_has_trial_approval(task, &record.suggestion.id);
    if matches!(task.risk, RiskClass::High | RiskClass::Critical) && !approved {
        return false;
    }
    if approved {
        return true;
    }

    match record.suggestion.trial_mode {
        TrialMode::Direct => true,
        TrialMode::Probation | TrialMode::Canary | TrialMode::Shadow => limits
            .max_traffic_share_basis_points
            .is_some_and(|basis_points| {
                deterministic_exposure_bucket(task, &record.suggestion.id)
                    < clamped_basis_points(basis_points)
            }),
    }
}

fn risk_from_trace_metadata(trace: &TaskTrace) -> RiskClass {
    match trace
        .metadata
        .get("task_risk")
        .or_else(|| trace.metadata.get("risk_class"))
        .map(|value| value.as_str())
    {
        Some("Low" | "low") => RiskClass::Low,
        Some("High" | "high") => RiskClass::High,
        Some("Critical" | "critical") => RiskClass::Critical,
        _ => RiskClass::Medium,
    }
}

fn trace_replay_estimated_delta(trace: &TaskTrace, weights: &FitnessWeights) -> f64 {
    let scored = score_trace(trace, weights);
    let failed = trace_failed(trace);
    let succeeded = trace_succeeded(trace);
    let mut delta = 0.0;

    if failed {
        delta += 3.0 + (-scored).max(0.0).min(30.0) * 0.35;
        if trace.merged == Some(false) || trace.reverted == Some(true) {
            delta += 3.0;
        }
        if trace.post_merge_regression == Some(true) || trace.human_override == Some(true) {
            delta += 3.0;
        }
        if trace.tests_passed == Some(false) || trace.review_passed == Some(false) {
            delta += 1.5;
        }
    } else if succeeded {
        delta -= 1.5 + scored.max(0.0).min(30.0) * 0.15;
        if !trace.tests_run.is_empty() || trace.review_passed == Some(true) {
            delta -= 0.5;
        }
    } else if scored < 0.0 {
        delta += (-scored).min(15.0) * 0.2;
    } else {
        delta += 0.25;
    }

    if failed && !trace.files_touched.is_empty() {
        delta += 0.5;
    }
    if succeeded && !trace.files_touched.is_empty() {
        delta -= 0.25;
    }

    delta.clamp(-12.0, 20.0)
}

fn trace_replay_unroutable_delta(
    trace: &TaskTrace,
    weights: &FitnessWeights,
    champion_had_route: bool,
) -> f64 {
    if !champion_had_route {
        return 0.0;
    }

    let scored = score_trace(trace, weights);
    if trace_succeeded(trace) {
        -(2.0 + scored.max(0.0).min(30.0) * 0.2)
    } else if trace_failed(trace) {
        -1.0
    } else {
        -0.5
    }
}

fn trace_failed(trace: &TaskTrace) -> bool {
    trace.merged == Some(false)
        || trace.reverted == Some(true)
        || trace.post_merge_regression == Some(true)
        || trace.human_override == Some(true)
        || trace.tests_passed == Some(false)
        || trace.review_passed == Some(false)
}

fn trace_succeeded(trace: &TaskTrace) -> bool {
    trace.merged == Some(true)
        && trace.reverted != Some(true)
        && trace.post_merge_regression != Some(true)
        && trace.human_override != Some(true)
        && trace.tests_passed != Some(false)
        && trace.review_passed != Some(false)
}

fn replay_fit_objective(replay: &CandidateReplaySummary) -> f64 {
    if replay.affected_route_count == 0 && replay.candidate_no_route_count == 0 {
        return 0.0;
    }
    replay.estimated_delta_score.clamp(-40.0, 60.0) + replay.affected_route_count as f64
}

fn add_replay_adjustment(score: &mut QueuedCandidateScore, replay: &CandidateReplaySummary) {
    if replay.affected_route_count == 0 && replay.candidate_no_route_count == 0 {
        return;
    }
    let adjustment = replay.estimated_delta_score.clamp(-20.0, 40.0);
    add_score(
        &mut score.total_score,
        &mut score.reasons,
        adjustment,
        format!(
            "historical_replay_score={:.3}_affected_routes={}_candidate_no_route={}",
            replay.estimated_delta_score,
            replay.affected_route_count,
            replay.candidate_no_route_count
        ),
    );
}

fn patch_affected_node_ids(patch: &vsm_core::OrganizationalGenomePatch) -> BTreeSet<NodeId> {
    let mut affected = BTreeSet::new();
    collect_patch_affected_node_ids(patch, &mut affected);
    affected
}

fn collect_patch_affected_node_ids(
    patch: &vsm_core::OrganizationalGenomePatch,
    affected: &mut BTreeSet<NodeId>,
) {
    match patch {
        vsm_core::OrganizationalGenomePatch::AddChild { child, .. } => {
            affected.insert(child.id.clone());
        }
        vsm_core::OrganizationalGenomePatch::PromoteLeafToMetasystem { node_id, .. }
        | vsm_core::OrganizationalGenomePatch::RemoveSubtree { node_id }
        | vsm_core::OrganizationalGenomePatch::AddPromptComponent { node_id, .. }
        | vsm_core::OrganizationalGenomePatch::RemovePromptComponent { node_id, .. }
        | vsm_core::OrganizationalGenomePatch::AddTool { node_id, .. }
        | vsm_core::OrganizationalGenomePatch::RemoveTool { node_id, .. }
        | vsm_core::OrganizationalGenomePatch::SetNodeStatus { node_id, .. } => {
            affected.insert(node_id.clone());
        }
        vsm_core::OrganizationalGenomePatch::AddChannel { channel } => {
            if let Some(node_id) = &channel.from {
                affected.insert(node_id.clone());
            }
            if let Some(node_id) = &channel.to {
                affected.insert(node_id.clone());
            }
        }
        vsm_core::OrganizationalGenomePatch::RemoveChannel { .. } => {}
        vsm_core::OrganizationalGenomePatch::Batch { patches } => {
            for patch in patches {
                collect_patch_affected_node_ids(patch, affected);
            }
        }
    }
}

fn suggestion_source_key(source: &GeneSuggestionSource) -> String {
    match source {
        GeneSuggestionSource::System3StarAudit => "system3_star_audit".to_string(),
        GeneSuggestionSource::System4FutureProbe => "system4_future_probe".to_string(),
        GeneSuggestionSource::System1ResourceBargain => "system1_resource_bargain".to_string(),
        GeneSuggestionSource::System2CoordinationSignal => {
            "system2_coordination_signal".to_string()
        }
        GeneSuggestionSource::AlgedonicSignal => "algedonic_signal".to_string(),
        GeneSuggestionSource::Other(value) => format!("other:{value}"),
    }
}

fn patch_kind_key(patch: &vsm_core::OrganizationalGenomePatch) -> &'static str {
    match patch {
        vsm_core::OrganizationalGenomePatch::AddChild { .. } => "add_child",
        vsm_core::OrganizationalGenomePatch::PromoteLeafToMetasystem { .. } => {
            "promote_leaf_to_metasystem"
        }
        vsm_core::OrganizationalGenomePatch::RemoveSubtree { .. } => "remove_subtree",
        vsm_core::OrganizationalGenomePatch::AddChannel { .. } => "add_channel",
        vsm_core::OrganizationalGenomePatch::RemoveChannel { .. } => "remove_channel",
        vsm_core::OrganizationalGenomePatch::AddPromptComponent { .. } => "add_prompt_component",
        vsm_core::OrganizationalGenomePatch::RemovePromptComponent { .. } => {
            "remove_prompt_component"
        }
        vsm_core::OrganizationalGenomePatch::AddTool { .. } => "add_tool",
        vsm_core::OrganizationalGenomePatch::RemoveTool { .. } => "remove_tool",
        vsm_core::OrganizationalGenomePatch::SetNodeStatus { .. } => "set_node_status",
        vsm_core::OrganizationalGenomePatch::Batch { .. } => "batch",
    }
}

fn patch_complexity_cost(patch: &vsm_core::OrganizationalGenomePatch) -> f64 {
    match patch {
        vsm_core::OrganizationalGenomePatch::AddChild { child, .. } => {
            10.0 + node_complexity_cost(child)
        }
        vsm_core::OrganizationalGenomePatch::PromoteLeafToMetasystem { .. } => 15.0,
        vsm_core::OrganizationalGenomePatch::RemoveSubtree { .. } => 4.0,
        vsm_core::OrganizationalGenomePatch::AddChannel { .. } => 6.0,
        vsm_core::OrganizationalGenomePatch::RemoveChannel { .. } => 2.0,
        vsm_core::OrganizationalGenomePatch::AddPromptComponent { component, .. } => {
            2.0 + component.text.len() as f64 / 500.0
        }
        vsm_core::OrganizationalGenomePatch::RemovePromptComponent { .. } => 1.0,
        vsm_core::OrganizationalGenomePatch::AddTool { tool, .. } => {
            4.0 + tool.permissions.len() as f64
        }
        vsm_core::OrganizationalGenomePatch::RemoveTool { .. } => 1.0,
        vsm_core::OrganizationalGenomePatch::SetNodeStatus { .. } => 1.0,
        vsm_core::OrganizationalGenomePatch::Batch { patches } => {
            patches.iter().map(patch_complexity_cost).sum::<f64>() + 2.0
        }
    }
}

fn node_complexity_cost(node: &vsm_core::ViableNode) -> f64 {
    let prompt = &node.prompt;
    let prompt_component_count = prompt.behavior_rules.len()
        + prompt.domain_hints.len()
        + prompt.codebase_conventions.len()
        + prompt.negative_constraints.len()
        + usize::from(prompt.output_contract.is_some());
    let permission_count = node.permissions.allowed_paths.len()
        + node.permissions.denied_paths.len()
        + node.permissions.allowed_tools.len()
        + node.permissions.denied_tools.len();
    1.0 + node.tools.len() as f64 * 2.0
        + prompt_component_count as f64
        + permission_count as f64 * 0.5
        + node.channels.len() as f64
        + node.children.len() as f64
}

fn add_history_adjustment(
    score: &mut QueuedCandidateScore,
    label: &str,
    history: &[StoredTrialRecord],
    matches_record: impl Fn(&StoredTrialRecord) -> bool,
    weight: f64,
) {
    let Some((average, count, adjustment)) = history_adjustment(history, matches_record, weight)
    else {
        return;
    };
    add_score(
        &mut score.total_score,
        &mut score.reasons,
        adjustment,
        format!("{label}_avg={average:.3}_n={count}"),
    );
}

fn history_adjustment_value(
    history: &[StoredTrialRecord],
    matches_record: impl Fn(&StoredTrialRecord) -> bool,
    weight: f64,
) -> f64 {
    history_adjustment(history, matches_record, weight)
        .map(|(_, _, adjustment)| adjustment)
        .unwrap_or(0.0)
}

fn history_adjustment(
    history: &[StoredTrialRecord],
    matches_record: impl Fn(&StoredTrialRecord) -> bool,
    weight: f64,
) -> Option<(f64, usize, f64)> {
    let realized: Vec<f64> = history
        .iter()
        .filter(|completed| matches_record(completed))
        .filter_map(realized_trial_score_per_trace)
        .collect();
    if realized.is_empty() {
        return None;
    }

    let average = realized.iter().sum::<f64>() / realized.len() as f64;
    Some((average, realized.len(), average.clamp(-20.0, 20.0) * weight))
}

fn realized_trial_score_per_trace(record: &StoredTrialRecord) -> Option<f64> {
    if record.trace_count == 0 {
        return None;
    }
    if !matches!(
        record.status,
        StoredTrialStatus::Promoted | StoredTrialStatus::Pruned | StoredTrialStatus::Archived
    ) {
        return None;
    }
    Some(record.total_score / record.trace_count as f64)
}

fn add_score(
    total_score: &mut f64,
    reasons: &mut Vec<String>,
    delta: f64,
    reason: impl Into<String>,
) {
    if delta == 0.0 {
        return;
    }
    *total_score += delta;
    reasons.push(format!("{}:{delta:+.1}", reason.into()));
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use vsm_core::{
        GeneSuggestion, GeneSuggestionSource, GenomeId, LeafOperationSpec, OrganizationalGenome,
        OrganizationalGenomePatch, PromptComponent, PromptOrigin, PromptSection, RiskClass, TaskId,
        TaskPacket, TaskTrace, TrialMode, ViableNode,
    };

    use crate::TaskRouter;

    use super::*;

    fn champion_and_review_suggestion() -> (OrganizationalGenome, GeneSuggestion, NodeId) {
        let root = ViableNode::new_metasystem("root");
        let root_id = root.id.clone();
        let mut genome = OrganizationalGenome::new(root);
        let coder = ViableNode::new_leaf("coder", LeafOperationSpec::coding());
        genome.add_child(&root_id, coder).expect("coder");

        let reviewer = ViableNode::new_leaf("reviewer", LeafOperationSpec::reviewer());
        let reviewer_id = reviewer.id.clone();
        let suggestion = GeneSuggestion::new(
            root_id.clone(),
            root_id,
            GeneSuggestionSource::System3StarAudit,
            OrganizationalGenomePatch::AddChild {
                parent_id: genome.root_node_id.clone(),
                child: reviewer,
            },
            "trial reviewer",
        );

        (genome, suggestion, reviewer_id)
    }

    fn approved_review_task(suggestion_id: &vsm_core::SuggestionId) -> TaskPacket {
        let mut task = review_task();
        task.metadata
            .insert("trial_approved".to_string(), suggestion_id.to_string());
        task
    }

    fn review_task() -> TaskPacket {
        let mut task = TaskPacket::new("review task", "review a candidate change");
        task.metadata
            .insert("required_capability".to_string(), "review".to_string());
        task
    }

    fn queued_record_for_suggestion(
        genome: &OrganizationalGenome,
        suggestion: GeneSuggestion,
    ) -> StoredTrialRecord {
        StoredTrialRecord::queued(
            genome.root_node_id.clone(),
            genome.id.clone(),
            GenomeId::new(),
            suggestion,
        )
    }

    #[test]
    fn trial_route_requires_registered_candidate_worker() {
        let (genome, suggestion, reviewer_id) = champion_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let suggestion_id = suggestion.id.clone();
        let mut manager = TrialManager::default();
        manager
            .start_trial(root_id.clone(), &genome, suggestion)
            .expect("start");

        let task = approved_review_task(&suggestion_id);
        let router = TaskRouter::default();

        let without_registration = manager
            .choose_trial_route(&router, &root_id, &task)
            .expect("route check");
        assert!(without_registration.is_none());

        manager.register_candidate_worker(reviewer_id.clone());
        let with_registration = manager
            .choose_trial_route(&router, &root_id, &task)
            .expect("route check")
            .expect("trial route");

        assert_eq!(with_registration.child_id, reviewer_id);
        assert_eq!(with_registration.suggestion_id, suggestion_id);
    }

    #[test]
    fn probation_without_traffic_share_requires_trial_approval() {
        let (genome, suggestion, reviewer_id) = champion_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let mut manager = TrialManager::default();
        manager
            .start_trial(root_id.clone(), &genome, suggestion.clone())
            .expect("start");
        manager.register_candidate_worker(reviewer_id);
        let router = TaskRouter::default();

        let without_approval = manager
            .choose_trial_route(&router, &root_id, &review_task())
            .expect("route check");
        assert!(without_approval.is_none());

        let with_approval = manager
            .choose_trial_route(&router, &root_id, &approved_review_task(&suggestion.id))
            .expect("route check");
        assert!(with_approval.is_some());
    }

    #[test]
    fn direct_trial_can_route_without_approval_or_traffic_share() {
        let (genome, mut suggestion, reviewer_id) = champion_and_review_suggestion();
        suggestion.trial_mode = TrialMode::Direct;
        let root_id = genome.root_node_id.clone();
        let mut manager = TrialManager::default();
        manager
            .start_trial(root_id.clone(), &genome, suggestion)
            .expect("start");
        manager.register_candidate_worker(reviewer_id);

        let route = manager
            .choose_trial_route(&TaskRouter::default(), &root_id, &review_task())
            .expect("route check")
            .expect("direct route");
        assert_eq!(route.trial_mode, TrialMode::Direct);
        assert!(route.exposure_basis_points.is_none());
        assert!(route.exposure_bucket.is_none());
    }

    #[test]
    fn canary_trial_respects_deterministic_traffic_share() {
        let (genome, mut suggestion, reviewer_id) = champion_and_review_suggestion();
        suggestion.trial_mode = TrialMode::Canary;
        suggestion.safety_limits.max_traffic_share_basis_points = Some(0);
        let root_id = genome.root_node_id.clone();
        let mut manager = TrialManager::default();
        manager
            .start_trial(root_id.clone(), &genome, suggestion.clone())
            .expect("start");
        manager.register_candidate_worker(reviewer_id.clone());

        let router = TaskRouter::default();
        let rejected = manager
            .choose_trial_route(&router, &root_id, &review_task())
            .expect("route check");
        assert!(rejected.is_none());

        let mut full_share_manager = TrialManager::default();
        suggestion.safety_limits.max_traffic_share_basis_points = Some(10_000);
        full_share_manager
            .start_trial(root_id.clone(), &genome, suggestion)
            .expect("start");
        full_share_manager.register_candidate_worker(reviewer_id);
        let route = full_share_manager
            .choose_trial_route(&router, &root_id, &review_task())
            .expect("route check")
            .expect("canary route");
        assert_eq!(route.trial_mode, TrialMode::Canary);
        assert_eq!(route.exposure_basis_points, Some(10_000));
        assert!(route.exposure_bucket.is_some_and(|bucket| bucket < 10_000));
    }

    #[test]
    fn high_risk_canary_requires_explicit_approval() {
        let (genome, mut suggestion, reviewer_id) = champion_and_review_suggestion();
        suggestion.trial_mode = TrialMode::Canary;
        suggestion.safety_limits.max_traffic_share_basis_points = Some(10_000);
        let root_id = genome.root_node_id.clone();
        let mut manager = TrialManager::default();
        manager
            .start_trial(root_id.clone(), &genome, suggestion.clone())
            .expect("start");
        manager.register_candidate_worker(reviewer_id);
        let router = TaskRouter::default();

        let mut high_risk = review_task();
        high_risk.risk = RiskClass::Critical;
        let without_approval = manager
            .choose_trial_route(&router, &root_id, &high_risk)
            .expect("route check");
        assert!(without_approval.is_none());

        high_risk
            .metadata
            .insert("trial_approved".to_string(), suggestion.id.to_string());
        let with_approval = manager
            .choose_trial_route(&router, &root_id, &high_risk)
            .expect("route check");
        assert!(with_approval.is_some());
    }

    #[test]
    fn shadow_trial_does_not_take_control_route() {
        let (genome, mut suggestion, reviewer_id) = champion_and_review_suggestion();
        suggestion.trial_mode = TrialMode::Shadow;
        let suggestion_id = suggestion.id.clone();
        let root_id = genome.root_node_id.clone();
        let mut manager = TrialManager::default();
        manager
            .start_trial(root_id.clone(), &genome, suggestion)
            .expect("start");
        manager.register_candidate_worker(reviewer_id);

        let route = manager
            .choose_trial_route(
                &TaskRouter::default(),
                &root_id,
                &approved_review_task(&suggestion_id),
            )
            .expect("route check");
        assert!(route.is_none());

        let shadow_route = manager
            .choose_shadow_route(
                &TaskRouter::default(),
                &root_id,
                &approved_review_task(&suggestion_id),
            )
            .expect("shadow route check")
            .expect("shadow route");
        assert_eq!(shadow_route.trial_mode, TrialMode::Shadow);
        assert!(shadow_route.is_shadow);
    }

    #[test]
    fn trial_task_tagging_sets_trace_metadata_keys() {
        let (_genome, suggestion, _reviewer_id) = champion_and_review_suggestion();
        let candidate_genome_id = GenomeId::new();
        let mut task = TaskPacket::new("task", "goal");

        tag_task_for_trial(&mut task, &suggestion.id, &candidate_genome_id);

        assert_eq!(
            task.metadata.get("trial_id").map(String::as_str),
            Some(suggestion.id.as_str())
        );
        assert_eq!(
            task.metadata
                .get("related_suggestion_id")
                .map(String::as_str),
            Some(suggestion.id.as_str())
        );
        assert_eq!(
            task.metadata.get("candidate_genome_id").map(String::as_str),
            Some(candidate_genome_id.as_str())
        );
    }

    #[test]
    fn trial_route_tagging_sets_mode_and_exposure_metadata() {
        let (_genome, suggestion, reviewer_id) = champion_and_review_suggestion();
        let candidate_genome_id = GenomeId::new();
        let mut task = TaskPacket::new("task", "goal");
        let decision = TrialRouteDecision {
            child_id: reviewer_id,
            reason: "test route".to_string(),
            genome_id: candidate_genome_id.clone(),
            suggestion_id: suggestion.id.clone(),
            trial_mode: TrialMode::Canary,
            exposure_basis_points: Some(250),
            exposure_bucket: Some(42),
            is_shadow: true,
        };

        tag_task_for_trial_route(&mut task, &decision);

        assert_eq!(
            task.metadata.get("trial_mode").map(String::as_str),
            Some("canary")
        );
        assert_eq!(
            task.metadata.get("trial_route_role").map(String::as_str),
            Some("shadow")
        );
        assert_eq!(
            task.metadata.get("trial_shadow").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            task.metadata
                .get("trial_exposure_basis_points")
                .map(String::as_str),
            Some("250")
        );
        assert_eq!(
            task.metadata
                .get("trial_exposure_bucket")
                .map(String::as_str),
            Some("42")
        );
        assert_eq!(
            task.metadata.get("candidate_genome_id").map(String::as_str),
            Some(candidate_genome_id.as_str())
        );
    }

    #[test]
    fn candidate_selection_prefers_bounded_canary_over_older_unbounded_direct() {
        let (genome, mut direct_suggestion, _reviewer_id) = champion_and_review_suggestion();
        direct_suggestion.trial_mode = TrialMode::Direct;
        direct_suggestion.source = GeneSuggestionSource::Other("manual".to_string());
        let direct_record = queued_record_for_suggestion(&genome, direct_suggestion);

        let reviewer = ViableNode::new_leaf("canary-reviewer", LeafOperationSpec::reviewer());
        let mut canary_suggestion = GeneSuggestion::new(
            genome.root_node_id.clone(),
            genome.root_node_id.clone(),
            GeneSuggestionSource::AlgedonicSignal,
            OrganizationalGenomePatch::AddChild {
                parent_id: genome.root_node_id.clone(),
                child: reviewer,
            },
            "bounded canary",
        );
        canary_suggestion.trial_mode = TrialMode::Canary;
        canary_suggestion.safety_limits.max_tasks = Some(10);
        canary_suggestion
            .safety_limits
            .max_traffic_share_basis_points = Some(500);
        canary_suggestion.evidence = vec![
            "urgent pain signal".to_string(),
            "bounded low-risk exposure".to_string(),
        ];
        let mut canary_record = queued_record_for_suggestion(&genome, canary_suggestion);
        canary_record.started_at = direct_record.started_at + Duration::seconds(1);

        let direct_score = score_queued_candidate(&direct_record);
        let canary_score = score_queued_candidate(&canary_record);
        assert!(canary_score.total_score > direct_score.total_score);

        let mut candidates = vec![
            (direct_record, direct_score),
            (canary_record.clone(), canary_score),
        ];
        candidates.sort_by(compare_queued_candidates);

        assert_eq!(candidates[0].0.trial_id, canary_record.trial_id);
    }

    #[test]
    fn candidate_selection_metadata_priority_can_override_default_score() {
        let (genome, suggestion, _reviewer_id) = champion_and_review_suggestion();
        let mut record = queued_record_for_suggestion(&genome, suggestion);
        let baseline_score = score_queued_candidate(&record).total_score;
        record
            .metadata
            .insert("selection_priority".to_string(), "50".to_string());

        let boosted_score = score_queued_candidate(&record);

        assert!(boosted_score.total_score > baseline_score);
        assert!(boosted_score
            .reasons
            .iter()
            .any(|reason| reason.contains("metadata_selection_priority")));
    }

    #[test]
    fn candidate_selection_uses_realized_history_for_matching_traits() {
        let (genome, mut candidate_suggestion, _reviewer_id) = champion_and_review_suggestion();
        candidate_suggestion.trial_mode = TrialMode::Canary;
        candidate_suggestion.source = GeneSuggestionSource::System3StarAudit;
        let candidate_record = queued_record_for_suggestion(&genome, candidate_suggestion);

        let reviewer = ViableNode::new_leaf("historical-reviewer", LeafOperationSpec::reviewer());
        let mut historical_suggestion = GeneSuggestion::new(
            genome.root_node_id.clone(),
            genome.root_node_id.clone(),
            GeneSuggestionSource::System3StarAudit,
            OrganizationalGenomePatch::AddChild {
                parent_id: genome.root_node_id.clone(),
                child: reviewer,
            },
            "historical canary",
        );
        historical_suggestion.trial_mode = TrialMode::Canary;
        let mut historical = queued_record_for_suggestion(&genome, historical_suggestion);
        historical.status = StoredTrialStatus::Promoted;
        historical.trace_count = 4;
        historical.total_score = 40.0;
        historical.completed_at = Some(chrono::Utc::now());

        let baseline = score_queued_candidate(&candidate_record);
        let with_history = score_queued_candidate_with_history(&candidate_record, &[historical]);

        assert!(with_history.total_score > baseline.total_score);
        assert!(with_history
            .reasons
            .iter()
            .any(|reason| reason.contains("history_same_source")));
        assert!(with_history
            .reasons
            .iter()
            .any(|reason| reason.contains("history_same_mode")));
        assert!(with_history
            .reasons
            .iter()
            .any(|reason| reason.contains("history_same_patch")));
    }

    #[test]
    fn pareto_frontier_excludes_dominated_candidates() {
        let (genome, mut dominated_suggestion, _reviewer_id) = champion_and_review_suggestion();
        dominated_suggestion.trial_mode = TrialMode::Direct;
        dominated_suggestion.source = GeneSuggestionSource::Other("manual".to_string());
        let dominated = queued_record_for_suggestion(&genome, dominated_suggestion);

        let reviewer = ViableNode::new_leaf("dominant-reviewer", LeafOperationSpec::reviewer());
        let mut dominant_suggestion = GeneSuggestion::new(
            genome.root_node_id.clone(),
            genome.root_node_id.clone(),
            GeneSuggestionSource::AlgedonicSignal,
            OrganizationalGenomePatch::AddChild {
                parent_id: genome.root_node_id.clone(),
                child: reviewer,
            },
            "dominant bounded canary",
        );
        dominant_suggestion.trial_mode = TrialMode::Canary;
        dominant_suggestion.safety_limits.max_tasks = Some(10);
        dominant_suggestion.safety_limits.max_token_budget = Some(5_000);
        dominant_suggestion
            .safety_limits
            .max_traffic_share_basis_points = Some(500);
        dominant_suggestion.evidence.push("pain signal".to_string());
        let dominant = queued_record_for_suggestion(&genome, dominant_suggestion);

        let dominated_eval = evaluate_queued_candidate(&dominated, &[]);
        let dominant_eval = evaluate_queued_candidate(&dominant, &[]);
        assert!(candidate_dominates(
            &dominant_eval.objectives,
            &dominated_eval.objectives
        ));

        let frontier = pareto_frontier_indices(&[dominated_eval, dominant_eval]);
        assert_eq!(frontier, vec![1]);
    }

    #[test]
    fn historical_replay_scores_candidate_routes_for_matching_traces() {
        let (genome, mut suggestion, _reviewer_id) = champion_and_review_suggestion();
        suggestion.trial_mode = TrialMode::Canary;
        suggestion.safety_limits.max_tasks = Some(10);
        suggestion.safety_limits.max_traffic_share_basis_points = Some(10_000);
        let record = queued_record_for_suggestion(&genome, suggestion.clone());
        let trial = MutationTrial::from_suggestion(&genome, suggestion).expect("candidate genome");

        let mut trace = TaskTrace::started(TaskId::new(), genome.id.clone(), NodeId::new());
        trace.metadata.insert(
            "task_metadata.required_capability".to_string(),
            "review".to_string(),
        );
        trace.files_touched.push("src/lib.rs".to_string());
        trace.outcome_score = -1.0;
        trace.merged = Some(false);

        let replay = replay_candidate_against_traces(
            &TaskRouter::default(),
            &genome.root_node_id,
            &genome,
            &trial.candidate_genome,
            &record,
            &[trace],
            &FitnessWeights::default(),
        )
        .expect("replay");

        assert_eq!(replay.trace_count, 1);
        assert_eq!(replay.eligible_trace_count, 1);
        assert_eq!(replay.candidate_route_count, 1);
        assert_eq!(replay.changed_route_count, 1);
        assert_eq!(replay.affected_route_count, 1);
        assert!(replay.replay_score > 0.0);
        assert_eq!(replay.replay_score, replay.estimated_delta_score);
        assert_eq!(replay.trace_evaluations.len(), 1);
        assert_eq!(
            replay.trace_evaluations[0].status,
            OfflineReplayTraceStatus::AffectedRoute
        );

        let evaluation = evaluate_queued_candidate_with_replay(&record, &[], &replay);
        assert!(evaluation.objectives.replay_fit > 0.0);
        assert!(evaluation
            .score
            .reasons
            .iter()
            .any(|reason| reason.contains("historical_replay_score")));
    }

    #[test]
    fn historical_replay_penalizes_touching_successful_affected_routes() {
        let root = ViableNode::new_metasystem("root");
        let root_id = root.id.clone();
        let mut genome = OrganizationalGenome::new(root);
        let coder = ViableNode::new_leaf("coder", LeafOperationSpec::coding());
        let coder_id = coder.id.clone();
        genome.add_child(&root_id, coder).expect("coder");

        let mut suggestion = GeneSuggestion::new(
            root_id.clone(),
            root_id.clone(),
            GeneSuggestionSource::System3StarAudit,
            OrganizationalGenomePatch::AddPromptComponent {
                node_id: coder_id.clone(),
                section: PromptSection::BehaviorRules,
                component: PromptComponent {
                    id: "stable-route-test".to_string(),
                    text: "preserve successful coding behavior".to_string(),
                    tags: vec!["replay-test".to_string()],
                    origin: PromptOrigin::Mutation,
                    active: true,
                },
            },
            "tune coder prompt",
        );
        suggestion.trial_mode = TrialMode::Canary;
        suggestion.safety_limits.max_tasks = Some(10);
        suggestion.safety_limits.max_traffic_share_basis_points = Some(10_000);
        let record = queued_record_for_suggestion(&genome, suggestion.clone());
        let trial = MutationTrial::from_suggestion(&genome, suggestion).expect("candidate genome");

        let mut trace = TaskTrace::started(TaskId::new(), genome.id.clone(), coder_id.clone());
        trace.files_touched.push("src/lib.rs".to_string());
        trace.tests_run.push("cargo test".to_string());
        trace.tests_passed = Some(true);
        trace.merged = Some(true);
        trace.outcome_score = 1.0;

        let replay = replay_candidate_against_traces(
            &TaskRouter::default(),
            &genome.root_node_id,
            &genome,
            &trial.candidate_genome,
            &record,
            &[trace],
            &FitnessWeights::default(),
        )
        .expect("replay");

        assert_eq!(replay.trace_count, 1);
        assert_eq!(replay.eligible_trace_count, 1);
        assert_eq!(replay.champion_route_count, 1);
        assert_eq!(replay.candidate_route_count, 1);
        assert_eq!(replay.changed_route_count, 0);
        assert_eq!(replay.affected_route_count, 1);
        assert!(replay.estimated_delta_score < 0.0);
        assert_eq!(replay.replay_score, replay.estimated_delta_score);
        assert_eq!(
            replay.trace_evaluations[0].status,
            OfflineReplayTraceStatus::AffectedRoute
        );

        let evaluation = evaluate_queued_candidate_with_replay(&record, &[], &replay);
        assert!(evaluation.objectives.replay_fit < 0.0);
    }
}
