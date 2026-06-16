use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use vsm_core::{
    FitnessWeights, GeneSuggestion, GenomeId, NodeId, OrganizationalGenome, TaskPacket, TaskTrace,
};
use vsm_ledger::{StoredTrialDecision, StoredTrialRecord, StoredTrialStatus};
use vsm_runtime::{MutationTrial, TrialConfig, TrialDecision, TrialEvaluation};

use crate::ControllerError;

const TRIAL_ID_METADATA_KEY: &str = "trial_id";
const TRIAL_APPROVED_METADATA_KEY: &str = "trial_approved";
const TRIAL_TASK_CLASS_METADATA_KEY: &str = "task_class";
const TRIAL_SUGGESTION_METADATA_KEY: &str = "related_suggestion_id";

#[derive(Clone, Debug)]
pub struct TrialRouteDecision {
    pub child_id: NodeId,
    pub reason: String,
    pub genome_id: GenomeId,
    pub suggestion_id: vsm_core::SuggestionId,
}

#[derive(Clone, Debug)]
pub struct StartedTrial {
    pub suggestion_id: vsm_core::SuggestionId,
    pub base_genome_id: GenomeId,
    pub candidate_genome_id: GenomeId,
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
    }

    fn task_passes_safety_limits(&self, task: &TaskPacket, trial: &MutationTrial) -> bool {
        let limits = &trial.suggestion.safety_limits;

        if limits.requires_approval && !task_has_trial_approval(task, &trial.suggestion.id) {
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

        if limits.max_traffic_share_basis_points.is_none()
            && !task_has_trial_approval(task, &trial.suggestion.id)
        {
            return false;
        }

        true
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

#[cfg(test)]
mod tests {
    use vsm_core::{
        GeneSuggestion, GeneSuggestionSource, GenomeId, LeafOperationSpec, OrganizationalGenome,
        OrganizationalGenomePatch, TaskPacket, ViableNode,
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
        let mut task = TaskPacket::new("review task", "review a candidate change");
        task.metadata
            .insert("required_capability".to_string(), "review".to_string());
        task.metadata
            .insert("trial_approved".to_string(), suggestion_id.to_string());
        task
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
}
