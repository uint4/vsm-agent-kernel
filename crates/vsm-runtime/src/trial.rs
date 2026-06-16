use std::collections::BTreeMap;
use vsm_core::{
    summarize_direct_fitness, FitnessWeights, GeneSuggestion, GenomeId, NodeId,
    OrganizationalGenome, PatchError, TaskTrace,
};

#[derive(Clone, Debug)]
pub struct TrialConfig {
    pub min_tasks_before_decision: u64,
    pub promote_margin: f64,
    pub prune_below: f64,
}

impl Default for TrialConfig {
    fn default() -> Self {
        Self {
            min_tasks_before_decision: 10,
            promote_margin: 1.0,
            prune_below: -10.0,
        }
    }
}

#[derive(Clone, Debug)]
pub enum TrialDecision {
    Continue,
    Promote,
    Prune,
}

#[derive(Clone, Debug)]
pub struct MutationTrial {
    pub suggestion: GeneSuggestion,
    pub base_genome_id: GenomeId,
    pub candidate_genome: OrganizationalGenome,
    pub traces: Vec<TaskTrace>,
}

impl MutationTrial {
    pub fn from_suggestion(
        base: &OrganizationalGenome,
        suggestion: GeneSuggestion,
    ) -> Result<Self, PatchError> {
        let mut candidate = base.clone();
        suggestion.patch.apply(&mut candidate)?;
        candidate.lineage.parent_genome_ids.push(base.id.clone());
        candidate
            .lineage
            .mutation_ids
            .push(suggestion.id.to_string());
        candidate.id = GenomeId::new();

        Ok(Self {
            suggestion,
            base_genome_id: base.id.clone(),
            candidate_genome: candidate,
            traces: vec![],
        })
    }

    pub fn record_trace(&mut self, trace: TaskTrace) {
        self.traces.push(trace);
    }

    pub fn decision(&self, config: &TrialConfig, weights: &FitnessWeights) -> TrialDecision {
        if self.traces.len() < config.min_tasks_before_decision as usize {
            return TrialDecision::Continue;
        }

        let summaries =
            summarize_direct_fitness(&self.traces, weights, &BTreeMap::<NodeId, f64>::new());
        let total: f64 = summaries.values().map(|s| s.final_score).sum();

        if total >= config.promote_margin {
            TrialDecision::Promote
        } else if total <= config.prune_below {
            TrialDecision::Prune
        } else {
            TrialDecision::Continue
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct TrialLedger {
    pub active: BTreeMap<String, MutationTrial>,
    pub archived: BTreeMap<String, MutationTrial>,
}

impl TrialLedger {
    pub fn add_trial(&mut self, trial: MutationTrial) {
        self.active.insert(trial.suggestion.id.to_string(), trial);
    }

    pub fn finish_trial(&mut self, suggestion_id: &str) -> Option<MutationTrial> {
        let trial = self.active.remove(suggestion_id)?;
        self.archived
            .insert(suggestion_id.to_string(), trial.clone());
        Some(trial)
    }
}
