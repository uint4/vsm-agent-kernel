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

#[derive(Clone, Debug, PartialEq)]
pub enum TrialDecision {
    Continue,
    Promote,
    Prune,
}

#[derive(Clone, Debug)]
pub struct TrialEvaluation {
    pub decision: TrialDecision,
    pub trace_count: usize,
    pub total_score: f64,
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
        self.evaluate(config, weights).decision
    }

    pub fn evaluate(&self, config: &TrialConfig, weights: &FitnessWeights) -> TrialEvaluation {
        if self.traces.len() < config.min_tasks_before_decision as usize {
            return TrialEvaluation {
                decision: TrialDecision::Continue,
                trace_count: self.traces.len(),
                total_score: 0.0,
            };
        }

        let summaries =
            summarize_direct_fitness(&self.traces, weights, &BTreeMap::<NodeId, f64>::new());
        let total: f64 = summaries.values().map(|s| s.final_score).sum();

        let decision = if total >= config.promote_margin {
            TrialDecision::Promote
        } else if total <= config.prune_below {
            TrialDecision::Prune
        } else {
            TrialDecision::Continue
        };

        TrialEvaluation {
            decision,
            trace_count: self.traces.len(),
            total_score: total,
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

#[cfg(test)]
mod tests {
    use vsm_core::{
        GeneSuggestion, GeneSuggestionSource, LeafOperationSpec, OrganizationalGenome,
        OrganizationalGenomePatch, TaskId, TaskTrace, ViableNode,
    };

    use super::*;

    fn trial_with_traces(scores: &[f64]) -> MutationTrial {
        let root = ViableNode::new_metasystem("root");
        let root_id = root.id.clone();
        let mut genome = OrganizationalGenome::new(root);
        let coder = ViableNode::new_leaf("coder", LeafOperationSpec::coding());
        let coder_id = genome.add_child(&root_id, coder).expect("child");

        let reviewer = ViableNode::new_leaf("reviewer", LeafOperationSpec::reviewer());
        let suggestion = GeneSuggestion::new(
            root_id.clone(),
            root_id,
            GeneSuggestionSource::System3StarAudit,
            OrganizationalGenomePatch::AddChild {
                parent_id: genome.root_node_id.clone(),
                child: reviewer,
            },
            "test suggestion",
        );

        let mut trial = MutationTrial::from_suggestion(&genome, suggestion).expect("trial");
        for score in scores {
            let mut trace = TaskTrace::started(
                TaskId::new(),
                trial.candidate_genome.id.clone(),
                coder_id.clone(),
            );
            trace.outcome_score = *score;
            trace
                .related_suggestion_ids
                .push(trial.suggestion.id.clone());
            trial.record_trace(trace);
        }
        trial
    }

    #[test]
    fn trial_continues_before_minimum_tasks() {
        let trial = trial_with_traces(&[100.0]);
        let evaluation = trial.evaluate(
            &TrialConfig {
                min_tasks_before_decision: 2,
                promote_margin: 1.0,
                prune_below: -1.0,
            },
            &FitnessWeights::default(),
        );

        assert_eq!(evaluation.decision, TrialDecision::Continue);
        assert_eq!(evaluation.trace_count, 1);
    }

    #[test]
    fn trial_promotes_above_margin() {
        let trial = trial_with_traces(&[2.0, 2.0]);
        let evaluation = trial.evaluate(
            &TrialConfig {
                min_tasks_before_decision: 2,
                promote_margin: 3.0,
                prune_below: -10.0,
            },
            &FitnessWeights::default(),
        );

        assert_eq!(evaluation.decision, TrialDecision::Promote);
    }

    #[test]
    fn trial_prunes_below_threshold() {
        let trial = trial_with_traces(&[-6.0, -6.0]);
        let evaluation = trial.evaluate(
            &TrialConfig {
                min_tasks_before_decision: 2,
                promote_margin: 3.0,
                prune_below: -10.0,
            },
            &FitnessWeights::default(),
        );

        assert_eq!(evaluation.decision, TrialDecision::Prune);
    }
}
