use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use vsm_core::{
    score_trace, GeneSuggestion, GeneSuggestionSource, LeafOperationSpec, NodeId,
    NodeLifecycleStatus, OrganizationalGenome, OrganizationalGenomePatch, PromptComponent,
    PromptOrigin, PromptSection, SuggestionId, System5Policy, TaskTrace, TrialMode, ViableNode,
};
use vsm_ledger::{
    EvolutionGenerationRecord, PopulationArchiveRecord, PopulationArchiveStatus, StoredTrialRecord,
    StoredTrialStatus,
};

const EVOLUTION_SOURCE: &str = "system3_evolution_generation";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EvolutionPolicy {
    pub name: String,
    pub population_size: usize,
    pub elite_count: usize,
    pub max_offspring_per_generation: usize,
    pub recent_trace_limit: usize,
    pub min_pressure_traces: usize,
    pub failure_ratio_for_review: f64,
    pub test_failure_ratio_for_tester: f64,
    pub high_average_token_threshold: u64,
    pub probation_max_tasks: u64,
    pub probation_token_budget: u64,
    pub canary_traffic_share_basis_points: u16,
    pub retirement_min_tasks: u64,
    pub retirement_average_score_below: f64,
}

impl Default for EvolutionPolicy {
    fn default() -> Self {
        Self {
            name: "system3_deterministic_ga_v1".to_string(),
            population_size: 6,
            elite_count: 4,
            max_offspring_per_generation: 4,
            recent_trace_limit: 200,
            min_pressure_traces: 5,
            failure_ratio_for_review: 0.25,
            test_failure_ratio_for_tester: 0.20,
            high_average_token_threshold: 120_000,
            probation_max_tasks: 10,
            probation_token_budget: 100_000,
            canary_traffic_share_basis_points: 1_000,
            retirement_min_tasks: 5,
            retirement_average_score_below: -5.0,
        }
    }
}

#[derive(Clone, Debug)]
pub struct EvolutionPlan {
    pub record: EvolutionGenerationRecord,
    pub suggestions: Vec<GeneSuggestion>,
}

#[derive(Clone, Debug)]
struct ParentCandidate {
    record: StoredTrialRecord,
    score: f64,
}

pub fn plan_evolution_generation(
    controller_node_id: &NodeId,
    generation: u64,
    champion: &OrganizationalGenome,
    completed_trials: &[StoredTrialRecord],
    population_archive: &[PopulationArchiveRecord],
    queued_trials: &[StoredTrialRecord],
    recent_traces: &[TaskTrace],
    policy: &EvolutionPolicy,
) -> Option<EvolutionPlan> {
    let capacity = policy
        .population_size
        .saturating_sub(queued_trials.len())
        .min(policy.max_offspring_per_generation);
    if capacity == 0 {
        return None;
    }

    let parent_candidates = select_parent_candidates(completed_trials, population_archive, policy);
    let parent_trial_ids = parent_candidates
        .iter()
        .map(|candidate| candidate.record.trial_id.clone())
        .collect::<Vec<_>>();
    let mut suggestions = Vec::new();

    maybe_push(
        &mut suggestions,
        capacity,
        review_growth_suggestion(
            controller_node_id,
            generation,
            champion,
            recent_traces,
            policy,
            &parent_trial_ids,
        ),
    );
    maybe_push(
        &mut suggestions,
        capacity,
        tester_growth_suggestion(
            controller_node_id,
            generation,
            champion,
            recent_traces,
            policy,
            &parent_trial_ids,
        ),
    );
    maybe_push(
        &mut suggestions,
        capacity,
        prompt_parsimony_suggestion(
            controller_node_id,
            generation,
            champion,
            recent_traces,
            policy,
            &parent_trial_ids,
        ),
    );
    maybe_push(
        &mut suggestions,
        capacity,
        retirement_suggestion(
            controller_node_id,
            generation,
            champion,
            recent_traces,
            policy,
            &parent_trial_ids,
        ),
    );
    maybe_push(
        &mut suggestions,
        capacity,
        recombination_suggestion(
            controller_node_id,
            generation,
            champion,
            &parent_candidates,
            policy,
        ),
    );

    if suggestions.is_empty() {
        return None;
    }

    let offspring_trial_ids = suggestions
        .iter()
        .map(|suggestion| suggestion.id.clone())
        .collect::<Vec<_>>();
    let mut operator_counts = BTreeMap::new();
    for suggestion in &suggestions {
        *operator_counts
            .entry(suggestion_operator(suggestion).to_string())
            .or_insert(0) += 1;
    }
    let mut record = EvolutionGenerationRecord::new(
        controller_node_id.clone(),
        generation,
        champion.id.clone(),
        policy.name.clone(),
        parent_trial_ids,
        offspring_trial_ids,
        operator_counts,
    );
    record.metadata.insert(
        "selection_policy".to_string(),
        "elite_empirical_score_plus_trace_pressure_v1".to_string(),
    );
    record.metadata.insert(
        "recent_trace_count".to_string(),
        recent_traces.len().to_string(),
    );
    record.metadata.insert(
        "queued_candidate_count".to_string(),
        queued_trials.len().to_string(),
    );

    Some(EvolutionPlan {
        record,
        suggestions,
    })
}

pub fn suggestion_operator(suggestion: &GeneSuggestion) -> &str {
    suggestion
        .evidence
        .iter()
        .find_map(|evidence| evidence.strip_prefix("evolution_operator="))
        .unwrap_or("unknown")
}

fn maybe_push(
    suggestions: &mut Vec<GeneSuggestion>,
    capacity: usize,
    suggestion: Option<GeneSuggestion>,
) {
    if suggestions.len() < capacity {
        if let Some(suggestion) = suggestion {
            suggestions.push(suggestion);
        }
    }
}

fn select_parent_candidates(
    completed_trials: &[StoredTrialRecord],
    population_archive: &[PopulationArchiveRecord],
    policy: &EvolutionPolicy,
) -> Vec<ParentCandidate> {
    let archive_score_by_trial = population_archive
        .iter()
        .filter(|record| archive_is_elite(&record.status))
        .map(|record| (record.trial_id.clone(), record.selection_score))
        .collect::<BTreeMap<_, _>>();
    let mut parents = completed_trials
        .iter()
        .filter(|record| {
            matches!(
                record.status,
                StoredTrialStatus::Promoted | StoredTrialStatus::Pruned
            )
        })
        .map(|record| ParentCandidate {
            record: record.clone(),
            score: archive_score_by_trial
                .get(&record.trial_id)
                .copied()
                .unwrap_or(record.total_score),
        })
        .collect::<Vec<_>>();
    parents.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| right.record.updated_at.cmp(&left.record.updated_at))
    });
    parents.truncate(policy.elite_count);
    parents
}

fn review_growth_suggestion(
    controller_node_id: &NodeId,
    generation: u64,
    champion: &OrganizationalGenome,
    recent_traces: &[TaskTrace],
    policy: &EvolutionPolicy,
    parent_trial_ids: &[SuggestionId],
) -> Option<GeneSuggestion> {
    if recent_traces.len() < policy.min_pressure_traces {
        return None;
    }
    let failure_ratio = ratio(recent_traces, trace_failed);
    if failure_ratio < policy.failure_ratio_for_review {
        return None;
    }
    if parent_has_leaf_capability(champion, controller_node_id, |node| {
        node.capabilities().can_review
    }) {
        return None;
    }

    let mut reviewer =
        ViableNode::new_leaf("evolved-review-probation", LeafOperationSpec::reviewer());
    reviewer.system_5 = System5Policy {
        identity: "Probationary reviewer generated by System 3 evolution.".to_string(),
        values: vec!["reduce failed or reverted coding tasks".to_string()],
        non_negotiable_constraints: vec!["Review and test only; do not write code.".to_string()],
        denied_capabilities: vec!["write_code".to_string()],
    };
    reviewer.status = NodeLifecycleStatus::Probation;
    reviewer
        .metadata
        .insert("evolution_generation".to_string(), generation.to_string());
    reviewer.metadata.insert(
        "evolution_operator".to_string(),
        "add_child_reviewer".to_string(),
    );

    bounded_suggestion(
        controller_node_id,
        generation,
        "add_child_reviewer",
        OrganizationalGenomePatch::AddChild {
            parent_id: controller_node_id.clone(),
            child: reviewer,
        },
        "Recent failed/reverted traces justify trying a probationary review child.",
        vec![format!("failure_ratio={failure_ratio:.3}")],
        policy,
        parent_trial_ids,
    )
}

fn tester_growth_suggestion(
    controller_node_id: &NodeId,
    generation: u64,
    champion: &OrganizationalGenome,
    recent_traces: &[TaskTrace],
    policy: &EvolutionPolicy,
    parent_trial_ids: &[SuggestionId],
) -> Option<GeneSuggestion> {
    if recent_traces.len() < policy.min_pressure_traces {
        return None;
    }
    let test_failure_ratio = ratio(recent_traces, |trace| trace.tests_passed == Some(false));
    if test_failure_ratio < policy.test_failure_ratio_for_tester {
        return None;
    }
    if parent_has_leaf_capability(champion, controller_node_id, |node| {
        node.capabilities().can_run_tests && !node.capabilities().can_write_code
    }) {
        return None;
    }

    let mut tester = ViableNode::new_leaf("evolved-test-probation", LeafOperationSpec::tester());
    tester.system_5 = System5Policy {
        identity: "Probationary tester generated by System 3 evolution.".to_string(),
        values: vec!["reduce repeated test failures before integration".to_string()],
        non_negotiable_constraints: vec![
            "Run and interpret tests only; do not write code.".to_string()
        ],
        denied_capabilities: vec!["write_code".to_string()],
    };
    tester.status = NodeLifecycleStatus::Probation;
    tester
        .metadata
        .insert("evolution_generation".to_string(), generation.to_string());
    tester.metadata.insert(
        "evolution_operator".to_string(),
        "add_child_tester".to_string(),
    );

    bounded_suggestion(
        controller_node_id,
        generation,
        "add_child_tester",
        OrganizationalGenomePatch::AddChild {
            parent_id: controller_node_id.clone(),
            child: tester,
        },
        "Recent test failures justify trying a probationary testing child.",
        vec![format!("test_failure_ratio={test_failure_ratio:.3}")],
        policy,
        parent_trial_ids,
    )
}

fn prompt_parsimony_suggestion(
    controller_node_id: &NodeId,
    generation: u64,
    champion: &OrganizationalGenome,
    recent_traces: &[TaskTrace],
    policy: &EvolutionPolicy,
    parent_trial_ids: &[SuggestionId],
) -> Option<GeneSuggestion> {
    if recent_traces.len() < policy.min_pressure_traces {
        return None;
    }
    let average_tokens = recent_traces
        .iter()
        .map(TaskTrace::token_total)
        .sum::<u64>()
        / recent_traces.len() as u64;
    if average_tokens < policy.high_average_token_threshold {
        return None;
    }
    let parent = champion.get_node(controller_node_id).ok()?;
    if prompt_has_tag(parent, "evolution_parsimony_guardrail") {
        return None;
    }

    let component = PromptComponent {
        id: format!("evolution-parsimony-{generation}"),
        text: "Prefer the smallest task/context/authority bundle that can satisfy acceptance criteria; split or retrieve context only when the expected reliability gain exceeds coordination and token cost.".to_string(),
        tags: vec![
            "evolution".to_string(),
            "evolution_parsimony_guardrail".to_string(),
        ],
        origin: PromptOrigin::Mutation,
        active: true,
    };

    bounded_suggestion(
        controller_node_id,
        generation,
        "add_prompt_parsimony_guardrail",
        OrganizationalGenomePatch::AddPromptComponent {
            node_id: controller_node_id.clone(),
            section: PromptSection::BehaviorRules,
            component,
        },
        "High average token use justifies trialing a parsimony guardrail.",
        vec![format!("average_tokens={average_tokens}")],
        policy,
        parent_trial_ids,
    )
}

fn retirement_suggestion(
    controller_node_id: &NodeId,
    generation: u64,
    champion: &OrganizationalGenome,
    recent_traces: &[TaskTrace],
    policy: &EvolutionPolicy,
    parent_trial_ids: &[SuggestionId],
) -> Option<GeneSuggestion> {
    let parent = champion.get_node(controller_node_id).ok()?;
    let active_children = parent
        .children
        .iter()
        .filter_map(|child_id| champion.get_node(child_id).ok())
        .filter(|child| child.status != NodeLifecycleStatus::Retired)
        .collect::<Vec<_>>();
    if active_children.len() <= 1 {
        return None;
    }

    let child_ids = active_children
        .iter()
        .map(|child| child.id.clone())
        .collect::<BTreeSet<_>>();
    let mut scores: BTreeMap<NodeId, (u64, f64)> = BTreeMap::new();
    let weights = vsm_core::FitnessWeights::default();
    for trace in recent_traces {
        if !child_ids.contains(&trace.assigned_node_id) {
            continue;
        }
        let entry = scores.entry(trace.assigned_node_id.clone()).or_default();
        entry.0 += 1;
        entry.1 += score_trace(trace, &weights);
    }

    let worst = scores
        .into_iter()
        .filter(|(_, (count, _))| *count >= policy.retirement_min_tasks)
        .map(|(node_id, (count, total_score))| (node_id, count, total_score / count as f64))
        .min_by(|left, right| {
            left.2
                .partial_cmp(&right.2)
                .unwrap_or(std::cmp::Ordering::Equal)
        })?;
    if worst.2 > policy.retirement_average_score_below {
        return None;
    }

    bounded_suggestion(
        controller_node_id,
        generation,
        "retire_low_fitness_child",
        OrganizationalGenomePatch::SetNodeStatus {
            node_id: worst.0.clone(),
            status: NodeLifecycleStatus::Retired,
        },
        "A child has sustained poor rolling direct fitness; trial retiring it from routing.",
        vec![
            format!("candidate_child={}", worst.0),
            format!("child_task_count={}", worst.1),
            format!("child_average_score={:.3}", worst.2),
        ],
        policy,
        parent_trial_ids,
    )
}

fn recombination_suggestion(
    controller_node_id: &NodeId,
    generation: u64,
    champion: &OrganizationalGenome,
    parents: &[ParentCandidate],
    policy: &EvolutionPolicy,
) -> Option<GeneSuggestion> {
    for left_index in 0..parents.len() {
        for right_index in (left_index + 1)..parents.len() {
            let left = &parents[left_index].record;
            let right = &parents[right_index].record;
            let patch = OrganizationalGenomePatch::Batch {
                patches: vec![
                    left.suggestion.patch.clone(),
                    right.suggestion.patch.clone(),
                ],
            };
            if !patch_applies(champion, &patch) {
                continue;
            }
            let parent_ids = vec![left.trial_id.clone(), right.trial_id.clone()];
            return bounded_suggestion(
                controller_node_id,
                generation,
                "recombine_elite_parent_patches",
                patch,
                "Compatible elite parent patches can be trialed together as a recombined offspring.",
                vec![
                    format!("left_parent={}", left.trial_id),
                    format!("right_parent={}", right.trial_id),
                ],
                policy,
                &parent_ids,
            );
        }
    }
    None
}

fn bounded_suggestion(
    controller_node_id: &NodeId,
    generation: u64,
    operator: &str,
    patch: OrganizationalGenomePatch,
    hypothesis: impl Into<String>,
    mut evidence: Vec<String>,
    policy: &EvolutionPolicy,
    parent_trial_ids: &[SuggestionId],
) -> Option<GeneSuggestion> {
    let mut suggestion = GeneSuggestion::new(
        controller_node_id.clone(),
        controller_node_id.clone(),
        GeneSuggestionSource::Other(EVOLUTION_SOURCE.to_string()),
        patch,
        hypothesis,
    );
    suggestion.trial_mode = TrialMode::Canary;
    suggestion.safety_limits.max_tasks = Some(policy.probation_max_tasks);
    suggestion.safety_limits.max_token_budget = Some(policy.probation_token_budget);
    suggestion.safety_limits.max_traffic_share_basis_points =
        Some(policy.canary_traffic_share_basis_points);
    suggestion.measurement_plan.window_tasks = Some(policy.probation_max_tasks);
    suggestion
        .measurement_plan
        .success_metrics
        .push("marginal_subtree_fitness".to_string());
    suggestion
        .measurement_plan
        .failure_metrics
        .push("coordination_overhead".to_string());
    suggestion
        .evidence
        .push(format!("evolution_generation={generation}"));
    suggestion
        .evidence
        .push(format!("evolution_operator={operator}"));
    if !parent_trial_ids.is_empty() {
        suggestion.evidence.push(format!(
            "evolution_parent_trial_ids={}",
            parent_trial_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    suggestion.evidence.append(&mut evidence);
    Some(suggestion)
}

fn ratio(traces: &[TaskTrace], predicate: impl Fn(&TaskTrace) -> bool) -> f64 {
    if traces.is_empty() {
        return 0.0;
    }
    let count = traces.iter().filter(|trace| predicate(trace)).count();
    count as f64 / traces.len() as f64
}

fn trace_failed(trace: &TaskTrace) -> bool {
    trace.merged == Some(false)
        || trace.reverted == Some(true)
        || trace.post_merge_regression == Some(true)
        || trace.human_override == Some(true)
        || trace.tests_passed == Some(false)
        || trace.review_passed == Some(false)
}

fn parent_has_leaf_capability(
    champion: &OrganizationalGenome,
    controller_node_id: &NodeId,
    predicate: impl Fn(&ViableNode) -> bool,
) -> bool {
    champion
        .get_node(controller_node_id)
        .ok()
        .into_iter()
        .flat_map(|parent| parent.children.iter())
        .filter_map(|child_id| champion.get_node(child_id).ok())
        .any(|child| {
            child.status != NodeLifecycleStatus::Retired && child.is_leaf() && predicate(child)
        })
}

fn prompt_has_tag(node: &ViableNode, tag: &str) -> bool {
    node.prompt
        .behavior_rules
        .iter()
        .chain(node.prompt.domain_hints.iter())
        .chain(node.prompt.codebase_conventions.iter())
        .chain(node.prompt.negative_constraints.iter())
        .chain(node.prompt.output_contract.iter())
        .any(|component| {
            component.active && component.tags.iter().any(|candidate| candidate == tag)
        })
}

fn patch_applies(champion: &OrganizationalGenome, patch: &OrganizationalGenomePatch) -> bool {
    let mut candidate = champion.clone();
    patch.apply(&mut candidate).is_ok()
}

fn archive_is_elite(status: &PopulationArchiveStatus) -> bool {
    matches!(
        status,
        PopulationArchiveStatus::SelectedForTrial
            | PopulationArchiveStatus::ParetoFrontier
            | PopulationArchiveStatus::Promoted
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use vsm_core::{GenomeId, TaskId};

    fn champion() -> (OrganizationalGenome, NodeId, NodeId) {
        let root = ViableNode::new_metasystem("root");
        let root_id = root.id.clone();
        let mut genome = OrganizationalGenome::new(root);
        let coder = ViableNode::new_leaf("coder", LeafOperationSpec::coding());
        let coder_id = genome.add_child(&root_id, coder).expect("coder");
        (genome, root_id, coder_id)
    }

    fn failed_trace(genome_id: GenomeId, node_id: NodeId) -> TaskTrace {
        let mut trace = TaskTrace::started(TaskId::new(), genome_id, node_id);
        trace.merged = Some(false);
        trace.tests_passed = Some(false);
        trace.outcome_score = -8.0;
        trace.input_tokens = 10_000;
        trace.output_tokens = 2_000;
        trace
    }

    #[test]
    fn generation_adds_reviewer_from_failure_pressure() {
        let (genome, root_id, coder_id) = champion();
        let traces = (0..5)
            .map(|_| failed_trace(genome.id.clone(), coder_id.clone()))
            .collect::<Vec<_>>();

        let plan = plan_evolution_generation(
            &root_id,
            1,
            &genome,
            &[],
            &[],
            &[],
            &traces,
            &EvolutionPolicy::default(),
        )
        .expect("plan");

        assert!(plan
            .suggestions
            .iter()
            .any(|suggestion| suggestion_operator(suggestion) == "add_child_reviewer"));
        assert_eq!(plan.record.generation, 1);
        assert_eq!(
            plan.record.offspring_trial_ids.len(),
            plan.suggestions.len()
        );
    }

    #[test]
    fn generation_respects_existing_reviewer() {
        let (mut genome, root_id, coder_id) = champion();
        genome
            .add_child(
                &root_id,
                ViableNode::new_leaf("reviewer", LeafOperationSpec::reviewer()),
            )
            .expect("reviewer");
        let traces = (0..5)
            .map(|_| failed_trace(genome.id.clone(), coder_id.clone()))
            .collect::<Vec<_>>();

        let plan = plan_evolution_generation(
            &root_id,
            1,
            &genome,
            &[],
            &[],
            &[],
            &traces,
            &EvolutionPolicy::default(),
        );

        assert!(plan
            .into_iter()
            .flat_map(|plan| plan.suggestions)
            .all(|suggestion| suggestion_operator(&suggestion) != "add_child_reviewer"));
    }
}
