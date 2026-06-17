use crate::{
    compare_queued_candidate_evaluations, evaluate_queued_candidate_with_replay,
    pareto_frontier_indices, plan_evolution_generation, replay_candidate_against_traces,
    suggestion_operator, tag_task_for_trial_route, trial_decision_key, ControllerError,
    DirectiveTaskMapper, EvolutionPolicy, System3StarAuditor, TaskRouter, TrialManager,
    OFFLINE_REPLAY_VERSION,
};
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tokio::sync::RwLock;
use vsm_core::{
    envelope_for_task, envelope_for_task_result, AuditReport, AuditRequest, BuiltinPayloadType,
    ChannelPriority, Command as VsmCommand, Directive, EnvironmentSignal, FitnessWeights,
    GeneSuggestion, GeneSuggestionSource, ManagementOperationDirective, MessageEnvelope, NodeId,
    OperationHandoff, OrganizationalGenome, ResourceAllocationDecision, ResourceAllocationStatus,
    ResourceBargain, RiskClass, Subscription, System2CoordinationKind, System2CoordinationSignal,
    TaskOutcomeStatus, TaskPacket, TaskResult, TaskTrace, ThreeFourHomeostatBalance,
    ThreeFourHomeostatSignal, Transport, VsmChannelType,
};
use vsm_ledger::{
    CandidateObjectiveSnapshot, EventFilter, EvolutionGenerationRecord, GenomeSnapshot,
    GenomeSnapshotRole, Ledger, LedgerEvent, LedgerEventKind, PopulationArchiveRecord,
    PopulationArchiveStatus, StoredTrialRecord, TraceWindow,
};
use vsm_runtime::{TrialConfig, TrialDecision, TrialEvaluation};

pub type SharedGenome = Arc<RwLock<OrganizationalGenome>>;

#[derive(Clone, Debug)]
pub struct ControllerConfig {
    pub queue_name: Option<String>,
    pub durable_subscription: bool,
    pub subscription_channels: Vec<VsmChannelType>,
    pub publish_channel: VsmChannelType,
    pub append_message_events: bool,
    pub mapper: DirectiveTaskMapper,
    pub router: TaskRouter,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            queue_name: None,
            durable_subscription: false,
            subscription_channels: vec![
                VsmChannelType::OperationToEnvironment,
                VsmChannelType::ResourceBargaining,
                VsmChannelType::Command,
                VsmChannelType::System2Coordination,
                VsmChannelType::Audit,
                VsmChannelType::ThreeFourHomeostat,
                VsmChannelType::ManagementToOperation,
                VsmChannelType::FutureProbeToEnvironment,
                VsmChannelType::EnvironmentToEnvironment,
                VsmChannelType::Algedonic,
            ],
            publish_channel: VsmChannelType::ResourceBargaining,
            append_message_events: true,
            mapper: DirectiveTaskMapper::default(),
            router: TaskRouter::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum ControllerHandleOutcome {
    Ignored,
    RoutedTask {
        task: TaskPacket,
        child_id: NodeId,
        reason: String,
    },
    DecomposedTasks {
        tasks: Vec<RoutedTaskOutcome>,
    },
    ReceivedTaskResult(TaskResult),
}

#[derive(Clone, Debug)]
pub struct RoutedTaskOutcome {
    pub task: TaskPacket,
    pub child_id: NodeId,
    pub reason: String,
}

pub struct ControllerRuntime {
    node_id: NodeId,
    genome: SharedGenome,
    transport: Arc<dyn Transport>,
    ledger: Arc<dyn Ledger>,
    trials: Arc<RwLock<TrialManager>>,
    config: ControllerConfig,
}

impl ControllerRuntime {
    pub fn new(
        node_id: NodeId,
        genome: SharedGenome,
        transport: Arc<dyn Transport>,
        ledger: Arc<dyn Ledger>,
    ) -> Self {
        Self {
            node_id,
            genome,
            transport,
            ledger,
            trials: Arc::new(RwLock::new(TrialManager::default())),
            config: ControllerConfig::default(),
        }
    }

    pub fn with_config(mut self, config: ControllerConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_trial_config(self, config: TrialConfig, weights: FitnessWeights) -> Self {
        Self {
            trials: Arc::new(RwLock::new(TrialManager::with_config(config, weights))),
            ..self
        }
    }

    pub async fn start_trial_from_suggestion(
        &self,
        suggestion: GeneSuggestion,
    ) -> Result<vsm_core::GenomeId, ControllerError> {
        let champion = { self.genome.read().await.clone() };
        let started = {
            let mut trials = self.trials.write().await;
            trials.start_trial(self.node_id.clone(), &champion, suggestion.clone())?
        };
        let candidate_genome = self
            .trials
            .read()
            .await
            .active_candidate_genome()
            .ok_or(ControllerError::NoActiveTrial)?;

        self.ledger
            .set_champion_genome(&self.node_id, champion.clone())
            .await?;
        self.ledger
            .save_genome_snapshot(GenomeSnapshot::new(
                candidate_genome,
                GenomeSnapshotRole::Candidate,
            ))
            .await?;
        self.ledger
            .write_trial_record(started.record.clone())
            .await?;

        self.ledger
            .append_event(
                LedgerEvent::new(
                    LedgerEventKind::TrialStarted,
                    serde_json::json!({
                        "suggestion_id": started.suggestion_id.to_string(),
                        "base_genome_id": started.base_genome_id.to_string(),
                        "candidate_genome_id": started.candidate_genome_id.to_string(),
                        "trial_mode": format!("{:?}", suggestion.trial_mode),
                    }),
                )?
                .with_node(self.node_id.clone())
                .with_genome(started.candidate_genome_id.clone())
                .with_suggestion(started.suggestion_id),
            )
            .await?;

        Ok(started.candidate_genome_id)
    }

    pub async fn queue_candidate_from_suggestion(
        &self,
        suggestion: GeneSuggestion,
    ) -> Result<vsm_core::GenomeId, ControllerError> {
        let champion = { self.genome.read().await.clone() };
        let queued = {
            let trials = self.trials.read().await;
            trials.queue_candidate(self.node_id.clone(), &champion, suggestion.clone())?
        };

        self.ledger
            .set_champion_genome(&self.node_id, champion.clone())
            .await?;
        let mut snapshot = GenomeSnapshot::new(
            queued.candidate_genome.clone(),
            GenomeSnapshotRole::Candidate,
        );
        snapshot.metadata.insert(
            "suggestion_id".to_string(),
            queued.suggestion_id.to_string(),
        );
        snapshot.metadata.insert(
            "base_genome_id".to_string(),
            queued.base_genome_id.to_string(),
        );
        self.ledger.save_genome_snapshot(snapshot).await?;
        self.ledger
            .write_trial_record(queued.record.clone())
            .await?;

        self.ledger
            .append_event(
                LedgerEvent::new(
                    LedgerEventKind::TrialQueued,
                    serde_json::json!({
                        "suggestion_id": queued.suggestion_id.to_string(),
                        "base_genome_id": queued.base_genome_id.to_string(),
                        "candidate_genome_id": queued.candidate_genome_id.to_string(),
                        "trial_mode": format!("{:?}", suggestion.trial_mode),
                    }),
                )?
                .with_node(self.node_id.clone())
                .with_genome(queued.candidate_genome_id.clone())
                .with_suggestion(queued.suggestion_id),
            )
            .await?;

        Ok(queued.candidate_genome_id)
    }

    pub async fn start_next_queued_trial(
        &self,
    ) -> Result<Option<vsm_core::GenomeId>, ControllerError> {
        if let Some(active_suggestion_id) = self.trials.read().await.active_suggestion_id() {
            return Err(ControllerError::TrialAlreadyActive(active_suggestion_id));
        }
        if let Some(active_record) = self.ledger.get_active_trial_record(&self.node_id).await? {
            return Err(ControllerError::TrialAlreadyActive(active_record.trial_id));
        }

        let champion = { self.genome.read().await.clone() };
        let champion_id = champion.id.clone();
        let queued_records = self.ledger.queued_trial_records(&self.node_id, 50).await?;
        let completed_history = self
            .ledger
            .completed_trial_records(&self.node_id, 200)
            .await?;
        let replay_traces = self
            .ledger
            .recent_task_traces(TraceWindow {
                since: None,
                limit: Some(200),
            })
            .await?;
        let environment_pressure = self.recent_environment_pressure().await?;
        let replay_weights = self.trials.read().await.fitness_weights();
        let mut selectable = Vec::new();

        for record in queued_records {
            if record.base_genome_id != champion_id {
                self.reject_queued_trial_record(
                    record,
                    format!("base genome superseded by current champion {}", champion_id),
                )
                .await?;
                continue;
            }

            let Some(candidate_snapshot) = self
                .ledger
                .get_genome_snapshot(&record.candidate_genome_id)
                .await?
            else {
                self.reject_queued_trial_record(
                    record.clone(),
                    format!(
                        "candidate genome snapshot missing: {}",
                        record.candidate_genome_id
                    ),
                )
                .await?;
                continue;
            };

            let replay = replay_candidate_against_traces(
                &self.config.router,
                &self.node_id,
                &champion,
                &candidate_snapshot.genome,
                &record,
                &replay_traces,
                &replay_weights,
            )?;
            let evaluation =
                evaluate_queued_candidate_with_replay(&record, &completed_history, &replay);
            let evaluation = apply_environment_pressure(&record, evaluation, &environment_pressure);
            selectable.push((record, candidate_snapshot, evaluation, replay));
        }

        let evaluations = selectable
            .iter()
            .map(|(_, _, evaluation, _)| evaluation.clone())
            .collect::<Vec<_>>();
        let frontier_indices = pareto_frontier_indices(&evaluations);
        let frontier_size = frontier_indices.len();
        let frontier_index_set = frontier_indices.iter().copied().collect::<BTreeSet<_>>();
        let archive_inputs = selectable
            .iter()
            .enumerate()
            .map(|(index, (record, _, evaluation, replay))| {
                (index, record.clone(), evaluation.clone(), replay.clone())
            })
            .collect::<Vec<_>>();
        let mut frontier_index = 0;
        selectable.retain(|_| {
            let keep = frontier_indices.contains(&frontier_index);
            frontier_index += 1;
            keep
        });
        selectable.sort_by(|left, right| {
            compare_queued_candidate_evaluations(
                &(left.0.clone(), left.2.clone()),
                &(right.0.clone(), right.2.clone()),
            )
        });

        if let Some((mut record, candidate_snapshot, evaluation, replay)) =
            selectable.into_iter().next()
        {
            self.write_population_archive_records(
                &archive_inputs,
                &frontier_index_set,
                &record.trial_id,
                frontier_size,
                &environment_pressure,
            )
            .await?;
            record.metadata.insert(
                "selection_policy".to_string(),
                "pareto_empirical_candidate_score_v1".to_string(),
            );
            record.metadata.insert(
                "selection_score".to_string(),
                format!("{:.3}", evaluation.score.total_score),
            );
            record.metadata.insert(
                "selection_reasons".to_string(),
                evaluation.score.reasons.join("|"),
            );
            record.metadata.insert(
                "pareto_frontier_size".to_string(),
                frontier_size.to_string(),
            );
            record.metadata.insert(
                "candidate_objectives".to_string(),
                format_candidate_objectives(&evaluation.objectives),
            );
            insert_environment_pressure_metadata(&mut record.metadata, &environment_pressure);
            insert_replay_metadata(&mut record.metadata, &replay)?;
            let started = {
                let mut trials = self.trials.write().await;
                trials.activate_queued_trial(record, candidate_snapshot.genome)?
            };
            self.ledger
                .write_trial_record(started.record.clone())
                .await?;
            self.ledger
                .append_event(
                    LedgerEvent::new(
                        LedgerEventKind::TrialStarted,
                        serde_json::json!({
                            "suggestion_id": started.suggestion_id.to_string(),
                            "base_genome_id": started.base_genome_id.to_string(),
                            "candidate_genome_id": started.candidate_genome_id.to_string(),
                            "source": "queued_candidate",
                            "selection_policy": "pareto_empirical_candidate_score_v1",
                            "selection_score": evaluation.score.total_score,
                            "selection_reasons": evaluation.score.reasons,
                            "pareto_frontier_size": frontier_size,
                            "candidate_objectives": {
                                "expected_value": evaluation.objectives.expected_value,
                                "safety": evaluation.objectives.safety,
                                "historical_fit": evaluation.objectives.historical_fit,
                                "replay_fit": evaluation.objectives.replay_fit,
                                "complexity_cost": evaluation.objectives.complexity_cost,
                                "exposure_cost": evaluation.objectives.exposure_cost,
                            },
                            "environment_pressure": environment_pressure.as_json(),
                            "historical_replay": {
                                "offline_replay_version": OFFLINE_REPLAY_VERSION,
                                "trace_count": replay.trace_count,
                                "base_genome_mismatch_count": replay.base_genome_mismatch_count,
                                "eligible_trace_count": replay.eligible_trace_count,
                                "safety_rejected_count": replay.safety_rejected_count,
                                "champion_route_count": replay.champion_route_count,
                                "candidate_route_count": replay.candidate_route_count,
                                "candidate_no_route_count": replay.candidate_no_route_count,
                                "changed_route_count": replay.changed_route_count,
                                "affected_route_count": replay.affected_route_count,
                                "baseline_score": replay.baseline_score,
                                "estimated_delta_score": replay.estimated_delta_score,
                                "replay_score": replay.replay_score,
                                "trace_evaluations": replay.trace_evaluations,
                                "reasons": replay.reasons,
                            },
                        }),
                    )?
                    .with_node(self.node_id.clone())
                    .with_genome(started.candidate_genome_id.clone())
                    .with_suggestion(started.suggestion_id),
                )
                .await?;
            return Ok(Some(started.candidate_genome_id));
        }

        Ok(None)
    }

    pub async fn run_evolution_generation(
        &self,
        policy: EvolutionPolicy,
    ) -> Result<Option<EvolutionGenerationRecord>, ControllerError> {
        let latest_generation = self
            .ledger
            .latest_evolution_generation_record(&self.node_id)
            .await?;
        let generation = latest_generation
            .map(|record| record.generation.saturating_add(1))
            .unwrap_or(1);
        let champion = { self.genome.read().await.clone() };
        let completed_history = self
            .ledger
            .completed_trial_records(&self.node_id, 500)
            .await?;
        let population_archive = self
            .ledger
            .population_archive_records(&self.node_id, 500)
            .await?;
        let queued_trials = self.ledger.queued_trial_records(&self.node_id, 500).await?;
        let recent_traces = self
            .ledger
            .recent_task_traces(TraceWindow {
                since: None,
                limit: Some(policy.recent_trace_limit.max(1)),
            })
            .await?;

        let Some(plan) = plan_evolution_generation(
            &self.node_id,
            generation,
            &champion,
            &completed_history,
            &population_archive,
            &queued_trials,
            &recent_traces,
            &policy,
        ) else {
            return Ok(None);
        };

        self.ledger
            .set_champion_genome(&self.node_id, champion)
            .await?;

        for suggestion in &plan.suggestions {
            self.ledger
                .append_event(
                    LedgerEvent::new(LedgerEventKind::GeneSuggestionCreated, suggestion)?
                        .with_node(self.node_id.clone())
                        .with_genome(plan.record.champion_genome_id.clone())
                        .with_suggestion(suggestion.id.clone()),
                )
                .await?;
            self.queue_candidate_from_suggestion(suggestion.clone())
                .await?;
            if let Some(mut record) = self.ledger.get_trial_record(&suggestion.id).await? {
                record.metadata.insert(
                    "evolution_generation".to_string(),
                    plan.record.generation.to_string(),
                );
                record
                    .metadata
                    .insert("evolution_policy".to_string(), policy.name.clone());
                record.metadata.insert(
                    "evolution_operator".to_string(),
                    suggestion_operator(suggestion).to_string(),
                );
                self.ledger.write_trial_record(record).await?;
            }
        }

        self.ledger
            .write_evolution_generation_record(plan.record.clone())
            .await?;
        self.ledger
            .append_event(
                LedgerEvent::new(
                    LedgerEventKind::EvolutionGenerationCreated,
                    serde_json::json!({
                        "generation": plan.record.generation,
                        "champion_genome_id": plan.record.champion_genome_id.to_string(),
                        "policy": plan.record.policy.clone(),
                        "parent_trial_ids": plan
                            .record
                            .parent_trial_ids
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>(),
                        "offspring_trial_ids": plan
                            .record
                            .offspring_trial_ids
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>(),
                        "mutation_operator_counts": plan.record.mutation_operator_counts.clone(),
                    }),
                )?
                .with_node(self.node_id.clone())
                .with_genome(plan.record.champion_genome_id.clone()),
            )
            .await?;

        Ok(Some(plan.record))
    }

    pub async fn register_trial_worker(&self, node_id: NodeId) -> Result<(), ControllerError> {
        let record = {
            let mut trials = self.trials.write().await;
            trials.register_candidate_worker(node_id);
            trials.active_record(self.node_id.clone())
        };
        if let Some(record) = record {
            self.ledger.write_trial_record(record).await?;
        }
        Ok(())
    }

    pub async fn active_candidate_genome(&self) -> Option<OrganizationalGenome> {
        self.trials.read().await.active_candidate_genome()
    }

    async fn recent_environment_pressure(
        &self,
    ) -> Result<EnvironmentPressureSummary, ControllerError> {
        let events = self
            .ledger
            .recent_events(EventFilter {
                kinds: environment_signal_event_kinds(),
                node_id: Some(self.node_id.clone()),
                limit: Some(100),
                ..EventFilter::default()
            })
            .await?;
        Ok(EnvironmentPressureSummary::from_events(&events))
    }

    async fn recent_coordination_pressure(
        &self,
        child_node_id: &NodeId,
    ) -> Result<CoordinationPressureSummary, ControllerError> {
        let events = self
            .ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::Other(
                    "system2_coordination_signal".to_string(),
                )],
                node_id: Some(self.node_id.clone()),
                limit: Some(100),
                ..EventFilter::default()
            })
            .await?;
        Ok(CoordinationPressureSummary::from_events(
            &events,
            child_node_id,
        ))
    }

    async fn subtree_pause_for_child(
        &self,
        child_node_id: &NodeId,
    ) -> Result<Option<SubtreePauseState>, ControllerError> {
        let events = self
            .ledger
            .recent_events(EventFilter {
                kinds: vec![
                    LedgerEventKind::Other("algedonic_subtree_paused".to_string()),
                    LedgerEventKind::Other("algedonic_subtree_resumed".to_string()),
                ],
                node_id: Some(self.node_id.clone()),
                limit: Some(500),
                ..EventFilter::default()
            })
            .await?;
        let genome = self.genome.read().await;
        let mut latest_pause: Option<SubtreePauseState> = None;
        let mut latest_resume_at: Option<DateTime<Utc>> = None;

        for event in events {
            let Some(target_node_id) = event
                .payload
                .get("target_node_id")
                .and_then(|value| value.as_str())
                .map(NodeId::from_string)
            else {
                continue;
            };
            let Ok(target_subtree) = genome.subtree_ids(&target_node_id) else {
                continue;
            };
            if !target_subtree
                .iter()
                .any(|node_id| node_id == child_node_id)
            {
                continue;
            }

            match &event.kind {
                LedgerEventKind::Other(name) if name == "algedonic_subtree_paused" => {
                    latest_pause = Some(SubtreePauseState {
                        target_node_id,
                        paused_at: event.created_at,
                        reason: event
                            .payload
                            .get("message")
                            .and_then(|value| value.as_str())
                            .unwrap_or("algedonic pause_subtree")
                            .to_string(),
                        severity: event
                            .payload
                            .get("severity")
                            .and_then(|value| value.as_u64())
                            .unwrap_or(1)
                            .min(10) as u8,
                        require_human_confirmation: event
                            .payload
                            .get("require_human_confirmation")
                            .and_then(|value| value.as_bool())
                            .unwrap_or(false),
                    });
                }
                LedgerEventKind::Other(name) if name == "algedonic_subtree_resumed" => {
                    latest_resume_at = Some(event.created_at);
                }
                _ => {}
            }
        }

        Ok(latest_pause.filter(|pause| {
            latest_resume_at
                .map(|resumed_at| resumed_at < pause.paused_at)
                .unwrap_or(true)
        }))
    }

    pub async fn load_persisted_champion(
        &self,
    ) -> Result<Option<vsm_core::GenomeId>, ControllerError> {
        let Some(champion) = self.ledger.get_champion_genome(&self.node_id).await? else {
            return Ok(None);
        };
        let genome_id = champion.id.clone();
        *self.genome.write().await = champion;
        Ok(Some(genome_id))
    }

    pub async fn restore_active_trial_from_ledger(
        &self,
    ) -> Result<Option<vsm_core::GenomeId>, ControllerError> {
        let Some(record) = self.ledger.get_active_trial_record(&self.node_id).await? else {
            return Ok(None);
        };
        let Some(candidate_snapshot) = self
            .ledger
            .get_genome_snapshot(&record.candidate_genome_id)
            .await?
        else {
            return Ok(None);
        };
        let traces = self
            .ledger
            .recent_task_traces(TraceWindow {
                since: Some(record.started_at),
                limit: Some(500),
            })
            .await?
            .into_iter()
            .filter(|trace| {
                trace
                    .related_suggestion_ids
                    .iter()
                    .any(|suggestion_id| suggestion_id == &record.trial_id)
            })
            .collect();
        let candidate_genome_id = candidate_snapshot.genome.id.clone();
        self.trials.write().await.restore_active_trial(
            record,
            candidate_snapshot.genome,
            traces,
        )?;
        Ok(Some(candidate_genome_id))
    }

    pub async fn evaluate_active_trial(&self) -> Result<TrialEvaluation, ControllerError> {
        let evaluation = { self.trials.read().await.evaluate()? };
        self.log_trial_decision(&evaluation).await?;
        self.apply_trial_decision(&evaluation).await?;
        Ok(evaluation)
    }

    async fn reject_queued_trial_record(
        &self,
        mut record: StoredTrialRecord,
        reason: impl Into<String>,
    ) -> Result<(), ControllerError> {
        let reason = reason.into();
        record.mark_rejected(reason.clone());
        self.ledger.write_trial_record(record.clone()).await?;
        self.ledger
            .append_event(
                LedgerEvent::new(
                    LedgerEventKind::TrialRejected,
                    serde_json::json!({
                        "trial_id": record.trial_id.to_string(),
                        "base_genome_id": record.base_genome_id.to_string(),
                        "candidate_genome_id": record.candidate_genome_id.to_string(),
                        "reason": reason,
                    }),
                )?
                .with_node(self.node_id.clone())
                .with_genome(record.candidate_genome_id)
                .with_suggestion(record.trial_id),
            )
            .await?;
        Ok(())
    }

    async fn write_population_archive_records(
        &self,
        archive_inputs: &[(
            usize,
            StoredTrialRecord,
            crate::QueuedCandidateEvaluation,
            crate::CandidateReplaySummary,
        )],
        frontier_indices: &BTreeSet<usize>,
        selected_trial_id: &vsm_core::SuggestionId,
        frontier_size: usize,
        environment_pressure: &EnvironmentPressureSummary,
    ) -> Result<(), ControllerError> {
        for (candidate_index, trial, evaluation, replay) in archive_inputs {
            let status = if &trial.trial_id == selected_trial_id {
                PopulationArchiveStatus::SelectedForTrial
            } else if frontier_indices.contains(candidate_index) {
                PopulationArchiveStatus::ParetoFrontier
            } else {
                PopulationArchiveStatus::Dominated
            };
            let mut archive = PopulationArchiveRecord::new(
                self.node_id.clone(),
                trial,
                status,
                "pareto_empirical_candidate_score_v1",
                evaluation.score.total_score,
                frontier_size,
                snapshot_candidate_objectives(&evaluation.objectives),
            );
            archive.metadata.insert(
                "selection_reasons".to_string(),
                evaluation.score.reasons.join("|"),
            );
            archive.metadata.insert(
                "trial_mode".to_string(),
                format!("{:?}", trial.suggestion.trial_mode),
            );
            archive.metadata.insert(
                "suggestion_source".to_string(),
                format!("{:?}", trial.suggestion.source),
            );
            insert_environment_pressure_metadata(&mut archive.metadata, environment_pressure);
            insert_replay_metadata(&mut archive.metadata, replay)?;
            self.ledger.write_population_archive_record(archive).await?;
        }
        Ok(())
    }

    pub async fn run_forever(&self) -> Result<(), ControllerError> {
        let mut stream = self.subscribe().await?;
        self.append_lifecycle_event(LedgerEventKind::ControllerStarted)
            .await?;

        while let Some(next) = stream.next().await {
            let envelope = next?;
            let _ = self.handle_envelope(envelope).await?;
        }

        Ok(())
    }

    pub async fn run_until_results(
        &self,
        max_results: usize,
    ) -> Result<Vec<TaskResult>, ControllerError> {
        let mut stream = self.subscribe().await?;
        self.append_lifecycle_event(LedgerEventKind::ControllerStarted)
            .await?;

        let mut results = Vec::new();
        while results.len() < max_results {
            let Some(next) = stream.next().await else {
                break;
            };
            let envelope = next?;
            if let ControllerHandleOutcome::ReceivedTaskResult(result) =
                self.handle_envelope(envelope).await?
            {
                results.push(result);
            }
        }
        Ok(results)
    }

    /// Runs an intermittent System 3* audit against this node's child activity.
    ///
    /// The auditor only proposes genes; it does not apply mutations. This keeps
    /// audit, trial, and selection as separate stages:
    ///
    /// audit -> gene suggestions -> bounded trial -> empirical selection.
    pub async fn run_system_3_star_audit<A>(
        &self,
        auditor: &A,
    ) -> Result<AuditReport, ControllerError>
    where
        A: System3StarAuditor,
    {
        let (genome_id, parent) = {
            let genome = self.genome.read().await;
            let parent = genome.get_node(&self.node_id)?.clone();
            if parent.children.is_empty() {
                return Err(ControllerError::NotMetasystem(parent.id.clone()));
            }
            (genome.id.clone(), parent)
        };

        self.ledger
            .append_event(
                LedgerEvent::new(
                    LedgerEventKind::AuditStarted,
                    serde_json::json!({
                        "node_id": self.node_id.to_string(),
                        "child_count": parent.children.len(),
                    }),
                )?
                .with_node(self.node_id.clone())
                .with_genome(genome_id.clone()),
            )
            .await?;

        let traces = self
            .ledger
            .subtree_task_traces(&self.node_id, TraceWindow::default())
            .await?;

        let recent_events = self
            .ledger
            .recent_events(EventFilter {
                node_id: Some(self.node_id.clone()),
                limit: Some(100),
                ..EventFilter::default()
            })
            .await?;

        let outcome = auditor.audit_children(&parent, traces, recent_events)?;

        for suggestion in &outcome.suggestions {
            self.ledger
                .append_event(
                    LedgerEvent::new(LedgerEventKind::GeneSuggestionCreated, suggestion)?
                        .with_node(self.node_id.clone())
                        .with_genome(genome_id.clone())
                        .with_suggestion(suggestion.id.clone()),
                )
                .await?;
        }

        self.ledger
            .append_event(
                LedgerEvent::new(LedgerEventKind::AuditCompleted, &outcome.report)?
                    .with_node(self.node_id.clone())
                    .with_genome(genome_id),
            )
            .await?;

        Ok(outcome.report)
    }

    async fn subscribe(&self) -> Result<vsm_core::EnvelopeStream, ControllerError> {
        let subscription = Subscription {
            channel_types: self.config.subscription_channels.clone(),
            target_node_id: Some(self.node_id.to_string()),
            queue_name: self.config.queue_name.clone(),
            durable: self.config.durable_subscription,
        };

        Ok(self.transport.subscribe(subscription).await?)
    }

    pub async fn handle_envelope(
        &self,
        envelope: MessageEnvelope,
    ) -> Result<ControllerHandleOutcome, ControllerError> {
        if self.config.append_message_events {
            self.ledger
                .append_event(LedgerEvent::for_message(
                    LedgerEventKind::MessageReceived,
                    &envelope,
                )?)
                .await?;
        }

        match envelope.payload_type.as_str() {
            payload if payload == BuiltinPayloadType::Directive.as_str() => {
                let directive: Directive = envelope.payload_as()?;
                let mut task = self.config.mapper.map(&directive);
                task.metadata
                    .insert("controller_node_id".to_string(), self.node_id.to_string());

                let event = LedgerEvent::new(LedgerEventKind::DirectiveAccepted, &directive)?
                    .with_node(self.node_id.clone())
                    .with_directive(directive.id.clone());
                self.ledger.append_event(event).await?;

                if let Some(decomposed_tasks) =
                    self.decompose_directive(&directive, &mut task).await?
                {
                    self.log_task_mapped(
                        &task,
                        Some(&directive),
                        &envelope,
                        "system3_capability_split_root",
                    )
                    .await?;
                    let routed_tasks = self
                        .route_decomposed_directive_tasks(decomposed_tasks, &envelope)
                        .await?;
                    return Ok(ControllerHandleOutcome::DecomposedTasks {
                        tasks: routed_tasks
                            .into_iter()
                            .map(RoutedTaskOutcome::from)
                            .collect(),
                    });
                }

                self.log_task_mapped(&task, Some(&directive), &envelope, "system3_single_task")
                    .await?;

                let route = self.route_task(task, Some(&envelope)).await?;
                Ok(ControllerHandleOutcome::RoutedTask {
                    task: route.task,
                    child_id: route.child_id,
                    reason: route.reason,
                })
            }
            payload if payload == BuiltinPayloadType::TaskPacket.as_str() => {
                let task: TaskPacket = envelope.payload_as()?;
                if task.parent_task_id.is_some() || !task.dependencies.is_empty() {
                    self.log_task_mapped(&task, None, &envelope, "system3_channel_task")
                        .await?;
                }
                let route = self.route_task(task, Some(&envelope)).await?;
                Ok(ControllerHandleOutcome::RoutedTask {
                    task: route.task,
                    child_id: route.child_id,
                    reason: route.reason,
                })
            }
            payload if payload == BuiltinPayloadType::Command.as_str() => {
                let command: VsmCommand = envelope.payload_as()?;
                let task = task_from_command(&command);
                self.ledger
                    .append_event(
                        LedgerEvent::new(
                            LedgerEventKind::Other("command_received".to_string()),
                            serde_json::json!({
                                "issued_by": command.issued_by.to_string(),
                                "target": command.target.to_string(),
                                "title": command.title,
                                "non_negotiable": command.non_negotiable,
                                "legal_or_policy_basis": command.legal_or_policy_basis,
                                "system5_identity": command.system5_identity,
                                "policy_values": command.policy_values,
                                "non_negotiable_constraints": command.non_negotiable_constraints,
                                "denied_capabilities": command.denied_capabilities,
                                "metadata": command.metadata,
                                "source_channel": format!("{:?}", envelope.channel_type),
                                "correlation_id": envelope.correlation_id,
                                "causation_id": envelope.causation_id.as_ref().map(ToString::to_string),
                            }),
                        )?
                        .with_node(self.node_id.clone())
                        .with_task(task.id.clone()),
                    )
                    .await?;
                self.log_task_mapped(&task, None, &envelope, "system3_command_channel")
                    .await?;
                let route = self.route_task(task, Some(&envelope)).await?;
                Ok(ControllerHandleOutcome::RoutedTask {
                    task: route.task,
                    child_id: route.child_id,
                    reason: route.reason,
                })
            }
            payload if payload == BuiltinPayloadType::ResourceBargain.as_str() => {
                let bargain: ResourceBargain = envelope.payload_as()?;
                self.handle_resource_bargain(bargain, &envelope).await?;
                Ok(ControllerHandleOutcome::Ignored)
            }
            payload if payload == BuiltinPayloadType::System2CoordinationSignal.as_str() => {
                let signal: System2CoordinationSignal = envelope.payload_as()?;
                if let Some(route) = self
                    .handle_system2_coordination_signal(signal, &envelope)
                    .await?
                {
                    return Ok(ControllerHandleOutcome::RoutedTask {
                        task: route.task,
                        child_id: route.child_id,
                        reason: route.reason,
                    });
                }
                Ok(ControllerHandleOutcome::Ignored)
            }
            payload if payload == BuiltinPayloadType::OperationHandoff.as_str() => {
                let handoff: OperationHandoff = envelope.payload_as()?;
                let route = self.handle_operation_handoff(handoff, &envelope).await?;
                Ok(ControllerHandleOutcome::RoutedTask {
                    task: route.task,
                    child_id: route.child_id,
                    reason: route.reason,
                })
            }
            payload if payload == BuiltinPayloadType::ManagementOperationDirective.as_str() => {
                let directive: ManagementOperationDirective = envelope.payload_as()?;
                let route = self
                    .handle_management_operation_directive(directive, &envelope)
                    .await?;
                Ok(ControllerHandleOutcome::RoutedTask {
                    task: route.task,
                    child_id: route.child_id,
                    reason: route.reason,
                })
            }
            payload if payload == BuiltinPayloadType::EnvironmentSignal.as_str() => {
                let signal: EnvironmentSignal = envelope.payload_as()?;
                self.record_environment_signal(signal, &envelope).await?;
                Ok(ControllerHandleOutcome::Ignored)
            }
            payload if payload == BuiltinPayloadType::AuditRequest.as_str() => {
                let request: AuditRequest = envelope.payload_as()?;
                self.record_audit_request(request, &envelope).await?;
                Ok(ControllerHandleOutcome::Ignored)
            }
            payload if payload == BuiltinPayloadType::AuditReport.as_str() => {
                let report: AuditReport = envelope.payload_as()?;
                self.ingest_audit_report(report, &envelope).await?;
                Ok(ControllerHandleOutcome::Ignored)
            }
            payload if payload == BuiltinPayloadType::GeneSuggestion.as_str() => {
                let suggestion: GeneSuggestion = envelope.payload_as()?;
                self.ingest_gene_suggestion(suggestion, &envelope).await?;
                Ok(ControllerHandleOutcome::Ignored)
            }
            payload if payload == BuiltinPayloadType::TaskResult.as_str() => {
                let result: TaskResult = envelope.payload_as()?;
                let event = LedgerEvent::new(LedgerEventKind::TaskResultReceived, &result)?
                    .with_node(self.node_id.clone())
                    .with_task(result.task_id.clone());
                self.ledger.append_event(event).await?;
                self.record_trial_trace_for_result(&result).await?;
                if result_is_shadow_trial(&result) {
                    return Ok(ControllerHandleOutcome::Ignored);
                }
                self.revise_decomposition_from_result(&result, &envelope)
                    .await?;
                self.forward_result_to_parent_if_needed(&result, &envelope)
                    .await?;
                Ok(ControllerHandleOutcome::ReceivedTaskResult(result))
            }
            payload if payload == BuiltinPayloadType::AlgedonicSignal.as_str() => {
                let signal: vsm_core::AlgedonicSignal = envelope.payload_as()?;
                self.handle_algedonic_signal(signal, &envelope).await?;
                Ok(ControllerHandleOutcome::Ignored)
            }
            payload if payload == BuiltinPayloadType::ThreeFourHomeostatSignal.as_str() => {
                let signal: ThreeFourHomeostatSignal = envelope.payload_as()?;
                self.handle_three_four_homeostat_signal(signal, &envelope)
                    .await?;
                Ok(ControllerHandleOutcome::Ignored)
            }
            _ => Ok(ControllerHandleOutcome::Ignored),
        }
    }

    async fn forward_result_to_parent_if_needed(
        &self,
        result: &TaskResult,
        incoming: &MessageEnvelope,
    ) -> Result<(), ControllerError> {
        let parent_id = {
            let genome = self.genome.read().await;
            genome.get_node(&self.node_id)?.parent_id.clone()
        };
        let Some(parent_id) = parent_id else {
            return Ok(());
        };

        let mut envelope = envelope_for_task_result(result)?
            .with_route(Some(self.node_id.clone()), Some(parent_id));
        envelope.channel_type = VsmChannelType::ManagementToOperation;
        envelope.priority = ChannelPriority::Normal;
        envelope.correlation_id = incoming
            .correlation_id
            .clone()
            .or_else(|| Some(incoming.id.to_string()));
        envelope.causation_id = Some(incoming.id.clone());
        envelope.trace = incoming.trace.clone();
        envelope.trace.push(self.node_id.clone());
        envelope.metadata.insert(
            "forwarded_by_controller_node_id".to_string(),
            self.node_id.to_string(),
        );
        envelope.metadata.insert(
            "forwarded_result_task_id".to_string(),
            result.task_id.to_string(),
        );
        envelope.metadata.insert(
            "vsm_outbound_channel".to_string(),
            format!("{:?}", envelope.channel_type),
        );
        envelope.metadata.insert(
            "source_payload_type".to_string(),
            BuiltinPayloadType::TaskResult.as_str().to_string(),
        );

        if self.config.append_message_events {
            self.ledger
                .append_event(LedgerEvent::for_message(
                    LedgerEventKind::MessagePublished,
                    &envelope,
                )?)
                .await?;
        }

        self.transport.publish(envelope).await?;
        Ok(())
    }

    async fn log_task_mapped(
        &self,
        task: &TaskPacket,
        directive: Option<&Directive>,
        envelope: &MessageEnvelope,
        decomposition_policy: &str,
    ) -> Result<(), ControllerError> {
        let payload = serde_json::json!({
            "controller_node_id": self.node_id.to_string(),
            "task_id": task.id.to_string(),
            "task_title": task.title.clone(),
            "directive_id": task.directive_id.as_ref().map(ToString::to_string),
            "parent_task_id": task.parent_task_id.as_ref().map(ToString::to_string),
            "dependency_task_ids": task.dependencies.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "decomposition_policy": decomposition_policy,
            "decomposition_authority": task
                .metadata
                .get("decomposition_authority")
                .cloned()
                .unwrap_or_else(|| "system3".to_string()),
            "source_channel": format!("{:?}", envelope.channel_type),
            "source_payload_type": envelope.payload_type.clone(),
            "causation_id": envelope.causation_id.as_ref().map(ToString::to_string),
            "correlation_id": envelope.correlation_id.clone(),
            "directive_title": directive.map(|directive| directive.title.clone()),
            "target_state": task.target_state.clone(),
            "risk": format!("{:?}", task.risk),
            "metadata": task.metadata.clone(),
        });
        let mut event = LedgerEvent::new(LedgerEventKind::TaskMapped, payload)?
            .with_node(self.node_id.clone())
            .with_task(task.id.clone());
        if let Some(directive_id) = task.directive_id.clone() {
            event = event.with_directive(directive_id);
        }
        if let Some(correlation_id) = envelope.correlation_id.clone() {
            event = event.with_correlation(correlation_id);
        }
        self.ledger.append_event(event).await?;
        Ok(())
    }

    async fn record_environment_signal(
        &self,
        signal: EnvironmentSignal,
        envelope: &MessageEnvelope,
    ) -> Result<(), ControllerError> {
        let event_name = environment_signal_event_name(&envelope.channel_type);
        let mut event = LedgerEvent::new(
            LedgerEventKind::Other(event_name.to_string()),
            serde_json::json!({
                "observed_by_node_id": signal.observed_by_node_id.as_ref().map(ToString::to_string),
                "target_node_id": signal.target_node_id.as_ref().map(ToString::to_string),
                "related_task_id": signal.related_task_id.as_ref().map(ToString::to_string),
                "related_suggestion_id": signal.related_suggestion_id.as_ref().map(ToString::to_string),
                "source_environment": signal.source_environment,
                "target_environment": signal.target_environment,
                "kind": format!("{:?}", signal.kind),
                "summary": signal.summary,
                "evidence": signal.evidence,
                "severity": signal.severity,
                "source_channel": format!("{:?}", envelope.channel_type),
                "source_payload_type": envelope.payload_type.clone(),
                "causation_id": envelope.causation_id.as_ref().map(ToString::to_string),
                "correlation_id": envelope.correlation_id.clone(),
                "metadata": signal.metadata,
            }),
        )?
        .with_node(self.node_id.clone());
        if let Some(task_id) = signal.related_task_id {
            event = event.with_task(task_id);
        }
        if let Some(suggestion_id) = signal.related_suggestion_id {
            event = event.with_suggestion(suggestion_id);
        }
        if let Some(correlation_id) = envelope.correlation_id.clone() {
            event = event.with_correlation(correlation_id);
        }
        event.metadata.insert(
            "source_channel".to_string(),
            format!("{:?}", envelope.channel_type),
        );
        event
            .metadata
            .insert("event_name".to_string(), event_name.to_string());
        self.ledger.append_event(event).await?;
        Ok(())
    }

    async fn handle_three_four_homeostat_signal(
        &self,
        signal: ThreeFourHomeostatSignal,
        envelope: &MessageEnvelope,
    ) -> Result<(), ControllerError> {
        let genome_id = { self.genome.read().await.id.clone() };
        let mut event = LedgerEvent::new(
            LedgerEventKind::Other("three_four_homeostat_signal".to_string()),
            serde_json::json!({
                "system_3_node_id": signal.system_3_node_id.to_string(),
                "system_4_node_id": signal.system_4_node_id.to_string(),
                "target_node_id": signal.target_node_id.to_string(),
                "related_task_id": signal.related_task_id.as_ref().map(ToString::to_string),
                "related_suggestion_id": signal.related_suggestion_id.as_ref().map(ToString::to_string),
                "kind": format!("{:?}", signal.kind),
                "balance": format!("{:?}", signal.balance),
                "present_summary": signal.present_summary.clone(),
                "future_summary": signal.future_summary.clone(),
                "recommendation": signal.recommendation.clone(),
                "evidence": signal.evidence.clone(),
                "severity": signal.severity,
                "suggested_patch_count": signal.suggested_patches.len(),
                "source_channel": format!("{:?}", envelope.channel_type),
                "source_payload_type": envelope.payload_type.clone(),
                "causation_id": envelope.causation_id.as_ref().map(ToString::to_string),
                "correlation_id": envelope.correlation_id.clone(),
                "metadata": signal.metadata.clone(),
            }),
        )?
        .with_node(self.node_id.clone())
        .with_genome(genome_id.clone());
        if let Some(task_id) = signal.related_task_id.clone() {
            event = event.with_task(task_id);
        }
        if let Some(suggestion_id) = signal.related_suggestion_id.clone() {
            event = event.with_suggestion(suggestion_id);
        }
        if let Some(correlation_id) = envelope.correlation_id.clone() {
            event = event.with_correlation(correlation_id);
        }
        event.metadata.insert(
            "source_channel".to_string(),
            format!("{:?}", envelope.channel_type),
        );
        event
            .metadata
            .insert("homeostat_kind".to_string(), format!("{:?}", signal.kind));
        event.metadata.insert(
            "homeostat_balance".to_string(),
            format!("{:?}", signal.balance),
        );
        if let Some(severity) = signal.severity {
            event
                .metadata
                .insert("severity".to_string(), severity.to_string());
        }
        self.ledger.append_event(event).await?;

        for suggestion in gene_suggestions_from_three_four_homeostat(&signal, envelope) {
            self.ingest_gene_suggestion_with_genome(suggestion, envelope, genome_id.clone())
                .await?;
        }

        Ok(())
    }

    async fn handle_system2_coordination_signal(
        &self,
        signal: System2CoordinationSignal,
        envelope: &MessageEnvelope,
    ) -> Result<Option<RoutedTask>, ControllerError> {
        let mut event = LedgerEvent::new(
            LedgerEventKind::Other("system2_coordination_signal".to_string()),
            serde_json::json!({
                "coordinator_node_id": signal.coordinator_node_id.to_string(),
                "source_node_id": signal.source_node_id.as_ref().map(ToString::to_string),
                "target_node_id": signal.target_node_id.as_ref().map(ToString::to_string),
                "affected_node_ids": signal.affected_node_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
                "affected_task_ids": signal.affected_task_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
                "kind": format!("{:?}", signal.kind),
                "summary": signal.summary.clone(),
                "evidence": signal.evidence.clone(),
                "severity": signal.severity,
                "source_channel": format!("{:?}", envelope.channel_type),
                "source_payload_type": envelope.payload_type.clone(),
                "causation_id": envelope.causation_id.as_ref().map(ToString::to_string),
                "correlation_id": envelope.correlation_id.clone(),
                "metadata": signal.metadata.clone(),
            }),
        )?
        .with_node(self.node_id.clone());
        if let Some(task_id) = signal.affected_task_ids.first().cloned() {
            event = event.with_task(task_id);
        }
        if let Some(correlation_id) = envelope.correlation_id.clone() {
            event = event.with_correlation(correlation_id);
        }
        event.metadata.insert(
            "source_channel".to_string(),
            format!("{:?}", envelope.channel_type),
        );
        event.metadata.insert(
            "coordination_kind".to_string(),
            format!("{:?}", signal.kind),
        );
        if let Some(severity) = signal.severity {
            event
                .metadata
                .insert("severity".to_string(), severity.to_string());
        }
        self.ledger.append_event(event).await?;

        if !system2_signal_requires_dampening(&signal) {
            return Ok(None);
        }

        let mut task = system2_dampening_task_from_signal(&signal, envelope);
        if let Some(target_child_id) = self.system2_signal_target_child(&signal).await? {
            task.assigned_to = Some(target_child_id);
        }

        let incoming = synthetic_system2_coordination_envelope(&task, envelope, &self.node_id)?;
        self.log_task_mapped(&task, None, &incoming, "system2_dampening_signal")
            .await?;
        let routed = self.route_task(task, Some(&incoming)).await?;
        self.ledger
            .append_event(
                LedgerEvent::new(
                    LedgerEventKind::Other("system2_dampening_task_created".to_string()),
                    serde_json::json!({
                        "coordination_task_id": routed.task.id.to_string(),
                        "child_node_id": routed.child_id.to_string(),
                        "kind": format!("{:?}", signal.kind),
                        "severity": signal.severity,
                        "affected_task_ids": signal.affected_task_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
                        "source_channel": format!("{:?}", envelope.channel_type),
                        "correlation_id": envelope.correlation_id.clone(),
                        "causation_id": envelope.causation_id.as_ref().map(ToString::to_string),
                    }),
                )?
                .with_node(self.node_id.clone())
                .with_task(routed.task.id.clone()),
            )
            .await?;

        Ok(Some(routed))
    }

    async fn system2_signal_target_child(
        &self,
        signal: &System2CoordinationSignal,
    ) -> Result<Option<NodeId>, ControllerError> {
        let genome = self.genome.read().await;
        let parent = genome.get_node(&self.node_id)?;
        let target = signal
            .target_node_id
            .as_ref()
            .into_iter()
            .chain(signal.affected_node_ids.iter())
            .find(|node_id| parent.children.iter().any(|child_id| child_id == *node_id))
            .cloned();
        Ok(target)
    }

    async fn handle_operation_handoff(
        &self,
        handoff: OperationHandoff,
        envelope: &MessageEnvelope,
    ) -> Result<RoutedTask, ControllerError> {
        let mut event = LedgerEvent::new(
            LedgerEventKind::Other("operation_handoff_received".to_string()),
            serde_json::json!({
                "source_node_id": handoff.source_node_id.to_string(),
                "target_node_id": handoff.target_node_id.to_string(),
                "related_task_id": handoff.related_task_id.as_ref().map(ToString::to_string),
                "dependency_task_ids": handoff.dependency_task_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
                "kind": format!("{:?}", handoff.kind),
                "title": handoff.title.clone(),
                "summary": handoff.summary.clone(),
                "artifact_count": handoff.artifacts.len(),
                "evidence": handoff.evidence.clone(),
                "metadata": handoff.metadata.clone(),
                "source_channel": format!("{:?}", envelope.channel_type),
                "source_payload_type": envelope.payload_type.clone(),
                "correlation_id": envelope.correlation_id.clone(),
                "causation_id": envelope.causation_id.as_ref().map(ToString::to_string),
            }),
        )?
        .with_node(self.node_id.clone());
        if let Some(task_id) = handoff.related_task_id.clone() {
            event = event.with_task(task_id);
        } else if let Some(task_id) = handoff.dependency_task_ids.first().cloned() {
            event = event.with_task(task_id);
        }
        if let Some(correlation_id) = envelope.correlation_id.clone() {
            event = event.with_correlation(correlation_id);
        }
        event.metadata.insert(
            "source_channel".to_string(),
            format!("{:?}", envelope.channel_type),
        );
        event.metadata.insert(
            "handoff_operation_kind".to_string(),
            format!("{:?}", handoff.kind),
        );
        self.ledger.append_event(event).await?;

        if !self
            .operation_handoff_targets_direct_child(&handoff)
            .await?
        {
            return Err(ControllerError::NoRouteableChild {
                node_id: self.node_id.clone(),
                task_title: handoff.title.clone(),
            });
        }

        let task = operation_handoff_task_from_handoff(&handoff, envelope);
        self.log_task_mapped(&task, None, envelope, "operation_handoff")
            .await?;
        self.route_task(task, Some(envelope)).await
    }

    async fn operation_handoff_targets_direct_child(
        &self,
        handoff: &OperationHandoff,
    ) -> Result<bool, ControllerError> {
        let genome = self.genome.read().await;
        let parent = genome.get_node(&self.node_id)?;
        Ok(parent
            .children
            .iter()
            .any(|child_id| child_id == &handoff.target_node_id))
    }

    async fn handle_management_operation_directive(
        &self,
        directive: ManagementOperationDirective,
        envelope: &MessageEnvelope,
    ) -> Result<RoutedTask, ControllerError> {
        let mut event = LedgerEvent::new(
            LedgerEventKind::Other("management_operation_directive_received".to_string()),
            serde_json::json!({
                "manager_node_id": directive.manager_node_id.to_string(),
                "operation_node_id": directive.operation_node_id.to_string(),
                "related_task_id": directive.related_task_id.as_ref().map(ToString::to_string),
                "dependency_task_ids": directive.dependency_task_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
                "kind": format!("{:?}", directive.kind),
                "title": directive.title.clone(),
                "target_state": directive.target_state.clone(),
                "risk": format!("{:?}", directive.risk),
                "metadata": directive.metadata.clone(),
                "source_channel": format!("{:?}", envelope.channel_type),
                "source_payload_type": envelope.payload_type.clone(),
                "correlation_id": envelope.correlation_id.clone(),
                "causation_id": envelope.causation_id.as_ref().map(ToString::to_string),
            }),
        )?
        .with_node(self.node_id.clone());
        if let Some(task_id) = directive.related_task_id.clone() {
            event = event.with_task(task_id);
        } else if let Some(task_id) = directive.dependency_task_ids.first().cloned() {
            event = event.with_task(task_id);
        }
        if let Some(correlation_id) = envelope.correlation_id.clone() {
            event = event.with_correlation(correlation_id);
        }
        event.metadata.insert(
            "source_channel".to_string(),
            format!("{:?}", envelope.channel_type),
        );
        event.metadata.insert(
            "management_operation_kind".to_string(),
            format!("{:?}", directive.kind),
        );
        self.ledger.append_event(event).await?;

        if !self
            .management_operation_targets_direct_child(&directive)
            .await?
        {
            return Err(ControllerError::NoRouteableChild {
                node_id: self.node_id.clone(),
                task_title: directive.title.clone(),
            });
        }

        let task = management_operation_task_from_directive(&directive, envelope);
        self.log_task_mapped(&task, None, envelope, "management_operation_directive")
            .await?;
        self.route_task(task, Some(envelope)).await
    }

    async fn management_operation_targets_direct_child(
        &self,
        directive: &ManagementOperationDirective,
    ) -> Result<bool, ControllerError> {
        let genome = self.genome.read().await;
        let parent = genome.get_node(&self.node_id)?;
        Ok(parent
            .children
            .iter()
            .any(|child_id| child_id == &directive.operation_node_id))
    }

    async fn handle_resource_bargain(
        &self,
        mut bargain: ResourceBargain,
        envelope: &MessageEnvelope,
    ) -> Result<(), ControllerError> {
        let proposed_task_id_mismatch = bargain.proposed_task.as_ref().and_then(|proposed_task| {
            bargain.task_id.as_ref().and_then(|task_id| {
                (task_id != &proposed_task.id).then(|| (task_id.clone(), proposed_task.id.clone()))
            })
        });
        if let Some(proposed_task) = &bargain.proposed_task {
            match &bargain.task_id {
                None => {
                    bargain.task_id = Some(proposed_task.id.clone());
                }
                _ => {}
            }
        }

        let environment_pressure = self.recent_environment_pressure().await?;
        let coordination_pressure = self
            .recent_coordination_pressure(&bargain.requested_by)
            .await?;
        let allocation_time = Utc::now();
        let epoch_key = resource_epoch_key(allocation_time);
        let epoch_budget = {
            let genome = self.genome.read().await;
            resource_epoch_budget(&genome, &self.node_id, &bargain.requested_by)
        };
        let resource_epoch = resource_epoch_accounting(
            self.ledger.as_ref(),
            &self.node_id,
            &bargain.requested_by,
            epoch_key,
            epoch_budget,
        )
        .await?;
        let subtree_pause = self.subtree_pause_for_child(&bargain.requested_by).await?;
        let decision = if let Some((requested_task_id, proposed_task_id)) =
            proposed_task_id_mismatch
        {
            resource_allocation_decision(
                &bargain,
                ResourceAllocationStatus::Denied,
                None,
                vec![],
                bargain.requested_tool_permissions.clone(),
                vec![],
                bargain.requested_context_refs.clone(),
                vec![format!(
                    "resource bargain task_id {requested_task_id} does not match proposed_task id {proposed_task_id}"
                )],
            )
        } else if let Some(pause) = &subtree_pause {
            resource_allocation_decision(
                &bargain,
                ResourceAllocationStatus::Denied,
                None,
                vec![],
                bargain.requested_tool_permissions.clone(),
                vec![],
                bargain.requested_context_refs.clone(),
                vec![format!(
                    "ResourceBargaining denied because subtree {} is paused by algedonic signal: {}",
                    pause.target_node_id, pause.reason
                )],
            )
        } else {
            let genome = self.genome.read().await;
            allocate_resource_bargain(
                &genome,
                &self.node_id,
                &bargain,
                Some(&environment_pressure),
                Some(&coordination_pressure),
                Some(&resource_epoch),
            )
        };

        let mut event = LedgerEvent::new(
            LedgerEventKind::Other("resource_allocation_decision".to_string()),
            &decision,
        )?
        .with_node(self.node_id.clone());
        if let Some(task_id) = decision.task_id.clone() {
            event = event.with_task(task_id);
        }
        if let Some(correlation_id) = envelope.correlation_id.clone() {
            event = event.with_correlation(correlation_id);
        }
        event.metadata.insert(
            "source_channel".to_string(),
            format!("{:?}", envelope.channel_type),
        );
        event.metadata.insert(
            "allocation_status".to_string(),
            format!("{:?}", decision.status),
        );
        event.metadata.insert(
            "requested_by".to_string(),
            decision.requested_by.to_string(),
        );
        add_resource_epoch_metadata(
            &mut event.metadata,
            &resource_epoch,
            decision.approved_tokens,
        );
        if resource_pressure_affects_allocation(&environment_pressure) {
            event.metadata.insert(
                "homeostat_pressure_count".to_string(),
                environment_pressure.homeostat_count.to_string(),
            );
            event.metadata.insert(
                "homeostat_pressure_max_severity".to_string(),
                environment_pressure.max_severity.to_string(),
            );
            event.metadata.insert(
                "homeostat_pressure_risk".to_string(),
                environment_pressure.risk_pressure.to_string(),
            );
        }
        if coordination_pressure.affects_allocation() {
            insert_coordination_pressure_metadata(&mut event.metadata, &coordination_pressure);
        }
        if let Some(pause) = &subtree_pause {
            insert_subtree_pause_metadata(&mut event.metadata, pause);
        }
        self.ledger.append_event(event).await?;

        let mut response = MessageEnvelope::new(
            VsmChannelType::ResourceBargaining,
            BuiltinPayloadType::ResourceAllocationDecision.as_str(),
            &decision,
        )?
        .with_route(
            Some(self.node_id.clone()),
            Some(decision.requested_by.clone()),
        );
        response.priority = match &decision.status {
            ResourceAllocationStatus::Approved => ChannelPriority::Normal,
            ResourceAllocationStatus::PartiallyApproved => ChannelPriority::High,
            ResourceAllocationStatus::Denied => ChannelPriority::High,
        };
        response.correlation_id = envelope
            .correlation_id
            .clone()
            .or_else(|| Some(envelope.id.to_string()));
        response.causation_id = Some(envelope.id.clone());
        response.trace = envelope.trace.clone();
        response.trace.push(self.node_id.clone());
        response.metadata.insert(
            "allocation_policy".to_string(),
            decision.allocation_policy.clone(),
        );
        response.metadata.insert(
            "allocation_status".to_string(),
            format!("{:?}", decision.status),
        );
        add_resource_epoch_metadata(
            &mut response.metadata,
            &resource_epoch,
            decision.approved_tokens,
        );
        if resource_pressure_affects_allocation(&environment_pressure) {
            response.metadata.insert(
                "homeostat_pressure_count".to_string(),
                environment_pressure.homeostat_count.to_string(),
            );
            response.metadata.insert(
                "homeostat_pressure_max_severity".to_string(),
                environment_pressure.max_severity.to_string(),
            );
        }
        if coordination_pressure.affects_allocation() {
            insert_coordination_pressure_metadata(&mut response.metadata, &coordination_pressure);
        }
        if let Some(pause) = &subtree_pause {
            insert_subtree_pause_metadata(&mut response.metadata, pause);
        }

        if self.config.append_message_events {
            self.ledger
                .append_event(LedgerEvent::for_message(
                    LedgerEventKind::MessagePublished,
                    &response,
                )?)
                .await?;
        }
        self.transport.publish(response).await?;

        if resource_allocation_accepts_work(&decision) {
            if let Some(proposed_task) = bargain.proposed_task {
                self.accept_resource_bargain_task(
                    proposed_task,
                    &decision,
                    envelope,
                    Some(&resource_epoch),
                    Some(&coordination_pressure),
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn accept_resource_bargain_task(
        &self,
        mut task: TaskPacket,
        decision: &ResourceAllocationDecision,
        envelope: &MessageEnvelope,
        resource_epoch: Option<&ResourceEpochAccounting>,
        coordination_pressure: Option<&CoordinationPressureSummary>,
    ) -> Result<(), ControllerError> {
        annotate_incoming_task_channel(&mut task, Some(envelope), &self.node_id);
        task.assigned_to = Some(decision.requested_by.clone());
        task.metadata.insert(
            "resource_bargain_status".to_string(),
            format!("{:?}", decision.status),
        );
        task.metadata.insert(
            "resource_bargain_policy".to_string(),
            decision.allocation_policy.clone(),
        );
        task.metadata.insert(
            "resource_bargain_request_message_id".to_string(),
            envelope.id.to_string(),
        );
        if let Some(resource_epoch) = resource_epoch {
            add_resource_epoch_metadata(
                &mut task.metadata,
                resource_epoch,
                decision.approved_tokens,
            );
        }
        if let Some(coordination_pressure) = coordination_pressure {
            if coordination_pressure.affects_allocation() {
                insert_coordination_pressure_metadata(&mut task.metadata, coordination_pressure);
            }
        }
        if let Some(correlation_id) = envelope.correlation_id.clone() {
            task.metadata.insert(
                "resource_bargain_correlation_id".to_string(),
                correlation_id,
            );
        }
        if let Some(tokens) = decision.approved_tokens {
            task.metadata.insert(
                "resource_bargain_approved_tokens".to_string(),
                tokens.to_string(),
            );
        }
        if !decision.approved_tool_permissions.is_empty() {
            task.metadata.insert(
                "resource_bargain_approved_tool_permissions".to_string(),
                decision.approved_tool_permissions.join(","),
            );
        }
        if !decision.approved_context_refs.is_empty() {
            task.metadata.insert(
                "resource_bargain_approved_context_refs".to_string(),
                decision.approved_context_refs.join(","),
            );
        }
        if !decision.denied_tool_permissions.is_empty() {
            task.metadata.insert(
                "resource_bargain_denied_tool_permissions".to_string(),
                decision.denied_tool_permissions.join(","),
            );
        }
        if !decision.denied_context_refs.is_empty() {
            task.metadata.insert(
                "resource_bargain_denied_context_refs".to_string(),
                decision.denied_context_refs.join(","),
            );
        }
        task.metadata
            .insert("routed_by".to_string(), self.node_id.to_string());
        task.metadata.insert(
            "routing_reason".to_string(),
            "accepted through ResourceBargaining allocation".to_string(),
        );

        let genome_id = { self.genome.read().await.id.clone() };
        let event = LedgerEvent::new(
            LedgerEventKind::Other("resource_bargain_work_accepted".to_string()),
            serde_json::json!({
                "controller_node_id": self.node_id.to_string(),
                "requested_by": decision.requested_by.to_string(),
                "task_id": task.id.to_string(),
                "allocation_status": format!("{:?}", decision.status),
                "approved_tokens": decision.approved_tokens,
                "approved_tool_permissions": decision.approved_tool_permissions.clone(),
                "approved_context_refs": decision.approved_context_refs.clone(),
                "source_channel": format!("{:?}", envelope.channel_type),
                "correlation_id": envelope.correlation_id.clone(),
                "causation_id": envelope.causation_id.as_ref().map(ToString::to_string),
                "resource_epoch_key": resource_epoch.map(|epoch| epoch.epoch_key.clone()),
                "resource_epoch_allocated_before_tokens": resource_epoch.map(|epoch| epoch.allocated_tokens),
                "resource_epoch_remaining_before_tokens": resource_epoch.and_then(ResourceEpochAccounting::remaining_tokens),
                "resource_epoch_remaining_after_tokens": resource_epoch.and_then(|epoch| epoch.remaining_tokens_after(decision.approved_tokens)),
                "coordination_pressure": coordination_pressure
                    .filter(|pressure| pressure.affects_allocation())
                    .map(CoordinationPressureSummary::as_json),
            }),
        )?
        .with_node(self.node_id.clone())
        .with_task(task.id.clone())
        .with_genome(genome_id.clone());
        self.ledger.append_event(event).await?;

        self.publish_routed_task(
            task,
            Some(envelope),
            decision.requested_by.clone(),
            "accepted through ResourceBargaining allocation".to_string(),
            genome_id,
            false,
            None,
            false,
        )
        .await?;

        Ok(())
    }

    async fn handle_algedonic_signal(
        &self,
        signal: vsm_core::AlgedonicSignal,
        envelope: &MessageEnvelope,
    ) -> Result<(), ControllerError> {
        let mut signal_event =
            LedgerEvent::for_message(LedgerEventKind::AlgedonicSignalReceived, envelope)?
                .with_node(self.node_id.clone());
        if let Some(task_id) = signal.related_task_id.clone() {
            signal_event = signal_event.with_task(task_id);
        }
        if let Some(suggestion_id) = signal.related_suggestion_id.clone() {
            signal_event = signal_event.with_suggestion(suggestion_id);
        }
        signal_event
            .metadata
            .insert("severity".to_string(), signal.severity.to_string());
        signal_event
            .metadata
            .insert("valence".to_string(), format!("{:?}", signal.valence));
        self.ledger.append_event(signal_event).await?;

        let Some(policy) = signal.override_policy.clone() else {
            return Ok(());
        };

        let mut actions = Vec::new();
        if policy.freeze_mutation {
            actions.push("freeze_mutation".to_string());
            let mut target_suggestion_ids = Vec::new();
            if let Some(suggestion_id) = signal.related_suggestion_id.clone() {
                target_suggestion_ids.push(suggestion_id);
            } else if let Some(active_suggestion_id) =
                self.trials.read().await.active_suggestion_id()
            {
                actions.push("freeze_mutation_no_related_suggestion_active_trial".to_string());
                target_suggestion_ids.push(active_suggestion_id);
            }

            if target_suggestion_ids.is_empty() {
                actions.push("freeze_mutation_no_related_suggestion".to_string());
            }

            for suggestion_id in target_suggestion_ids {
                if self
                    .freeze_active_trial_from_algedonic(&suggestion_id, &signal)
                    .await?
                {
                    actions.push(format!("active_trial_frozen={suggestion_id}"));
                    continue;
                }

                if let Some(mut record) = self.ledger.get_trial_record(&suggestion_id).await? {
                    if record.status == vsm_ledger::StoredTrialStatus::Queued {
                        record.mark_rejected(format!(
                            "algedonic freeze_mutation: {}",
                            signal.message
                        ));
                        self.ledger.write_trial_record(record.clone()).await?;
                        self.ledger
                            .append_event(
                                LedgerEvent::new(
                                    LedgerEventKind::TrialRejected,
                                    serde_json::json!({
                                        "trial_id": record.trial_id.to_string(),
                                        "base_genome_id": record.base_genome_id.to_string(),
                                        "candidate_genome_id": record.candidate_genome_id.to_string(),
                                        "reason": "algedonic freeze_mutation",
                                        "message": signal.message.clone(),
                                    }),
                                )?
                                .with_node(self.node_id.clone())
                                .with_genome(record.candidate_genome_id)
                                .with_suggestion(record.trial_id),
                            )
                            .await?;
                    } else if record.status == vsm_ledger::StoredTrialStatus::Active {
                        record
                            .mark_frozen(format!("algedonic freeze_mutation: {}", signal.message));
                        self.ledger.write_trial_record(record.clone()).await?;
                        self.append_trial_frozen_event(&record, &signal, "ledger_active_record")
                            .await?;
                        actions.push(format!("active_trial_record_frozen={suggestion_id}"));
                    } else {
                        actions.push(format!(
                            "freeze_mutation_recorded_for_nonqueued_trial={}",
                            suggestion_id
                        ));
                    }
                } else {
                    actions.push(format!("freeze_mutation_no_trial_record={suggestion_id}"));
                }
            }
        }
        if policy.pause_subtree {
            let target_node_id = signal
                .target_node_id
                .clone()
                .unwrap_or_else(|| self.node_id.clone());
            self.append_algedonic_subtree_pause_event(&signal, &target_node_id, &policy)
                .await?;
            actions.push(format!("subtree_paused={target_node_id}"));
        }
        if policy.resume_subtree {
            let target_node_id = signal
                .target_node_id
                .clone()
                .unwrap_or_else(|| self.node_id.clone());
            self.append_algedonic_subtree_resume_event(&signal, &target_node_id, &policy)
                .await?;
            actions.push(format!("subtree_resumed={target_node_id}"));
        }
        if policy.escalate_to_root {
            if self
                .escalate_algedonic_to_parent_if_needed(&signal, envelope)
                .await?
            {
                actions.push("escalated_to_parent".to_string());
            } else {
                actions.push("escalation_at_root".to_string());
            }
        }
        if policy.require_human_confirmation {
            actions.push("require_human_confirmation".to_string());
        }

        let mut override_event = LedgerEvent::new(
            LedgerEventKind::Other("algedonic_override_applied".to_string()),
            serde_json::json!({
                "source_channel": format!("{:?}", envelope.channel_type),
                "valence": format!("{:?}", signal.valence),
                "severity": signal.severity,
                "target_node_id": signal.target_node_id.as_ref().map(ToString::to_string),
                "related_task_id": signal.related_task_id.as_ref().map(ToString::to_string),
                "related_suggestion_id": signal.related_suggestion_id.as_ref().map(ToString::to_string),
                "message": signal.message.clone(),
                "actions": actions,
                "policy": {
                    "pause_subtree": policy.pause_subtree,
                    "resume_subtree": policy.resume_subtree,
                    "escalate_to_root": policy.escalate_to_root,
                    "freeze_mutation": policy.freeze_mutation,
                    "require_human_confirmation": policy.require_human_confirmation,
                },
            }),
        )?
        .with_node(self.node_id.clone());
        if let Some(task_id) = signal.related_task_id {
            override_event = override_event.with_task(task_id);
        }
        if let Some(suggestion_id) = signal.related_suggestion_id {
            override_event = override_event.with_suggestion(suggestion_id);
        }
        self.ledger.append_event(override_event).await?;
        Ok(())
    }

    async fn append_algedonic_subtree_pause_event(
        &self,
        signal: &vsm_core::AlgedonicSignal,
        target_node_id: &NodeId,
        policy: &vsm_core::AlgedonicOverridePolicy,
    ) -> Result<(), ControllerError> {
        self.ledger
            .append_event(
                LedgerEvent::new(
                    LedgerEventKind::Other("algedonic_subtree_paused".to_string()),
                    serde_json::json!({
                        "target_node_id": target_node_id.to_string(),
                        "valence": format!("{:?}", signal.valence),
                        "severity": signal.severity,
                        "message": signal.message.clone(),
                        "source": format!("{:?}", signal.source),
                        "related_task_id": signal.related_task_id.as_ref().map(ToString::to_string),
                        "related_suggestion_id": signal.related_suggestion_id.as_ref().map(ToString::to_string),
                        "require_human_confirmation": policy.require_human_confirmation,
                    }),
                )?
                .with_node(self.node_id.clone()),
            )
            .await?;
        Ok(())
    }

    async fn append_algedonic_subtree_resume_event(
        &self,
        signal: &vsm_core::AlgedonicSignal,
        target_node_id: &NodeId,
        policy: &vsm_core::AlgedonicOverridePolicy,
    ) -> Result<(), ControllerError> {
        self.ledger
            .append_event(
                LedgerEvent::new(
                    LedgerEventKind::Other("algedonic_subtree_resumed".to_string()),
                    serde_json::json!({
                        "target_node_id": target_node_id.to_string(),
                        "valence": format!("{:?}", signal.valence),
                        "severity": signal.severity,
                        "message": signal.message.clone(),
                        "source": format!("{:?}", signal.source),
                        "related_task_id": signal.related_task_id.as_ref().map(ToString::to_string),
                        "related_suggestion_id": signal.related_suggestion_id.as_ref().map(ToString::to_string),
                        "require_human_confirmation": policy.require_human_confirmation,
                    }),
                )?
                .with_node(self.node_id.clone()),
            )
            .await?;
        Ok(())
    }

    async fn escalate_algedonic_to_parent_if_needed(
        &self,
        signal: &vsm_core::AlgedonicSignal,
        incoming: &MessageEnvelope,
    ) -> Result<bool, ControllerError> {
        let parent_id = {
            let genome = self.genome.read().await;
            genome.get_node(&self.node_id)?.parent_id.clone()
        };
        let Some(parent_id) = parent_id else {
            self.ledger
                .append_event(
                    LedgerEvent::new(
                        LedgerEventKind::Other("algedonic_escalation_at_root".to_string()),
                        serde_json::json!({
                            "controller_node_id": self.node_id.to_string(),
                            "target_node_id": signal.target_node_id.as_ref().map(ToString::to_string),
                            "severity": signal.severity,
                            "message": signal.message.clone(),
                        }),
                    )?
                    .with_node(self.node_id.clone()),
                )
                .await?;
            return Ok(false);
        };

        let mut envelope = MessageEnvelope::new(
            VsmChannelType::Algedonic,
            BuiltinPayloadType::AlgedonicSignal.as_str(),
            signal,
        )?
        .with_route(Some(self.node_id.clone()), Some(parent_id));
        envelope.priority = ChannelPriority::Interrupt;
        envelope.correlation_id = incoming
            .correlation_id
            .clone()
            .or_else(|| Some(incoming.id.to_string()));
        envelope.causation_id = Some(incoming.id.clone());
        envelope.trace = incoming.trace.clone();
        envelope.trace.push(self.node_id.clone());
        envelope
            .metadata
            .insert("algedonic_escalation".to_string(), "true".to_string());
        envelope
            .metadata
            .insert("escalated_by_node_id".to_string(), self.node_id.to_string());

        self.ledger
            .append_event(
                LedgerEvent::new(
                    LedgerEventKind::Other("algedonic_escalated".to_string()),
                    serde_json::json!({
                        "from_node_id": self.node_id.to_string(),
                        "to_node_id": envelope.target_node_id.as_ref().map(ToString::to_string),
                        "target_node_id": signal.target_node_id.as_ref().map(ToString::to_string),
                        "severity": signal.severity,
                        "message": signal.message.clone(),
                    }),
                )?
                .with_node(self.node_id.clone()),
            )
            .await?;
        if self.config.append_message_events {
            self.ledger
                .append_event(LedgerEvent::for_message(
                    LedgerEventKind::MessagePublished,
                    &envelope,
                )?)
                .await?;
        }
        self.transport.publish(envelope).await?;
        Ok(true)
    }

    async fn freeze_active_trial_from_algedonic(
        &self,
        suggestion_id: &vsm_core::SuggestionId,
        signal: &vsm_core::AlgedonicSignal,
    ) -> Result<bool, ControllerError> {
        let Some(active_suggestion_id) = self.trials.read().await.active_suggestion_id() else {
            return Ok(false);
        };
        if &active_suggestion_id != suggestion_id {
            return Ok(false);
        }

        let completed = {
            self.trials.write().await.freeze_active(
                self.node_id.clone(),
                format!("algedonic freeze_mutation: {}", signal.message),
            )?
        };
        self.ledger
            .write_trial_record(completed.record.clone())
            .await?;
        self.append_trial_frozen_event(&completed.record, signal, "active_trial")
            .await?;
        Ok(true)
    }

    async fn append_trial_frozen_event(
        &self,
        record: &StoredTrialRecord,
        signal: &vsm_core::AlgedonicSignal,
        source: &str,
    ) -> Result<(), ControllerError> {
        self.ledger
            .append_event(
                LedgerEvent::new(
                    LedgerEventKind::TrialFrozen,
                    serde_json::json!({
                        "trial_id": record.trial_id.to_string(),
                        "base_genome_id": record.base_genome_id.to_string(),
                        "candidate_genome_id": record.candidate_genome_id.to_string(),
                        "trace_count": record.trace_count,
                        "total_score": record.total_score,
                        "reason": "algedonic freeze_mutation",
                        "message": signal.message.clone(),
                        "source": source,
                    }),
                )?
                .with_node(self.node_id.clone())
                .with_genome(record.candidate_genome_id.clone())
                .with_suggestion(record.trial_id.clone()),
            )
            .await?;
        Ok(())
    }

    async fn record_audit_request(
        &self,
        request: AuditRequest,
        envelope: &MessageEnvelope,
    ) -> Result<(), ControllerError> {
        let genome_id = { self.genome.read().await.id.clone() };
        let mut event = LedgerEvent::new(
            LedgerEventKind::AuditStarted,
            serde_json::json!({
                "requested_by": request.requested_by.to_string(),
                "target_node_id": request.target_node_id.to_string(),
                "window_tasks": request.window_tasks,
                "include_gene_suggestions": request.include_gene_suggestions,
                "source_channel": format!("{:?}", envelope.channel_type),
                "source_payload_type": envelope.payload_type.clone(),
                "causation_id": envelope.causation_id.as_ref().map(ToString::to_string),
                "correlation_id": envelope.correlation_id.clone(),
            }),
        )?
        .with_node(self.node_id.clone())
        .with_genome(genome_id);
        event.metadata.insert(
            "source_channel".to_string(),
            format!("{:?}", envelope.channel_type),
        );
        if let Some(correlation_id) = envelope.correlation_id.clone() {
            event = event.with_correlation(correlation_id);
        }
        self.ledger.append_event(event).await?;
        Ok(())
    }

    async fn ingest_audit_report(
        &self,
        report: AuditReport,
        envelope: &MessageEnvelope,
    ) -> Result<(), ControllerError> {
        let genome_id = { self.genome.read().await.id.clone() };
        let suggestions = gene_suggestions_from_audit_report(&self.node_id, &report, envelope);

        let mut audit_event = LedgerEvent::new(LedgerEventKind::AuditCompleted, &report)?
            .with_node(self.node_id.clone())
            .with_genome(genome_id.clone());
        audit_event.metadata.insert(
            "source_channel".to_string(),
            format!("{:?}", envelope.channel_type),
        );
        audit_event.metadata.insert(
            "suggestion_count".to_string(),
            suggestions.len().to_string(),
        );
        if let Some(correlation_id) = envelope.correlation_id.clone() {
            audit_event = audit_event.with_correlation(correlation_id);
        }
        self.ledger.append_event(audit_event).await?;

        for suggestion in suggestions {
            self.ingest_gene_suggestion_with_genome(suggestion, envelope, genome_id.clone())
                .await?;
        }

        Ok(())
    }

    async fn ingest_gene_suggestion(
        &self,
        suggestion: GeneSuggestion,
        envelope: &MessageEnvelope,
    ) -> Result<vsm_core::GenomeId, ControllerError> {
        let genome_id = { self.genome.read().await.id.clone() };
        self.ingest_gene_suggestion_with_genome(suggestion, envelope, genome_id)
            .await
    }

    async fn ingest_gene_suggestion_with_genome(
        &self,
        suggestion: GeneSuggestion,
        envelope: &MessageEnvelope,
        genome_id: vsm_core::GenomeId,
    ) -> Result<vsm_core::GenomeId, ControllerError> {
        let mut suggestion = suggestion;
        if !suggestion
            .evidence
            .iter()
            .any(|evidence| evidence.starts_with("source_channel="))
        {
            suggestion
                .evidence
                .push(format!("source_channel={:?}", envelope.channel_type));
        }

        let mut suggestion_event =
            LedgerEvent::new(LedgerEventKind::GeneSuggestionCreated, &suggestion)?
                .with_node(self.node_id.clone())
                .with_genome(genome_id)
                .with_suggestion(suggestion.id.clone());
        suggestion_event.metadata.insert(
            "source_channel".to_string(),
            format!("{:?}", envelope.channel_type),
        );
        suggestion_event.metadata.insert(
            "source_payload_type".to_string(),
            envelope.payload_type.clone(),
        );
        if let Some(correlation_id) = envelope.correlation_id.clone() {
            suggestion_event = suggestion_event.with_correlation(correlation_id);
        }
        self.ledger.append_event(suggestion_event).await?;

        self.queue_candidate_from_suggestion(suggestion).await
    }

    async fn route_task(
        &self,
        mut task: TaskPacket,
        incoming: Option<&MessageEnvelope>,
    ) -> Result<RoutedTask, ControllerError> {
        annotate_incoming_task_channel(&mut task, incoming, &self.node_id);

        let maybe_trial_route = {
            let trials = self.trials.read().await;
            trials.choose_trial_route(&self.config.router, &self.node_id, &task)?
        };

        if let Some(trial_route) = maybe_trial_route {
            task.assigned_to = Some(trial_route.child_id.clone());
            task.metadata
                .insert("routed_by".to_string(), self.node_id.to_string());
            task.metadata
                .insert("routing_reason".to_string(), trial_route.reason.clone());
            tag_task_for_trial_route(&mut task, &trial_route);

            self.publish_routed_task(
                task,
                incoming,
                trial_route.child_id,
                trial_route.reason,
                trial_route.genome_id,
                true,
                Some(trial_route.suggestion_id),
                false,
            )
            .await
        } else {
            let maybe_shadow_route = {
                let trials = self.trials.read().await;
                trials.choose_shadow_route(&self.config.router, &self.node_id, &task)?
            };
            if maybe_shadow_route.is_none() {
                self.log_trial_fallback_if_approved(&task).await?;
            }
            let shadow_task = maybe_shadow_route.as_ref().map(|_| task.clone());

            let (genome_id, parent, decision) = {
                let genome = self.genome.read().await;
                let parent = genome.get_node(&self.node_id)?.clone();
                if parent.children.is_empty() {
                    return Err(ControllerError::NotMetasystem(parent.id.clone()));
                }
                let decision = self.config.router.choose_child(&genome, &parent, &task)?;
                (genome.id.clone(), parent, decision)
            };

            task.assigned_to = Some(decision.child_id.clone());
            task.metadata
                .insert("routed_by".to_string(), self.node_id.to_string());
            task.metadata
                .insert("routing_reason".to_string(), decision.reason.clone());

            let reason = decision.reason.clone();
            let child_id = decision.child_id.clone();
            let _ = parent;
            let routed = self
                .publish_routed_task(
                    task, incoming, child_id, reason, genome_id, false, None, false,
                )
                .await?;
            if let (Some(mut shadow_task), Some(shadow_route)) = (shadow_task, maybe_shadow_route) {
                self.publish_shadow_task(&mut shadow_task, incoming, shadow_route)
                    .await?;
            }
            Ok(routed)
        }
    }

    async fn decompose_directive(
        &self,
        directive: &Directive,
        root_task: &mut TaskPacket,
    ) -> Result<Option<Vec<DecomposedTaskPlan>>, ControllerError> {
        if !directive_requests_decomposition(directive) {
            return Ok(None);
        }

        let pressure = self.recent_environment_pressure().await?;
        let genome = self.genome.read().await;
        let parent = genome.get_node(&self.node_id)?;
        if !parent.system_3.can_decompose_tasks {
            return Ok(None);
        }

        root_task.metadata.insert(
            "decomposition_policy".to_string(),
            "system3_capability_split_v1".to_string(),
        );
        root_task
            .metadata
            .insert("decomposition_authority".to_string(), "system3".to_string());
        root_task
            .metadata
            .insert("decomposition_root".to_string(), "true".to_string());
        annotate_decomposition_pressure(root_task, &pressure);

        let implementation_target =
            target_child_for_capability(&genome, parent, DecompositionRole::Implementation);
        let mut implementation =
            decomposed_child_task(root_task, DecompositionRole::Implementation);
        implementation.metadata.insert(
            "requires_code_write".to_string(),
            self.config.mapper.default_requires_code_write.to_string(),
        );
        implementation
            .metadata
            .insert("required_capability".to_string(), "write_code".to_string());
        if let Some(target_child) = implementation_target {
            implementation
                .metadata
                .insert("target_child".to_string(), target_child);
        }

        let implementation_id = implementation.id.clone();
        let mut plans = vec![DecomposedTaskPlan {
            task: implementation,
            channel_type: VsmChannelType::ResourceBargaining,
            decomposition_policy: "system3_capability_split_implementation",
        }];

        if directive_requires_tests(directive) {
            if let Some(target_child) =
                target_child_for_capability(&genome, parent, DecompositionRole::Test)
            {
                let mut test_task = decomposed_child_task(root_task, DecompositionRole::Test);
                test_task.dependencies.push(implementation_id.clone());
                test_task
                    .metadata
                    .insert("requires_code_write".to_string(), "false".to_string());
                test_task
                    .metadata
                    .insert("required_capability".to_string(), "run_tests".to_string());
                test_task
                    .metadata
                    .insert("target_child".to_string(), target_child);
                plans.push(DecomposedTaskPlan {
                    task: test_task,
                    channel_type: VsmChannelType::System2Coordination,
                    decomposition_policy: "system3_capability_split_test",
                });
            }
        }

        if directive_requires_review(directive) || homeostat_pressure_requires_review(&pressure) {
            if let Some(target_child) =
                target_child_for_capability(&genome, parent, DecompositionRole::Review)
            {
                let mut review_task = decomposed_child_task(root_task, DecompositionRole::Review);
                review_task.dependencies.push(implementation_id);
                review_task
                    .metadata
                    .insert("requires_code_write".to_string(), "false".to_string());
                review_task
                    .metadata
                    .insert("required_capability".to_string(), "review".to_string());
                review_task
                    .metadata
                    .insert("target_child".to_string(), target_child);
                plans.push(DecomposedTaskPlan {
                    task: review_task,
                    channel_type: VsmChannelType::System2Coordination,
                    decomposition_policy: "system3_capability_split_review",
                });
            }
        }

        if directive_requires_integration(directive)
            || homeostat_pressure_requires_integration(&pressure)
        {
            if let Some(target_child) =
                target_child_for_capability(&genome, parent, DecompositionRole::Integration)
            {
                let mut integration_task =
                    decomposed_child_task(root_task, DecompositionRole::Integration);
                integration_task.dependencies = plans
                    .iter()
                    .map(|plan| plan.task.id.clone())
                    .collect::<Vec<_>>();
                integration_task
                    .metadata
                    .insert("requires_code_write".to_string(), "false".to_string());
                integration_task
                    .metadata
                    .insert("required_capability".to_string(), "integrate".to_string());
                integration_task
                    .metadata
                    .insert("target_child".to_string(), target_child);
                plans.push(DecomposedTaskPlan {
                    task: integration_task,
                    channel_type: VsmChannelType::System2Coordination,
                    decomposition_policy: "system3_capability_split_integration",
                });
            }
        }

        root_task.metadata.insert(
            "decomposition_task_count".to_string(),
            plans.len().to_string(),
        );
        Ok(Some(plans))
    }

    async fn route_decomposed_directive_tasks(
        &self,
        plans: Vec<DecomposedTaskPlan>,
        directive_envelope: &MessageEnvelope,
    ) -> Result<Vec<RoutedTask>, ControllerError> {
        let mut routed_tasks = Vec::with_capacity(plans.len());
        for plan in plans {
            let incoming = synthetic_decomposition_envelope(
                &plan.task,
                plan.channel_type,
                directive_envelope,
                &self.node_id,
            )?;
            self.log_task_mapped(&plan.task, None, &incoming, plan.decomposition_policy)
                .await?;
            routed_tasks.push(self.route_task(plan.task, Some(&incoming)).await?);
        }
        Ok(routed_tasks)
    }

    async fn revise_decomposition_from_result(
        &self,
        result: &TaskResult,
        result_envelope: &MessageEnvelope,
    ) -> Result<Option<RoutedTask>, ControllerError> {
        if !result_requires_decomposition_revision(result) {
            return Ok(None);
        }
        let Some(role) = result.metadata.get("decomposition_role").cloned() else {
            return Ok(None);
        };
        let revision_depth = result
            .metadata
            .get("decomposition_revision_depth")
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(0);
        if revision_depth >= 1 {
            return Ok(None);
        }

        let mut task = TaskPacket::new(
            format!("Revise {role} after {:?}", result.status),
            decomposition_revision_goal(result),
        );
        task.parent_task_id = result
            .metadata
            .get("parent_task_id")
            .map(|value| vsm_core::TaskId::from_string(value.clone()));
        task.directive_id = result
            .metadata
            .get("directive_id")
            .map(|value| vsm_core::DirectiveId::from_string(value.clone()));
        task.dependencies.push(result.task_id.clone());
        task.risk = decomposition_revision_risk(result);
        task.metadata.insert(
            "decomposition_policy".to_string(),
            "system3_result_revision_v1".to_string(),
        );
        task.metadata
            .insert("decomposition_authority".to_string(), "system3".to_string());
        task.metadata
            .insert("decomposition_role".to_string(), role.clone());
        task.metadata.insert(
            "decomposition_revision_depth".to_string(),
            (revision_depth + 1).to_string(),
        );
        task.metadata.insert(
            "decomposition_revision_of_task_id".to_string(),
            result.task_id.to_string(),
        );
        task.metadata.insert(
            "decomposition_result_status".to_string(),
            format!("{:?}", result.status),
        );
        task.metadata.insert(
            "decomposition_result_produced_by".to_string(),
            result.produced_by.to_string(),
        );
        task.metadata
            .insert("requires_code_write".to_string(), "false".to_string());
        if let Some(required_capability) = result.metadata.get("required_capability") {
            task.metadata.insert(
                "required_capability".to_string(),
                required_capability.clone(),
            );
        }
        if let Some(target_child) = result.metadata.get("target_child") {
            task.metadata
                .insert("target_child".to_string(), target_child.clone());
        }
        if let Some(error) = &result.error {
            task.metadata
                .insert("decomposition_result_error".to_string(), error.clone());
        }

        let incoming =
            synthetic_decomposition_revision_envelope(&task, result_envelope, &self.node_id)?;
        self.log_task_mapped(&task, None, &incoming, "system3_result_revision")
            .await?;
        let routed = self.route_task(task, Some(&incoming)).await?;

        self.ledger
            .append_event(
                LedgerEvent::new(
                    LedgerEventKind::Other("decomposition_revision_created".to_string()),
                    serde_json::json!({
                        "failed_task_id": result.task_id.to_string(),
                        "revision_task_id": routed.task.id.to_string(),
                        "decomposition_role": role,
                        "result_status": format!("{:?}", result.status),
                        "child_node_id": routed.child_id.to_string(),
                        "source_channel": format!("{:?}", result_envelope.channel_type),
                        "revision_channel": "System2Coordination",
                        "correlation_id": result_envelope.correlation_id.clone(),
                        "causation_id": result_envelope.causation_id.as_ref().map(ToString::to_string),
                    }),
                )?
                .with_node(self.node_id.clone())
                .with_task(routed.task.id.clone()),
            )
            .await?;

        Ok(Some(routed))
    }

    async fn publish_shadow_task(
        &self,
        task: &mut TaskPacket,
        incoming: Option<&MessageEnvelope>,
        shadow_route: crate::TrialRouteDecision,
    ) -> Result<(), ControllerError> {
        task.assigned_to = Some(shadow_route.child_id.clone());
        task.metadata
            .insert("routed_by".to_string(), self.node_id.to_string());
        task.metadata
            .insert("routing_reason".to_string(), shadow_route.reason.clone());
        tag_task_for_trial_route(task, &shadow_route);

        let _ = self
            .publish_routed_task(
                task.clone(),
                incoming,
                shadow_route.child_id,
                shadow_route.reason,
                shadow_route.genome_id,
                true,
                Some(shadow_route.suggestion_id),
                true,
            )
            .await?;
        Ok(())
    }

    async fn log_trial_fallback_if_approved(
        &self,
        task: &TaskPacket,
    ) -> Result<(), ControllerError> {
        if !task.metadata.contains_key("trial_approved") {
            return Ok(());
        }
        let Some(suggestion_id) = self.trials.read().await.active_suggestion_id() else {
            return Ok(());
        };

        self.ledger
            .append_event(
                LedgerEvent::new(
                    LedgerEventKind::TrialRejected,
                    serde_json::json!({
                        "trial_id": suggestion_id.to_string(),
                        "task_id": task.id.to_string(),
                        "reason": "candidate route unavailable; falling back to champion routing",
                    }),
                )?
                .with_node(self.node_id.clone())
                .with_task(task.id.clone())
                .with_suggestion(suggestion_id),
            )
            .await?;
        Ok(())
    }

    async fn publish_routed_task(
        &self,
        mut task: TaskPacket,
        incoming: Option<&MessageEnvelope>,
        child_id: NodeId,
        reason: String,
        genome_id: vsm_core::GenomeId,
        is_trial_route: bool,
        suggestion_id: Option<vsm_core::SuggestionId>,
        is_shadow_route: bool,
    ) -> Result<RoutedTask, ControllerError> {
        if let Some(pause) = self.subtree_pause_for_child(&child_id).await? {
            self.append_subtree_route_blocked_event(&task, &child_id, &pause, incoming)
                .await?;
            return Err(ControllerError::NoRouteableChild {
                node_id: self.node_id.clone(),
                task_title: format!("{} (subtree paused by algedonic signal)", task.title),
            });
        }

        annotate_outbound_task_channel(&mut task, &child_id);

        let parent_name = {
            let genome = self.genome.read().await;
            genome
                .get_node(&self.node_id)
                .map(|node| node.name.clone())
                .unwrap_or_else(|_| "unknown".to_string())
        };

        let mut event = LedgerEvent::new(
            LedgerEventKind::TaskRouted,
            &TaskRoutedPayload {
                parent_node_id: self.node_id.clone(),
                child_node_id: child_id.clone(),
                task_id: task.id.clone(),
                task_title: task.title.clone(),
                directive_id: task.directive_id.as_ref().map(ToString::to_string),
                parent_task_id: task.parent_task_id.as_ref().map(ToString::to_string),
                dependency_task_ids: task.dependencies.iter().map(ToString::to_string).collect(),
                reason: reason.clone(),
                parent_name,
                is_trial_route,
                is_shadow_route,
                suggestion_id: suggestion_id.as_ref().map(ToString::to_string),
                routed_genome_id: genome_id.to_string(),
                source_channel: incoming.map(|incoming| format!("{:?}", incoming.channel_type)),
                outbound_channel: format!(
                    "{:?}",
                    routed_task_channel(&self.config.publish_channel, incoming)
                ),
                channel_priority: format!("{:?}", routed_task_priority(&task, incoming)),
                causation_id: incoming
                    .and_then(|incoming| incoming.causation_id.as_ref())
                    .map(ToString::to_string),
                correlation_id: incoming.and_then(|incoming| incoming.correlation_id.clone()),
                trial_mode: task.metadata.get("trial_mode").cloned(),
                trial_exposure_basis_points: task
                    .metadata
                    .get("trial_exposure_basis_points")
                    .cloned(),
                trial_exposure_bucket: task.metadata.get("trial_exposure_bucket").cloned(),
                trial_route_role: task.metadata.get("trial_route_role").cloned(),
                handoff_kind: task.metadata.get("handoff_kind").cloned(),
                handoff_channel: task.metadata.get("handoff_channel").cloned(),
                handoff_source_node_id: task.metadata.get("handoff_source_node_id").cloned(),
                handoff_target_node_id: task.metadata.get("handoff_target_node_id").cloned(),
                handoff_via_controller_node_id: task
                    .metadata
                    .get("handoff_via_controller_node_id")
                    .cloned(),
                handoff_correlation_id: task.metadata.get("handoff_correlation_id").cloned(),
                handoff_causation_id: task.metadata.get("handoff_causation_id").cloned(),
                handoff_envelope_id: task.metadata.get("handoff_envelope_id").cloned(),
                handoff_operation_kind: task.metadata.get("handoff_operation_kind").cloned(),
                handoff_artifact_count: task.metadata.get("handoff_artifact_count").cloned(),
                handoff_related_task_id: task.metadata.get("handoff_related_task_id").cloned(),
                handoff_dependency_task_ids: task
                    .metadata
                    .get("handoff_dependency_task_ids")
                    .cloned(),
                management_kind: task.metadata.get("management_kind").cloned(),
                management_channel: task.metadata.get("management_channel").cloned(),
                management_source_node_id: task.metadata.get("management_source_node_id").cloned(),
                management_target_node_id: task.metadata.get("management_target_node_id").cloned(),
                management_via_controller_node_id: task
                    .metadata
                    .get("management_via_controller_node_id")
                    .cloned(),
                management_correlation_id: task.metadata.get("management_correlation_id").cloned(),
                management_causation_id: task.metadata.get("management_causation_id").cloned(),
                management_envelope_id: task.metadata.get("management_envelope_id").cloned(),
                management_operation_kind: task.metadata.get("management_operation_kind").cloned(),
                management_directive_message_id: task
                    .metadata
                    .get("management_directive_message_id")
                    .cloned(),
                management_policy: task.metadata.get("management_policy").cloned(),
                management_related_task_id: task
                    .metadata
                    .get("management_related_task_id")
                    .cloned(),
                management_dependency_task_ids: task
                    .metadata
                    .get("management_dependency_task_ids")
                    .cloned(),
                coordination_kind: task.metadata.get("coordination_kind").cloned(),
                coordination_signal_message_id: task
                    .metadata
                    .get("coordination_signal_message_id")
                    .cloned(),
                coordination_source_node_id: task
                    .metadata
                    .get("coordination_source_node_id")
                    .cloned(),
                coordination_target_node_id: task
                    .metadata
                    .get("coordination_target_node_id")
                    .cloned(),
                coordination_severity: task.metadata.get("coordination_severity").cloned(),
                coordination_policy: task.metadata.get("coordination_policy").cloned(),
                command_issued_by: task.metadata.get("command_issued_by").cloned(),
                command_target_node_id: task.metadata.get("command_target_node_id").cloned(),
                command_non_negotiable: task.metadata.get("command_non_negotiable").cloned(),
                command_legal_or_policy_basis: task
                    .metadata
                    .get("command_legal_or_policy_basis")
                    .cloned(),
                command_system5_identity: task.metadata.get("command_system5_identity").cloned(),
                command_policy_values: task.metadata.get("command_policy_values").cloned(),
                command_policy_constraints: task
                    .metadata
                    .get("command_policy_constraints")
                    .cloned(),
                command_denied_capabilities: task
                    .metadata
                    .get("command_denied_capabilities")
                    .cloned(),
            },
        )?
        .with_genome(genome_id.clone())
        .with_node(self.node_id.clone())
        .with_task(task.id.clone());
        if let Some(directive_id) = task.directive_id.clone() {
            event = event.with_directive(directive_id);
        }
        self.ledger.append_event(event).await?;

        if is_trial_route {
            if let Some(suggestion_id) = suggestion_id.clone() {
                self.ledger
                    .append_event(
                        LedgerEvent::new(
                            LedgerEventKind::TrialTaskRouted,
                            serde_json::json!({
                                "trial_id": suggestion_id.to_string(),
                                "task_id": task.id.to_string(),
                                "child_node_id": child_id.to_string(),
                                "candidate_genome_id": genome_id.to_string(),
                                "is_shadow_route": is_shadow_route,
                                "trial_mode": task.metadata.get("trial_mode").cloned(),
                                "trial_route_role": task.metadata.get("trial_route_role").cloned(),
                            }),
                        )?
                        .with_node(self.node_id.clone())
                        .with_task(task.id.clone())
                        .with_genome(genome_id.clone())
                        .with_suggestion(suggestion_id),
                    )
                    .await?;
            }
            self.trials.write().await.mark_task_routed();
            if let Some(record) = self.trials.read().await.active_record(self.node_id.clone()) {
                self.ledger.write_trial_record(record).await?;
            }
        }

        let mut envelope = envelope_for_task(&task)?
            .with_route(Some(self.node_id.clone()), Some(child_id.clone()));
        envelope.channel_type = routed_task_channel(&self.config.publish_channel, incoming);
        envelope.priority = routed_task_priority(&task, incoming);
        envelope.correlation_id = incoming
            .and_then(|incoming| incoming.correlation_id.clone())
            .or_else(|| incoming.map(|incoming| incoming.id.to_string()))
            .or_else(|| Some(task.id.to_string()));
        envelope.causation_id = incoming.map(|incoming| incoming.id.clone());
        envelope.trace = incoming
            .map(|incoming| incoming.trace.clone())
            .unwrap_or_default();
        envelope.trace.push(self.node_id.clone());
        envelope.metadata = routed_envelope_metadata(&reason, incoming, &envelope.channel_type);
        copy_channel_metadata_to_envelope(&task, &mut envelope);

        if self.config.append_message_events {
            self.ledger
                .append_event(LedgerEvent::for_message(
                    LedgerEventKind::MessagePublished,
                    &envelope,
                )?)
                .await?;
        }

        self.transport.publish(envelope).await?;

        Ok(RoutedTask {
            task,
            child_id,
            reason,
        })
    }

    async fn append_subtree_route_blocked_event(
        &self,
        task: &TaskPacket,
        child_id: &NodeId,
        pause: &SubtreePauseState,
        incoming: Option<&MessageEnvelope>,
    ) -> Result<(), ControllerError> {
        let mut event = LedgerEvent::new(
            LedgerEventKind::Other("algedonic_subtree_route_blocked".to_string()),
            serde_json::json!({
                "controller_node_id": self.node_id.to_string(),
                "child_node_id": child_id.to_string(),
                "task_id": task.id.to_string(),
                "task_title": task.title.clone(),
                "paused_target_node_id": pause.target_node_id.to_string(),
                "paused_at": pause.paused_at.to_rfc3339(),
                "reason": pause.reason.clone(),
                "severity": pause.severity,
                "require_human_confirmation": pause.require_human_confirmation,
                "source_channel": incoming.map(|incoming| format!("{:?}", incoming.channel_type)),
            }),
        )?
        .with_node(self.node_id.clone())
        .with_task(task.id.clone());
        if let Some(directive_id) = task.directive_id.clone() {
            event = event.with_directive(directive_id);
        }
        if let Some(correlation_id) = incoming.and_then(|incoming| incoming.correlation_id.clone())
        {
            event = event.with_correlation(correlation_id);
        }
        self.ledger.append_event(event).await?;
        Ok(())
    }

    async fn record_trial_trace_for_result(
        &self,
        result: &TaskResult,
    ) -> Result<(), ControllerError> {
        let Some(result_suggestion_id) = result
            .metadata
            .get("related_suggestion_id")
            .or_else(|| result.metadata.get("trial_id"))
            .map(|value| vsm_core::SuggestionId::from_string(value.clone()))
        else {
            return Ok(());
        };

        let Some(active_suggestion_id) = self.trials.read().await.active_suggestion_id() else {
            return Ok(());
        };
        if active_suggestion_id != result_suggestion_id {
            return Ok(());
        }

        let trace = self
            .find_task_trace(&result.task_id, &result_suggestion_id)
            .await?;
        let Some(trace) = trace else {
            return Ok(());
        };

        let evaluation = {
            let mut trials = self.trials.write().await;
            trials.record_trace(trace.clone())
        };

        if let Some(evaluation) = evaluation {
            if let Some(record) = self.trials.read().await.active_record(self.node_id.clone()) {
                self.ledger.write_trial_record(record).await?;
            }
            self.ledger
                .append_event(
                    LedgerEvent::new(
                        LedgerEventKind::TrialTraceRecorded,
                        serde_json::json!({
                            "trial_id": result_suggestion_id.to_string(),
                            "trace_id": trace.id.to_string(),
                            "task_id": trace.task_id.to_string(),
                            "trace_count": evaluation.trace_count,
                            "total_score": evaluation.total_score,
                            "decision": trial_decision_key(&evaluation.decision),
                        }),
                    )?
                    .with_node(self.node_id.clone())
                    .with_task(trace.task_id.clone())
                    .with_genome(trace.genome_id.clone())
                    .with_suggestion(result_suggestion_id),
                )
                .await?;
            self.log_trial_decision(&evaluation).await?;
            self.apply_trial_decision(&evaluation).await?;
        }

        Ok(())
    }

    async fn find_task_trace(
        &self,
        task_id: &vsm_core::TaskId,
        suggestion_id: &vsm_core::SuggestionId,
    ) -> Result<Option<TaskTrace>, ControllerError> {
        let traces = self
            .ledger
            .recent_task_traces(TraceWindow {
                since: None,
                limit: Some(500),
            })
            .await?;
        Ok(traces.into_iter().rev().find(|trace| {
            &trace.task_id == task_id
                && trace
                    .related_suggestion_ids
                    .iter()
                    .any(|related| related == suggestion_id)
        }))
    }

    async fn log_trial_decision(
        &self,
        evaluation: &TrialEvaluation,
    ) -> Result<(), ControllerError> {
        let Some(suggestion_id) = self.trials.read().await.active_suggestion_id() else {
            return Ok(());
        };
        self.ledger
            .append_event(
                LedgerEvent::new(
                    LedgerEventKind::TrialDecisionRecorded,
                    serde_json::json!({
                        "trial_id": suggestion_id.to_string(),
                        "decision": trial_decision_key(&evaluation.decision),
                        "trace_count": evaluation.trace_count,
                        "total_score": evaluation.total_score,
                    }),
                )?
                .with_node(self.node_id.clone())
                .with_suggestion(suggestion_id),
            )
            .await?;
        Ok(())
    }

    async fn apply_trial_decision(
        &self,
        evaluation: &TrialEvaluation,
    ) -> Result<(), ControllerError> {
        match evaluation.decision {
            TrialDecision::Continue => Ok(()),
            TrialDecision::Promote => {
                let completed = {
                    self.trials
                        .write()
                        .await
                        .promote_active(self.node_id.clone())?
                };
                {
                    let mut genome = self.genome.write().await;
                    *genome = completed.candidate_genome.clone();
                }
                self.ledger
                    .set_champion_genome(&self.node_id, completed.candidate_genome.clone())
                    .await?;
                self.ledger
                    .write_trial_record(completed.record.clone())
                    .await?;
                self.ledger
                    .append_event(
                        LedgerEvent::new(
                            LedgerEventKind::TrialPromoted,
                            serde_json::json!({
                                "trial_id": completed.suggestion_id.to_string(),
                                "base_genome_id": completed.base_genome_id.to_string(),
                                "candidate_genome_id": completed.candidate_genome_id.to_string(),
                                "trace_count": completed.evaluation.trace_count,
                                "total_score": completed.evaluation.total_score,
                            }),
                        )?
                        .with_node(self.node_id.clone())
                        .with_genome(completed.candidate_genome_id.clone())
                        .with_suggestion(completed.suggestion_id.clone()),
                    )
                    .await?;
                self.ledger
                    .append_event(
                        LedgerEvent::new(
                            LedgerEventKind::GenomePatchApplied,
                            serde_json::json!({
                                "trial_id": completed.suggestion_id.to_string(),
                                "new_champion_genome_id": completed.candidate_genome_id.to_string(),
                            }),
                        )?
                        .with_node(self.node_id.clone())
                        .with_genome(completed.candidate_genome_id)
                        .with_suggestion(completed.suggestion_id),
                    )
                    .await?;
                Ok(())
            }
            TrialDecision::Prune => {
                let completed = {
                    self.trials
                        .write()
                        .await
                        .prune_active(self.node_id.clone())?
                };
                self.ledger
                    .write_trial_record(completed.record.clone())
                    .await?;
                self.ledger
                    .append_event(
                        LedgerEvent::new(
                            LedgerEventKind::TrialPruned,
                            serde_json::json!({
                                "trial_id": completed.suggestion_id.to_string(),
                                "base_genome_id": completed.base_genome_id.to_string(),
                                "candidate_genome_id": completed.candidate_genome_id.to_string(),
                                "trace_count": completed.evaluation.trace_count,
                                "total_score": completed.evaluation.total_score,
                            }),
                        )?
                        .with_node(self.node_id.clone())
                        .with_genome(completed.base_genome_id)
                        .with_suggestion(completed.suggestion_id),
                    )
                    .await?;
                Ok(())
            }
        }
    }

    async fn append_lifecycle_event(&self, kind: LedgerEventKind) -> Result<(), ControllerError> {
        let genome_id = { self.genome.read().await.id.clone() };
        let event = LedgerEvent::new(
            kind,
            serde_json::json!({
                "node_id": self.node_id.to_string(),
                "role": "controller"
            }),
        )?
        .with_node(self.node_id.clone())
        .with_genome(genome_id);
        self.ledger.append_event(event).await?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct RoutedTask {
    task: TaskPacket,
    child_id: NodeId,
    reason: String,
}

impl From<RoutedTask> for RoutedTaskOutcome {
    fn from(value: RoutedTask) -> Self {
        Self {
            task: value.task,
            child_id: value.child_id,
            reason: value.reason,
        }
    }
}

#[derive(Clone, Debug)]
struct DecomposedTaskPlan {
    task: TaskPacket,
    channel_type: VsmChannelType,
    decomposition_policy: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DecompositionRole {
    Implementation,
    Test,
    Review,
    Integration,
}

#[derive(Clone, Debug, serde::Serialize)]
struct TaskRoutedPayload {
    parent_node_id: NodeId,
    child_node_id: NodeId,
    task_id: vsm_core::TaskId,
    task_title: String,
    directive_id: Option<String>,
    parent_task_id: Option<String>,
    dependency_task_ids: Vec<String>,
    reason: String,
    parent_name: String,
    is_trial_route: bool,
    is_shadow_route: bool,
    suggestion_id: Option<String>,
    routed_genome_id: String,
    source_channel: Option<String>,
    outbound_channel: String,
    channel_priority: String,
    causation_id: Option<String>,
    correlation_id: Option<String>,
    trial_mode: Option<String>,
    trial_exposure_basis_points: Option<String>,
    trial_exposure_bucket: Option<String>,
    trial_route_role: Option<String>,
    handoff_kind: Option<String>,
    handoff_channel: Option<String>,
    handoff_source_node_id: Option<String>,
    handoff_target_node_id: Option<String>,
    handoff_via_controller_node_id: Option<String>,
    handoff_correlation_id: Option<String>,
    handoff_causation_id: Option<String>,
    handoff_envelope_id: Option<String>,
    handoff_operation_kind: Option<String>,
    handoff_artifact_count: Option<String>,
    handoff_related_task_id: Option<String>,
    handoff_dependency_task_ids: Option<String>,
    management_kind: Option<String>,
    management_channel: Option<String>,
    management_source_node_id: Option<String>,
    management_target_node_id: Option<String>,
    management_via_controller_node_id: Option<String>,
    management_correlation_id: Option<String>,
    management_causation_id: Option<String>,
    management_envelope_id: Option<String>,
    management_operation_kind: Option<String>,
    management_directive_message_id: Option<String>,
    management_policy: Option<String>,
    management_related_task_id: Option<String>,
    management_dependency_task_ids: Option<String>,
    coordination_kind: Option<String>,
    coordination_signal_message_id: Option<String>,
    coordination_source_node_id: Option<String>,
    coordination_target_node_id: Option<String>,
    coordination_severity: Option<String>,
    coordination_policy: Option<String>,
    command_issued_by: Option<String>,
    command_target_node_id: Option<String>,
    command_non_negotiable: Option<String>,
    command_legal_or_policy_basis: Option<String>,
    command_system5_identity: Option<String>,
    command_policy_values: Option<String>,
    command_policy_constraints: Option<String>,
    command_denied_capabilities: Option<String>,
}

fn result_is_shadow_trial(result: &TaskResult) -> bool {
    result
        .metadata
        .get("trial_shadow")
        .map(|value| value == "true")
        .unwrap_or(false)
        || result
            .metadata
            .get("trial_route_role")
            .map(|value| value == "shadow")
            .unwrap_or(false)
}

fn task_from_command(command: &VsmCommand) -> TaskPacket {
    let mut task = TaskPacket::new(command.title.clone(), command.body.clone());
    task.assigned_to = Some(command.target.clone());
    task.risk = if command.legal_or_policy_basis.is_some()
        || !command.non_negotiable_constraints.is_empty()
        || !command.denied_capabilities.is_empty()
    {
        RiskClass::Critical
    } else if command.non_negotiable {
        RiskClass::High
    } else {
        RiskClass::Medium
    };

    if command.non_negotiable {
        task.constraints.push("non-negotiable command".to_string());
    }
    if let Some(basis) = &command.legal_or_policy_basis {
        task.constraints.push(format!("policy basis: {basis}"));
        task.authority_refs.push(basis.clone());
        task.metadata
            .insert("legal_or_policy_basis".to_string(), basis.clone());
        task.metadata
            .insert("command_legal_or_policy_basis".to_string(), basis.clone());
    }
    if let Some(identity) = &command.system5_identity {
        task.constraints
            .push(format!("system5 identity: {identity}"));
        task.metadata
            .insert("command_system5_identity".to_string(), identity.clone());
    }
    for value in &command.policy_values {
        task.constraints.push(format!("policy value: {value}"));
    }
    if !command.policy_values.is_empty() {
        task.metadata.insert(
            "command_policy_values".to_string(),
            command.policy_values.join("|"),
        );
    }
    for constraint in &command.non_negotiable_constraints {
        task.constraints
            .push(format!("non-negotiable policy constraint: {constraint}"));
    }
    if !command.non_negotiable_constraints.is_empty() {
        task.metadata.insert(
            "command_policy_constraints".to_string(),
            command.non_negotiable_constraints.join("|"),
        );
    }
    for capability in &command.denied_capabilities {
        task.constraints
            .push(format!("denied capability: {capability}"));
    }
    if !command.denied_capabilities.is_empty() {
        task.metadata.insert(
            "command_denied_capabilities".to_string(),
            command.denied_capabilities.join("|"),
        );
    }
    for (key, value) in &command.metadata {
        if key == "required_capability" || key == "requires_code_write" || key == "target_child" {
            task.metadata.insert(key.clone(), value.clone());
        }
        task.metadata
            .insert(format!("command_metadata.{key}"), value.clone());
    }

    task.metadata
        .insert("vsm_channel".to_string(), "Command".to_string());
    task.metadata.insert(
        "command_issued_by".to_string(),
        command.issued_by.to_string(),
    );
    task.metadata.insert(
        "command_non_negotiable".to_string(),
        command.non_negotiable.to_string(),
    );
    task.metadata.insert(
        "command_target_node_id".to_string(),
        command.target.to_string(),
    );
    task
}

fn directive_requests_decomposition(directive: &Directive) -> bool {
    directive_bool_metadata(directive, "decompose")
        || directive
            .metadata
            .get("decomposition_policy")
            .is_some_and(|policy| policy == "system3_capability_split")
        || directive
            .metadata
            .get("task.metadata.decomposition_policy")
            .is_some_and(|policy| policy == "system3_capability_split")
}

fn directive_requires_tests(directive: &Directive) -> bool {
    directive_bool_metadata(directive, "requires_tests")
        || directive.metadata.contains_key("test_targets")
        || directive
            .metadata
            .contains_key("task.metadata.test_targets")
}

fn directive_requires_review(directive: &Directive) -> bool {
    directive_bool_metadata(directive, "requires_review")
        || matches!(directive.risk, RiskClass::High | RiskClass::Critical)
}

fn directive_requires_integration(directive: &Directive) -> bool {
    directive_bool_metadata(directive, "requires_integration")
}

fn directive_bool_metadata(directive: &Directive, key: &str) -> bool {
    directive
        .metadata
        .get(key)
        .or_else(|| directive.metadata.get(&format!("task.metadata.{key}")))
        .map(|value| value == "true" || value == "1" || value.eq_ignore_ascii_case("yes"))
        .unwrap_or(false)
}

fn annotate_decomposition_pressure(
    root_task: &mut TaskPacket,
    pressure: &EnvironmentPressureSummary,
) {
    if pressure.is_empty() {
        return;
    }

    root_task.metadata.insert(
        "decomposition_pressure_policy".to_string(),
        "environment_three_four_pressure_v1".to_string(),
    );
    root_task.metadata.insert(
        "environment_signal_count".to_string(),
        pressure.event_count.to_string(),
    );
    root_task.metadata.insert(
        "three_four_homeostat_count".to_string(),
        pressure.homeostat_count.to_string(),
    );
    root_task.metadata.insert(
        "environment_risk_pressure".to_string(),
        format!("{:.3}", pressure.risk_pressure),
    );
    root_task.metadata.insert(
        "environment_opportunity_pressure".to_string(),
        format!("{:.3}", pressure.opportunity_pressure),
    );
    root_task.metadata.insert(
        "environment_feedback_pressure".to_string(),
        format!("{:.3}", pressure.feedback_pressure),
    );
    root_task.metadata.insert(
        "environment_max_severity".to_string(),
        pressure.max_severity.to_string(),
    );
}

fn homeostat_pressure_requires_review(pressure: &EnvironmentPressureSummary) -> bool {
    pressure.homeostat_count > 0 && (pressure.risk_pressure >= 6.0 || pressure.max_severity >= 7)
}

fn homeostat_pressure_requires_integration(pressure: &EnvironmentPressureSummary) -> bool {
    pressure.homeostat_count > 0
        && pressure.risk_pressure >= 6.0
        && pressure.opportunity_pressure >= 6.0
}

fn decomposed_child_task(root_task: &TaskPacket, role: DecompositionRole) -> TaskPacket {
    let mut task = TaskPacket::new(
        decomposed_task_title(root_task, role),
        decomposed_task_goal(root_task, role),
    );
    task.directive_id = root_task.directive_id.clone();
    task.parent_task_id = Some(root_task.id.clone());
    task.target_state = root_task.target_state.clone();
    task.scope = root_task.scope.clone();
    task.constraints = root_task.constraints.clone();
    task.context_refs = root_task.context_refs.clone();
    task.authority_refs = root_task.authority_refs.clone();
    task.risk = root_task.risk.clone();
    task.static_predicates = root_task.static_predicates.clone();
    task.metadata = root_task.metadata.clone();
    task.metadata.remove("target_child");
    task.metadata.insert(
        "decomposition_policy".to_string(),
        "system3_capability_split_v1".to_string(),
    );
    task.metadata
        .insert("decomposition_authority".to_string(), "system3".to_string());
    task.metadata.insert(
        "decomposition_parent_task_id".to_string(),
        root_task.id.to_string(),
    );
    task.metadata.insert(
        "decomposition_role".to_string(),
        decomposition_role_key(role).to_string(),
    );
    task
}

fn decomposed_task_title(root_task: &TaskPacket, role: DecompositionRole) -> String {
    match role {
        DecompositionRole::Implementation => format!("Implement: {}", root_task.title),
        DecompositionRole::Test => format!("Test: {}", root_task.title),
        DecompositionRole::Review => format!("Review: {}", root_task.title),
        DecompositionRole::Integration => format!("Integrate: {}", root_task.title),
    }
}

fn decomposed_task_goal(root_task: &TaskPacket, role: DecompositionRole) -> String {
    match role {
        DecompositionRole::Implementation => root_task.goal.clone(),
        DecompositionRole::Test => {
            format!(
                "Verify the implementation for directive task {}",
                root_task.id
            )
        }
        DecompositionRole::Review => {
            format!(
                "Review the implementation for directive task {}",
                root_task.id
            )
        }
        DecompositionRole::Integration => {
            format!(
                "Integrate dependent outputs for directive task {}",
                root_task.id
            )
        }
    }
}

fn decomposition_role_key(role: DecompositionRole) -> &'static str {
    match role {
        DecompositionRole::Implementation => "implementation",
        DecompositionRole::Test => "test",
        DecompositionRole::Review => "review",
        DecompositionRole::Integration => "integration",
    }
}

fn target_child_for_capability(
    genome: &OrganizationalGenome,
    parent: &vsm_core::ViableNode,
    role: DecompositionRole,
) -> Option<String> {
    let preferred_kind = preferred_leaf_kind(role);
    if let Some(kind) = preferred_kind {
        if let Some(child) = parent.children.iter().find_map(|child_id| {
            let child = genome.get_node(child_id).ok()?;
            if child.status == vsm_core::NodeLifecycleStatus::Retired {
                return None;
            }
            (child.leaf_operation.kind.as_ref() == Some(&kind)).then_some(child)
        }) {
            return Some(child.name.clone());
        }
    }

    parent.children.iter().find_map(|child_id| {
        let child = genome.get_node(child_id).ok()?;
        if child.status == vsm_core::NodeLifecycleStatus::Retired {
            return None;
        }
        capability_supports_role(&child.capabilities(), role).then(|| child.name.clone())
    })
}

fn preferred_leaf_kind(role: DecompositionRole) -> Option<vsm_core::LeafOperationKind> {
    match role {
        DecompositionRole::Implementation => Some(vsm_core::LeafOperationKind::Coding),
        DecompositionRole::Test => Some(vsm_core::LeafOperationKind::Testing),
        DecompositionRole::Review => Some(vsm_core::LeafOperationKind::Reviewing),
        DecompositionRole::Integration => Some(vsm_core::LeafOperationKind::Integration),
    }
}

fn capability_supports_role(
    capabilities: &vsm_core::CapabilitySet,
    role: DecompositionRole,
) -> bool {
    match role {
        DecompositionRole::Implementation => capabilities.can_write_code,
        DecompositionRole::Test => capabilities.can_run_tests,
        DecompositionRole::Review => capabilities.can_review,
        DecompositionRole::Integration => capabilities.can_integrate,
    }
}

fn synthetic_decomposition_envelope(
    task: &TaskPacket,
    channel_type: VsmChannelType,
    directive_envelope: &MessageEnvelope,
    controller_node_id: &NodeId,
) -> Result<MessageEnvelope, ControllerError> {
    let mut envelope =
        MessageEnvelope::new(channel_type, BuiltinPayloadType::TaskPacket.as_str(), task)?
            .with_route(
                Some(controller_node_id.clone()),
                Some(controller_node_id.clone()),
            );
    envelope.correlation_id = directive_envelope
        .correlation_id
        .clone()
        .or_else(|| Some(directive_envelope.id.to_string()));
    envelope.causation_id = Some(directive_envelope.id.clone());
    envelope.trace = directive_envelope.trace.clone();
    envelope.metadata.insert(
        "decomposition_policy".to_string(),
        "system3_capability_split_v1".to_string(),
    );
    envelope.metadata.insert(
        "decomposition_role".to_string(),
        task.metadata
            .get("decomposition_role")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string()),
    );
    envelope.metadata.insert(
        "source_directive_message_id".to_string(),
        directive_envelope.id.to_string(),
    );
    Ok(envelope)
}

fn synthetic_system2_coordination_envelope(
    task: &TaskPacket,
    signal_envelope: &MessageEnvelope,
    controller_node_id: &NodeId,
) -> Result<MessageEnvelope, ControllerError> {
    let mut envelope = MessageEnvelope::new(
        VsmChannelType::System2Coordination,
        BuiltinPayloadType::TaskPacket.as_str(),
        task,
    )?
    .with_route(
        Some(controller_node_id.clone()),
        Some(controller_node_id.clone()),
    );
    envelope.correlation_id = signal_envelope
        .correlation_id
        .clone()
        .or_else(|| Some(signal_envelope.id.to_string()));
    envelope.causation_id = Some(signal_envelope.id.clone());
    envelope.trace = signal_envelope.trace.clone();
    envelope.metadata.insert(
        "coordination_policy".to_string(),
        "system2_dampening_v1".to_string(),
    );
    envelope.metadata.insert(
        "coordination_signal_message_id".to_string(),
        signal_envelope.id.to_string(),
    );
    Ok(envelope)
}

fn system2_signal_requires_dampening(signal: &System2CoordinationSignal) -> bool {
    let severity = signal.severity.unwrap_or(0);
    match &signal.kind {
        System2CoordinationKind::Contention | System2CoordinationKind::Oscillation => severity >= 5,
        System2CoordinationKind::DependencyBlocked => severity >= 7,
        System2CoordinationKind::DependencyReady
        | System2CoordinationKind::HandoffNotice
        | System2CoordinationKind::Other(_) => false,
    }
}

fn system2_dampening_task_from_signal(
    signal: &System2CoordinationSignal,
    signal_envelope: &MessageEnvelope,
) -> TaskPacket {
    let kind = format!("{:?}", signal.kind);
    let mut task = TaskPacket::new(
        format!("Dampen System 2 {kind}"),
        system2_dampening_goal(signal),
    );
    task.dependencies = signal.affected_task_ids.clone();
    task.risk = match signal.severity.unwrap_or(0) {
        8..=u8::MAX => RiskClass::High,
        _ => RiskClass::Medium,
    };
    task.metadata.insert(
        "coordination_policy".to_string(),
        "system2_dampening_v1".to_string(),
    );
    task.metadata
        .insert("coordination_kind".to_string(), kind.clone());
    task.metadata.insert(
        "coordination_signal_message_id".to_string(),
        signal_envelope.id.to_string(),
    );
    task.metadata.insert(
        "coordination_source_node_id".to_string(),
        signal
            .source_node_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| signal.coordinator_node_id.to_string()),
    );
    if let Some(target_node_id) = &signal.target_node_id {
        task.metadata.insert(
            "coordination_target_node_id".to_string(),
            target_node_id.to_string(),
        );
        task.metadata
            .insert("target_child".to_string(), target_node_id.to_string());
    }
    if let Some(severity) = signal.severity {
        task.metadata
            .insert("coordination_severity".to_string(), severity.to_string());
    }
    if !signal.affected_node_ids.is_empty() {
        task.metadata.insert(
            "coordination_affected_node_ids".to_string(),
            signal
                .affected_node_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    if !signal.affected_task_ids.is_empty() {
        task.metadata.insert(
            "coordination_affected_task_ids".to_string(),
            signal
                .affected_task_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    task.metadata
        .insert("requires_code_write".to_string(), "false".to_string());
    task.metadata
        .insert("decomposition_authority".to_string(), "system2".to_string());
    task.metadata.insert(
        "vsm_coordination_summary".to_string(),
        signal.summary.clone(),
    );
    task
}

fn system2_dampening_goal(signal: &System2CoordinationSignal) -> String {
    let mut goal = format!(
        "Coordinate affected System 1 units to dampen {:?}. Summary: {}",
        signal.kind, signal.summary
    );
    if !signal.evidence.is_empty() {
        goal.push_str(" Evidence: ");
        goal.push_str(&signal.evidence.join("; "));
    }
    goal
}

fn operation_handoff_task_from_handoff(
    handoff: &OperationHandoff,
    envelope: &MessageEnvelope,
) -> TaskPacket {
    let mut task = TaskPacket::new(handoff.title.clone(), operation_handoff_goal(handoff));
    task.assigned_to = Some(handoff.target_node_id.clone());
    task.parent_task_id = handoff.related_task_id.clone();
    task.dependencies = handoff.dependency_task_ids.clone();
    task.risk = match &handoff.kind {
        vsm_core::OperationHandoffKind::DependencyBlocked => RiskClass::High,
        _ => RiskClass::Medium,
    };
    task.metadata.insert(
        "handoff_kind".to_string(),
        "operation_to_operation".to_string(),
    );
    task.metadata.insert(
        "handoff_operation_kind".to_string(),
        format!("{:?}", handoff.kind),
    );
    task.metadata.insert(
        "handoff_source_node_id".to_string(),
        handoff.source_node_id.to_string(),
    );
    task.metadata.insert(
        "handoff_target_node_id".to_string(),
        handoff.target_node_id.to_string(),
    );
    task.metadata
        .insert("handoff_envelope_id".to_string(), envelope.id.to_string());
    task.metadata.insert(
        "handoff_artifact_count".to_string(),
        handoff.artifacts.len().to_string(),
    );
    task.metadata
        .insert("handoff_summary".to_string(), handoff.summary.clone());
    if let Some(related_task_id) = &handoff.related_task_id {
        task.metadata.insert(
            "handoff_related_task_id".to_string(),
            related_task_id.to_string(),
        );
    }
    if !handoff.dependency_task_ids.is_empty() {
        task.metadata.insert(
            "handoff_dependency_task_ids".to_string(),
            handoff
                .dependency_task_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    let artifact_refs = handoff
        .artifacts
        .iter()
        .filter_map(|artifact| artifact.uri.clone())
        .collect::<Vec<_>>();
    if !artifact_refs.is_empty() {
        task.context_refs.extend(artifact_refs.clone());
        task.metadata
            .insert("handoff_artifact_refs".to_string(), artifact_refs.join(","));
    }
    for (key, value) in &handoff.metadata {
        task.metadata
            .insert(format!("handoff_metadata.{key}"), value.clone());
    }
    task.metadata
        .insert("requires_code_write".to_string(), "false".to_string());
    task.metadata.insert(
        "source_payload_type".to_string(),
        BuiltinPayloadType::OperationHandoff.as_str().to_string(),
    );
    task
}

fn operation_handoff_goal(handoff: &OperationHandoff) -> String {
    let mut goal = format!(
        "Continue operation handoff from {} to {}. Summary: {}",
        handoff.source_node_id, handoff.target_node_id, handoff.summary
    );
    if !handoff.artifacts.is_empty() {
        let artifact_kinds = handoff
            .artifacts
            .iter()
            .map(|artifact| artifact.kind.clone())
            .collect::<Vec<_>>()
            .join(", ");
        goal.push_str(&format!(" Artifacts: {artifact_kinds}."));
    }
    if !handoff.evidence.is_empty() {
        goal.push_str(" Evidence: ");
        goal.push_str(&handoff.evidence.join("; "));
    }
    goal
}

fn management_operation_task_from_directive(
    directive: &ManagementOperationDirective,
    envelope: &MessageEnvelope,
) -> TaskPacket {
    let mut task = TaskPacket::new(directive.title.clone(), directive.body.clone());
    task.assigned_to = Some(directive.operation_node_id.clone());
    task.parent_task_id = directive.related_task_id.clone();
    task.dependencies = directive.dependency_task_ids.clone();
    task.target_state = directive.target_state.clone();
    task.constraints = directive.constraints.clone();
    task.context_refs = directive.context_refs.clone();
    task.authority_refs = directive.authority_refs.clone();
    task.risk = directive.risk.clone();
    task.metadata.insert(
        "management_kind".to_string(),
        "management_to_operation".to_string(),
    );
    task.metadata.insert(
        "management_operation_kind".to_string(),
        format!("{:?}", directive.kind),
    );
    task.metadata.insert(
        "management_source_node_id".to_string(),
        directive.manager_node_id.to_string(),
    );
    task.metadata.insert(
        "management_target_node_id".to_string(),
        directive.operation_node_id.to_string(),
    );
    task.metadata.insert(
        "management_directive_message_id".to_string(),
        envelope.id.to_string(),
    );
    task.metadata.insert(
        "management_policy".to_string(),
        "management_operation_directive_v1".to_string(),
    );
    if let Some(related_task_id) = &directive.related_task_id {
        task.metadata.insert(
            "management_related_task_id".to_string(),
            related_task_id.to_string(),
        );
    }
    if !directive.dependency_task_ids.is_empty() {
        task.metadata.insert(
            "management_dependency_task_ids".to_string(),
            directive
                .dependency_task_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    for (key, value) in &directive.metadata {
        if key == "requires_code_write" || key == "required_capability" || key == "target_child" {
            task.metadata.insert(key.clone(), value.clone());
        }
        task.metadata
            .insert(format!("management_metadata.{key}"), value.clone());
    }
    task.metadata.insert(
        "source_payload_type".to_string(),
        BuiltinPayloadType::ManagementOperationDirective
            .as_str()
            .to_string(),
    );
    task
}

fn result_requires_decomposition_revision(result: &TaskResult) -> bool {
    matches!(
        result.status,
        TaskOutcomeStatus::Failed | TaskOutcomeStatus::Rejected | TaskOutcomeStatus::NeedsHuman
    ) && result.metadata.contains_key("decomposition_role")
}

fn decomposition_revision_goal(result: &TaskResult) -> String {
    let mut goal = format!(
        "Revise decomposed task {} after {:?}. Child summary: {}",
        result.task_id, result.status, result.summary
    );
    if let Some(error) = &result.error {
        goal.push_str(&format!(" Error: {error}"));
    }
    goal
}

fn decomposition_revision_risk(result: &TaskResult) -> RiskClass {
    match result.status {
        TaskOutcomeStatus::NeedsHuman => RiskClass::High,
        TaskOutcomeStatus::Failed | TaskOutcomeStatus::Rejected => RiskClass::Medium,
        TaskOutcomeStatus::Completed | TaskOutcomeStatus::Noop => RiskClass::Low,
    }
}

fn synthetic_decomposition_revision_envelope(
    task: &TaskPacket,
    result_envelope: &MessageEnvelope,
    controller_node_id: &NodeId,
) -> Result<MessageEnvelope, ControllerError> {
    let mut envelope = MessageEnvelope::new(
        VsmChannelType::System2Coordination,
        BuiltinPayloadType::TaskPacket.as_str(),
        task,
    )?
    .with_route(
        Some(controller_node_id.clone()),
        Some(controller_node_id.clone()),
    );
    envelope.correlation_id = result_envelope
        .correlation_id
        .clone()
        .or_else(|| Some(result_envelope.id.to_string()));
    envelope.causation_id = Some(result_envelope.id.clone());
    envelope.trace = result_envelope.trace.clone();
    envelope.metadata.insert(
        "decomposition_policy".to_string(),
        "system3_result_revision_v1".to_string(),
    );
    envelope.metadata.insert(
        "decomposition_revision_of_task_id".to_string(),
        task.metadata
            .get("decomposition_revision_of_task_id")
            .cloned()
            .unwrap_or_default(),
    );
    Ok(envelope)
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ResourceEpochBudget {
    token_budget: Option<u64>,
    message_budget: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ResourceEpochAccounting {
    epoch_key: String,
    token_budget: Option<u64>,
    message_budget: Option<u64>,
    allocated_tokens: u64,
    allocation_count: u64,
}

impl ResourceEpochAccounting {
    fn remaining_tokens(&self) -> Option<u64> {
        self.token_budget
            .map(|budget| budget.saturating_sub(self.allocated_tokens))
    }

    fn remaining_tokens_after(&self, approved_tokens: Option<u64>) -> Option<u64> {
        self.remaining_tokens()
            .map(|remaining| remaining.saturating_sub(approved_tokens.unwrap_or(0)))
    }

    fn remaining_messages(&self) -> Option<u64> {
        self.message_budget
            .map(|budget| budget.saturating_sub(self.allocation_count))
    }
}

fn resource_epoch_key(now: DateTime<Utc>) -> String {
    now.format("%Y-%m-%dT%H:00:00Z").to_string()
}

async fn resource_epoch_accounting(
    ledger: &dyn Ledger,
    controller_node_id: &NodeId,
    requested_by: &NodeId,
    epoch_key: String,
    budget: ResourceEpochBudget,
) -> Result<ResourceEpochAccounting, ControllerError> {
    let events = ledger
        .recent_events(EventFilter {
            kinds: vec![LedgerEventKind::Other(
                "resource_allocation_decision".to_string(),
            )],
            node_id: Some(controller_node_id.clone()),
            limit: Some(10_000),
            ..EventFilter::default()
        })
        .await?;

    let mut allocated_tokens = 0_u64;
    let mut allocation_count = 0_u64;
    let requested_by_key = requested_by.to_string();
    for event in events {
        if event.metadata.get("resource_epoch_key").map(String::as_str) != Some(epoch_key.as_str())
        {
            continue;
        }
        if event.metadata.get("requested_by").map(String::as_str) != Some(requested_by_key.as_str())
        {
            continue;
        }
        allocation_count = allocation_count.saturating_add(1);
        allocated_tokens = allocated_tokens.saturating_add(
            event
                .metadata
                .get("resource_epoch_approved_tokens")
                .and_then(|value| value.parse::<u64>().ok())
                .or_else(|| {
                    event
                        .payload
                        .get("approved_tokens")
                        .and_then(|value| value.as_u64())
                })
                .unwrap_or(0),
        );
    }

    Ok(ResourceEpochAccounting {
        epoch_key,
        token_budget: budget.token_budget,
        message_budget: budget.message_budget,
        allocated_tokens,
        allocation_count,
    })
}

fn add_resource_epoch_metadata(
    metadata: &mut BTreeMap<String, String>,
    epoch: &ResourceEpochAccounting,
    approved_tokens: Option<u64>,
) {
    metadata.insert("resource_epoch_key".to_string(), epoch.epoch_key.clone());
    metadata.insert(
        "resource_epoch_allocation_count_before".to_string(),
        epoch.allocation_count.to_string(),
    );
    metadata.insert(
        "resource_epoch_allocation_count_after".to_string(),
        epoch.allocation_count.saturating_add(1).to_string(),
    );
    metadata.insert(
        "resource_epoch_allocated_before_tokens".to_string(),
        epoch.allocated_tokens.to_string(),
    );
    metadata.insert(
        "resource_epoch_approved_tokens".to_string(),
        approved_tokens.unwrap_or(0).to_string(),
    );
    if let Some(budget) = epoch.token_budget {
        metadata.insert(
            "resource_epoch_budget_tokens".to_string(),
            budget.to_string(),
        );
    }
    if let Some(remaining) = epoch.remaining_tokens() {
        metadata.insert(
            "resource_epoch_remaining_before_tokens".to_string(),
            remaining.to_string(),
        );
    }
    if let Some(remaining) = epoch.remaining_tokens_after(approved_tokens) {
        metadata.insert(
            "resource_epoch_remaining_after_tokens".to_string(),
            remaining.to_string(),
        );
    }
    if let Some(budget) = epoch.message_budget {
        metadata.insert(
            "resource_epoch_message_budget".to_string(),
            budget.to_string(),
        );
    }
    if let Some(remaining) = epoch.remaining_messages() {
        metadata.insert(
            "resource_epoch_remaining_messages_before".to_string(),
            remaining.to_string(),
        );
    }
    if let Some(remaining) = epoch
        .remaining_messages()
        .map(|remaining| remaining.saturating_sub(1))
    {
        metadata.insert(
            "resource_epoch_remaining_messages_after".to_string(),
            remaining.to_string(),
        );
    }
}

fn allocate_resource_bargain(
    genome: &OrganizationalGenome,
    controller_node_id: &NodeId,
    bargain: &ResourceBargain,
    environment_pressure: Option<&EnvironmentPressureSummary>,
    coordination_pressure: Option<&CoordinationPressureSummary>,
    resource_epoch: Option<&ResourceEpochAccounting>,
) -> ResourceAllocationDecision {
    let mut reasons = Vec::new();
    let mut approved_tokens = None;
    let mut approved_tool_permissions = Vec::new();
    let mut denied_tool_permissions = Vec::new();
    let mut approved_context_refs = Vec::new();
    let mut denied_context_refs = Vec::new();

    let parent = match genome.get_node(controller_node_id) {
        Ok(parent) => parent,
        Err(error) => {
            return resource_allocation_decision(
                bargain,
                ResourceAllocationStatus::Denied,
                None,
                vec![],
                bargain.requested_tool_permissions.clone(),
                vec![],
                bargain.requested_context_refs.clone(),
                vec![format!("controller node unavailable: {error}")],
            );
        }
    };
    let requester = match genome.get_node(&bargain.requested_by) {
        Ok(requester) => requester,
        Err(error) => {
            return resource_allocation_decision(
                bargain,
                ResourceAllocationStatus::Denied,
                None,
                vec![],
                bargain.requested_tool_permissions.clone(),
                vec![],
                bargain.requested_context_refs.clone(),
                vec![format!("requesting node unavailable: {error}")],
            );
        }
    };

    if !parent
        .children
        .iter()
        .any(|child_id| child_id == &requester.id)
    {
        return resource_allocation_decision(
            bargain,
            ResourceAllocationStatus::Denied,
            None,
            vec![],
            bargain.requested_tool_permissions.clone(),
            vec![],
            bargain.requested_context_refs.clone(),
            vec![
                "requesting node is not a direct System 1 child of this System 3 controller"
                    .to_string(),
            ],
        );
    }

    if !parent.system_3.can_allocate_budget {
        return resource_allocation_decision(
            bargain,
            ResourceAllocationStatus::Denied,
            None,
            vec![],
            bargain.requested_tool_permissions.clone(),
            vec![],
            bargain.requested_context_refs.clone(),
            vec!["System 3 resource allocation disabled by genome policy".to_string()],
        );
    }

    if resource_epoch.and_then(ResourceEpochAccounting::remaining_messages) == Some(0) {
        return resource_allocation_decision(
            bargain,
            ResourceAllocationStatus::Denied,
            None,
            vec![],
            bargain.requested_tool_permissions.clone(),
            vec![],
            bargain.requested_context_refs.clone(),
            vec!["ResourceBargaining message budget exhausted for this epoch".to_string()],
        );
    }

    if let Some(requested_tokens) = bargain.requested_tokens {
        let mut token_cap = resource_task_token_cap(parent, requester);
        if let Some(pressure_cap) = environment_pressure
            .and_then(|pressure| homeostat_resource_token_cap(pressure, requested_tokens))
        {
            let applies = token_cap.map(|cap| pressure_cap < cap).unwrap_or(true);
            token_cap = Some(
                token_cap
                    .map(|cap| cap.min(pressure_cap))
                    .unwrap_or(pressure_cap),
            );
            if applies {
                reasons.push(format!(
                    "requested_tokens={} capped_to={} by Three-Four homeostat resource pressure",
                    requested_tokens,
                    token_cap.unwrap_or(pressure_cap)
                ));
            }
        }
        if let Some(coordination_cap) = coordination_pressure
            .and_then(|pressure| coordination_pressure_token_cap(pressure, requested_tokens))
        {
            let applies = token_cap.map(|cap| coordination_cap < cap).unwrap_or(true);
            token_cap = Some(
                token_cap
                    .map(|cap| cap.min(coordination_cap))
                    .unwrap_or(coordination_cap),
            );
            if applies {
                reasons.push(format!(
                    "requested_tokens={} capped_to={} by System 2 coordination pressure",
                    requested_tokens,
                    token_cap.unwrap_or(coordination_cap)
                ));
            }
        }
        if let Some(remaining_tokens) =
            resource_epoch.and_then(ResourceEpochAccounting::remaining_tokens)
        {
            let applies = token_cap.map(|cap| remaining_tokens < cap).unwrap_or(true);
            token_cap = Some(
                token_cap
                    .map(|cap| cap.min(remaining_tokens))
                    .unwrap_or(remaining_tokens),
            );
            if remaining_tokens == 0 {
                reasons
                    .push("ResourceBargaining token budget exhausted for this epoch".to_string());
            } else if applies && requested_tokens > remaining_tokens {
                reasons.push(format!(
                    "requested_tokens={} capped_to={} by ResourceBargaining epoch budget",
                    requested_tokens, remaining_tokens
                ));
            }
        }
        approved_tokens = match token_cap {
            Some(cap) if cap == 0 => {
                reasons
                    .push("token request denied because effective token cap is zero".to_string());
                None
            }
            Some(cap) if requested_tokens > cap => {
                reasons.push(format!(
                    "requested_tokens={} capped_to={} by System 3 resource policy",
                    requested_tokens, cap
                ));
                Some(cap)
            }
            Some(_) | None => Some(requested_tokens),
        };
    }

    for permission in &bargain.requested_tool_permissions {
        if tool_permission_allowed(requester, permission) {
            approved_tool_permissions.push(permission.clone());
        } else {
            denied_tool_permissions.push(permission.clone());
            reasons.push(format!("tool_permission_denied={permission}"));
        }
    }

    for context_ref in &bargain.requested_context_refs {
        if context_ref_allowed(requester, context_ref) {
            approved_context_refs.push(context_ref.clone());
        } else {
            denied_context_refs.push(context_ref.clone());
            reasons.push(format!("context_ref_denied={context_ref}"));
        }
    }

    let requested_count = bargain.requested_tokens.map(|_| 1).unwrap_or(0)
        + bargain.requested_tool_permissions.len()
        + bargain.requested_context_refs.len();
    let approved_count = approved_tokens.map(|_| 1).unwrap_or(0)
        + approved_tool_permissions.len()
        + approved_context_refs.len();
    let token_partially_approved = bargain
        .requested_tokens
        .zip(approved_tokens)
        .is_some_and(|(requested, approved)| approved < requested);
    let denied_count = denied_tool_permissions.len()
        + denied_context_refs.len()
        + usize::from(bargain.requested_tokens.is_some() && approved_tokens.is_none())
        + usize::from(token_partially_approved);

    let status = if requested_count == 0 {
        reasons.push("no concrete resource request supplied".to_string());
        ResourceAllocationStatus::Denied
    } else if approved_count == requested_count && denied_count == 0 {
        reasons.push("resource bargain approved within System 3 policy".to_string());
        ResourceAllocationStatus::Approved
    } else if approved_count > 0 {
        ResourceAllocationStatus::PartiallyApproved
    } else {
        ResourceAllocationStatus::Denied
    };

    resource_allocation_decision(
        bargain,
        status,
        approved_tokens,
        approved_tool_permissions,
        denied_tool_permissions,
        approved_context_refs,
        denied_context_refs,
        reasons,
    )
}

fn resource_allocation_accepts_work(decision: &ResourceAllocationDecision) -> bool {
    matches!(
        decision.status,
        ResourceAllocationStatus::Approved | ResourceAllocationStatus::PartiallyApproved
    )
}

fn resource_allocation_decision(
    bargain: &ResourceBargain,
    status: ResourceAllocationStatus,
    approved_tokens: Option<u64>,
    approved_tool_permissions: Vec<String>,
    denied_tool_permissions: Vec<String>,
    approved_context_refs: Vec<String>,
    denied_context_refs: Vec<String>,
    reasons: Vec<String>,
) -> ResourceAllocationDecision {
    ResourceAllocationDecision {
        requested_by: bargain.requested_by.clone(),
        task_id: bargain.task_id.clone(),
        status,
        approved_tokens,
        approved_tool_permissions,
        denied_tool_permissions,
        approved_context_refs,
        denied_context_refs,
        reasons,
        allocation_policy: "system3_genome_resource_policy_v1".to_string(),
        created_at: Utc::now(),
    }
}

fn environment_signal_event_name(channel_type: &VsmChannelType) -> &'static str {
    match channel_type {
        VsmChannelType::OperationToEnvironment => "operation_environment_signal",
        VsmChannelType::FutureProbeToEnvironment => "future_probe_environment_signal",
        VsmChannelType::EnvironmentToEnvironment => "environment_interaction_signal",
        _ => "environment_signal",
    }
}

fn environment_signal_event_kinds() -> Vec<LedgerEventKind> {
    vec![
        LedgerEventKind::Other("operation_environment_signal".to_string()),
        LedgerEventKind::Other("future_probe_environment_signal".to_string()),
        LedgerEventKind::Other("environment_interaction_signal".to_string()),
        LedgerEventKind::Other("three_four_homeostat_signal".to_string()),
    ]
}

#[derive(Clone, Debug, Default)]
struct EnvironmentPressureSummary {
    event_count: u64,
    risk_pressure: f64,
    opportunity_pressure: f64,
    feedback_pressure: f64,
    future_probe_count: u64,
    environment_interaction_count: u64,
    homeostat_count: u64,
    max_severity: u8,
    reasons: Vec<String>,
}

impl EnvironmentPressureSummary {
    fn from_events(events: &[LedgerEvent]) -> Self {
        let mut summary = Self::default();
        for event in events {
            summary.event_count += 1;
            let is_homeostat_event = matches!(
                &event.kind,
                LedgerEventKind::Other(name) if name == "three_four_homeostat_signal"
            );
            let severity = event
                .payload
                .get("severity")
                .and_then(|value| value.as_u64())
                .unwrap_or(1)
                .min(10) as u8;
            summary.max_severity = summary.max_severity.max(severity);

            let kind = event
                .payload
                .get("kind")
                .and_then(|value| value.as_str())
                .unwrap_or("Observation");
            let source_channel = event
                .payload
                .get("source_channel")
                .and_then(|value| value.as_str())
                .or_else(|| {
                    event
                        .metadata
                        .get("source_channel")
                        .map(std::string::String::as_str)
                })
                .unwrap_or("Unknown");
            let pressure = f64::from(severity.max(1));

            match kind {
                "Risk" => summary.risk_pressure += pressure,
                "Opportunity" => summary.opportunity_pressure += pressure,
                "UserFeedback" => summary.feedback_pressure += pressure,
                "DependencyChange" => summary.risk_pressure += pressure * 0.75,
                "CapabilityChange" => summary.opportunity_pressure += pressure * 0.75,
                "FutureRisk" | "PresentConstraint" | "ResourceImbalance" | "CoordinationDebt" => {
                    summary.risk_pressure += pressure
                }
                "FutureOpportunity" | "CapabilityGap" => summary.opportunity_pressure += pressure,
                _ => {}
            }
            match source_channel {
                "FutureProbeToEnvironment" => {
                    summary.future_probe_count += 1;
                    if kind != "Opportunity" {
                        summary.opportunity_pressure += pressure * 0.35;
                    }
                }
                "EnvironmentToEnvironment" => {
                    summary.environment_interaction_count += 1;
                    if kind != "Risk" {
                        summary.risk_pressure += pressure * 0.35;
                    }
                }
                "ThreeFourHomeostat" => {
                    summary.homeostat_count += 1;
                    let balance = event
                        .payload
                        .get("balance")
                        .and_then(|value| value.as_str())
                        .unwrap_or("Balanced");
                    match balance {
                        "FutureDominant" => summary.opportunity_pressure += pressure * 0.5,
                        "PresentDominant" | "Conflict" => summary.risk_pressure += pressure * 0.5,
                        "Balanced" => summary.feedback_pressure += pressure * 0.25,
                        _ => {}
                    }
                }
                _ => {}
            }
            if is_homeostat_event && source_channel != "ThreeFourHomeostat" {
                summary.homeostat_count += 1;
            }

            if summary.reasons.len() < 8 {
                summary.reasons.push(format!(
                    "environment_signal channel={source_channel} kind={kind} severity={severity}"
                ));
            }
        }
        summary
    }

    fn as_json(&self) -> serde_json::Value {
        serde_json::json!({
            "event_count": self.event_count,
            "risk_pressure": self.risk_pressure,
            "opportunity_pressure": self.opportunity_pressure,
            "feedback_pressure": self.feedback_pressure,
            "future_probe_count": self.future_probe_count,
            "environment_interaction_count": self.environment_interaction_count,
            "homeostat_count": self.homeostat_count,
            "max_severity": self.max_severity,
            "reasons": self.reasons,
        })
    }

    fn is_empty(&self) -> bool {
        self.event_count == 0
    }
}

#[derive(Clone, Debug, Default)]
struct CoordinationPressureSummary {
    event_count: u64,
    contention_count: u64,
    oscillation_count: u64,
    dependency_blocked_count: u64,
    pressure: f64,
    max_severity: u8,
    reasons: Vec<String>,
}

#[derive(Clone, Debug)]
struct SubtreePauseState {
    target_node_id: NodeId,
    paused_at: DateTime<Utc>,
    reason: String,
    severity: u8,
    require_human_confirmation: bool,
}

impl CoordinationPressureSummary {
    fn from_events(events: &[LedgerEvent], child_node_id: &NodeId) -> Self {
        let child_key = child_node_id.to_string();
        let mut summary = Self::default();
        for event in events {
            if !coordination_event_affects_child(event, &child_key) {
                continue;
            }
            let kind = event
                .payload
                .get("kind")
                .and_then(|value| value.as_str())
                .or_else(|| {
                    event
                        .metadata
                        .get("coordination_kind")
                        .map(std::string::String::as_str)
                })
                .unwrap_or("Other");
            let severity = event
                .payload
                .get("severity")
                .and_then(|value| value.as_u64())
                .or_else(|| {
                    event
                        .metadata
                        .get("severity")
                        .and_then(|value| value.parse::<u64>().ok())
                })
                .unwrap_or(1)
                .min(10) as u8;
            let pressure = f64::from(severity.max(1));
            match kind {
                "Contention" => {
                    summary.event_count += 1;
                    summary.contention_count += 1;
                    summary.pressure += pressure;
                }
                "Oscillation" => {
                    summary.event_count += 1;
                    summary.oscillation_count += 1;
                    summary.pressure += pressure * 1.25;
                }
                "DependencyBlocked" => {
                    summary.event_count += 1;
                    summary.dependency_blocked_count += 1;
                    summary.pressure += pressure * 0.75;
                }
                _ => continue,
            }
            summary.max_severity = summary.max_severity.max(severity);
            if summary.reasons.len() < 8 {
                summary.reasons.push(format!(
                    "system2_coordination kind={kind} severity={severity}"
                ));
            }
        }
        summary
    }

    fn affects_allocation(&self) -> bool {
        self.event_count > 0 && (self.max_severity >= 5 || self.pressure >= 8.0)
    }

    fn as_json(&self) -> serde_json::Value {
        serde_json::json!({
            "event_count": self.event_count,
            "contention_count": self.contention_count,
            "oscillation_count": self.oscillation_count,
            "dependency_blocked_count": self.dependency_blocked_count,
            "pressure": self.pressure,
            "max_severity": self.max_severity,
            "reasons": self.reasons,
        })
    }
}

fn coordination_event_affects_child(event: &LedgerEvent, child_key: &str) -> bool {
    event
        .payload
        .get("source_node_id")
        .and_then(|value| value.as_str())
        == Some(child_key)
        || event
            .payload
            .get("target_node_id")
            .and_then(|value| value.as_str())
            == Some(child_key)
        || event
            .payload
            .get("affected_node_ids")
            .and_then(|value| value.as_array())
            .is_some_and(|nodes| nodes.iter().any(|node| node.as_str() == Some(child_key)))
}

fn insert_coordination_pressure_metadata(
    metadata: &mut BTreeMap<String, String>,
    pressure: &CoordinationPressureSummary,
) {
    metadata.insert(
        "coordination_pressure_count".to_string(),
        pressure.event_count.to_string(),
    );
    metadata.insert(
        "coordination_contention_count".to_string(),
        pressure.contention_count.to_string(),
    );
    metadata.insert(
        "coordination_oscillation_count".to_string(),
        pressure.oscillation_count.to_string(),
    );
    metadata.insert(
        "coordination_dependency_blocked_count".to_string(),
        pressure.dependency_blocked_count.to_string(),
    );
    metadata.insert(
        "coordination_pressure".to_string(),
        format!("{:.3}", pressure.pressure),
    );
    metadata.insert(
        "coordination_max_severity".to_string(),
        pressure.max_severity.to_string(),
    );
    if !pressure.reasons.is_empty() {
        metadata.insert(
            "coordination_pressure_reasons".to_string(),
            pressure.reasons.join("|"),
        );
    }
}

fn insert_subtree_pause_metadata(
    metadata: &mut BTreeMap<String, String>,
    pause: &SubtreePauseState,
) {
    metadata.insert("algedonic_subtree_paused".to_string(), "true".to_string());
    metadata.insert(
        "algedonic_paused_target_node_id".to_string(),
        pause.target_node_id.to_string(),
    );
    metadata.insert(
        "algedonic_paused_at".to_string(),
        pause.paused_at.to_rfc3339(),
    );
    metadata.insert("algedonic_pause_reason".to_string(), pause.reason.clone());
    metadata.insert(
        "algedonic_pause_severity".to_string(),
        pause.severity.to_string(),
    );
    metadata.insert(
        "algedonic_pause_requires_human_confirmation".to_string(),
        pause.require_human_confirmation.to_string(),
    );
}

fn apply_environment_pressure(
    record: &StoredTrialRecord,
    mut evaluation: crate::QueuedCandidateEvaluation,
    pressure: &EnvironmentPressureSummary,
) -> crate::QueuedCandidateEvaluation {
    if pressure.is_empty() {
        return evaluation;
    }

    let risk_bonus = environment_source_match(
        &record.suggestion.source,
        pressure.risk_pressure,
        &[
            (GeneSuggestionSourceKind::AlgedonicSignal, 1.0),
            (GeneSuggestionSourceKind::System3StarAudit, 0.85),
            (GeneSuggestionSourceKind::System1ResourceBargain, 0.65),
            (GeneSuggestionSourceKind::System2CoordinationSignal, 0.50),
            (GeneSuggestionSourceKind::System4FutureProbe, 0.25),
            (GeneSuggestionSourceKind::Other, 0.20),
        ],
        18.0,
        0.8,
    );
    if risk_bonus > 0.0 {
        evaluation.objectives.expected_value += risk_bonus;
        evaluation.score.total_score += risk_bonus;
        evaluation.score.reasons.push(format!(
            "environment_risk_pressure={:.3}",
            pressure.risk_pressure
        ));
    }

    let opportunity_bonus = environment_source_match(
        &record.suggestion.source,
        pressure.opportunity_pressure,
        &[
            (GeneSuggestionSourceKind::System4FutureProbe, 1.0),
            (GeneSuggestionSourceKind::System3StarAudit, 0.35),
            (GeneSuggestionSourceKind::System2CoordinationSignal, 0.30),
            (GeneSuggestionSourceKind::System1ResourceBargain, 0.25),
            (GeneSuggestionSourceKind::Other, 0.50),
            (GeneSuggestionSourceKind::AlgedonicSignal, 0.15),
        ],
        16.0,
        0.9,
    );
    if opportunity_bonus > 0.0 {
        evaluation.objectives.expected_value += opportunity_bonus;
        evaluation.score.total_score += opportunity_bonus;
        evaluation.score.reasons.push(format!(
            "environment_opportunity_pressure={:.3}",
            pressure.opportunity_pressure
        ));
    }

    let feedback_bonus = environment_source_match(
        &record.suggestion.source,
        pressure.feedback_pressure,
        &[
            (GeneSuggestionSourceKind::System1ResourceBargain, 0.75),
            (GeneSuggestionSourceKind::System3StarAudit, 0.50),
            (GeneSuggestionSourceKind::System2CoordinationSignal, 0.40),
            (GeneSuggestionSourceKind::System4FutureProbe, 0.25),
            (GeneSuggestionSourceKind::AlgedonicSignal, 0.25),
            (GeneSuggestionSourceKind::Other, 0.25),
        ],
        8.0,
        0.4,
    );
    if feedback_bonus > 0.0 {
        evaluation.objectives.expected_value += feedback_bonus;
        evaluation.score.total_score += feedback_bonus;
        evaluation.score.reasons.push(format!(
            "environment_feedback_pressure={:.3}",
            pressure.feedback_pressure
        ));
    }

    if pressure.risk_pressure > 0.0 && candidate_is_bounded(record) {
        let safety_bonus = (pressure.risk_pressure * 0.3).min(8.0);
        evaluation.objectives.safety += safety_bonus;
        evaluation.score.total_score += safety_bonus;
        evaluation.score.reasons.push(format!(
            "environment_bounded_safety_bonus={safety_bonus:.3}"
        ));
    }

    evaluation
}

fn candidate_is_bounded(record: &StoredTrialRecord) -> bool {
    let limits = &record.suggestion.safety_limits;
    limits.requires_approval
        || limits.max_tasks.is_some()
        || limits.max_token_budget.is_some()
        || limits.max_traffic_share_basis_points.is_some()
        || matches!(record.suggestion.trial_mode, vsm_core::TrialMode::Shadow)
}

#[derive(Clone, Copy)]
enum GeneSuggestionSourceKind {
    System3StarAudit,
    System4FutureProbe,
    System1ResourceBargain,
    System2CoordinationSignal,
    AlgedonicSignal,
    Other,
}

fn environment_source_match(
    source: &GeneSuggestionSource,
    pressure: f64,
    weights: &[(GeneSuggestionSourceKind, f64)],
    max_bonus: f64,
    scale: f64,
) -> f64 {
    if pressure <= 0.0 {
        return 0.0;
    }
    let source_kind = source_kind(source);
    let weight = weights
        .iter()
        .find_map(|(kind, weight)| source_kind_matches(source_kind, *kind).then_some(*weight))
        .unwrap_or(0.0);
    (pressure * scale * weight).min(max_bonus)
}

fn source_kind(source: &GeneSuggestionSource) -> GeneSuggestionSourceKind {
    match source {
        GeneSuggestionSource::System3StarAudit => GeneSuggestionSourceKind::System3StarAudit,
        GeneSuggestionSource::System4FutureProbe => GeneSuggestionSourceKind::System4FutureProbe,
        GeneSuggestionSource::System1ResourceBargain => {
            GeneSuggestionSourceKind::System1ResourceBargain
        }
        GeneSuggestionSource::System2CoordinationSignal => {
            GeneSuggestionSourceKind::System2CoordinationSignal
        }
        GeneSuggestionSource::AlgedonicSignal => GeneSuggestionSourceKind::AlgedonicSignal,
        GeneSuggestionSource::Other(_) => GeneSuggestionSourceKind::Other,
    }
}

fn source_kind_matches(left: GeneSuggestionSourceKind, right: GeneSuggestionSourceKind) -> bool {
    matches!(
        (left, right),
        (
            GeneSuggestionSourceKind::System3StarAudit,
            GeneSuggestionSourceKind::System3StarAudit
        ) | (
            GeneSuggestionSourceKind::System4FutureProbe,
            GeneSuggestionSourceKind::System4FutureProbe
        ) | (
            GeneSuggestionSourceKind::System1ResourceBargain,
            GeneSuggestionSourceKind::System1ResourceBargain
        ) | (
            GeneSuggestionSourceKind::System2CoordinationSignal,
            GeneSuggestionSourceKind::System2CoordinationSignal
        ) | (
            GeneSuggestionSourceKind::AlgedonicSignal,
            GeneSuggestionSourceKind::AlgedonicSignal
        ) | (
            GeneSuggestionSourceKind::Other,
            GeneSuggestionSourceKind::Other
        )
    )
}

fn insert_environment_pressure_metadata(
    metadata: &mut BTreeMap<String, String>,
    pressure: &EnvironmentPressureSummary,
) {
    metadata.insert(
        "environment_signal_count".to_string(),
        pressure.event_count.to_string(),
    );
    metadata.insert(
        "environment_risk_pressure".to_string(),
        format!("{:.3}", pressure.risk_pressure),
    );
    metadata.insert(
        "environment_opportunity_pressure".to_string(),
        format!("{:.3}", pressure.opportunity_pressure),
    );
    metadata.insert(
        "environment_feedback_pressure".to_string(),
        format!("{:.3}", pressure.feedback_pressure),
    );
    metadata.insert(
        "environment_max_severity".to_string(),
        pressure.max_severity.to_string(),
    );
    metadata.insert(
        "three_four_homeostat_count".to_string(),
        pressure.homeostat_count.to_string(),
    );
    if !pressure.reasons.is_empty() {
        metadata.insert(
            "environment_pressure_reasons".to_string(),
            pressure.reasons.join("|"),
        );
    }
}

fn resource_task_token_cap(
    parent: &vsm_core::ViableNode,
    requester: &vsm_core::ViableNode,
) -> Option<u64> {
    let mut caps = Vec::new();
    if let Some(cap) = parent.system_3.default_task_budget_tokens {
        caps.push(cap);
    }
    if let Some(cap) = requester.context_policy.max_total_task_tokens {
        caps.push(cap);
    }
    caps.into_iter().min()
}

fn resource_epoch_budget(
    genome: &OrganizationalGenome,
    controller_node_id: &NodeId,
    requester_node_id: &NodeId,
) -> ResourceEpochBudget {
    let mut token_budgets = Vec::new();
    let mut message_budgets = Vec::new();
    for channel in &genome.channels {
        if channel.channel_type != VsmChannelType::ResourceBargaining {
            continue;
        }
        let connects_parent_child = channel.from.as_ref() == Some(controller_node_id)
            && channel.to.as_ref() == Some(requester_node_id);
        let connects_child_parent = channel.from.as_ref() == Some(requester_node_id)
            && channel.to.as_ref() == Some(controller_node_id);
        if connects_parent_child || connects_child_parent {
            if let Some(cap) = channel.max_token_budget_per_epoch {
                token_budgets.push(cap);
            }
            if let Some(cap) = channel.max_messages_per_epoch {
                message_budgets.push(cap);
            }
        }
    }
    ResourceEpochBudget {
        token_budget: token_budgets.into_iter().min(),
        message_budget: message_budgets.into_iter().min(),
    }
}

fn resource_pressure_affects_allocation(pressure: &EnvironmentPressureSummary) -> bool {
    pressure.homeostat_count > 0 && pressure.max_severity >= 7 && pressure.risk_pressure >= 7.0
}

fn homeostat_resource_token_cap(
    pressure: &EnvironmentPressureSummary,
    requested_tokens: u64,
) -> Option<u64> {
    if !resource_pressure_affects_allocation(pressure) {
        return None;
    }
    let divisor = if pressure.max_severity >= 9 || pressure.risk_pressure >= 12.0 {
        4
    } else {
        2
    };
    Some((requested_tokens / divisor).max(1))
}

fn coordination_pressure_token_cap(
    pressure: &CoordinationPressureSummary,
    requested_tokens: u64,
) -> Option<u64> {
    if !pressure.affects_allocation() {
        return None;
    }
    let divisor = if pressure.max_severity >= 8
        || pressure.oscillation_count > 1
        || pressure.pressure >= 16.0
    {
        4
    } else {
        2
    };
    Some((requested_tokens / divisor).max(1))
}

fn tool_permission_allowed(node: &vsm_core::ViableNode, permission: &str) -> bool {
    if node
        .permissions
        .denied_tools
        .iter()
        .any(|denied| denied == permission)
    {
        return false;
    }
    if !node.permissions.allowed_tools.is_empty() {
        return node
            .permissions
            .allowed_tools
            .iter()
            .any(|allowed| allowed == permission);
    }
    if node.tools.is_empty() {
        return true;
    }
    node.tools.iter().any(|tool| {
        tool.enabled
            && (tool.name == permission
                || tool
                    .permissions
                    .iter()
                    .any(|tool_permission| tool_permission == permission))
    })
}

fn context_ref_allowed(node: &vsm_core::ViableNode, context_ref: &str) -> bool {
    if node.context_policy.fixed_context_refs.is_empty()
        && node.context_policy.retrievable_context_refs.is_empty()
    {
        return true;
    }
    node.context_policy
        .fixed_context_refs
        .iter()
        .chain(node.context_policy.retrievable_context_refs.iter())
        .any(|allowed| allowed == context_ref)
}

fn gene_suggestions_from_audit_report(
    controller_node_id: &NodeId,
    report: &AuditReport,
    envelope: &MessageEnvelope,
) -> Vec<GeneSuggestion> {
    let suggested_by = envelope
        .source_node_id
        .clone()
        .unwrap_or_else(|| controller_node_id.clone());
    let evidence = audit_report_evidence(report, envelope);
    let hypothesis = audit_report_hypothesis(report);

    report
        .suggested_patches
        .iter()
        .cloned()
        .map(|patch| {
            let mut suggestion = GeneSuggestion::new(
                suggested_by.clone(),
                report.target_node_id.clone(),
                GeneSuggestionSource::System3StarAudit,
                patch,
                hypothesis.clone(),
            );
            suggestion.evidence = evidence.clone();
            suggestion.safety_limits.max_tasks = Some(10);
            suggestion.safety_limits.max_token_budget = Some(100_000);
            suggestion.safety_limits.requires_approval = true;
            suggestion
        })
        .collect()
}

fn audit_report_evidence(report: &AuditReport, envelope: &MessageEnvelope) -> Vec<String> {
    let mut evidence = vec![format!("source_channel={:?}", envelope.channel_type)];
    if let Some(correlation_id) = &envelope.correlation_id {
        evidence.push(format!("correlation_id={correlation_id}"));
    }

    for finding in &report.findings {
        evidence.push(format!(
            "finding={} severity={} related_nodes={} related_tasks={}",
            finding.title,
            finding.severity,
            finding.related_nodes.len(),
            finding.related_tasks.len()
        ));
        evidence.extend(finding.evidence.iter().cloned());
    }

    if evidence.len() == 1 {
        evidence.push("audit report supplied suggested patch without findings".to_string());
    }

    evidence
}

fn audit_report_hypothesis(report: &AuditReport) -> String {
    if report.findings.is_empty() {
        return "System 3* audit report suggested a bounded organizational mutation.".to_string();
    }

    let titles = report
        .findings
        .iter()
        .map(|finding| finding.title.as_str())
        .collect::<Vec<_>>()
        .join("; ");
    format!("System 3* audit report suggests a bounded organizational mutation: {titles}")
}

fn gene_suggestions_from_three_four_homeostat(
    signal: &ThreeFourHomeostatSignal,
    envelope: &MessageEnvelope,
) -> Vec<GeneSuggestion> {
    let evidence = three_four_homeostat_evidence(signal, envelope);
    let hypothesis = three_four_homeostat_hypothesis(signal);

    signal
        .suggested_patches
        .iter()
        .cloned()
        .map(|patch| {
            let mut suggestion = GeneSuggestion::new(
                signal.system_4_node_id.clone(),
                signal.target_node_id.clone(),
                GeneSuggestionSource::System4FutureProbe,
                patch,
                hypothesis.clone(),
            );
            suggestion.evidence = evidence.clone();
            suggestion.trial_mode = vsm_core::TrialMode::Canary;
            suggestion.safety_limits.max_tasks = Some(10);
            suggestion.safety_limits.max_token_budget = Some(100_000);
            suggestion.safety_limits.max_traffic_share_basis_points = Some(1_000);
            suggestion.safety_limits.requires_approval = signal.severity.unwrap_or(1) >= 7
                || matches!(signal.balance, ThreeFourHomeostatBalance::Conflict);
            suggestion
                .measurement_plan
                .success_metrics
                .push("three_four_homeostat_expected_value".to_string());
            suggestion
                .measurement_plan
                .failure_metrics
                .push("present_operational_regression".to_string());
            suggestion
        })
        .collect()
}

fn three_four_homeostat_evidence(
    signal: &ThreeFourHomeostatSignal,
    envelope: &MessageEnvelope,
) -> Vec<String> {
    let mut evidence = vec![
        format!("source_channel={:?}", envelope.channel_type),
        format!("three_four_kind={:?}", signal.kind),
        format!("three_four_balance={:?}", signal.balance),
    ];
    if let Some(severity) = signal.severity {
        evidence.push(format!("severity={severity}"));
    }
    if let Some(correlation_id) = &envelope.correlation_id {
        evidence.push(format!("correlation_id={correlation_id}"));
    }
    if !signal.present_summary.is_empty() {
        evidence.push(format!("present_summary={}", signal.present_summary));
    }
    if !signal.future_summary.is_empty() {
        evidence.push(format!("future_summary={}", signal.future_summary));
    }
    evidence.push(format!("recommendation={}", signal.recommendation));
    evidence.extend(signal.evidence.iter().cloned());
    evidence
}

fn three_four_homeostat_hypothesis(signal: &ThreeFourHomeostatSignal) -> String {
    format!(
        "Three-Four homeostat recommends a bounded adaptation: {} Present: {} Future: {}",
        signal.recommendation, signal.present_summary, signal.future_summary
    )
}

fn insert_replay_metadata(
    metadata: &mut BTreeMap<String, String>,
    replay: &crate::CandidateReplaySummary,
) -> Result<(), ControllerError> {
    metadata.insert(
        "offline_replay_version".to_string(),
        OFFLINE_REPLAY_VERSION.to_string(),
    );
    metadata.insert(
        "replay_trace_count".to_string(),
        replay.trace_count.to_string(),
    );
    metadata.insert(
        "replay_base_genome_mismatch_count".to_string(),
        replay.base_genome_mismatch_count.to_string(),
    );
    metadata.insert(
        "replay_eligible_trace_count".to_string(),
        replay.eligible_trace_count.to_string(),
    );
    metadata.insert(
        "replay_safety_rejected_count".to_string(),
        replay.safety_rejected_count.to_string(),
    );
    metadata.insert(
        "replay_champion_route_count".to_string(),
        replay.champion_route_count.to_string(),
    );
    metadata.insert(
        "replay_candidate_route_count".to_string(),
        replay.candidate_route_count.to_string(),
    );
    metadata.insert(
        "replay_candidate_no_route_count".to_string(),
        replay.candidate_no_route_count.to_string(),
    );
    metadata.insert(
        "replay_changed_route_count".to_string(),
        replay.changed_route_count.to_string(),
    );
    metadata.insert(
        "replay_affected_route_count".to_string(),
        replay.affected_route_count.to_string(),
    );
    metadata.insert(
        "replay_baseline_score".to_string(),
        format!("{:.3}", replay.baseline_score),
    );
    metadata.insert(
        "replay_estimated_delta_score".to_string(),
        format!("{:.3}", replay.estimated_delta_score),
    );
    metadata.insert(
        "replay_score".to_string(),
        format!("{:.3}", replay.replay_score),
    );
    metadata.insert(
        "replay_trace_evaluations".to_string(),
        serde_json::to_string(&replay.trace_evaluations)?,
    );
    Ok(())
}

fn format_candidate_objectives(objectives: &crate::CandidateObjectives) -> String {
    format!(
        "expected_value={:.3};safety={:.3};historical_fit={:.3};replay_fit={:.3};complexity_cost={:.3};exposure_cost={:.3}",
        objectives.expected_value,
        objectives.safety,
        objectives.historical_fit,
        objectives.replay_fit,
        objectives.complexity_cost,
        objectives.exposure_cost
    )
}

fn snapshot_candidate_objectives(
    objectives: &crate::CandidateObjectives,
) -> CandidateObjectiveSnapshot {
    CandidateObjectiveSnapshot {
        expected_value: objectives.expected_value,
        safety: objectives.safety,
        historical_fit: objectives.historical_fit,
        replay_fit: objectives.replay_fit,
        complexity_cost: objectives.complexity_cost,
        exposure_cost: objectives.exposure_cost,
    }
}

fn task_priority(task: &TaskPacket) -> ChannelPriority {
    match &task.risk {
        vsm_core::RiskClass::Low => ChannelPriority::Normal,
        vsm_core::RiskClass::Medium => ChannelPriority::Normal,
        vsm_core::RiskClass::High => ChannelPriority::High,
        vsm_core::RiskClass::Critical => ChannelPriority::Critical,
    }
}

fn routed_task_channel(
    configured_default: &VsmChannelType,
    incoming: Option<&MessageEnvelope>,
) -> VsmChannelType {
    match incoming.map(|incoming| &incoming.channel_type) {
        Some(VsmChannelType::Command) => VsmChannelType::Command,
        Some(VsmChannelType::System2Coordination) => VsmChannelType::System2Coordination,
        Some(VsmChannelType::Audit) => VsmChannelType::Audit,
        Some(VsmChannelType::ThreeFourHomeostat) => VsmChannelType::ThreeFourHomeostat,
        Some(VsmChannelType::ManagementToOperation) => VsmChannelType::ManagementToOperation,
        Some(VsmChannelType::OperationToOperation) => VsmChannelType::OperationToOperation,
        Some(VsmChannelType::Algedonic) => VsmChannelType::Algedonic,
        _ => configured_default.clone(),
    }
}

fn routed_task_priority(task: &TaskPacket, incoming: Option<&MessageEnvelope>) -> ChannelPriority {
    let base = task_priority(task);
    match incoming.map(|incoming| &incoming.channel_type) {
        Some(VsmChannelType::Command) => {
            if task
                .metadata
                .get("command_non_negotiable")
                .map(|value| value == "true")
                .unwrap_or(false)
                || matches!(task.risk, RiskClass::Critical)
            {
                ChannelPriority::Critical
            } else {
                elevate_priority(base, ChannelPriority::High)
            }
        }
        Some(VsmChannelType::System2Coordination)
        | Some(VsmChannelType::Audit)
        | Some(VsmChannelType::ThreeFourHomeostat) => elevate_priority(base, ChannelPriority::High),
        Some(VsmChannelType::Algedonic) => ChannelPriority::Interrupt,
        _ => base,
    }
}

const HANDOFF_METADATA_KEYS: &[&str] = &[
    "handoff_kind",
    "handoff_channel",
    "handoff_source_node_id",
    "handoff_target_node_id",
    "handoff_via_controller_node_id",
    "handoff_correlation_id",
    "handoff_causation_id",
    "handoff_envelope_id",
    "handoff_operation_kind",
    "handoff_artifact_count",
    "handoff_related_task_id",
    "handoff_dependency_task_ids",
    "handoff_artifact_refs",
    "handoff_summary",
];

const MANAGEMENT_METADATA_KEYS: &[&str] = &[
    "management_kind",
    "management_channel",
    "management_source_node_id",
    "management_target_node_id",
    "management_via_controller_node_id",
    "management_correlation_id",
    "management_causation_id",
    "management_envelope_id",
    "management_operation_kind",
    "management_directive_message_id",
    "management_policy",
    "management_related_task_id",
    "management_dependency_task_ids",
];

const COORDINATION_METADATA_KEYS: &[&str] = &[
    "coordination_policy",
    "coordination_kind",
    "coordination_signal_message_id",
    "coordination_source_node_id",
    "coordination_target_node_id",
    "coordination_severity",
    "coordination_affected_node_ids",
    "coordination_affected_task_ids",
];

const COMMAND_METADATA_KEYS: &[&str] = &[
    "command_issued_by",
    "command_target_node_id",
    "command_non_negotiable",
    "command_legal_or_policy_basis",
    "command_system5_identity",
    "command_policy_values",
    "command_policy_constraints",
    "command_denied_capabilities",
];

fn annotate_incoming_task_channel(
    task: &mut TaskPacket,
    incoming: Option<&MessageEnvelope>,
    controller_node_id: &NodeId,
) {
    let Some(incoming) = incoming else {
        return;
    };

    task.metadata.insert(
        "vsm_source_channel".to_string(),
        format!("{:?}", incoming.channel_type),
    );
    task.metadata.insert(
        "vsm_source_payload_type".to_string(),
        incoming.payload_type.clone(),
    );

    match incoming.channel_type {
        VsmChannelType::OperationToOperation => {
            annotate_incoming_peer_handoff(task, incoming, controller_node_id);
        }
        VsmChannelType::ManagementToOperation => {
            annotate_incoming_management_to_operation(task, incoming, controller_node_id);
        }
        _ => {}
    }
}

fn annotate_incoming_peer_handoff(
    task: &mut TaskPacket,
    incoming: &MessageEnvelope,
    controller_node_id: &NodeId,
) {
    task.metadata.insert(
        "handoff_kind".to_string(),
        "operation_to_operation".to_string(),
    );
    task.metadata.insert(
        "handoff_channel".to_string(),
        "OperationToOperation".to_string(),
    );
    task.metadata.insert(
        "handoff_via_controller_node_id".to_string(),
        controller_node_id.to_string(),
    );
    task.metadata
        .insert("handoff_envelope_id".to_string(), incoming.id.to_string());
    if let Some(source_node_id) = &incoming.source_node_id {
        task.metadata.insert(
            "handoff_source_node_id".to_string(),
            source_node_id.to_string(),
        );
    }
    if let Some(correlation_id) = &incoming.correlation_id {
        task.metadata
            .insert("handoff_correlation_id".to_string(), correlation_id.clone());
    }
    if let Some(causation_id) = &incoming.causation_id {
        task.metadata
            .insert("handoff_causation_id".to_string(), causation_id.to_string());
    }
}

fn annotate_incoming_management_to_operation(
    task: &mut TaskPacket,
    incoming: &MessageEnvelope,
    controller_node_id: &NodeId,
) {
    task.metadata.insert(
        "management_kind".to_string(),
        "management_to_operation".to_string(),
    );
    task.metadata.insert(
        "management_channel".to_string(),
        "ManagementToOperation".to_string(),
    );
    task.metadata.insert(
        "management_via_controller_node_id".to_string(),
        controller_node_id.to_string(),
    );
    task.metadata.insert(
        "management_envelope_id".to_string(),
        incoming.id.to_string(),
    );
    if let Some(source_node_id) = &incoming.source_node_id {
        task.metadata.insert(
            "management_source_node_id".to_string(),
            source_node_id.to_string(),
        );
    }
    if let Some(correlation_id) = &incoming.correlation_id {
        task.metadata.insert(
            "management_correlation_id".to_string(),
            correlation_id.clone(),
        );
    }
    if let Some(causation_id) = &incoming.causation_id {
        task.metadata.insert(
            "management_causation_id".to_string(),
            causation_id.to_string(),
        );
    }
}

fn annotate_outbound_task_channel(task: &mut TaskPacket, child_id: &NodeId) {
    if task.metadata.get("handoff_kind").map(String::as_str) == Some("operation_to_operation") {
        task.metadata
            .insert("handoff_target_node_id".to_string(), child_id.to_string());
    }
    if task.metadata.get("management_kind").map(String::as_str) == Some("management_to_operation") {
        task.metadata.insert(
            "management_target_node_id".to_string(),
            child_id.to_string(),
        );
    }
}

fn copy_channel_metadata_to_envelope(task: &TaskPacket, envelope: &mut MessageEnvelope) {
    for key in HANDOFF_METADATA_KEYS {
        if let Some(value) = task.metadata.get(*key) {
            envelope.metadata.insert((*key).to_string(), value.clone());
        }
    }
    for key in MANAGEMENT_METADATA_KEYS {
        if let Some(value) = task.metadata.get(*key) {
            envelope.metadata.insert((*key).to_string(), value.clone());
        }
    }
    for key in COORDINATION_METADATA_KEYS {
        if let Some(value) = task.metadata.get(*key) {
            envelope.metadata.insert((*key).to_string(), value.clone());
        }
    }
    for key in COMMAND_METADATA_KEYS {
        if let Some(value) = task.metadata.get(*key) {
            envelope.metadata.insert((*key).to_string(), value.clone());
        }
    }
}

fn elevate_priority(base: ChannelPriority, minimum: ChannelPriority) -> ChannelPriority {
    if priority_rank(&base) >= priority_rank(&minimum) {
        base
    } else {
        minimum
    }
}

fn priority_rank(priority: &ChannelPriority) -> u8 {
    match priority {
        ChannelPriority::Low => 0,
        ChannelPriority::Normal => 1,
        ChannelPriority::High => 2,
        ChannelPriority::Critical => 3,
        ChannelPriority::Interrupt => 4,
    }
}

fn routed_envelope_metadata(
    reason: &str,
    incoming: Option<&MessageEnvelope>,
    outbound_channel: &VsmChannelType,
) -> BTreeMap<String, String> {
    let mut metadata = BTreeMap::new();
    metadata.insert("routing_reason".to_string(), reason.to_string());
    metadata.insert(
        "vsm_outbound_channel".to_string(),
        format!("{:?}", outbound_channel),
    );
    if let Some(incoming) = incoming {
        metadata.insert(
            "vsm_source_channel".to_string(),
            format!("{:?}", incoming.channel_type),
        );
        metadata.insert(
            "source_payload_type".to_string(),
            incoming.payload_type.clone(),
        );
    }
    metadata
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use vsm_core::{
        envelope_for_directive, envelope_for_task, Directive, GeneSuggestionSource, GenomeId,
        LeafOperationSpec, OrganizationalGenome, OrganizationalGenomePatch, Subscription,
        System5Policy, TaskId, TaskPacket, TaskTrace, ThreeFourHomeostatKind, Transport, TrialMode,
        ViableNode,
    };
    use vsm_ledger::{
        InMemoryLedger, PopulationArchiveStatus, StoredTrialRecord, StoredTrialStatus,
    };
    use vsm_runtime::InMemoryTransport;

    fn genome_and_review_suggestion() -> (OrganizationalGenome, GeneSuggestion, vsm_core::NodeId) {
        let root = ViableNode::new_metasystem("root");
        let root_id = root.id.clone();
        let mut genome = OrganizationalGenome::new(root);
        let coder = ViableNode::new_leaf("coder", LeafOperationSpec::coding());
        genome.add_child(&root_id, coder).expect("coder");

        let mut reviewer = ViableNode::new_leaf("reviewer", LeafOperationSpec::reviewer());
        reviewer.system_5 = System5Policy {
            identity: "Candidate reviewer leaf.".to_string(),
            values: vec![],
            non_negotiable_constraints: vec!["Do not write code.".to_string()],
            denied_capabilities: vec!["write_code".to_string()],
        };
        let reviewer_id = reviewer.id.clone();

        let suggestion = GeneSuggestion::new(
            root_id.clone(),
            root_id,
            GeneSuggestionSource::System3StarAudit,
            OrganizationalGenomePatch::AddChild {
                parent_id: genome.root_node_id.clone(),
                child: reviewer,
            },
            "queue reviewer",
        );

        (genome, suggestion, reviewer_id)
    }

    fn genome_with_coder_reviewer_tester() -> (
        OrganizationalGenome,
        vsm_core::NodeId,
        vsm_core::NodeId,
        vsm_core::NodeId,
    ) {
        let root = ViableNode::new_metasystem("root");
        let root_id = root.id.clone();
        let mut genome = OrganizationalGenome::new(root);
        let coder = ViableNode::new_leaf("coder", LeafOperationSpec::coding());
        let coder_id = coder.id.clone();
        let reviewer = ViableNode::new_leaf("reviewer", LeafOperationSpec::reviewer());
        let reviewer_id = reviewer.id.clone();
        let tester = ViableNode::new_leaf("tester", LeafOperationSpec::tester());
        let tester_id = tester.id.clone();
        genome.add_child(&root_id, coder).expect("coder child");
        genome
            .add_child(&root_id, reviewer)
            .expect("reviewer child");
        genome.add_child(&root_id, tester).expect("tester child");
        (genome, coder_id, reviewer_id, tester_id)
    }

    fn controller_for(
        genome: OrganizationalGenome,
        ledger: Arc<InMemoryLedger>,
    ) -> (ControllerRuntime, SharedGenome) {
        let root_id = genome.root_node_id.clone();
        let shared_genome = Arc::new(RwLock::new(genome));
        let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new(16));
        let ledger: Arc<dyn Ledger> = ledger;
        let controller = ControllerRuntime::new(root_id, shared_genome.clone(), transport, ledger);
        (controller, shared_genome)
    }

    #[test]
    fn controller_default_subscribes_to_environment_channels() {
        let config = ControllerConfig::default();
        for channel in [
            VsmChannelType::OperationToEnvironment,
            VsmChannelType::FutureProbeToEnvironment,
            VsmChannelType::EnvironmentToEnvironment,
        ] {
            assert!(config.subscription_channels.contains(&channel));
        }
    }

    #[tokio::test]
    async fn evolution_generation_queues_offspring_and_persists_generation() {
        let (genome, _suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let coder_id = genome
            .get_node(&root_id)
            .expect("root")
            .children
            .first()
            .expect("coder")
            .clone();
        let genome_id = genome.id.clone();
        let ledger = Arc::new(InMemoryLedger::new());
        let (controller, _shared_genome) = controller_for(genome, ledger.clone());

        for _ in 0..5 {
            let mut trace = TaskTrace::started(TaskId::new(), genome_id.clone(), coder_id.clone());
            trace.merged = Some(false);
            trace.tests_passed = Some(false);
            trace.outcome_score = -8.0;
            trace.input_tokens = 10_000;
            trace.output_tokens = 2_000;
            ledger.write_task_trace(trace).await.expect("trace");
        }

        let mut policy = EvolutionPolicy::default();
        policy.max_offspring_per_generation = 1;
        let generation = controller
            .run_evolution_generation(policy)
            .await
            .expect("run generation")
            .expect("generation");

        assert_eq!(generation.generation, 1);
        assert_eq!(generation.offspring_trial_ids.len(), 1);
        assert_eq!(
            generation
                .mutation_operator_counts
                .get("add_child_reviewer"),
            Some(&1)
        );

        let queued = ledger
            .queued_trial_records(&root_id, 10)
            .await
            .expect("queued trials");
        assert_eq!(queued.len(), 1);
        assert_eq!(
            queued[0]
                .metadata
                .get("evolution_generation")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            queued[0]
                .metadata
                .get("evolution_operator")
                .map(String::as_str),
            Some("add_child_reviewer")
        );

        let latest = ledger
            .latest_evolution_generation_record(&root_id)
            .await
            .expect("latest generation")
            .expect("generation exists");
        assert_eq!(latest.offspring_trial_ids, generation.offspring_trial_ids);

        let events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::EvolutionGenerationCreated],
                node_id: Some(root_id),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("generation events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload["generation"].as_u64(), Some(1));
    }

    #[tokio::test]
    async fn directive_mapping_records_task_mapped_and_routed_lineage() {
        let (genome, _suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let ledger = Arc::new(InMemoryLedger::new());
        let shared_genome = Arc::new(RwLock::new(genome));
        let transport = Arc::new(InMemoryTransport::new(16));
        let _keepalive = transport
            .subscribe(Subscription {
                channel_types: vec![],
                target_node_id: None,
                queue_name: None,
                durable: false,
            })
            .await
            .expect("subscribe");
        let controller = ControllerRuntime::new(
            root_id.clone(),
            shared_genome,
            transport,
            ledger.clone() as Arc<dyn Ledger>,
        );
        let mut directive = Directive::new("user", "Implement feature", "make the change");
        directive.desired_state = Some("feature implemented".to_string());
        directive.metadata.insert(
            "task.metadata.decomposition_authority".to_string(),
            "system3_model".to_string(),
        );
        let envelope = envelope_for_directive(&directive)
            .expect("directive envelope")
            .with_route(None, Some(root_id.clone()));

        let outcome = controller
            .handle_envelope(envelope)
            .await
            .expect("handle directive");
        let ControllerHandleOutcome::RoutedTask { task, .. } = outcome else {
            panic!("expected routed task");
        };
        assert_eq!(task.directive_id, Some(directive.id.clone()));

        let mapped = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::TaskMapped],
                directive_id: Some(directive.id.clone()),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("mapped events");
        assert_eq!(mapped.len(), 1);
        assert_eq!(mapped[0].task_id, Some(task.id.clone()));
        assert_eq!(
            mapped[0].payload["decomposition_authority"].as_str(),
            Some("system3_model")
        );
        assert_eq!(
            mapped[0].payload["source_channel"].as_str(),
            Some("OperationToEnvironment")
        );

        let routed = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::TaskRouted],
                directive_id: Some(directive.id.clone()),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("routed events");
        assert_eq!(routed.len(), 1);
        assert_eq!(routed[0].task_id, Some(task.id.clone()));
        assert_eq!(
            routed[0].payload["directive_id"].as_str(),
            Some(directive.id.as_str())
        );
        assert!(routed[0].payload["dependency_task_ids"]
            .as_array()
            .is_some_and(Vec::is_empty));
    }

    #[tokio::test]
    async fn directive_decomposition_emits_dependency_aware_channel_tasks() {
        let (genome, coder_id, reviewer_id, tester_id) = genome_with_coder_reviewer_tester();
        let root_id = genome.root_node_id.clone();
        let ledger = Arc::new(InMemoryLedger::new());
        let shared_genome = Arc::new(RwLock::new(genome));
        let transport = Arc::new(InMemoryTransport::new(16));
        let _keepalive = transport
            .subscribe(Subscription {
                channel_types: vec![],
                target_node_id: None,
                queue_name: None,
                durable: false,
            })
            .await
            .expect("subscribe");
        let controller = ControllerRuntime::new(
            root_id.clone(),
            shared_genome,
            transport,
            ledger.clone() as Arc<dyn Ledger>,
        );

        let mut directive = Directive::new(
            "user",
            "Implement audited change",
            "make the implementation, test it, and review it",
        );
        directive
            .metadata
            .insert("decompose".to_string(), "true".to_string());
        directive
            .metadata
            .insert("requires_tests".to_string(), "true".to_string());
        directive
            .metadata
            .insert("requires_review".to_string(), "true".to_string());
        let envelope = envelope_for_directive(&directive)
            .expect("directive envelope")
            .with_route(None, Some(root_id.clone()));

        let outcome = controller
            .handle_envelope(envelope)
            .await
            .expect("handle decomposed directive");
        let ControllerHandleOutcome::DecomposedTasks { tasks } = outcome else {
            panic!("expected decomposed tasks");
        };
        assert_eq!(tasks.len(), 3);

        let implementation = tasks
            .iter()
            .find(|task| {
                task.task
                    .metadata
                    .get("decomposition_role")
                    .map(String::as_str)
                    == Some("implementation")
            })
            .expect("implementation task");
        let review = tasks
            .iter()
            .find(|task| {
                task.task
                    .metadata
                    .get("decomposition_role")
                    .map(String::as_str)
                    == Some("review")
            })
            .expect("review task");
        let test = tasks
            .iter()
            .find(|task| {
                task.task
                    .metadata
                    .get("decomposition_role")
                    .map(String::as_str)
                    == Some("test")
            })
            .expect("test task");
        assert_eq!(implementation.child_id, coder_id);
        assert_eq!(review.child_id, reviewer_id);
        assert_eq!(test.child_id, tester_id);
        assert!(implementation.task.dependencies.is_empty());
        assert_eq!(
            review.task.dependencies,
            vec![implementation.task.id.clone()]
        );
        assert_eq!(test.task.dependencies, vec![implementation.task.id.clone()]);

        let mapped = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::TaskMapped],
                directive_id: Some(directive.id.clone()),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("mapped events");
        assert_eq!(mapped.len(), 4);
        let root_event = mapped
            .iter()
            .find(|event| {
                event.payload["decomposition_policy"].as_str()
                    == Some("system3_capability_split_root")
            })
            .expect("root mapped event");
        let root_task_id = root_event.task_id.clone().expect("root task id");
        for routed in [&implementation.task, &review.task, &test.task] {
            assert_eq!(routed.parent_task_id, Some(root_task_id.clone()));
        }

        let routed = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::TaskRouted],
                directive_id: Some(directive.id.clone()),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("routed events");
        assert_eq!(routed.len(), 3);
        let implementation_route = routed
            .iter()
            .find(|event| event.task_id == Some(implementation.task.id.clone()))
            .expect("implementation route");
        assert_eq!(
            implementation_route.payload["source_channel"].as_str(),
            Some("ResourceBargaining")
        );
        assert_eq!(
            implementation_route.payload["outbound_channel"].as_str(),
            Some("ResourceBargaining")
        );
        for dependent in [&review.task, &test.task] {
            let route = routed
                .iter()
                .find(|event| event.task_id == Some(dependent.id.clone()))
                .expect("dependent route");
            assert_eq!(
                route.payload["source_channel"].as_str(),
                Some("System2Coordination")
            );
            assert_eq!(
                route.payload["outbound_channel"].as_str(),
                Some("System2Coordination")
            );
            assert_eq!(
                route.payload["dependency_task_ids"][0].as_str(),
                Some(implementation.task.id.as_str())
            );
        }
    }

    #[tokio::test]
    async fn failed_decomposed_result_creates_revision_coordination_task() {
        let (genome, coder_id, _reviewer_id, _tester_id) = genome_with_coder_reviewer_tester();
        let root_id = genome.root_node_id.clone();
        let ledger = Arc::new(InMemoryLedger::new());
        let shared_genome = Arc::new(RwLock::new(genome));
        let transport = Arc::new(InMemoryTransport::new(16));
        let mut coder_stream = transport
            .subscribe(Subscription {
                channel_types: vec![VsmChannelType::System2Coordination],
                target_node_id: Some(coder_id.to_string()),
                queue_name: None,
                durable: false,
            })
            .await
            .expect("subscribe coder");
        let controller = ControllerRuntime::new(
            root_id.clone(),
            shared_genome,
            transport,
            ledger.clone() as Arc<dyn Ledger>,
        );

        let parent_task_id = TaskId::new();
        let failed_task_id = TaskId::new();
        let directive_id = vsm_core::DirectiveId::new();
        let mut result = TaskResult::failed(
            failed_task_id.clone(),
            coder_id.clone(),
            "implementation failed",
            "compiler error in generated patch",
        );
        result
            .metadata
            .insert("directive_id".to_string(), directive_id.to_string());
        result
            .metadata
            .insert("parent_task_id".to_string(), parent_task_id.to_string());
        result.metadata.insert(
            "decomposition_role".to_string(),
            "implementation".to_string(),
        );
        result.metadata.insert(
            "decomposition_policy".to_string(),
            "system3_capability_split_v1".to_string(),
        );
        result
            .metadata
            .insert("required_capability".to_string(), "write_code".to_string());
        result
            .metadata
            .insert("target_child".to_string(), "coder".to_string());
        let envelope = vsm_core::envelope_for_task_result(&result)
            .expect("result envelope")
            .with_route(Some(coder_id.clone()), Some(root_id.clone()));

        let outcome = controller
            .handle_envelope(envelope)
            .await
            .expect("handle failed decomposed result");
        assert!(matches!(
            outcome,
            ControllerHandleOutcome::ReceivedTaskResult(_)
        ));

        let published =
            tokio::time::timeout(std::time::Duration::from_millis(250), coder_stream.next())
                .await
                .expect("published revision task")
                .expect("stream item")
                .expect("published envelope");
        assert_eq!(published.channel_type, VsmChannelType::System2Coordination);
        let revision_task: TaskPacket = published.payload_as().expect("revision task");
        assert_eq!(revision_task.parent_task_id, Some(parent_task_id.clone()));
        assert_eq!(revision_task.directive_id, Some(directive_id.clone()));
        assert_eq!(revision_task.dependencies, vec![failed_task_id.clone()]);
        assert_eq!(
            revision_task
                .metadata
                .get("decomposition_policy")
                .map(String::as_str),
            Some("system3_result_revision_v1")
        );
        assert_eq!(
            revision_task
                .metadata
                .get("decomposition_revision_depth")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(revision_task.assigned_to, Some(coder_id.clone()));

        let mapped = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::TaskMapped],
                task_id: Some(revision_task.id.clone()),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("mapped events");
        assert_eq!(mapped.len(), 1);
        assert_eq!(
            mapped[0].payload["decomposition_policy"].as_str(),
            Some("system3_result_revision")
        );
        assert_eq!(
            mapped[0].payload["dependency_task_ids"][0].as_str(),
            Some(failed_task_id.as_str())
        );

        let routed = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::TaskRouted],
                task_id: Some(revision_task.id.clone()),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("routed events");
        assert_eq!(routed.len(), 1);
        assert_eq!(
            routed[0].payload["source_channel"].as_str(),
            Some("System2Coordination")
        );
        assert_eq!(
            routed[0].payload["outbound_channel"].as_str(),
            Some("System2Coordination")
        );

        let revision_events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::Other(
                    "decomposition_revision_created".to_string(),
                )],
                task_id: Some(revision_task.id),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("revision events");
        assert_eq!(revision_events.len(), 1);
        assert_eq!(
            revision_events[0].payload["failed_task_id"].as_str(),
            Some(failed_task_id.as_str())
        );
    }

    #[tokio::test]
    async fn operation_environment_signal_records_system1_environment_event() {
        let (genome, _suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let child_id = genome
            .get_node(&root_id)
            .expect("root")
            .children
            .first()
            .cloned()
            .expect("child");
        let ledger = Arc::new(InMemoryLedger::new());
        let (controller, _shared_genome) = controller_for(genome, ledger.clone());
        let task_id = TaskId::new();
        let mut signal = EnvironmentSignal::new(
            vsm_core::EnvironmentSignalKind::UserFeedback,
            "user",
            "User reports the generated patch fixed the original issue.",
        );
        signal.observed_by_node_id = Some(child_id.clone());
        signal.target_node_id = Some(child_id.clone());
        signal.related_task_id = Some(task_id.clone());
        signal.severity = Some(2);
        signal.evidence.push("feedback=positive".to_string());
        let envelope = MessageEnvelope::new(
            VsmChannelType::OperationToEnvironment,
            BuiltinPayloadType::EnvironmentSignal.as_str(),
            &signal,
        )
        .expect("environment signal envelope")
        .with_route(Some(child_id.clone()), Some(root_id.clone()));

        let outcome = controller
            .handle_envelope(envelope)
            .await
            .expect("handle environment signal");
        assert!(matches!(outcome, ControllerHandleOutcome::Ignored));

        let events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::Other(
                    "operation_environment_signal".to_string(),
                )],
                task_id: Some(task_id),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("environment events");
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].metadata.get("source_channel").map(String::as_str),
            Some("OperationToEnvironment")
        );
        assert_eq!(events[0].payload["kind"].as_str(), Some("UserFeedback"));
        assert_eq!(
            events[0].payload["observed_by_node_id"].as_str(),
            Some(child_id.as_str())
        );
    }

    #[tokio::test]
    async fn future_probe_environment_signal_records_system4_event() {
        let (genome, _suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let system4_id = NodeId::new();
        let suggestion_id = vsm_core::SuggestionId::new();
        let ledger = Arc::new(InMemoryLedger::new());
        let (controller, _shared_genome) = controller_for(genome, ledger.clone());
        let mut signal = EnvironmentSignal::new(
            vsm_core::EnvironmentSignalKind::Opportunity,
            "crate-index",
            "A new model-provider adapter version may simplify worker integration.",
        );
        signal.observed_by_node_id = Some(system4_id.clone());
        signal.target_node_id = Some(root_id.clone());
        signal.related_suggestion_id = Some(suggestion_id.clone());
        signal.severity = Some(4);
        signal
            .metadata
            .insert("forecast_horizon".to_string(), "next_epoch".to_string());
        let envelope = MessageEnvelope::new(
            VsmChannelType::FutureProbeToEnvironment,
            BuiltinPayloadType::EnvironmentSignal.as_str(),
            &signal,
        )
        .expect("future probe envelope")
        .with_route(Some(system4_id.clone()), Some(root_id.clone()));

        controller
            .handle_envelope(envelope)
            .await
            .expect("handle future probe signal");

        let events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::Other(
                    "future_probe_environment_signal".to_string(),
                )],
                node_id: Some(root_id.clone()),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("future probe events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].suggestion_id, Some(suggestion_id));
        assert_eq!(
            events[0].metadata.get("source_channel").map(String::as_str),
            Some("FutureProbeToEnvironment")
        );
        assert_eq!(events[0].payload["kind"].as_str(), Some("Opportunity"));
        assert_eq!(
            events[0].payload["metadata"]["forecast_horizon"].as_str(),
            Some("next_epoch")
        );
    }

    #[tokio::test]
    async fn environment_interaction_signal_records_environment_to_environment_event() {
        let (genome, _suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let ledger = Arc::new(InMemoryLedger::new());
        let (controller, _shared_genome) = controller_for(genome, ledger.clone());
        let mut signal = EnvironmentSignal::new(
            vsm_core::EnvironmentSignalKind::Risk,
            "package-registry",
            "CI images and the package registry are disagreeing on dependency availability.",
        );
        signal.target_environment = Some("ci".to_string());
        signal.target_node_id = Some(root_id.clone());
        signal.severity = Some(8);
        signal
            .evidence
            .push("registry=crate_missing ci=build_blocked".to_string());
        let envelope = MessageEnvelope::new(
            VsmChannelType::EnvironmentToEnvironment,
            BuiltinPayloadType::EnvironmentSignal.as_str(),
            &signal,
        )
        .expect("environment interaction envelope")
        .with_route(None, Some(root_id.clone()));

        controller
            .handle_envelope(envelope)
            .await
            .expect("handle environment interaction");

        let events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::Other(
                    "environment_interaction_signal".to_string(),
                )],
                node_id: Some(root_id.clone()),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("environment interaction events");
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].metadata.get("source_channel").map(String::as_str),
            Some("EnvironmentToEnvironment")
        );
        assert_eq!(events[0].payload["kind"].as_str(), Some("Risk"));
        assert_eq!(events[0].payload["severity"].as_u64(), Some(8));
        assert_eq!(events[0].payload["target_environment"].as_str(), Some("ci"));
    }

    #[tokio::test]
    async fn task_packet_lineage_is_recorded_from_channel_traffic() {
        let (genome, _suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let ledger = Arc::new(InMemoryLedger::new());
        let shared_genome = Arc::new(RwLock::new(genome));
        let transport = Arc::new(InMemoryTransport::new(16));
        let _keepalive = transport
            .subscribe(Subscription {
                channel_types: vec![],
                target_node_id: None,
                queue_name: None,
                durable: false,
            })
            .await
            .expect("subscribe");
        let controller = ControllerRuntime::new(
            root_id.clone(),
            shared_genome,
            transport,
            ledger.clone() as Arc<dyn Ledger>,
        );
        let parent_task_id = TaskId::new();
        let dependency_id = TaskId::new();
        let mut task = TaskPacket::new("Child task", "continue decomposed work");
        task.parent_task_id = Some(parent_task_id.clone());
        task.dependencies.push(dependency_id.clone());
        task.metadata
            .insert("requires_code_write".to_string(), "true".to_string());
        let envelope = envelope_for_task(&task)
            .expect("task envelope")
            .with_route(None, Some(root_id.clone()));

        controller
            .handle_envelope(envelope)
            .await
            .expect("handle task");

        let mapped = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::TaskMapped],
                task_id: Some(task.id.clone()),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("mapped events");
        assert_eq!(mapped.len(), 1);
        assert_eq!(
            mapped[0].payload["parent_task_id"].as_str(),
            Some(parent_task_id.as_str())
        );
        assert_eq!(
            mapped[0].payload["dependency_task_ids"][0].as_str(),
            Some(dependency_id.as_str())
        );

        let routed = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::TaskRouted],
                task_id: Some(task.id.clone()),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("routed events");
        assert_eq!(
            routed[0].payload["parent_task_id"].as_str(),
            Some(parent_task_id.as_str())
        );
        assert_eq!(
            routed[0].payload["dependency_task_ids"][0].as_str(),
            Some(dependency_id.as_str())
        );
        assert_eq!(
            routed[0].payload["source_channel"].as_str(),
            Some("ResourceBargaining")
        );
    }

    #[tokio::test]
    async fn command_channel_routes_non_negotiable_task_on_command_channel() {
        let (genome, _suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let child_id = genome
            .get_node(&root_id)
            .expect("root")
            .children
            .first()
            .cloned()
            .expect("child");
        let ledger = Arc::new(InMemoryLedger::new());
        let shared_genome = Arc::new(RwLock::new(genome));
        let transport = Arc::new(InMemoryTransport::new(16));
        let mut child_stream = transport
            .subscribe(Subscription {
                channel_types: vec![VsmChannelType::Command],
                target_node_id: Some(child_id.to_string()),
                queue_name: None,
                durable: false,
            })
            .await
            .expect("subscribe child");
        let controller = ControllerRuntime::new(
            root_id.clone(),
            shared_genome,
            transport,
            ledger.clone() as Arc<dyn Ledger>,
        );
        let command = vsm_core::Command {
            issued_by: root_id.clone(),
            target: child_id.clone(),
            title: "Freeze unsafe deployment".to_string(),
            body: "Do not deploy this build until the policy review passes.".to_string(),
            non_negotiable: true,
            legal_or_policy_basis: Some("release-policy".to_string()),
            system5_identity: Some("Release safety policy".to_string()),
            policy_values: vec!["protect production stability".to_string()],
            non_negotiable_constraints: vec!["deployment must remain frozen".to_string()],
            denied_capabilities: vec!["deploy".to_string()],
            metadata: BTreeMap::from([("policy_version".to_string(), "2026.06".to_string())]),
        };
        let envelope = MessageEnvelope::new(
            VsmChannelType::Command,
            BuiltinPayloadType::Command.as_str(),
            &command,
        )
        .expect("command envelope")
        .with_route(Some(root_id.clone()), Some(root_id.clone()));

        let outcome = controller
            .handle_envelope(envelope)
            .await
            .expect("handle command");
        let ControllerHandleOutcome::RoutedTask {
            task,
            child_id: routed_child,
            ..
        } = outcome
        else {
            panic!("expected routed command task");
        };
        assert_eq!(routed_child, child_id);
        assert_eq!(task.assigned_to, Some(child_id.clone()));
        assert_eq!(
            task.metadata
                .get("command_non_negotiable")
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            task.metadata
                .get("command_system5_identity")
                .map(String::as_str),
            Some("Release safety policy")
        );
        assert_eq!(
            task.metadata
                .get("command_policy_constraints")
                .map(String::as_str),
            Some("deployment must remain frozen")
        );
        assert!(task
            .constraints
            .iter()
            .any(|constraint| constraint.contains("deployment must remain frozen")));
        assert_eq!(
            task.metadata
                .get("command_denied_capabilities")
                .map(String::as_str),
            Some("deploy")
        );

        let published =
            tokio::time::timeout(std::time::Duration::from_millis(250), child_stream.next())
                .await
                .expect("published command task")
                .expect("stream item")
                .expect("published envelope");
        assert_eq!(published.channel_type, VsmChannelType::Command);
        assert_eq!(published.priority, ChannelPriority::Critical);
        assert_eq!(
            published
                .metadata
                .get("vsm_outbound_channel")
                .map(String::as_str),
            Some("Command")
        );
        assert_eq!(
            published
                .metadata
                .get("command_system5_identity")
                .map(String::as_str),
            Some("Release safety policy")
        );
        assert_eq!(
            published
                .metadata
                .get("command_denied_capabilities")
                .map(String::as_str),
            Some("deploy")
        );
        let published_task: TaskPacket = published.payload_as().expect("published task");
        assert_eq!(published_task.id, task.id);

        let routed = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::TaskRouted],
                task_id: Some(task.id.clone()),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("routed events");
        assert_eq!(routed.len(), 1);
        assert_eq!(
            routed[0].payload["source_channel"].as_str(),
            Some("Command")
        );
        assert_eq!(
            routed[0].payload["outbound_channel"].as_str(),
            Some("Command")
        );
        assert_eq!(
            routed[0].payload["channel_priority"].as_str(),
            Some("Critical")
        );
        assert_eq!(
            routed[0].payload["command_system5_identity"].as_str(),
            Some("Release safety policy")
        );
        assert_eq!(
            routed[0].payload["command_policy_constraints"].as_str(),
            Some("deployment must remain frozen")
        );

        let mapped = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::TaskMapped],
                task_id: Some(task.id.clone()),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("mapped events");
        assert_eq!(
            mapped[0].payload["decomposition_policy"].as_str(),
            Some("system3_command_channel")
        );
        assert_eq!(
            mapped[0].payload["metadata"]["command_policy_values"].as_str(),
            Some("protect production stability")
        );

        let command_events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::Other("command_received".to_string())],
                task_id: Some(task.id.clone()),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("command events");
        assert_eq!(
            command_events[0].payload["system5_identity"].as_str(),
            Some("Release safety policy")
        );
        assert_eq!(
            command_events[0].payload["non_negotiable_constraints"][0].as_str(),
            Some("deployment must remain frozen")
        );
    }

    #[tokio::test]
    async fn system2_coordination_tasks_preserve_coordination_channel() {
        let (genome, _suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let child_id = genome
            .get_node(&root_id)
            .expect("root")
            .children
            .first()
            .cloned()
            .expect("child");
        let ledger = Arc::new(InMemoryLedger::new());
        let shared_genome = Arc::new(RwLock::new(genome));
        let transport = Arc::new(InMemoryTransport::new(16));
        let mut child_stream = transport
            .subscribe(Subscription {
                channel_types: vec![VsmChannelType::System2Coordination],
                target_node_id: Some(child_id.to_string()),
                queue_name: None,
                durable: false,
            })
            .await
            .expect("subscribe child");
        let controller = ControllerRuntime::new(
            root_id.clone(),
            shared_genome,
            transport,
            ledger.clone() as Arc<dyn Ledger>,
        );
        let mut task = TaskPacket::new("Coordinate handoff", "dampen duplicated handoff work");
        task.metadata
            .insert("requires_code_write".to_string(), "true".to_string());
        let envelope = MessageEnvelope::new(
            VsmChannelType::System2Coordination,
            BuiltinPayloadType::TaskPacket.as_str(),
            &task,
        )
        .expect("coordination envelope")
        .with_route(None, Some(root_id.clone()));

        controller
            .handle_envelope(envelope)
            .await
            .expect("handle coordination task");

        let published =
            tokio::time::timeout(std::time::Duration::from_millis(250), child_stream.next())
                .await
                .expect("published coordination task")
                .expect("stream item")
                .expect("published envelope");
        assert_eq!(published.channel_type, VsmChannelType::System2Coordination);
        assert_eq!(published.priority, ChannelPriority::High);
        assert_eq!(
            published
                .metadata
                .get("vsm_source_channel")
                .map(String::as_str),
            Some("System2Coordination")
        );

        let routed = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::TaskRouted],
                task_id: Some(task.id),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("routed events");
        assert_eq!(
            routed[0].payload["source_channel"].as_str(),
            Some("System2Coordination")
        );
        assert_eq!(
            routed[0].payload["outbound_channel"].as_str(),
            Some("System2Coordination")
        );
        assert_eq!(routed[0].payload["channel_priority"].as_str(), Some("High"));
    }

    #[tokio::test]
    async fn system2_contention_signal_creates_dampening_task() {
        let (genome, coder_id, reviewer_id, _tester_id) = genome_with_coder_reviewer_tester();
        let root_id = genome.root_node_id.clone();
        let ledger = Arc::new(InMemoryLedger::new());
        let shared_genome = Arc::new(RwLock::new(genome));
        let transport = Arc::new(InMemoryTransport::new(16));
        let mut reviewer_stream = transport
            .subscribe(Subscription {
                channel_types: vec![VsmChannelType::System2Coordination],
                target_node_id: Some(reviewer_id.to_string()),
                queue_name: None,
                durable: false,
            })
            .await
            .expect("subscribe reviewer");
        let controller = ControllerRuntime::new(
            root_id.clone(),
            shared_genome,
            transport,
            ledger.clone() as Arc<dyn Ledger>,
        );
        let affected_task_id = TaskId::new();
        let mut signal = System2CoordinationSignal::new(
            root_id.clone(),
            System2CoordinationKind::Contention,
            "coder and reviewer are duplicating ownership of the same handoff",
        );
        signal.source_node_id = Some(coder_id.clone());
        signal.target_node_id = Some(reviewer_id.clone());
        signal.affected_node_ids = vec![coder_id, reviewer_id.clone()];
        signal.affected_task_ids = vec![affected_task_id.clone()];
        signal.severity = Some(6);
        signal
            .evidence
            .push("two children claimed the same review dependency".to_string());
        let envelope = MessageEnvelope::new(
            VsmChannelType::System2Coordination,
            BuiltinPayloadType::System2CoordinationSignal.as_str(),
            &signal,
        )
        .expect("system2 signal envelope")
        .with_route(Some(root_id.clone()), Some(root_id.clone()));

        let outcome = controller
            .handle_envelope(envelope)
            .await
            .expect("handle system2 signal");
        let ControllerHandleOutcome::RoutedTask {
            task,
            child_id,
            reason: _,
        } = outcome
        else {
            panic!("expected dampening task route");
        };
        assert_eq!(child_id, reviewer_id.clone());
        assert_eq!(task.assigned_to, Some(reviewer_id.clone()));
        assert_eq!(
            task.metadata.get("coordination_kind").map(String::as_str),
            Some("Contention")
        );
        assert_eq!(task.dependencies, vec![affected_task_id.clone()]);

        let published = tokio::time::timeout(
            std::time::Duration::from_millis(250),
            reviewer_stream.next(),
        )
        .await
        .expect("published dampening task")
        .expect("stream item")
        .expect("published envelope");
        assert_eq!(published.channel_type, VsmChannelType::System2Coordination);
        assert_eq!(published.priority, ChannelPriority::High);
        assert_eq!(
            published
                .metadata
                .get("coordination_kind")
                .map(String::as_str),
            Some("Contention")
        );
        let published_task: TaskPacket = published.payload_as().expect("published task");
        assert_eq!(
            published_task
                .metadata
                .get("coordination_policy")
                .map(String::as_str),
            Some("system2_dampening_v1")
        );

        let signal_events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::Other(
                    "system2_coordination_signal".to_string(),
                )],
                task_id: Some(affected_task_id),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("system2 signal events");
        assert_eq!(signal_events.len(), 1);
        assert_eq!(
            signal_events[0]
                .metadata
                .get("coordination_kind")
                .map(String::as_str),
            Some("Contention")
        );

        let routed = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::TaskRouted],
                task_id: Some(task.id.clone()),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("routed events");
        assert_eq!(routed.len(), 1);
        assert_eq!(
            routed[0].payload["source_channel"].as_str(),
            Some("System2Coordination")
        );
        assert_eq!(
            routed[0].payload["coordination_kind"].as_str(),
            Some("Contention")
        );

        let dampening_events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::Other(
                    "system2_dampening_task_created".to_string(),
                )],
                task_id: Some(task.id),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("dampening events");
        assert_eq!(dampening_events.len(), 1);
    }

    #[tokio::test]
    async fn management_to_operation_preserves_management_channel_and_lineage() {
        let (genome, _suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let child_id = genome
            .get_node(&root_id)
            .expect("root")
            .children
            .first()
            .cloned()
            .expect("child");
        let ledger = Arc::new(InMemoryLedger::new());
        let shared_genome = Arc::new(RwLock::new(genome));
        let transport = Arc::new(InMemoryTransport::new(16));
        let mut child_stream = transport
            .subscribe(Subscription {
                channel_types: vec![VsmChannelType::ManagementToOperation],
                target_node_id: Some(child_id.to_string()),
                queue_name: None,
                durable: false,
            })
            .await
            .expect("subscribe child");
        let controller = ControllerRuntime::new(
            root_id.clone(),
            shared_genome,
            transport,
            ledger.clone() as Arc<dyn Ledger>,
        );

        let mut task = TaskPacket::new("Apply local management decision", "execute managed work");
        task.metadata
            .insert("requires_code_write".to_string(), "true".to_string());
        let mut envelope = MessageEnvelope::new(
            VsmChannelType::ManagementToOperation,
            BuiltinPayloadType::TaskPacket.as_str(),
            &task,
        )
        .expect("management envelope")
        .with_route(Some(root_id.clone()), Some(root_id.clone()));
        envelope.correlation_id = Some("management-correlation".to_string());

        let outcome = controller
            .handle_envelope(envelope)
            .await
            .expect("handle management task");
        let ControllerHandleOutcome::RoutedTask {
            task: routed_task,
            child_id: routed_child,
            ..
        } = outcome
        else {
            panic!("expected routed task");
        };
        assert_eq!(routed_child, child_id);
        assert_eq!(
            routed_task
                .metadata
                .get("management_kind")
                .map(String::as_str),
            Some("management_to_operation")
        );
        assert_eq!(
            routed_task
                .metadata
                .get("management_source_node_id")
                .map(String::as_str),
            Some(root_id.as_str())
        );

        let published =
            tokio::time::timeout(std::time::Duration::from_millis(250), child_stream.next())
                .await
                .expect("published management task")
                .expect("stream item")
                .expect("published envelope");
        assert_eq!(
            published.channel_type,
            VsmChannelType::ManagementToOperation
        );
        assert_eq!(published.target_node_id, Some(child_id.clone()));
        assert_eq!(
            published
                .metadata
                .get("management_kind")
                .map(String::as_str),
            Some("management_to_operation")
        );
        assert_eq!(
            published
                .metadata
                .get("management_source_node_id")
                .map(String::as_str),
            Some(root_id.as_str())
        );
        assert_eq!(
            published
                .metadata
                .get("management_target_node_id")
                .map(String::as_str),
            Some(child_id.as_str())
        );
        assert_eq!(
            published
                .metadata
                .get("management_via_controller_node_id")
                .map(String::as_str),
            Some(root_id.as_str())
        );
        assert_eq!(
            published
                .metadata
                .get("vsm_source_channel")
                .map(String::as_str),
            Some("ManagementToOperation")
        );
        let published_task: TaskPacket = published.payload_as().expect("published task");
        assert_eq!(published_task.assigned_to, Some(child_id.clone()));
        assert_eq!(
            published_task
                .metadata
                .get("management_target_node_id")
                .map(String::as_str),
            Some(child_id.as_str())
        );

        let routed = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::TaskRouted],
                task_id: Some(task.id),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("routed events");
        assert_eq!(routed.len(), 1);
        assert_eq!(
            routed[0].payload["source_channel"].as_str(),
            Some("ManagementToOperation")
        );
        assert_eq!(
            routed[0].payload["outbound_channel"].as_str(),
            Some("ManagementToOperation")
        );
        assert_eq!(
            routed[0].payload["management_kind"].as_str(),
            Some("management_to_operation")
        );
        assert_eq!(
            routed[0].payload["management_source_node_id"].as_str(),
            Some(root_id.as_str())
        );
        assert_eq!(
            routed[0].payload["management_target_node_id"].as_str(),
            Some(child_id.as_str())
        );
        assert_eq!(
            routed[0].payload["management_correlation_id"].as_str(),
            Some("management-correlation")
        );
    }

    #[tokio::test]
    async fn typed_management_operation_directive_creates_targeted_task() {
        let (genome, _suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let child_id = genome
            .get_node(&root_id)
            .expect("root")
            .children
            .first()
            .cloned()
            .expect("child");
        let ledger = Arc::new(InMemoryLedger::new());
        let shared_genome = Arc::new(RwLock::new(genome));
        let transport = Arc::new(InMemoryTransport::new(16));
        let mut child_stream = transport
            .subscribe(Subscription {
                channel_types: vec![VsmChannelType::ManagementToOperation],
                target_node_id: Some(child_id.to_string()),
                queue_name: None,
                durable: false,
            })
            .await
            .expect("subscribe child");
        let controller = ControllerRuntime::new(
            root_id.clone(),
            shared_genome,
            transport,
            ledger.clone() as Arc<dyn Ledger>,
        );

        let related_task_id = TaskId::new();
        let dependency_task_id = TaskId::new();
        let mut directive = ManagementOperationDirective::new(
            root_id.clone(),
            child_id.clone(),
            vsm_core::ManagementOperationKind::AssignWork,
            "Execute managed implementation slice",
            "Carry out the accepted operation under local management policy.",
        );
        directive.related_task_id = Some(related_task_id.clone());
        directive
            .dependency_task_ids
            .push(dependency_task_id.clone());
        directive.target_state = Some("managed slice complete".to_string());
        directive
            .constraints
            .push("respect current resource bargain".to_string());
        directive
            .context_refs
            .push("repo://managed-slice".to_string());
        directive
            .authority_refs
            .push("policy://local-management".to_string());
        directive.risk = RiskClass::High;
        directive
            .metadata
            .insert("requires_code_write".to_string(), "true".to_string());
        directive
            .metadata
            .insert("required_capability".to_string(), "write_code".to_string());
        let mut envelope = MessageEnvelope::new(
            VsmChannelType::ManagementToOperation,
            BuiltinPayloadType::ManagementOperationDirective.as_str(),
            &directive,
        )
        .expect("management directive envelope")
        .with_route(Some(root_id.clone()), Some(root_id.clone()));
        envelope.correlation_id = Some("management-directive-correlation".to_string());

        let outcome = controller
            .handle_envelope(envelope)
            .await
            .expect("handle management directive");
        let ControllerHandleOutcome::RoutedTask {
            task,
            child_id: routed_child,
            reason: _,
        } = outcome
        else {
            panic!("expected routed management task");
        };
        assert_eq!(routed_child, child_id.clone());
        assert_eq!(task.assigned_to, Some(child_id.clone()));
        assert_eq!(task.parent_task_id, Some(related_task_id.clone()));
        assert_eq!(task.dependencies, vec![dependency_task_id.clone()]);
        assert_eq!(task.target_state.as_deref(), Some("managed slice complete"));
        assert_eq!(
            task.metadata
                .get("management_operation_kind")
                .map(String::as_str),
            Some("AssignWork")
        );
        assert_eq!(
            task.metadata.get("management_policy").map(String::as_str),
            Some("management_operation_directive_v1")
        );

        let published =
            tokio::time::timeout(std::time::Duration::from_millis(250), child_stream.next())
                .await
                .expect("published management task")
                .expect("stream item")
                .expect("published envelope");
        assert_eq!(
            published.channel_type,
            VsmChannelType::ManagementToOperation
        );
        assert_eq!(
            published.payload_type,
            BuiltinPayloadType::TaskPacket.as_str()
        );
        assert_eq!(
            published
                .metadata
                .get("management_operation_kind")
                .map(String::as_str),
            Some("AssignWork")
        );
        assert_eq!(
            published
                .metadata
                .get("management_policy")
                .map(String::as_str),
            Some("management_operation_directive_v1")
        );
        let published_task: TaskPacket = published.payload_as().expect("published task");
        assert_eq!(published_task.assigned_to, Some(child_id));

        let directive_events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::Other(
                    "management_operation_directive_received".to_string(),
                )],
                task_id: Some(related_task_id),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("management directive events");
        assert_eq!(directive_events.len(), 1);
        assert_eq!(
            directive_events[0]
                .metadata
                .get("management_operation_kind")
                .map(String::as_str),
            Some("AssignWork")
        );

        let routed = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::TaskRouted],
                task_id: Some(task.id),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("routed events");
        assert_eq!(routed.len(), 1);
        assert_eq!(
            routed[0].payload["source_channel"].as_str(),
            Some("ManagementToOperation")
        );
        assert_eq!(
            routed[0].payload["management_operation_kind"].as_str(),
            Some("AssignWork")
        );
        assert_eq!(
            routed[0].payload["management_policy"].as_str(),
            Some("management_operation_directive_v1")
        );
    }

    #[tokio::test]
    async fn operation_to_operation_handoff_preserves_peer_channel_and_lineage() {
        let root = ViableNode::new_metasystem("root");
        let root_id = root.id.clone();
        let mut genome = OrganizationalGenome::new(root);
        let coder = ViableNode::new_leaf("coder", LeafOperationSpec::coding());
        let coder_id = coder.id.clone();
        let reviewer = ViableNode::new_leaf("reviewer", LeafOperationSpec::reviewer());
        let reviewer_id = reviewer.id.clone();
        genome.add_child(&root_id, coder).expect("coder child");
        genome
            .add_child(&root_id, reviewer)
            .expect("reviewer child");

        let ledger = Arc::new(InMemoryLedger::new());
        let shared_genome = Arc::new(RwLock::new(genome));
        let transport = Arc::new(InMemoryTransport::new(16));
        let mut reviewer_stream = transport
            .subscribe(Subscription {
                channel_types: vec![VsmChannelType::OperationToOperation],
                target_node_id: Some(reviewer_id.to_string()),
                queue_name: None,
                durable: false,
            })
            .await
            .expect("subscribe reviewer");
        let controller = ControllerRuntime::new(
            root_id.clone(),
            shared_genome,
            transport,
            ledger.clone() as Arc<dyn Ledger>,
        );

        let mut task = TaskPacket::new("Review peer handoff", "continue peer work-in-progress");
        task.metadata
            .insert("target_child".to_string(), "reviewer".to_string());
        task.metadata
            .insert("required_capability".to_string(), "review".to_string());
        let mut envelope = MessageEnvelope::new(
            VsmChannelType::OperationToOperation,
            BuiltinPayloadType::TaskPacket.as_str(),
            &task,
        )
        .expect("handoff envelope")
        .with_route(Some(coder_id.clone()), Some(root_id.clone()));
        envelope.correlation_id = Some("peer-handoff-correlation".to_string());

        let outcome = controller
            .handle_envelope(envelope)
            .await
            .expect("handle handoff task");
        let ControllerHandleOutcome::RoutedTask {
            task: routed_task,
            child_id,
            ..
        } = outcome
        else {
            panic!("expected routed task");
        };
        assert_eq!(child_id, reviewer_id);
        assert_eq!(
            routed_task.metadata.get("handoff_kind").map(String::as_str),
            Some("operation_to_operation")
        );
        assert_eq!(
            routed_task
                .metadata
                .get("handoff_source_node_id")
                .map(String::as_str),
            Some(coder_id.as_str())
        );

        let published = tokio::time::timeout(
            std::time::Duration::from_millis(250),
            reviewer_stream.next(),
        )
        .await
        .expect("published handoff task")
        .expect("stream item")
        .expect("published envelope");
        assert_eq!(published.channel_type, VsmChannelType::OperationToOperation);
        assert_eq!(published.target_node_id, Some(reviewer_id.clone()));
        assert_eq!(
            published.metadata.get("handoff_kind").map(String::as_str),
            Some("operation_to_operation")
        );
        assert_eq!(
            published
                .metadata
                .get("handoff_source_node_id")
                .map(String::as_str),
            Some(coder_id.as_str())
        );
        assert_eq!(
            published
                .metadata
                .get("handoff_target_node_id")
                .map(String::as_str),
            Some(reviewer_id.as_str())
        );
        assert_eq!(
            published
                .metadata
                .get("handoff_via_controller_node_id")
                .map(String::as_str),
            Some(root_id.as_str())
        );
        assert_eq!(
            published
                .metadata
                .get("vsm_source_channel")
                .map(String::as_str),
            Some("OperationToOperation")
        );
        let published_task: TaskPacket = published.payload_as().expect("published task");
        assert_eq!(published_task.assigned_to, Some(reviewer_id.clone()));
        assert_eq!(
            published_task
                .metadata
                .get("handoff_target_node_id")
                .map(String::as_str),
            Some(reviewer_id.as_str())
        );

        let routed = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::TaskRouted],
                task_id: Some(task.id),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("routed events");
        assert_eq!(routed.len(), 1);
        assert_eq!(
            routed[0].payload["source_channel"].as_str(),
            Some("OperationToOperation")
        );
        assert_eq!(
            routed[0].payload["outbound_channel"].as_str(),
            Some("OperationToOperation")
        );
        assert_eq!(
            routed[0].payload["handoff_kind"].as_str(),
            Some("operation_to_operation")
        );
        assert_eq!(
            routed[0].payload["handoff_source_node_id"].as_str(),
            Some(coder_id.as_str())
        );
        assert_eq!(
            routed[0].payload["handoff_target_node_id"].as_str(),
            Some(reviewer_id.as_str())
        );
        assert_eq!(
            routed[0].payload["handoff_correlation_id"].as_str(),
            Some("peer-handoff-correlation")
        );
    }

    #[tokio::test]
    async fn typed_operation_handoff_creates_targeted_peer_task() {
        let root = ViableNode::new_metasystem("root");
        let root_id = root.id.clone();
        let mut genome = OrganizationalGenome::new(root);
        let coder = ViableNode::new_leaf("coder", LeafOperationSpec::coding());
        let coder_id = coder.id.clone();
        let reviewer = ViableNode::new_leaf("reviewer", LeafOperationSpec::reviewer());
        let reviewer_id = reviewer.id.clone();
        genome.add_child(&root_id, coder).expect("coder child");
        genome
            .add_child(&root_id, reviewer)
            .expect("reviewer child");

        let ledger = Arc::new(InMemoryLedger::new());
        let shared_genome = Arc::new(RwLock::new(genome));
        let transport = Arc::new(InMemoryTransport::new(16));
        let mut reviewer_stream = transport
            .subscribe(Subscription {
                channel_types: vec![VsmChannelType::OperationToOperation],
                target_node_id: Some(reviewer_id.to_string()),
                queue_name: None,
                durable: false,
            })
            .await
            .expect("subscribe reviewer");
        let controller = ControllerRuntime::new(
            root_id.clone(),
            shared_genome,
            transport,
            ledger.clone() as Arc<dyn Ledger>,
        );

        let related_task_id = TaskId::new();
        let dependency_task_id = TaskId::new();
        let mut artifact = vsm_core::TaskArtifact::inline("patch", "diff --git a/src/lib.rs");
        artifact.uri = Some("artifact://patch/123".to_string());
        let mut handoff = OperationHandoff::new(
            coder_id.clone(),
            reviewer_id.clone(),
            vsm_core::OperationHandoffKind::ReviewRequest,
            "Review handed-off patch",
            "Coder produced a patch and needs peer review before integration.",
        );
        handoff.related_task_id = Some(related_task_id.clone());
        handoff.dependency_task_ids.push(dependency_task_id.clone());
        handoff.artifacts.push(artifact);
        handoff
            .evidence
            .push("patch builds locally and awaits review".to_string());
        handoff
            .metadata
            .insert("review_focus".to_string(), "ownership boundary".to_string());
        let mut envelope = MessageEnvelope::new(
            VsmChannelType::OperationToOperation,
            BuiltinPayloadType::OperationHandoff.as_str(),
            &handoff,
        )
        .expect("operation handoff envelope")
        .with_route(Some(coder_id.clone()), Some(root_id.clone()));
        envelope.correlation_id = Some("typed-handoff-correlation".to_string());

        let outcome = controller
            .handle_envelope(envelope)
            .await
            .expect("handle typed operation handoff");
        let ControllerHandleOutcome::RoutedTask {
            task,
            child_id,
            reason: _,
        } = outcome
        else {
            panic!("expected routed handoff task");
        };
        assert_eq!(child_id, reviewer_id.clone());
        assert_eq!(task.assigned_to, Some(reviewer_id.clone()));
        assert_eq!(task.parent_task_id, Some(related_task_id.clone()));
        assert_eq!(task.dependencies, vec![dependency_task_id.clone()]);
        assert_eq!(task.context_refs, vec!["artifact://patch/123".to_string()]);
        assert_eq!(
            task.metadata
                .get("handoff_operation_kind")
                .map(String::as_str),
            Some("ReviewRequest")
        );

        let published = tokio::time::timeout(
            std::time::Duration::from_millis(250),
            reviewer_stream.next(),
        )
        .await
        .expect("published handoff task")
        .expect("stream item")
        .expect("published envelope");
        assert_eq!(published.channel_type, VsmChannelType::OperationToOperation);
        assert_eq!(
            published.payload_type,
            BuiltinPayloadType::TaskPacket.as_str()
        );
        assert_eq!(
            published
                .metadata
                .get("handoff_operation_kind")
                .map(String::as_str),
            Some("ReviewRequest")
        );
        assert_eq!(
            published
                .metadata
                .get("handoff_artifact_refs")
                .map(String::as_str),
            Some("artifact://patch/123")
        );
        let published_task: TaskPacket = published.payload_as().expect("published task");
        assert_eq!(published_task.assigned_to, Some(reviewer_id.clone()));

        let handoff_events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::Other(
                    "operation_handoff_received".to_string(),
                )],
                task_id: Some(related_task_id),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("handoff events");
        assert_eq!(handoff_events.len(), 1);
        assert_eq!(
            handoff_events[0]
                .metadata
                .get("handoff_operation_kind")
                .map(String::as_str),
            Some("ReviewRequest")
        );
        assert_eq!(
            handoff_events[0].payload["artifact_count"].as_u64(),
            Some(1)
        );

        let routed = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::TaskRouted],
                task_id: Some(task.id),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("routed events");
        assert_eq!(routed.len(), 1);
        assert_eq!(
            routed[0].payload["source_channel"].as_str(),
            Some("OperationToOperation")
        );
        assert_eq!(
            routed[0].payload["handoff_operation_kind"].as_str(),
            Some("ReviewRequest")
        );
        assert_eq!(
            routed[0].payload["handoff_artifact_count"].as_str(),
            Some("1")
        );
    }

    #[tokio::test]
    async fn resource_bargain_publishes_policy_bounded_allocation_decision() {
        let (mut genome, _suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let child_id = genome
            .get_node(&root_id)
            .expect("root")
            .children
            .first()
            .cloned()
            .expect("child");
        genome
            .get_node_mut(&root_id)
            .expect("root mut")
            .system_3
            .default_task_budget_tokens = Some(1_000);
        let child = genome.get_node_mut(&child_id).expect("child mut");
        child
            .permissions
            .allowed_tools
            .push("read_filesystem".to_string());
        child.permissions.denied_tools.push("network".to_string());
        child
            .context_policy
            .retrievable_context_refs
            .push("repo://src".to_string());

        let ledger = Arc::new(InMemoryLedger::new());
        let shared_genome = Arc::new(RwLock::new(genome));
        let transport = Arc::new(InMemoryTransport::new(16));
        let mut child_stream = transport
            .subscribe(Subscription {
                channel_types: vec![VsmChannelType::ResourceBargaining],
                target_node_id: Some(child_id.to_string()),
                queue_name: None,
                durable: false,
            })
            .await
            .expect("subscribe child");
        let controller = ControllerRuntime::new(
            root_id.clone(),
            shared_genome,
            transport,
            ledger.clone() as Arc<dyn Ledger>,
        );
        let bargain = ResourceBargain {
            requested_by: child_id.clone(),
            task_id: Some(TaskId::new()),
            proposed_task: None,
            requested_tokens: Some(1_500),
            requested_tool_permissions: vec!["read_filesystem".to_string(), "network".to_string()],
            requested_context_refs: vec!["repo://src".to_string(), "secret://prod".to_string()],
            justification: "Need bounded resources for implementation task".to_string(),
        };
        let envelope = MessageEnvelope::new(
            VsmChannelType::ResourceBargaining,
            BuiltinPayloadType::ResourceBargain.as_str(),
            &bargain,
        )
        .expect("resource bargain envelope")
        .with_route(Some(child_id.clone()), Some(root_id.clone()));

        let outcome = controller
            .handle_envelope(envelope)
            .await
            .expect("handle resource bargain");
        assert!(matches!(outcome, ControllerHandleOutcome::Ignored));

        let published =
            tokio::time::timeout(std::time::Duration::from_millis(250), child_stream.next())
                .await
                .expect("published allocation decision")
                .expect("stream item")
                .expect("published envelope");
        assert_eq!(published.channel_type, VsmChannelType::ResourceBargaining);
        assert_eq!(
            published.payload_type,
            BuiltinPayloadType::ResourceAllocationDecision.as_str()
        );
        assert_eq!(published.priority, ChannelPriority::High);
        let decision: ResourceAllocationDecision =
            published.payload_as().expect("allocation decision");
        assert_eq!(decision.requested_by, child_id);
        assert_eq!(decision.status, ResourceAllocationStatus::PartiallyApproved);
        assert_eq!(decision.approved_tokens, Some(1_000));
        assert_eq!(
            decision.approved_tool_permissions,
            vec!["read_filesystem".to_string()]
        );
        assert_eq!(
            decision.denied_tool_permissions,
            vec!["network".to_string()]
        );
        assert_eq!(
            decision.approved_context_refs,
            vec!["repo://src".to_string()]
        );
        assert_eq!(
            decision.denied_context_refs,
            vec!["secret://prod".to_string()]
        );

        let allocation_events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::Other(
                    "resource_allocation_decision".to_string(),
                )],
                task_id: bargain.task_id.clone(),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("allocation events");
        assert_eq!(allocation_events.len(), 1);
        assert_eq!(
            allocation_events[0]
                .metadata
                .get("allocation_status")
                .map(String::as_str),
            Some("PartiallyApproved")
        );
    }

    #[tokio::test]
    async fn resource_bargain_epoch_budget_accumulates_approved_tokens() {
        let (mut genome, _suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let child_id = genome
            .get_node(&root_id)
            .expect("root")
            .children
            .first()
            .cloned()
            .expect("child");
        genome
            .get_node_mut(&root_id)
            .expect("root mut")
            .system_3
            .default_task_budget_tokens = Some(2_000);
        for channel in &mut genome.channels {
            if channel.channel_type == VsmChannelType::ResourceBargaining
                && ((channel.from.as_ref() == Some(&root_id)
                    && channel.to.as_ref() == Some(&child_id))
                    || (channel.from.as_ref() == Some(&child_id)
                        && channel.to.as_ref() == Some(&root_id)))
            {
                channel.max_token_budget_per_epoch = Some(1_000);
            }
        }

        let ledger = Arc::new(InMemoryLedger::new());
        let shared_genome = Arc::new(RwLock::new(genome));
        let transport = Arc::new(InMemoryTransport::new(16));
        let mut child_stream = transport
            .subscribe(Subscription {
                channel_types: vec![VsmChannelType::ResourceBargaining],
                target_node_id: Some(child_id.to_string()),
                queue_name: None,
                durable: false,
            })
            .await
            .expect("subscribe child");
        let controller = ControllerRuntime::new(
            root_id.clone(),
            shared_genome,
            transport,
            ledger.clone() as Arc<dyn Ledger>,
        );

        let first_task_id = TaskId::new();
        let first_bargain = ResourceBargain {
            requested_by: child_id.clone(),
            task_id: Some(first_task_id),
            proposed_task: None,
            requested_tokens: Some(700),
            requested_tool_permissions: vec![],
            requested_context_refs: vec![],
            justification: "Need first epoch slice".to_string(),
        };
        let first_envelope = MessageEnvelope::new(
            VsmChannelType::ResourceBargaining,
            BuiltinPayloadType::ResourceBargain.as_str(),
            &first_bargain,
        )
        .expect("first resource bargain envelope")
        .with_route(Some(child_id.clone()), Some(root_id.clone()));
        controller
            .handle_envelope(first_envelope)
            .await
            .expect("handle first resource bargain");
        let first_published =
            tokio::time::timeout(std::time::Duration::from_millis(250), child_stream.next())
                .await
                .expect("first published allocation decision")
                .expect("stream item")
                .expect("published first envelope");
        let first_decision: ResourceAllocationDecision =
            first_published.payload_as().expect("first decision");
        assert_eq!(first_decision.status, ResourceAllocationStatus::Approved);
        assert_eq!(first_decision.approved_tokens, Some(700));
        assert_eq!(
            first_published
                .metadata
                .get("resource_epoch_budget_tokens")
                .map(String::as_str),
            Some("1000")
        );
        assert_eq!(
            first_published
                .metadata
                .get("resource_epoch_remaining_after_tokens")
                .map(String::as_str),
            Some("300")
        );

        let second_task_id = TaskId::new();
        let second_bargain = ResourceBargain {
            requested_by: child_id.clone(),
            task_id: Some(second_task_id.clone()),
            proposed_task: None,
            requested_tokens: Some(700),
            requested_tool_permissions: vec![],
            requested_context_refs: vec![],
            justification: "Need second epoch slice".to_string(),
        };
        let second_envelope = MessageEnvelope::new(
            VsmChannelType::ResourceBargaining,
            BuiltinPayloadType::ResourceBargain.as_str(),
            &second_bargain,
        )
        .expect("second resource bargain envelope")
        .with_route(Some(child_id), Some(root_id));
        controller
            .handle_envelope(second_envelope)
            .await
            .expect("handle second resource bargain");
        let second_published =
            tokio::time::timeout(std::time::Duration::from_millis(250), child_stream.next())
                .await
                .expect("second published allocation decision")
                .expect("stream item")
                .expect("published second envelope");
        let second_decision: ResourceAllocationDecision =
            second_published.payload_as().expect("second decision");
        assert_eq!(
            second_decision.status,
            ResourceAllocationStatus::PartiallyApproved
        );
        assert_eq!(second_decision.approved_tokens, Some(300));
        assert!(second_decision
            .reasons
            .iter()
            .any(|reason| reason.contains("ResourceBargaining epoch budget")));
        assert_eq!(
            second_published
                .metadata
                .get("resource_epoch_allocated_before_tokens")
                .map(String::as_str),
            Some("700")
        );
        assert_eq!(
            second_published
                .metadata
                .get("resource_epoch_remaining_before_tokens")
                .map(String::as_str),
            Some("300")
        );
        assert_eq!(
            second_published
                .metadata
                .get("resource_epoch_remaining_after_tokens")
                .map(String::as_str),
            Some("0")
        );

        let allocation_events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::Other(
                    "resource_allocation_decision".to_string(),
                )],
                task_id: Some(second_task_id),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("allocation events");
        assert_eq!(
            allocation_events[0]
                .metadata
                .get("resource_epoch_allocated_before_tokens")
                .map(String::as_str),
            Some("700")
        );
    }

    #[tokio::test]
    async fn resource_bargain_epoch_message_budget_denies_after_cap() {
        let (mut genome, _suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let child_id = genome
            .get_node(&root_id)
            .expect("root")
            .children
            .first()
            .cloned()
            .expect("child");
        let child = genome.get_node_mut(&child_id).expect("child mut");
        child
            .permissions
            .allowed_tools
            .push("read_filesystem".to_string());
        for channel in &mut genome.channels {
            if channel.channel_type == VsmChannelType::ResourceBargaining
                && ((channel.from.as_ref() == Some(&root_id)
                    && channel.to.as_ref() == Some(&child_id))
                    || (channel.from.as_ref() == Some(&child_id)
                        && channel.to.as_ref() == Some(&root_id)))
            {
                channel.max_messages_per_epoch = Some(1);
            }
        }

        let ledger = Arc::new(InMemoryLedger::new());
        let shared_genome = Arc::new(RwLock::new(genome));
        let transport = Arc::new(InMemoryTransport::new(16));
        let mut child_stream = transport
            .subscribe(Subscription {
                channel_types: vec![VsmChannelType::ResourceBargaining],
                target_node_id: Some(child_id.to_string()),
                queue_name: None,
                durable: false,
            })
            .await
            .expect("subscribe child");
        let controller = ControllerRuntime::new(
            root_id.clone(),
            shared_genome,
            transport,
            ledger.clone() as Arc<dyn Ledger>,
        );

        for title in ["first", "second"] {
            let bargain = ResourceBargain {
                requested_by: child_id.clone(),
                task_id: Some(TaskId::new()),
                proposed_task: None,
                requested_tokens: None,
                requested_tool_permissions: vec!["read_filesystem".to_string()],
                requested_context_refs: vec![],
                justification: format!("Need {title} resource bargain message"),
            };
            let envelope = MessageEnvelope::new(
                VsmChannelType::ResourceBargaining,
                BuiltinPayloadType::ResourceBargain.as_str(),
                &bargain,
            )
            .expect("resource bargain envelope")
            .with_route(Some(child_id.clone()), Some(root_id.clone()));
            controller
                .handle_envelope(envelope)
                .await
                .expect("handle resource bargain");
        }

        let first_published =
            tokio::time::timeout(std::time::Duration::from_millis(250), child_stream.next())
                .await
                .expect("first published allocation decision")
                .expect("stream item")
                .expect("published first envelope");
        let first_decision: ResourceAllocationDecision =
            first_published.payload_as().expect("first decision");
        assert_eq!(first_decision.status, ResourceAllocationStatus::Approved);

        let second_published =
            tokio::time::timeout(std::time::Duration::from_millis(250), child_stream.next())
                .await
                .expect("second published allocation decision")
                .expect("stream item")
                .expect("published second envelope");
        let second_decision: ResourceAllocationDecision =
            second_published.payload_as().expect("second decision");
        assert_eq!(second_decision.status, ResourceAllocationStatus::Denied);
        assert!(second_decision
            .reasons
            .iter()
            .any(|reason| reason.contains("message budget exhausted")));
        assert_eq!(
            second_published
                .metadata
                .get("resource_epoch_remaining_messages_before")
                .map(String::as_str),
            Some("0")
        );
    }

    #[tokio::test]
    async fn resource_bargain_accepts_and_routes_proposed_task() {
        let (mut genome, _suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let child_id = genome
            .get_node(&root_id)
            .expect("root")
            .children
            .first()
            .cloned()
            .expect("child");
        genome
            .get_node_mut(&root_id)
            .expect("root mut")
            .system_3
            .default_task_budget_tokens = Some(1_000);

        let ledger = Arc::new(InMemoryLedger::new());
        let shared_genome = Arc::new(RwLock::new(genome));
        let transport = Arc::new(InMemoryTransport::new(16));
        let mut child_stream = transport
            .subscribe(Subscription {
                channel_types: vec![VsmChannelType::ResourceBargaining],
                target_node_id: Some(child_id.to_string()),
                queue_name: None,
                durable: false,
            })
            .await
            .expect("subscribe child");
        let controller = ControllerRuntime::new(
            root_id.clone(),
            shared_genome,
            transport,
            ledger.clone() as Arc<dyn Ledger>,
        );

        let mut proposed_task = TaskPacket::new(
            "Accepted bargaining task",
            "Execute work after System 3 allocates resources",
        );
        proposed_task.risk = RiskClass::Low;
        let proposed_task_id = proposed_task.id.clone();
        let bargain = ResourceBargain {
            requested_by: child_id.clone(),
            task_id: None,
            proposed_task: Some(proposed_task),
            requested_tokens: Some(500),
            requested_tool_permissions: vec![],
            requested_context_refs: vec![],
            justification: "Accept delegated work if budget is available".to_string(),
        };
        let envelope = MessageEnvelope::new(
            VsmChannelType::ResourceBargaining,
            BuiltinPayloadType::ResourceBargain.as_str(),
            &bargain,
        )
        .expect("resource bargain envelope")
        .with_route(Some(child_id.clone()), Some(root_id.clone()));

        controller
            .handle_envelope(envelope)
            .await
            .expect("handle resource bargain");

        let decision_envelope =
            tokio::time::timeout(std::time::Duration::from_millis(250), child_stream.next())
                .await
                .expect("published allocation decision")
                .expect("stream item")
                .expect("published decision envelope");
        assert_eq!(
            decision_envelope.payload_type,
            BuiltinPayloadType::ResourceAllocationDecision.as_str()
        );
        let decision: ResourceAllocationDecision =
            decision_envelope.payload_as().expect("allocation decision");
        assert_eq!(decision.status, ResourceAllocationStatus::Approved);
        assert_eq!(decision.task_id, Some(proposed_task_id.clone()));

        let task_envelope =
            tokio::time::timeout(std::time::Duration::from_millis(250), child_stream.next())
                .await
                .expect("published accepted task")
                .expect("stream item")
                .expect("published task envelope");
        assert_eq!(
            task_envelope.channel_type,
            VsmChannelType::ResourceBargaining
        );
        assert_eq!(
            task_envelope.payload_type,
            BuiltinPayloadType::TaskPacket.as_str()
        );
        assert_eq!(
            task_envelope
                .metadata
                .get("vsm_source_channel")
                .map(String::as_str),
            Some("ResourceBargaining")
        );
        let accepted_task: TaskPacket = task_envelope.payload_as().expect("accepted task");
        assert_eq!(accepted_task.id, proposed_task_id);
        assert_eq!(accepted_task.assigned_to, Some(child_id));
        assert_eq!(
            accepted_task
                .metadata
                .get("resource_bargain_status")
                .map(String::as_str),
            Some("Approved")
        );
        assert_eq!(
            accepted_task
                .metadata
                .get("resource_bargain_approved_tokens")
                .map(String::as_str),
            Some("500")
        );

        let accepted_events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::Other(
                    "resource_bargain_work_accepted".to_string(),
                )],
                task_id: Some(proposed_task_id),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("accepted work events");
        assert_eq!(accepted_events.len(), 1);
    }

    #[tokio::test]
    async fn resource_bargain_uses_three_four_pressure_to_constrain_tokens() {
        let (mut genome, _suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let child_id = genome
            .get_node(&root_id)
            .expect("root")
            .children
            .first()
            .cloned()
            .expect("child");
        genome
            .get_node_mut(&root_id)
            .expect("root mut")
            .system_3
            .default_task_budget_tokens = Some(1_000);

        let ledger = Arc::new(InMemoryLedger::new());
        let shared_genome = Arc::new(RwLock::new(genome));
        let transport = Arc::new(InMemoryTransport::new(16));
        let mut child_stream = transport
            .subscribe(Subscription {
                channel_types: vec![VsmChannelType::ResourceBargaining],
                target_node_id: Some(child_id.to_string()),
                queue_name: None,
                durable: false,
            })
            .await
            .expect("subscribe child");
        let controller = ControllerRuntime::new(
            root_id.clone(),
            shared_genome,
            transport,
            ledger.clone() as Arc<dyn Ledger>,
        );

        let system4_id = NodeId::new();
        let mut signal = ThreeFourHomeostatSignal::new(
            root_id.clone(),
            system4_id.clone(),
            root_id.clone(),
            ThreeFourHomeostatKind::ResourceImbalance,
            ThreeFourHomeostatBalance::Conflict,
            "Reduce present token exposure until the resource imbalance is resolved.",
        );
        signal.present_summary = "current work is over-consuming implementation budget".to_string();
        signal.future_summary = "future reliability work needs reserved capacity".to_string();
        signal.severity = Some(8);
        let homeostat_envelope = MessageEnvelope::new(
            VsmChannelType::ThreeFourHomeostat,
            BuiltinPayloadType::ThreeFourHomeostatSignal.as_str(),
            &signal,
        )
        .expect("homeostat envelope")
        .with_route(Some(system4_id), Some(root_id.clone()));
        controller
            .handle_envelope(homeostat_envelope)
            .await
            .expect("record homeostat pressure");

        let bargain = ResourceBargain {
            requested_by: child_id,
            task_id: Some(TaskId::new()),
            proposed_task: None,
            requested_tokens: Some(800),
            requested_tool_permissions: vec![],
            requested_context_refs: vec![],
            justification: "Need tokens for present implementation work".to_string(),
        };
        let envelope = MessageEnvelope::new(
            VsmChannelType::ResourceBargaining,
            BuiltinPayloadType::ResourceBargain.as_str(),
            &bargain,
        )
        .expect("resource bargain envelope")
        .with_route(None, Some(root_id.clone()));
        controller
            .handle_envelope(envelope)
            .await
            .expect("handle pressure-aware resource bargain");

        let published =
            tokio::time::timeout(std::time::Duration::from_millis(250), child_stream.next())
                .await
                .expect("published allocation decision")
                .expect("stream item")
                .expect("published decision envelope");
        assert_eq!(
            published
                .metadata
                .get("homeostat_pressure_count")
                .map(String::as_str),
            Some("1")
        );
        let decision: ResourceAllocationDecision =
            published.payload_as().expect("allocation decision");
        assert_eq!(decision.status, ResourceAllocationStatus::PartiallyApproved);
        assert_eq!(decision.approved_tokens, Some(200));
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason.contains("Three-Four homeostat resource pressure")));

        let allocation_events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::Other(
                    "resource_allocation_decision".to_string(),
                )],
                task_id: bargain.task_id,
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("allocation events");
        assert_eq!(allocation_events.len(), 1);
        assert_eq!(
            allocation_events[0]
                .metadata
                .get("homeostat_pressure_count")
                .map(String::as_str),
            Some("1")
        );
    }

    #[tokio::test]
    async fn resource_bargain_uses_system2_contention_pressure_to_constrain_tokens() {
        let (mut genome, coder_id, reviewer_id, _tester_id) = genome_with_coder_reviewer_tester();
        let root_id = genome.root_node_id.clone();
        genome
            .get_node_mut(&root_id)
            .expect("root mut")
            .system_3
            .default_task_budget_tokens = Some(1_000);

        let ledger = Arc::new(InMemoryLedger::new());
        let shared_genome = Arc::new(RwLock::new(genome));
        let transport = Arc::new(InMemoryTransport::new(16));
        let mut coder_stream = transport
            .subscribe(Subscription {
                channel_types: vec![
                    VsmChannelType::ResourceBargaining,
                    VsmChannelType::System2Coordination,
                ],
                target_node_id: Some(coder_id.to_string()),
                queue_name: None,
                durable: false,
            })
            .await
            .expect("subscribe coder");
        let controller = ControllerRuntime::new(
            root_id.clone(),
            shared_genome,
            transport,
            ledger.clone() as Arc<dyn Ledger>,
        );

        let affected_task_id = TaskId::new();
        let mut signal = System2CoordinationSignal::new(
            root_id.clone(),
            System2CoordinationKind::Contention,
            "implementation child is contending with review work on the same dependency",
        );
        signal.source_node_id = Some(reviewer_id);
        signal.target_node_id = Some(coder_id.clone());
        signal.affected_node_ids = vec![coder_id.clone()];
        signal.affected_task_ids = vec![affected_task_id];
        signal.severity = Some(8);
        let signal_envelope = MessageEnvelope::new(
            VsmChannelType::System2Coordination,
            BuiltinPayloadType::System2CoordinationSignal.as_str(),
            &signal,
        )
        .expect("system2 signal envelope")
        .with_route(Some(root_id.clone()), Some(root_id.clone()));
        controller
            .handle_envelope(signal_envelope)
            .await
            .expect("handle system2 contention");

        let dampening_envelope =
            tokio::time::timeout(std::time::Duration::from_millis(250), coder_stream.next())
                .await
                .expect("published dampening task")
                .expect("stream item")
                .expect("published dampening envelope");
        assert_eq!(
            dampening_envelope.channel_type,
            VsmChannelType::System2Coordination
        );

        let bargain = ResourceBargain {
            requested_by: coder_id.clone(),
            task_id: Some(TaskId::new()),
            proposed_task: None,
            requested_tokens: Some(800),
            requested_tool_permissions: vec![],
            requested_context_refs: vec![],
            justification: "Need implementation tokens while System 2 reports contention"
                .to_string(),
        };
        let bargain_envelope = MessageEnvelope::new(
            VsmChannelType::ResourceBargaining,
            BuiltinPayloadType::ResourceBargain.as_str(),
            &bargain,
        )
        .expect("resource bargain envelope")
        .with_route(Some(coder_id), Some(root_id));
        controller
            .handle_envelope(bargain_envelope)
            .await
            .expect("handle contention-aware resource bargain");

        let published =
            tokio::time::timeout(std::time::Duration::from_millis(250), coder_stream.next())
                .await
                .expect("published allocation decision")
                .expect("stream item")
                .expect("published decision envelope");
        assert_eq!(published.channel_type, VsmChannelType::ResourceBargaining);
        let decision: ResourceAllocationDecision =
            published.payload_as().expect("allocation decision");
        assert_eq!(decision.status, ResourceAllocationStatus::PartiallyApproved);
        assert_eq!(decision.approved_tokens, Some(200));
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason.contains("System 2 coordination pressure")));
        assert_eq!(
            published
                .metadata
                .get("coordination_pressure_count")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            published
                .metadata
                .get("coordination_max_severity")
                .map(String::as_str),
            Some("8")
        );

        let allocation_events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::Other(
                    "resource_allocation_decision".to_string(),
                )],
                task_id: bargain.task_id,
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("allocation events");
        assert_eq!(
            allocation_events[0]
                .metadata
                .get("coordination_contention_count")
                .map(String::as_str),
            Some("1")
        );
    }

    #[tokio::test]
    async fn resource_bargain_respects_system3_allocation_switch() {
        let (mut genome, _suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let child_id = genome
            .get_node(&root_id)
            .expect("root")
            .children
            .first()
            .cloned()
            .expect("child");
        genome
            .get_node_mut(&root_id)
            .expect("root mut")
            .system_3
            .can_allocate_budget = false;

        let ledger = Arc::new(InMemoryLedger::new());
        let shared_genome = Arc::new(RwLock::new(genome));
        let transport = Arc::new(InMemoryTransport::new(16));
        let mut child_stream = transport
            .subscribe(Subscription {
                channel_types: vec![VsmChannelType::ResourceBargaining],
                target_node_id: Some(child_id.to_string()),
                queue_name: None,
                durable: false,
            })
            .await
            .expect("subscribe child");
        let controller = ControllerRuntime::new(
            root_id.clone(),
            shared_genome,
            transport,
            ledger.clone() as Arc<dyn Ledger>,
        );
        let bargain = ResourceBargain {
            requested_by: child_id,
            task_id: Some(TaskId::new()),
            proposed_task: None,
            requested_tokens: Some(500),
            requested_tool_permissions: vec![],
            requested_context_refs: vec![],
            justification: "Need task budget".to_string(),
        };
        let envelope = MessageEnvelope::new(
            VsmChannelType::ResourceBargaining,
            BuiltinPayloadType::ResourceBargain.as_str(),
            &bargain,
        )
        .expect("resource bargain envelope")
        .with_route(None, Some(root_id));

        controller
            .handle_envelope(envelope)
            .await
            .expect("handle resource bargain");

        let published =
            tokio::time::timeout(std::time::Duration::from_millis(250), child_stream.next())
                .await
                .expect("published allocation decision")
                .expect("stream item")
                .expect("published envelope");
        let decision: ResourceAllocationDecision =
            published.payload_as().expect("allocation decision");
        assert_eq!(decision.status, ResourceAllocationStatus::Denied);
        assert_eq!(decision.approved_tokens, None);
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason.contains("disabled by genome policy")));
    }

    #[tokio::test]
    async fn audit_report_channel_creates_suggestion_and_queued_candidate() {
        let (genome, _suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let ledger = Arc::new(InMemoryLedger::new());
        let (controller, shared_genome) = controller_for(genome, ledger.clone());

        let audit_node_id = NodeId::new();
        let reviewer = ViableNode::new_leaf("audit-reviewer", LeafOperationSpec::reviewer());
        let reviewer_id = reviewer.id.clone();
        let report = vsm_core::AuditReport {
            target_node_id: root_id.clone(),
            findings: vec![vsm_core::AuditFinding {
                title: "Review bottleneck observed".to_string(),
                evidence: vec!["failed review handoff ratio exceeded threshold".to_string()],
                severity: 7,
                related_nodes: vec![root_id.clone()],
                related_tasks: vec![],
            }],
            suggested_patches: vec![OrganizationalGenomePatch::AddChild {
                parent_id: root_id.clone(),
                child: reviewer,
            }],
        };
        let envelope = MessageEnvelope::new(
            VsmChannelType::Audit,
            BuiltinPayloadType::AuditReport.as_str(),
            &report,
        )
        .expect("audit report envelope")
        .with_route(Some(audit_node_id.clone()), Some(root_id.clone()));

        let outcome = controller
            .handle_envelope(envelope)
            .await
            .expect("handle audit report");
        assert!(matches!(outcome, ControllerHandleOutcome::Ignored));
        assert!(!shared_genome.read().await.nodes.contains_key(&reviewer_id));

        let audit_events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::AuditCompleted],
                node_id: Some(root_id.clone()),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("audit events");
        assert_eq!(audit_events.len(), 1);
        assert_eq!(
            audit_events[0]
                .metadata
                .get("source_channel")
                .map(String::as_str),
            Some("Audit")
        );
        assert_eq!(
            audit_events[0]
                .metadata
                .get("suggestion_count")
                .map(String::as_str),
            Some("1")
        );

        let suggestion_events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::GeneSuggestionCreated],
                node_id: Some(root_id.clone()),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("suggestion events");
        assert_eq!(suggestion_events.len(), 1);
        assert_eq!(
            suggestion_events[0]
                .metadata
                .get("source_channel")
                .map(String::as_str),
            Some("Audit")
        );

        let queued = ledger
            .queued_trial_records(&root_id, 10)
            .await
            .expect("queued trials");
        assert_eq!(queued.len(), 1);
        assert_eq!(
            queued[0].suggestion.source,
            GeneSuggestionSource::System3StarAudit
        );
        assert_eq!(queued[0].suggestion.suggested_by_node_id, audit_node_id);
        assert!(queued[0].suggestion.safety_limits.requires_approval);
        assert!(queued[0]
            .suggestion
            .evidence
            .iter()
            .any(|evidence| evidence.contains("Review bottleneck observed")));
    }

    #[tokio::test]
    async fn three_four_homeostat_gene_suggestion_is_queued_not_applied() {
        let (genome, _suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let system4_id = NodeId::new();
        let ledger = Arc::new(InMemoryLedger::new());
        let (controller, shared_genome) = controller_for(genome, ledger.clone());

        let future_probe_leaf =
            ViableNode::new_leaf("future-probe-reviewer", LeafOperationSpec::reviewer());
        let future_probe_leaf_id = future_probe_leaf.id.clone();
        let mut suggestion = GeneSuggestion::new(
            system4_id.clone(),
            root_id.clone(),
            GeneSuggestionSource::System4FutureProbe,
            OrganizationalGenomePatch::AddChild {
                parent_id: root_id.clone(),
                child: future_probe_leaf,
            },
            "Future probe predicts review capacity pressure.",
        );
        suggestion
            .evidence
            .push("forecast=review_queue_growth".to_string());
        let suggestion_id = suggestion.id.clone();
        let envelope = MessageEnvelope::new(
            VsmChannelType::ThreeFourHomeostat,
            BuiltinPayloadType::GeneSuggestion.as_str(),
            &suggestion,
        )
        .expect("gene suggestion envelope")
        .with_route(Some(system4_id.clone()), Some(root_id.clone()));

        let outcome = controller
            .handle_envelope(envelope)
            .await
            .expect("handle gene suggestion");
        assert!(matches!(outcome, ControllerHandleOutcome::Ignored));
        assert!(!shared_genome
            .read()
            .await
            .nodes
            .contains_key(&future_probe_leaf_id));

        let suggestion_events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::GeneSuggestionCreated],
                node_id: Some(root_id.clone()),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("suggestion events");
        assert_eq!(suggestion_events.len(), 1);
        assert_eq!(
            suggestion_events[0].suggestion_id,
            Some(suggestion_id.clone())
        );
        assert_eq!(
            suggestion_events[0]
                .metadata
                .get("source_channel")
                .map(String::as_str),
            Some("ThreeFourHomeostat")
        );

        let queued = ledger
            .queued_trial_records(&root_id, 10)
            .await
            .expect("queued trials");
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].trial_id, suggestion_id);
        assert_eq!(
            queued[0].suggestion.source,
            GeneSuggestionSource::System4FutureProbe
        );
        assert!(queued[0]
            .suggestion
            .evidence
            .iter()
            .any(|evidence| evidence == "source_channel=ThreeFourHomeostat"));
    }

    #[tokio::test]
    async fn three_four_homeostat_signal_records_balance_and_queues_patches() {
        let (genome, _suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let system4_id = NodeId::new();
        let ledger = Arc::new(InMemoryLedger::new());
        let (controller, shared_genome) = controller_for(genome, ledger.clone());

        let future_probe_leaf =
            ViableNode::new_leaf("future-probe-tester", LeafOperationSpec::tester());
        let future_probe_leaf_id = future_probe_leaf.id.clone();
        let mut signal = ThreeFourHomeostatSignal::new(
            root_id.clone(),
            system4_id.clone(),
            root_id.clone(),
            ThreeFourHomeostatKind::FutureOpportunity,
            ThreeFourHomeostatBalance::FutureDominant,
            "Trial a tester leaf before the next dependency upgrade wave.",
        );
        signal.present_summary = "current child mix can ship present work".to_string();
        signal.future_summary =
            "future dependency churn is likely to need more test capacity".to_string();
        signal.severity = Some(5);
        signal
            .evidence
            .push("forecast=dependency_upgrade_wave".to_string());
        signal
            .suggested_patches
            .push(OrganizationalGenomePatch::AddChild {
                parent_id: root_id.clone(),
                child: future_probe_leaf,
            });
        let envelope = MessageEnvelope::new(
            VsmChannelType::ThreeFourHomeostat,
            BuiltinPayloadType::ThreeFourHomeostatSignal.as_str(),
            &signal,
        )
        .expect("homeostat envelope")
        .with_route(Some(system4_id.clone()), Some(root_id.clone()));

        let outcome = controller
            .handle_envelope(envelope)
            .await
            .expect("handle homeostat signal");
        assert!(matches!(outcome, ControllerHandleOutcome::Ignored));
        assert!(!shared_genome
            .read()
            .await
            .nodes
            .contains_key(&future_probe_leaf_id));

        let homeostat_events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::Other(
                    "three_four_homeostat_signal".to_string(),
                )],
                node_id: Some(root_id.clone()),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("homeostat events");
        assert_eq!(homeostat_events.len(), 1);
        assert_eq!(
            homeostat_events[0]
                .metadata
                .get("source_channel")
                .map(String::as_str),
            Some("ThreeFourHomeostat")
        );
        assert_eq!(
            homeostat_events[0].payload["kind"].as_str(),
            Some("FutureOpportunity")
        );
        assert_eq!(
            homeostat_events[0].payload["balance"].as_str(),
            Some("FutureDominant")
        );
        assert_eq!(
            homeostat_events[0].payload["suggested_patch_count"].as_u64(),
            Some(1)
        );

        let queued = ledger
            .queued_trial_records(&root_id, 10)
            .await
            .expect("queued trials");
        assert_eq!(queued.len(), 1);
        assert_eq!(
            queued[0].suggestion.source,
            GeneSuggestionSource::System4FutureProbe
        );
        assert_eq!(queued[0].suggestion.suggested_by_node_id, system4_id);
        assert_eq!(queued[0].suggestion.trial_mode, TrialMode::Canary);
        assert_eq!(
            queued[0]
                .suggestion
                .safety_limits
                .max_traffic_share_basis_points,
            Some(1_000)
        );
        assert!(!queued[0].suggestion.safety_limits.requires_approval);
        assert!(queued[0]
            .suggestion
            .evidence
            .iter()
            .any(|evidence| evidence == "three_four_balance=FutureDominant"));
    }

    #[tokio::test]
    async fn three_four_homeostat_pressure_adds_review_to_decomposition() {
        let (genome, _coder_id, reviewer_id, _tester_id) = genome_with_coder_reviewer_tester();
        let root_id = genome.root_node_id.clone();
        let system4_id = NodeId::new();
        let ledger = Arc::new(InMemoryLedger::new());
        let shared_genome = Arc::new(RwLock::new(genome));
        let transport = Arc::new(InMemoryTransport::new(16));
        let _keepalive = transport
            .subscribe(Subscription {
                channel_types: vec![],
                target_node_id: None,
                queue_name: None,
                durable: false,
            })
            .await
            .expect("subscribe");
        let controller = ControllerRuntime::new(
            root_id.clone(),
            shared_genome,
            transport,
            ledger.clone() as Arc<dyn Ledger>,
        );

        let mut signal = ThreeFourHomeostatSignal::new(
            root_id.clone(),
            system4_id.clone(),
            root_id.clone(),
            ThreeFourHomeostatKind::FutureRisk,
            ThreeFourHomeostatBalance::Conflict,
            "Preserve present safety by adding review to decomposed work.",
        );
        signal.present_summary = "present implementation path is capacity constrained".to_string();
        signal.future_summary = "future dependency churn raises regression risk".to_string();
        signal.severity = Some(8);
        let homeostat_envelope = MessageEnvelope::new(
            VsmChannelType::ThreeFourHomeostat,
            BuiltinPayloadType::ThreeFourHomeostatSignal.as_str(),
            &signal,
        )
        .expect("homeostat envelope")
        .with_route(Some(system4_id), Some(root_id.clone()));
        controller
            .handle_envelope(homeostat_envelope)
            .await
            .expect("record homeostat pressure");

        let mut directive = Directive::new(
            "user",
            "Implement pressure-aware change",
            "make the implementation and test it",
        );
        directive
            .metadata
            .insert("decompose".to_string(), "true".to_string());
        directive
            .metadata
            .insert("requires_tests".to_string(), "true".to_string());
        let directive_envelope = envelope_for_directive(&directive)
            .expect("directive envelope")
            .with_route(None, Some(root_id.clone()));

        let outcome = controller
            .handle_envelope(directive_envelope)
            .await
            .expect("handle decomposed directive");
        let ControllerHandleOutcome::DecomposedTasks { tasks } = outcome else {
            panic!("expected decomposed tasks");
        };
        assert_eq!(tasks.len(), 3);

        let review = tasks
            .iter()
            .find(|task| {
                task.task
                    .metadata
                    .get("decomposition_role")
                    .map(String::as_str)
                    == Some("review")
            })
            .expect("review task from homeostat pressure");
        assert_eq!(review.child_id, reviewer_id);
        assert_eq!(
            review
                .task
                .metadata
                .get("decomposition_pressure_policy")
                .map(String::as_str),
            Some("environment_three_four_pressure_v1")
        );
        assert_eq!(
            review
                .task
                .metadata
                .get("three_four_homeostat_count")
                .map(String::as_str),
            Some("1")
        );

        let routed = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::TaskRouted],
                task_id: Some(review.task.id.clone()),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("routed review events");
        assert_eq!(routed.len(), 1);
        assert_eq!(
            routed[0].payload["source_channel"].as_str(),
            Some("System2Coordination")
        );
    }

    #[tokio::test]
    async fn algedonic_pause_blocks_and_resume_reopens_subtree_routing() {
        let (genome, _suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let child_id = genome
            .get_node(&root_id)
            .expect("root")
            .children
            .first()
            .cloned()
            .expect("child");
        let ledger = Arc::new(InMemoryLedger::new());
        let shared_genome = Arc::new(RwLock::new(genome));
        let transport = Arc::new(InMemoryTransport::new(16));
        let mut child_stream = transport
            .subscribe(Subscription {
                channel_types: vec![VsmChannelType::ResourceBargaining],
                target_node_id: Some(child_id.to_string()),
                queue_name: None,
                durable: false,
            })
            .await
            .expect("subscribe child");
        let controller = ControllerRuntime::new(
            root_id.clone(),
            shared_genome,
            transport,
            ledger.clone() as Arc<dyn Ledger>,
        );

        let mut pause = vsm_core::AlgedonicSignal::pain(
            vsm_core::AlgedonicSource::User,
            9,
            "pause this child until the human confirms the failure is resolved",
        );
        pause.target_node_id = Some(child_id.clone());
        pause.override_policy = Some(vsm_core::AlgedonicOverridePolicy {
            pause_subtree: true,
            require_human_confirmation: true,
            ..vsm_core::AlgedonicOverridePolicy::default()
        });
        let pause_envelope = MessageEnvelope::new(
            VsmChannelType::Algedonic,
            BuiltinPayloadType::AlgedonicSignal.as_str(),
            &pause,
        )
        .expect("pause envelope")
        .with_route(None, Some(root_id.clone()));
        controller
            .handle_envelope(pause_envelope)
            .await
            .expect("handle pause");

        let blocked_task = TaskPacket::new("Blocked task", "should not route while paused");
        let blocked_task_id = blocked_task.id.clone();
        let blocked_envelope = envelope_for_task(&blocked_task)
            .expect("blocked task envelope")
            .with_route(None, Some(root_id.clone()));
        let blocked = controller.handle_envelope(blocked_envelope).await;
        assert!(matches!(
            blocked,
            Err(ControllerError::NoRouteableChild { .. })
        ));
        let blocked_events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::Other(
                    "algedonic_subtree_route_blocked".to_string(),
                )],
                task_id: Some(blocked_task_id),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("blocked events");
        assert_eq!(blocked_events.len(), 1);
        assert_eq!(
            blocked_events[0].payload["paused_target_node_id"].as_str(),
            Some(child_id.as_str())
        );

        let mut resume = vsm_core::AlgedonicSignal::pain(
            vsm_core::AlgedonicSource::User,
            3,
            "resume child after human confirmation",
        );
        resume.valence = vsm_core::AlgedonicValence::Pleasure;
        resume.target_node_id = Some(child_id.clone());
        resume.override_policy = Some(vsm_core::AlgedonicOverridePolicy {
            resume_subtree: true,
            require_human_confirmation: true,
            ..vsm_core::AlgedonicOverridePolicy::default()
        });
        let resume_envelope = MessageEnvelope::new(
            VsmChannelType::Algedonic,
            BuiltinPayloadType::AlgedonicSignal.as_str(),
            &resume,
        )
        .expect("resume envelope")
        .with_route(None, Some(root_id.clone()));
        controller
            .handle_envelope(resume_envelope)
            .await
            .expect("handle resume");

        let resumed_task = TaskPacket::new("Resumed task", "should route after resume");
        let resumed_task_id = resumed_task.id.clone();
        let resumed_envelope = envelope_for_task(&resumed_task)
            .expect("resumed task envelope")
            .with_route(None, Some(root_id));
        let routed = controller
            .handle_envelope(resumed_envelope)
            .await
            .expect("route after resume");
        assert!(matches!(routed, ControllerHandleOutcome::RoutedTask { .. }));
        let published =
            tokio::time::timeout(std::time::Duration::from_millis(250), child_stream.next())
                .await
                .expect("published resumed task")
                .expect("stream item")
                .expect("published envelope");
        assert_eq!(published.channel_type, VsmChannelType::ResourceBargaining);
        let routed_events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::TaskRouted],
                task_id: Some(resumed_task_id),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("routed events");
        assert_eq!(routed_events.len(), 1);
    }

    #[tokio::test]
    async fn algedonic_pause_denies_resource_bargain_for_target_child() {
        let (mut genome, _suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let child_id = genome
            .get_node(&root_id)
            .expect("root")
            .children
            .first()
            .cloned()
            .expect("child");
        genome
            .get_node_mut(&root_id)
            .expect("root mut")
            .system_3
            .default_task_budget_tokens = Some(1_000);

        let ledger = Arc::new(InMemoryLedger::new());
        let shared_genome = Arc::new(RwLock::new(genome));
        let transport = Arc::new(InMemoryTransport::new(16));
        let mut child_stream = transport
            .subscribe(Subscription {
                channel_types: vec![VsmChannelType::ResourceBargaining],
                target_node_id: Some(child_id.to_string()),
                queue_name: None,
                durable: false,
            })
            .await
            .expect("subscribe child");
        let controller = ControllerRuntime::new(
            root_id.clone(),
            shared_genome,
            transport,
            ledger.clone() as Arc<dyn Ledger>,
        );

        let mut pause = vsm_core::AlgedonicSignal::pain(
            vsm_core::AlgedonicSource::Ci,
            8,
            "child is causing repeated integration failures",
        );
        pause.target_node_id = Some(child_id.clone());
        pause.override_policy = Some(vsm_core::AlgedonicOverridePolicy {
            pause_subtree: true,
            ..vsm_core::AlgedonicOverridePolicy::default()
        });
        let pause_envelope = MessageEnvelope::new(
            VsmChannelType::Algedonic,
            BuiltinPayloadType::AlgedonicSignal.as_str(),
            &pause,
        )
        .expect("pause envelope")
        .with_route(None, Some(root_id.clone()));
        controller
            .handle_envelope(pause_envelope)
            .await
            .expect("handle pause");

        let bargain = ResourceBargain {
            requested_by: child_id.clone(),
            task_id: Some(TaskId::new()),
            proposed_task: None,
            requested_tokens: Some(500),
            requested_tool_permissions: vec![],
            requested_context_refs: vec![],
            justification: "request work while paused".to_string(),
        };
        let bargain_envelope = MessageEnvelope::new(
            VsmChannelType::ResourceBargaining,
            BuiltinPayloadType::ResourceBargain.as_str(),
            &bargain,
        )
        .expect("resource bargain envelope")
        .with_route(Some(child_id), Some(root_id));
        controller
            .handle_envelope(bargain_envelope)
            .await
            .expect("handle paused bargain");

        let published =
            tokio::time::timeout(std::time::Duration::from_millis(250), child_stream.next())
                .await
                .expect("published allocation decision")
                .expect("stream item")
                .expect("published decision envelope");
        let decision: ResourceAllocationDecision =
            published.payload_as().expect("allocation decision");
        assert_eq!(decision.status, ResourceAllocationStatus::Denied);
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason.contains("paused by algedonic signal")));
        assert_eq!(
            published
                .metadata
                .get("algedonic_subtree_paused")
                .map(String::as_str),
            Some("true")
        );
    }

    #[tokio::test]
    async fn algedonic_escalation_forwards_signal_to_parent_controller() {
        let root = ViableNode::new_metasystem("root");
        let root_id = root.id.clone();
        let mut genome = OrganizationalGenome::new(root);
        let backend = ViableNode::new_metasystem("backend-system");
        let backend_id = backend.id.clone();
        genome
            .add_child(&root_id, backend)
            .expect("backend metasystem");

        let ledger = Arc::new(InMemoryLedger::new());
        let shared_genome = Arc::new(RwLock::new(genome));
        let transport = Arc::new(InMemoryTransport::new(16));
        let mut root_stream = transport
            .subscribe(Subscription {
                channel_types: vec![VsmChannelType::Algedonic],
                target_node_id: Some(root_id.to_string()),
                queue_name: None,
                durable: false,
            })
            .await
            .expect("subscribe root");
        let controller = ControllerRuntime::new(
            backend_id.clone(),
            shared_genome,
            transport,
            ledger.clone() as Arc<dyn Ledger>,
        );

        let mut signal = vsm_core::AlgedonicSignal::pain(
            vsm_core::AlgedonicSource::ChildNode,
            9,
            "backend child reports severe pain that root must see",
        );
        signal.target_node_id = Some(backend_id.clone());
        signal.override_policy = Some(vsm_core::AlgedonicOverridePolicy {
            escalate_to_root: true,
            ..vsm_core::AlgedonicOverridePolicy::default()
        });
        let envelope = MessageEnvelope::new(
            VsmChannelType::Algedonic,
            BuiltinPayloadType::AlgedonicSignal.as_str(),
            &signal,
        )
        .expect("algedonic envelope")
        .with_route(None, Some(backend_id.clone()));

        controller
            .handle_envelope(envelope)
            .await
            .expect("handle escalated algedonic signal");

        let published =
            tokio::time::timeout(std::time::Duration::from_millis(250), root_stream.next())
                .await
                .expect("published escalated signal")
                .expect("stream item")
                .expect("published envelope");
        assert_eq!(published.channel_type, VsmChannelType::Algedonic);
        assert_eq!(published.priority, ChannelPriority::Interrupt);
        assert_eq!(
            published
                .metadata
                .get("algedonic_escalation")
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            published
                .metadata
                .get("escalated_by_node_id")
                .map(String::as_str),
            Some(backend_id.as_str())
        );

        let escalated_events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::Other("algedonic_escalated".to_string())],
                node_id: Some(backend_id),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("escalated events");
        assert_eq!(escalated_events.len(), 1);
        assert_eq!(
            escalated_events[0].payload["to_node_id"].as_str(),
            Some(root_id.as_str())
        );
    }

    #[tokio::test]
    async fn algedonic_freeze_rejects_related_queued_mutation() {
        let (genome, suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let suggestion_id = suggestion.id.clone();
        let ledger = Arc::new(InMemoryLedger::new());
        let (controller, _shared_genome) = controller_for(genome, ledger.clone());
        controller
            .queue_candidate_from_suggestion(suggestion)
            .await
            .expect("queue candidate");

        let mut signal = vsm_core::AlgedonicSignal::pain(
            vsm_core::AlgedonicSource::Ci,
            9,
            "candidate caused a severe CI failure",
        );
        signal.target_node_id = Some(root_id.clone());
        signal.related_suggestion_id = Some(suggestion_id.clone());
        signal.override_policy = Some(vsm_core::AlgedonicOverridePolicy {
            freeze_mutation: true,
            require_human_confirmation: true,
            ..vsm_core::AlgedonicOverridePolicy::default()
        });
        let envelope = MessageEnvelope::new(
            VsmChannelType::Algedonic,
            BuiltinPayloadType::AlgedonicSignal.as_str(),
            &signal,
        )
        .expect("algedonic envelope")
        .with_route(None, Some(root_id.clone()));

        let outcome = controller
            .handle_envelope(envelope)
            .await
            .expect("handle algedonic signal");
        assert!(matches!(outcome, ControllerHandleOutcome::Ignored));

        let record = ledger
            .get_trial_record(&suggestion_id)
            .await
            .expect("trial record")
            .expect("queued record");
        assert_eq!(record.status, StoredTrialStatus::Rejected);
        assert!(record
            .metadata
            .get("rejection_reason")
            .is_some_and(|reason| reason.contains("algedonic freeze_mutation")));

        let override_events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::Other(
                    "algedonic_override_applied".to_string(),
                )],
                node_id: Some(root_id.clone()),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("override events");
        assert_eq!(override_events.len(), 1);
        assert_eq!(
            override_events[0].payload["actions"][0].as_str(),
            Some("freeze_mutation")
        );

        let rejected_events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::TrialRejected],
                node_id: Some(root_id),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("rejected events");
        assert_eq!(rejected_events.len(), 1);
        assert_eq!(
            rejected_events[0].payload["reason"].as_str(),
            Some("algedonic freeze_mutation")
        );
    }

    #[tokio::test]
    async fn algedonic_freeze_stops_related_active_mutation_trial() {
        let (genome, suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let suggestion_id = suggestion.id.clone();
        let ledger = Arc::new(InMemoryLedger::new());
        let (controller, _shared_genome) = controller_for(genome, ledger.clone());
        controller
            .start_trial_from_suggestion(suggestion)
            .await
            .expect("start active trial");
        assert!(controller.active_candidate_genome().await.is_some());

        let mut signal = vsm_core::AlgedonicSignal::pain(
            vsm_core::AlgedonicSource::User,
            10,
            "active trial is causing unacceptable operational risk",
        );
        signal.target_node_id = Some(root_id.clone());
        signal.related_suggestion_id = Some(suggestion_id.clone());
        signal.override_policy = Some(vsm_core::AlgedonicOverridePolicy {
            freeze_mutation: true,
            require_human_confirmation: true,
            ..vsm_core::AlgedonicOverridePolicy::default()
        });
        let envelope = MessageEnvelope::new(
            VsmChannelType::Algedonic,
            BuiltinPayloadType::AlgedonicSignal.as_str(),
            &signal,
        )
        .expect("algedonic envelope")
        .with_route(None, Some(root_id.clone()));

        let outcome = controller
            .handle_envelope(envelope)
            .await
            .expect("handle active algedonic freeze");
        assert!(matches!(outcome, ControllerHandleOutcome::Ignored));
        assert!(controller.active_candidate_genome().await.is_none());
        assert!(ledger
            .get_active_trial_record(&root_id)
            .await
            .expect("active record query")
            .is_none());

        let record = ledger
            .get_trial_record(&suggestion_id)
            .await
            .expect("trial record")
            .expect("frozen record");
        assert_eq!(record.status, StoredTrialStatus::Frozen);
        assert!(record
            .metadata
            .get("freeze_reason")
            .is_some_and(|reason| reason.contains("algedonic freeze_mutation")));

        let frozen_events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::TrialFrozen],
                node_id: Some(root_id.clone()),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("frozen events");
        assert_eq!(frozen_events.len(), 1);
        assert_eq!(
            frozen_events[0].payload["trial_id"].as_str(),
            Some(suggestion_id.as_str())
        );
        assert_eq!(
            frozen_events[0].payload["reason"].as_str(),
            Some("algedonic freeze_mutation")
        );

        let override_events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::Other(
                    "algedonic_override_applied".to_string(),
                )],
                node_id: Some(root_id),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("override events");
        let expected_action = format!("active_trial_frozen={suggestion_id}");
        assert!(override_events[0].payload["actions"]
            .as_array()
            .expect("actions")
            .iter()
            .any(|action| action.as_str() == Some(expected_action.as_str())));
    }

    #[tokio::test]
    async fn queued_candidate_is_not_active_until_started() {
        let (genome, suggestion, reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let ledger = Arc::new(InMemoryLedger::new());
        let (controller, _shared_genome) = controller_for(genome, ledger.clone());

        let candidate_genome_id = controller
            .queue_candidate_from_suggestion(suggestion.clone())
            .await
            .expect("queue candidate");

        assert!(controller.active_candidate_genome().await.is_none());
        let queued = ledger
            .queued_trial_records(&root_id, 10)
            .await
            .expect("queued records");
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].trial_id, suggestion.id);
        assert_eq!(queued[0].status, StoredTrialStatus::Queued);

        let started = controller
            .start_next_queued_trial()
            .await
            .expect("start queued candidate");
        assert_eq!(started, Some(candidate_genome_id.clone()));

        let active = controller
            .active_candidate_genome()
            .await
            .expect("active candidate");
        assert_eq!(active.id, candidate_genome_id);
        assert!(active.nodes.contains_key(&reviewer_id));

        let active_record = ledger
            .get_active_trial_record(&root_id)
            .await
            .expect("active record query")
            .expect("active record");
        assert_eq!(active_record.trial_id, suggestion.id);
        assert_eq!(active_record.status, StoredTrialStatus::Active);
        assert!(ledger
            .queued_trial_records(&root_id, 10)
            .await
            .expect("queued records")
            .is_empty());
    }

    #[tokio::test]
    async fn queued_activation_selects_best_scored_candidate_not_fifo() {
        let (genome, mut low_suggestion, _reviewer_id) = genome_and_review_suggestion();
        low_suggestion.trial_mode = TrialMode::Direct;
        low_suggestion.source = GeneSuggestionSource::Other("manual".to_string());
        let root_id = genome.root_node_id.clone();
        let ledger = Arc::new(InMemoryLedger::new());
        let (controller, _shared_genome) = controller_for(genome, ledger.clone());

        let low_candidate = controller
            .queue_candidate_from_suggestion(low_suggestion)
            .await
            .expect("queue low candidate");

        let reviewer = ViableNode::new_leaf("high-scored-reviewer", LeafOperationSpec::reviewer());
        let mut high_suggestion = GeneSuggestion::new(
            root_id.clone(),
            root_id.clone(),
            GeneSuggestionSource::AlgedonicSignal,
            OrganizationalGenomePatch::AddChild {
                parent_id: root_id.clone(),
                child: reviewer,
            },
            "high scored candidate",
        );
        high_suggestion.trial_mode = TrialMode::Canary;
        high_suggestion.safety_limits.max_tasks = Some(10);
        high_suggestion.safety_limits.max_traffic_share_basis_points = Some(500);
        high_suggestion.evidence.push("pain signal".to_string());
        let high_candidate = controller
            .queue_candidate_from_suggestion(high_suggestion)
            .await
            .expect("queue high candidate");

        let started = controller
            .start_next_queued_trial()
            .await
            .expect("start best candidate")
            .expect("candidate started");

        assert_eq!(started, high_candidate);
        assert_ne!(started, low_candidate);

        let active = ledger
            .get_active_trial_record(&root_id)
            .await
            .expect("active record")
            .expect("active trial");
        assert_eq!(active.candidate_genome_id, high_candidate);
        assert_eq!(
            active.metadata.get("selection_policy").map(String::as_str),
            Some("pareto_empirical_candidate_score_v1")
        );
        assert!(active.metadata.contains_key("selection_score"));
        assert!(active.metadata.contains_key("pareto_frontier_size"));
        assert!(active.metadata.contains_key("candidate_objectives"));
    }

    #[tokio::test]
    async fn queued_activation_filters_dominated_candidate_before_score_tie_break() {
        let (genome, mut dominated_suggestion, _reviewer_id) = genome_and_review_suggestion();
        dominated_suggestion.trial_mode = TrialMode::Direct;
        dominated_suggestion.source = GeneSuggestionSource::Other("manual".to_string());
        let dominated_suggestion_id = dominated_suggestion.id.clone();
        let root_id = genome.root_node_id.clone();
        let ledger = Arc::new(InMemoryLedger::new());
        let (controller, _shared_genome) = controller_for(genome, ledger.clone());

        let dominated_candidate = controller
            .queue_candidate_from_suggestion(dominated_suggestion)
            .await
            .expect("queue dominated candidate");
        let mut dominated_record = ledger
            .get_trial_record(&dominated_suggestion_id)
            .await
            .expect("load dominated record")
            .expect("dominated record");
        dominated_record
            .metadata
            .insert("selection_priority".to_string(), "1000".to_string());
        ledger
            .write_trial_record(dominated_record)
            .await
            .expect("boost dominated candidate");

        let reviewer = ViableNode::new_leaf("dominant-reviewer", LeafOperationSpec::reviewer());
        let mut dominant_suggestion = GeneSuggestion::new(
            root_id.clone(),
            root_id.clone(),
            GeneSuggestionSource::AlgedonicSignal,
            OrganizationalGenomePatch::AddChild {
                parent_id: root_id.clone(),
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
        let dominant_candidate = controller
            .queue_candidate_from_suggestion(dominant_suggestion)
            .await
            .expect("queue dominant candidate");

        let started = controller
            .start_next_queued_trial()
            .await
            .expect("start best candidate")
            .expect("candidate started");

        assert_eq!(started, dominant_candidate);
        assert_ne!(started, dominated_candidate);
        let active = ledger
            .get_active_trial_record(&root_id)
            .await
            .expect("active record")
            .expect("active trial");
        assert_eq!(
            active
                .metadata
                .get("pareto_frontier_size")
                .map(String::as_str),
            Some("1")
        );
    }

    #[tokio::test]
    async fn queued_activation_persists_population_archive_frontier() {
        let (genome, mut dominated_suggestion, _reviewer_id) = genome_and_review_suggestion();
        dominated_suggestion.trial_mode = TrialMode::Direct;
        dominated_suggestion.source = GeneSuggestionSource::Other("manual".to_string());
        let root_id = genome.root_node_id.clone();
        let ledger = Arc::new(InMemoryLedger::new());
        let (controller, _shared_genome) = controller_for(genome, ledger.clone());

        let dominated_candidate = controller
            .queue_candidate_from_suggestion(dominated_suggestion)
            .await
            .expect("queue dominated candidate");

        let reviewer = ViableNode::new_leaf("archive-reviewer", LeafOperationSpec::reviewer());
        let mut dominant_suggestion = GeneSuggestion::new(
            root_id.clone(),
            root_id.clone(),
            GeneSuggestionSource::AlgedonicSignal,
            OrganizationalGenomePatch::AddChild {
                parent_id: root_id.clone(),
                child: reviewer,
            },
            "archive dominant canary",
        );
        dominant_suggestion.trial_mode = TrialMode::Canary;
        dominant_suggestion.safety_limits.max_tasks = Some(10);
        dominant_suggestion.safety_limits.max_token_budget = Some(5_000);
        dominant_suggestion
            .safety_limits
            .max_traffic_share_basis_points = Some(500);
        dominant_suggestion.evidence.push("pain signal".to_string());
        let dominant_candidate = controller
            .queue_candidate_from_suggestion(dominant_suggestion)
            .await
            .expect("queue dominant candidate");

        let started = controller
            .start_next_queued_trial()
            .await
            .expect("start best candidate")
            .expect("candidate started");
        assert_eq!(started, dominant_candidate);

        let population = ledger
            .population_archive_records(&root_id, 10)
            .await
            .expect("population archive");
        assert_eq!(population.len(), 2);
        let selected = population
            .iter()
            .find(|record| record.candidate_genome_id == dominant_candidate)
            .expect("selected archive");
        assert_eq!(selected.status, PopulationArchiveStatus::SelectedForTrial);
        assert_eq!(selected.pareto_frontier_size, 1);
        assert!(selected.selection_score > 0.0);
        assert!(selected.objectives.safety > 0.0);

        let dominated = population
            .iter()
            .find(|record| record.candidate_genome_id == dominated_candidate)
            .expect("dominated archive");
        assert_eq!(dominated.status, PopulationArchiveStatus::Dominated);

        let pareto = ledger
            .pareto_archive_records(&root_id, 10)
            .await
            .expect("pareto archive");
        assert_eq!(pareto.len(), 1);
        assert_eq!(pareto[0].candidate_genome_id, dominant_candidate);
    }

    #[tokio::test]
    async fn queued_activation_records_historical_replay_summary() {
        let (genome, mut suggestion, _reviewer_id) = genome_and_review_suggestion();
        suggestion.trial_mode = TrialMode::Canary;
        suggestion.safety_limits.max_tasks = Some(10);
        suggestion.safety_limits.max_traffic_share_basis_points = Some(10_000);
        let root_id = genome.root_node_id.clone();
        let genome_id = genome.id.clone();
        let ledger = Arc::new(InMemoryLedger::new());
        let (controller, _shared_genome) = controller_for(genome, ledger.clone());

        let candidate = controller
            .queue_candidate_from_suggestion(suggestion)
            .await
            .expect("queue candidate");

        let mut trace = TaskTrace::started(TaskId::new(), genome_id, vsm_core::NodeId::new());
        trace.metadata.insert(
            "task_metadata.required_capability".to_string(),
            "review".to_string(),
        );
        trace.files_touched.push("src/lib.rs".to_string());
        trace.outcome_score = -1.0;
        trace.merged = Some(false);
        ledger.write_task_trace(trace).await.expect("write trace");

        let started = controller
            .start_next_queued_trial()
            .await
            .expect("start candidate")
            .expect("candidate started");
        assert_eq!(started, candidate);

        let active = ledger
            .get_active_trial_record(&root_id)
            .await
            .expect("active record")
            .expect("active trial");
        assert_eq!(
            active
                .metadata
                .get("replay_affected_route_count")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            active
                .metadata
                .get("offline_replay_version")
                .map(String::as_str),
            Some(OFFLINE_REPLAY_VERSION)
        );
        assert!(active
            .metadata
            .get("candidate_objectives")
            .is_some_and(|value| value.contains("replay_fit=")));
        assert!(active
            .metadata
            .get("replay_score")
            .and_then(|value| value.parse::<f64>().ok())
            .is_some_and(|score| score > 0.0));
        assert!(active
            .metadata
            .get("replay_estimated_delta_score")
            .and_then(|value| value.parse::<f64>().ok())
            .is_some_and(|score| score > 0.0));
        assert!(active
            .metadata
            .get("replay_trace_evaluations")
            .is_some_and(|value| value.contains("AffectedRoute")));
    }

    #[tokio::test]
    async fn queued_activation_uses_future_probe_environment_opportunity_pressure() {
        let (genome, _suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let ledger = Arc::new(InMemoryLedger::new());
        let (controller, _shared_genome) = controller_for(genome, ledger.clone());

        let audit_reviewer =
            ViableNode::new_leaf("audit-pressure-reviewer", LeafOperationSpec::reviewer());
        let mut audit_suggestion = GeneSuggestion::new(
            root_id.clone(),
            root_id.clone(),
            GeneSuggestionSource::System3StarAudit,
            OrganizationalGenomePatch::AddChild {
                parent_id: root_id.clone(),
                child: audit_reviewer,
            },
            "audit pressure reviewer",
        );
        audit_suggestion.trial_mode = TrialMode::Canary;
        audit_suggestion.safety_limits.max_tasks = Some(10);
        audit_suggestion
            .safety_limits
            .max_traffic_share_basis_points = Some(500);
        let audit_candidate = controller
            .queue_candidate_from_suggestion(audit_suggestion)
            .await
            .expect("queue audit candidate");

        let future_reviewer =
            ViableNode::new_leaf("future-pressure-reviewer", LeafOperationSpec::reviewer());
        let mut future_suggestion = GeneSuggestion::new(
            root_id.clone(),
            root_id.clone(),
            GeneSuggestionSource::System4FutureProbe,
            OrganizationalGenomePatch::AddChild {
                parent_id: root_id.clone(),
                child: future_reviewer,
            },
            "future probe opportunity reviewer",
        );
        future_suggestion.trial_mode = TrialMode::Canary;
        future_suggestion.safety_limits.max_tasks = Some(10);
        future_suggestion
            .safety_limits
            .max_traffic_share_basis_points = Some(500);
        let future_candidate = controller
            .queue_candidate_from_suggestion(future_suggestion)
            .await
            .expect("queue future candidate");

        for summary in [
            "New model-provider adapter can reduce worker integration cost.",
            "Future probe sees upcoming review queue pressure.",
        ] {
            let mut signal = EnvironmentSignal::new(
                vsm_core::EnvironmentSignalKind::Opportunity,
                "future-probe",
                summary,
            );
            signal.observed_by_node_id = Some(NodeId::new());
            signal.target_node_id = Some(root_id.clone());
            signal.severity = Some(10);
            let envelope = MessageEnvelope::new(
                VsmChannelType::FutureProbeToEnvironment,
                BuiltinPayloadType::EnvironmentSignal.as_str(),
                &signal,
            )
            .expect("future signal envelope")
            .with_route(signal.observed_by_node_id.clone(), Some(root_id.clone()));
            controller
                .handle_envelope(envelope)
                .await
                .expect("record future pressure");
        }

        let started = controller
            .start_next_queued_trial()
            .await
            .expect("start queued")
            .expect("candidate started");

        assert_eq!(started, future_candidate);
        assert_ne!(started, audit_candidate);
        let active = ledger
            .get_active_trial_record(&root_id)
            .await
            .expect("active record")
            .expect("active trial");
        assert_eq!(
            active
                .metadata
                .get("environment_signal_count")
                .map(String::as_str),
            Some("2")
        );
        assert!(active
            .metadata
            .get("selection_reasons")
            .is_some_and(|reasons| reasons.contains("environment_opportunity_pressure")));
        assert!(active
            .metadata
            .get("candidate_objectives")
            .is_some_and(|objectives| objectives.contains("expected_value=")));

        let archive = ledger
            .population_archive_records(&root_id, 10)
            .await
            .expect("population archive");
        let selected = archive
            .iter()
            .find(|record| record.candidate_genome_id == future_candidate)
            .expect("selected archive");
        assert_eq!(
            selected
                .metadata
                .get("environment_signal_count")
                .map(String::as_str),
            Some("2")
        );
    }

    #[tokio::test]
    async fn queued_activation_records_environment_risk_pressure() {
        let (genome, mut suggestion, _reviewer_id) = genome_and_review_suggestion();
        suggestion.trial_mode = TrialMode::Canary;
        suggestion.safety_limits.max_tasks = Some(10);
        suggestion.safety_limits.max_token_budget = Some(5_000);
        let root_id = genome.root_node_id.clone();
        let ledger = Arc::new(InMemoryLedger::new());
        let (controller, _shared_genome) = controller_for(genome, ledger.clone());
        let candidate = controller
            .queue_candidate_from_suggestion(suggestion)
            .await
            .expect("queue candidate");

        let mut signal = EnvironmentSignal::new(
            vsm_core::EnvironmentSignalKind::Risk,
            "package-registry",
            "Registry and CI availability conflict raises delivery risk.",
        );
        signal.target_environment = Some("ci".to_string());
        signal.target_node_id = Some(root_id.clone());
        signal.severity = Some(9);
        let envelope = MessageEnvelope::new(
            VsmChannelType::EnvironmentToEnvironment,
            BuiltinPayloadType::EnvironmentSignal.as_str(),
            &signal,
        )
        .expect("risk envelope")
        .with_route(None, Some(root_id.clone()));
        controller
            .handle_envelope(envelope)
            .await
            .expect("record risk pressure");

        let started = controller
            .start_next_queued_trial()
            .await
            .expect("start queued")
            .expect("candidate started");
        assert_eq!(started, candidate);

        let active = ledger
            .get_active_trial_record(&root_id)
            .await
            .expect("active record")
            .expect("active trial");
        assert_eq!(
            active
                .metadata
                .get("environment_signal_count")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            active
                .metadata
                .get("environment_max_severity")
                .map(String::as_str),
            Some("9")
        );
        assert!(active
            .metadata
            .get("selection_reasons")
            .is_some_and(|reasons| reasons.contains("environment_risk_pressure")));
        assert!(active
            .metadata
            .get("selection_reasons")
            .is_some_and(|reasons| reasons.contains("environment_bounded_safety_bonus")));
    }

    #[tokio::test]
    async fn queued_activation_uses_completed_trial_history() {
        let (genome, mut historically_good_suggestion, _reviewer_id) =
            genome_and_review_suggestion();
        historically_good_suggestion.trial_mode = TrialMode::Canary;
        historically_good_suggestion.source = GeneSuggestionSource::System3StarAudit;
        let root_id = genome.root_node_id.clone();
        let genome_id = genome.id.clone();
        let ledger = Arc::new(InMemoryLedger::new());
        let (controller, _shared_genome) = controller_for(genome, ledger.clone());

        let historically_good_candidate = controller
            .queue_candidate_from_suggestion(historically_good_suggestion.clone())
            .await
            .expect("queue history-matching candidate");

        let reviewer = ViableNode::new_leaf("urgent-reviewer", LeafOperationSpec::reviewer());
        let mut urgent_suggestion = GeneSuggestion::new(
            root_id.clone(),
            root_id.clone(),
            GeneSuggestionSource::AlgedonicSignal,
            OrganizationalGenomePatch::AddChild {
                parent_id: root_id.clone(),
                child: reviewer,
            },
            "urgent but no matching history",
        );
        urgent_suggestion.trial_mode = TrialMode::Direct;
        urgent_suggestion.safety_limits.max_tasks = Some(10);
        let urgent_candidate = controller
            .queue_candidate_from_suggestion(urgent_suggestion)
            .await
            .expect("queue urgent candidate");

        let historical_reviewer =
            ViableNode::new_leaf("historical-reviewer", LeafOperationSpec::reviewer());
        let mut historical_suggestion = GeneSuggestion::new(
            root_id.clone(),
            root_id.clone(),
            GeneSuggestionSource::System3StarAudit,
            OrganizationalGenomePatch::AddChild {
                parent_id: root_id.clone(),
                child: historical_reviewer,
            },
            "historical good canary",
        );
        historical_suggestion.trial_mode = TrialMode::Canary;
        let mut historical_record = StoredTrialRecord::active(
            root_id.clone(),
            genome_id,
            GenomeId::new(),
            historical_suggestion,
        );
        historical_record.status = StoredTrialStatus::Promoted;
        historical_record.trace_count = 2;
        historical_record.total_score = 100.0;
        historical_record.completed_at = Some(chrono::Utc::now());
        ledger
            .write_trial_record(historical_record)
            .await
            .expect("historical trial");

        let started = controller
            .start_next_queued_trial()
            .await
            .expect("start best candidate")
            .expect("candidate started");

        assert_eq!(started, historically_good_candidate);
        assert_ne!(started, urgent_candidate);
        let active = ledger
            .get_active_trial_record(&root_id)
            .await
            .expect("active record")
            .expect("active trial");
        assert!(active
            .metadata
            .get("selection_reasons")
            .is_some_and(|reasons| reasons.contains("history_same_source")));
    }

    #[tokio::test]
    async fn stale_queued_candidate_is_rejected_when_champion_moves() {
        let (genome, suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let ledger = Arc::new(InMemoryLedger::new());
        let (controller, shared_genome) = controller_for(genome, ledger.clone());

        controller
            .queue_candidate_from_suggestion(suggestion.clone())
            .await
            .expect("queue candidate");

        shared_genome.write().await.id = GenomeId::new();

        let started = controller
            .start_next_queued_trial()
            .await
            .expect("start next queued");
        assert!(started.is_none());
        assert!(controller.active_candidate_genome().await.is_none());

        let record = ledger
            .get_trial_record(&suggestion.id)
            .await
            .expect("trial record")
            .expect("record exists");
        assert_eq!(record.status, StoredTrialStatus::Rejected);
        assert!(record
            .metadata
            .get("rejection_reason")
            .is_some_and(|reason| reason.starts_with("base genome superseded")));

        let rejected_events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::TrialRejected],
                node_id: Some(root_id),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("rejected events");
        assert_eq!(rejected_events.len(), 1);
    }

    #[tokio::test]
    async fn durable_active_trial_blocks_queued_activation_after_restart() {
        let (genome, suggestion, _reviewer_id) = genome_and_review_suggestion();
        let root_id = genome.root_node_id.clone();
        let champion_id = genome.id.clone();
        let ledger = Arc::new(InMemoryLedger::new());
        let (controller, _shared_genome) = controller_for(genome, ledger.clone());

        controller
            .queue_candidate_from_suggestion(suggestion)
            .await
            .expect("queue candidate");

        let reviewer = ViableNode::new_leaf("active-reviewer", LeafOperationSpec::reviewer());
        let active_suggestion = GeneSuggestion::new(
            root_id.clone(),
            root_id.clone(),
            GeneSuggestionSource::System3StarAudit,
            OrganizationalGenomePatch::AddChild {
                parent_id: root_id,
                child: reviewer,
            },
            "already active elsewhere",
        );
        let active_trial_id = active_suggestion.id.clone();
        ledger
            .write_trial_record(StoredTrialRecord::active(
                controller.node_id.clone(),
                champion_id,
                GenomeId::new(),
                active_suggestion,
            ))
            .await
            .expect("write durable active");

        let err = controller
            .start_next_queued_trial()
            .await
            .expect_err("durable active trial should block activation");
        match err {
            ControllerError::TrialAlreadyActive(suggestion_id) => {
                assert_eq!(suggestion_id, active_trial_id);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn shadow_trial_publishes_candidate_copy_after_champion_route() {
        let (genome, mut suggestion, reviewer_id) = genome_and_review_suggestion();
        suggestion.trial_mode = TrialMode::Shadow;
        let suggestion_id = suggestion.id.clone();
        let root_id = genome.root_node_id.clone();
        let ledger = Arc::new(InMemoryLedger::new());
        let shared_genome = Arc::new(RwLock::new(genome));
        let transport = Arc::new(InMemoryTransport::new(16));
        let _keepalive = transport
            .subscribe(Subscription {
                channel_types: vec![],
                target_node_id: None,
                queue_name: None,
                durable: false,
            })
            .await
            .expect("subscribe");
        let controller = ControllerRuntime::new(
            root_id.clone(),
            shared_genome,
            transport,
            ledger.clone() as Arc<dyn Ledger>,
        );

        controller
            .start_trial_from_suggestion(suggestion)
            .await
            .expect("start shadow trial");
        controller
            .register_trial_worker(reviewer_id.clone())
            .await
            .expect("register shadow worker");

        let mut task = TaskPacket::new("shadow task", "publish primary and shadow work");
        task.metadata
            .insert("target_child".to_string(), "reviewer".to_string());
        task.metadata
            .insert("trial_approved".to_string(), suggestion_id.to_string());

        let routed = controller.route_task(task, None).await.expect("route task");
        assert_ne!(routed.child_id, reviewer_id);

        let task_events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::TaskRouted],
                node_id: Some(root_id.clone()),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("task route events");
        assert_eq!(task_events.len(), 2);
        assert!(task_events
            .iter()
            .any(|event| event.payload["is_shadow_route"] == false));

        let shadow_event = task_events
            .iter()
            .find(|event| event.payload["is_shadow_route"] == true)
            .expect("shadow route event");
        assert_eq!(
            shadow_event.payload["child_node_id"].as_str(),
            Some(reviewer_id.as_str())
        );
        assert_eq!(shadow_event.payload["trial_mode"].as_str(), Some("shadow"));
        assert_eq!(
            shadow_event.payload["trial_route_role"].as_str(),
            Some("shadow")
        );

        let trial_events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::TrialTaskRouted],
                node_id: Some(root_id.clone()),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("trial route events");
        assert_eq!(trial_events.len(), 1);
        assert_eq!(trial_events[0].payload["is_shadow_route"], true);

        let record = ledger
            .get_active_trial_record(&root_id)
            .await
            .expect("active record")
            .expect("active trial");
        assert_eq!(record.routed_tasks, 1);
    }

    #[tokio::test]
    async fn shadow_trial_result_is_not_returned_as_controlling_result() {
        let (genome, _suggestion, reviewer_id) = genome_and_review_suggestion();
        let ledger = Arc::new(InMemoryLedger::new());
        let (controller, _shared_genome) = controller_for(genome, ledger.clone());

        let mut result = TaskResult::completed(vsm_core::TaskId::new(), reviewer_id, "shadow ok");
        result
            .metadata
            .insert("trial_shadow".to_string(), "true".to_string());
        result
            .metadata
            .insert("trial_route_role".to_string(), "shadow".to_string());

        let envelope = vsm_core::envelope_for_task_result(&result)
            .expect("result envelope")
            .with_route(None, Some(controller.node_id.clone()));
        let outcome = controller
            .handle_envelope(envelope)
            .await
            .expect("handle result");
        assert!(matches!(outcome, ControllerHandleOutcome::Ignored));

        let events = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::TaskResultReceived],
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("result events");
        assert_eq!(events.len(), 1);
    }
}
