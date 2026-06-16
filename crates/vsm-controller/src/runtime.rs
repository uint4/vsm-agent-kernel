use crate::{
    compare_queued_candidate_evaluations, evaluate_queued_candidate_with_replay,
    pareto_frontier_indices, replay_candidate_against_traces, tag_task_for_trial_route,
    trial_decision_key, ControllerError, DirectiveTaskMapper, System3StarAuditor, TaskRouter,
    TrialManager, OFFLINE_REPLAY_VERSION,
};
use chrono::Utc;
use futures_util::StreamExt;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tokio::sync::RwLock;
use vsm_core::{
    envelope_for_task, AuditReport, AuditRequest, BuiltinPayloadType, ChannelPriority,
    Command as VsmCommand, Directive, EnvironmentSignal, FitnessWeights, GeneSuggestion,
    GeneSuggestionSource, MessageEnvelope, NodeId, OrganizationalGenome,
    ResourceAllocationDecision, ResourceAllocationStatus, ResourceBargain, RiskClass, Subscription,
    TaskPacket, TaskResult, TaskTrace, Transport, VsmChannelType,
};
use vsm_ledger::{
    CandidateObjectiveSnapshot, EventFilter, GenomeSnapshot, GenomeSnapshotRole, Ledger,
    LedgerEvent, LedgerEventKind, PopulationArchiveRecord, PopulationArchiveStatus,
    StoredTrialRecord, TraceWindow,
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
    ReceivedTaskResult(TaskResult),
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
            let evaluation =
                apply_environment_pressure(&record, evaluation, &environment_pressure);
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

    async fn recent_environment_pressure(&self) -> Result<EnvironmentPressureSummary, ControllerError> {
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
                Ok(ControllerHandleOutcome::ReceivedTaskResult(result))
            }
            payload if payload == BuiltinPayloadType::AlgedonicSignal.as_str() => {
                let signal: vsm_core::AlgedonicSignal = envelope.payload_as()?;
                self.handle_algedonic_signal(signal, &envelope).await?;
                Ok(ControllerHandleOutcome::Ignored)
            }
            _ => Ok(ControllerHandleOutcome::Ignored),
        }
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

    async fn handle_resource_bargain(
        &self,
        bargain: ResourceBargain,
        envelope: &MessageEnvelope,
    ) -> Result<(), ControllerError> {
        let decision = {
            let genome = self.genome.read().await;
            allocate_resource_bargain(&genome, &self.node_id, &bargain)
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

        if self.config.append_message_events {
            self.ledger
                .append_event(LedgerEvent::for_message(
                    LedgerEventKind::MessagePublished,
                    &response,
                )?)
                .await?;
        }
        self.transport.publish(response).await?;
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
            if let Some(suggestion_id) = signal.related_suggestion_id.clone() {
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
                    } else {
                        actions.push(format!(
                            "freeze_mutation_recorded_for_nonqueued_trial={}",
                            suggestion_id
                        ));
                    }
                } else {
                    actions.push(format!("freeze_mutation_no_trial_record={suggestion_id}"));
                }
            } else {
                actions.push("freeze_mutation_no_related_suggestion".to_string());
            }
        }
        if policy.pause_subtree {
            actions.push("pause_subtree_requested".to_string());
        }
        if policy.escalate_to_root {
            actions.push("escalate_to_root_requested".to_string());
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
    management_kind: Option<String>,
    management_channel: Option<String>,
    management_source_node_id: Option<String>,
    management_target_node_id: Option<String>,
    management_via_controller_node_id: Option<String>,
    management_correlation_id: Option<String>,
    management_causation_id: Option<String>,
    management_envelope_id: Option<String>,
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
    task.risk = if command.legal_or_policy_basis.is_some() {
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
    task
}

fn allocate_resource_bargain(
    genome: &OrganizationalGenome,
    controller_node_id: &NodeId,
    bargain: &ResourceBargain,
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

    if let Some(requested_tokens) = bargain.requested_tokens {
        let token_cap = resource_token_cap(genome, parent, requester);
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
    let denied_count = denied_tool_permissions.len()
        + denied_context_refs.len()
        + usize::from(bargain.requested_tokens.is_some() && approved_tokens.is_none());

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

fn resource_token_cap(
    genome: &OrganizationalGenome,
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
    for channel in &genome.channels {
        if channel.channel_type != VsmChannelType::ResourceBargaining {
            continue;
        }
        let connects_parent_child =
            channel.from.as_ref() == Some(&parent.id) && channel.to.as_ref() == Some(&requester.id);
        let connects_child_parent =
            channel.from.as_ref() == Some(&requester.id) && channel.to.as_ref() == Some(&parent.id);
        if connects_parent_child || connects_child_parent {
            if let Some(cap) = channel.max_token_budget_per_epoch {
                caps.push(cap);
            }
        }
    }
    caps.into_iter().min()
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
        System5Policy, TaskId, TaskPacket, TaskTrace, Transport, TrialMode, ViableNode,
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

        let mapped = ledger
            .recent_events(EventFilter {
                kinds: vec![LedgerEventKind::TaskMapped],
                task_id: Some(task.id),
                limit: Some(10),
                ..EventFilter::default()
            })
            .await
            .expect("mapped events");
        assert_eq!(
            mapped[0].payload["decomposition_policy"].as_str(),
            Some("system3_command_channel")
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
