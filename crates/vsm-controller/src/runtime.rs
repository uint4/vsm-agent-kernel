use crate::{
    compare_queued_candidate_evaluations, evaluate_queued_candidate_with_replay,
    pareto_frontier_indices, replay_candidate_against_traces, tag_task_for_trial_route,
    trial_decision_key, ControllerError, DirectiveTaskMapper, System3StarAuditor, TaskRouter,
    TrialManager,
};
use futures_util::StreamExt;
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::RwLock;
use vsm_core::{
    envelope_for_task, AuditReport, BuiltinPayloadType, ChannelPriority, Directive, FitnessWeights,
    GeneSuggestion, MessageEnvelope, NodeId, OrganizationalGenome, Subscription, TaskPacket,
    TaskResult, TaskTrace, Transport, VsmChannelType,
};
use vsm_ledger::{
    EventFilter, GenomeSnapshot, GenomeSnapshotRole, Ledger, LedgerEvent, LedgerEventKind,
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
                VsmChannelType::ManagementToOperation,
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
            selectable.push((record, candidate_snapshot, evaluation, replay));
        }

        let evaluations = selectable
            .iter()
            .map(|(_, _, evaluation, _)| evaluation.clone())
            .collect::<Vec<_>>();
        let frontier_indices = pareto_frontier_indices(&evaluations);
        let frontier_size = frontier_indices.len();
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
            record.metadata.insert(
                "replay_trace_count".to_string(),
                replay.trace_count.to_string(),
            );
            record.metadata.insert(
                "replay_eligible_trace_count".to_string(),
                replay.eligible_trace_count.to_string(),
            );
            record.metadata.insert(
                "replay_candidate_route_count".to_string(),
                replay.candidate_route_count.to_string(),
            );
            record.metadata.insert(
                "replay_affected_route_count".to_string(),
                replay.affected_route_count.to_string(),
            );
            record.metadata.insert(
                "replay_score".to_string(),
                format!("{:.3}", replay.replay_score),
            );
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
                            "historical_replay": {
                                "trace_count": replay.trace_count,
                                "eligible_trace_count": replay.eligible_trace_count,
                                "candidate_route_count": replay.candidate_route_count,
                                "changed_route_count": replay.changed_route_count,
                                "affected_route_count": replay.affected_route_count,
                                "replay_score": replay.replay_score,
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

                let route = self.route_task(task, Some(&envelope)).await?;
                Ok(ControllerHandleOutcome::RoutedTask {
                    task: route.task,
                    child_id: route.child_id,
                    reason: route.reason,
                })
            }
            payload if payload == BuiltinPayloadType::TaskPacket.as_str() => {
                let task: TaskPacket = envelope.payload_as()?;
                let route = self.route_task(task, Some(&envelope)).await?;
                Ok(ControllerHandleOutcome::RoutedTask {
                    task: route.task,
                    child_id: route.child_id,
                    reason: route.reason,
                })
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
                let event =
                    LedgerEvent::for_message(LedgerEventKind::AlgedonicSignalReceived, &envelope)?;
                self.ledger.append_event(event).await?;
                Ok(ControllerHandleOutcome::Ignored)
            }
            _ => Ok(ControllerHandleOutcome::Ignored),
        }
    }

    async fn route_task(
        &self,
        mut task: TaskPacket,
        incoming: Option<&MessageEnvelope>,
    ) -> Result<RoutedTask, ControllerError> {
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
        task: TaskPacket,
        incoming: Option<&MessageEnvelope>,
        child_id: NodeId,
        reason: String,
        genome_id: vsm_core::GenomeId,
        is_trial_route: bool,
        suggestion_id: Option<vsm_core::SuggestionId>,
        is_shadow_route: bool,
    ) -> Result<RoutedTask, ControllerError> {
        let parent_name = {
            let genome = self.genome.read().await;
            genome
                .get_node(&self.node_id)
                .map(|node| node.name.clone())
                .unwrap_or_else(|_| "unknown".to_string())
        };

        let event = LedgerEvent::new(
            LedgerEventKind::TaskRouted,
            &TaskRoutedPayload {
                parent_node_id: self.node_id.clone(),
                child_node_id: child_id.clone(),
                task_id: task.id.clone(),
                task_title: task.title.clone(),
                reason: reason.clone(),
                parent_name,
                is_trial_route,
                is_shadow_route,
                suggestion_id: suggestion_id.as_ref().map(ToString::to_string),
                routed_genome_id: genome_id.to_string(),
                trial_mode: task.metadata.get("trial_mode").cloned(),
                trial_exposure_basis_points: task
                    .metadata
                    .get("trial_exposure_basis_points")
                    .cloned(),
                trial_exposure_bucket: task.metadata.get("trial_exposure_bucket").cloned(),
                trial_route_role: task.metadata.get("trial_route_role").cloned(),
            },
        )?
        .with_genome(genome_id.clone())
        .with_node(self.node_id.clone())
        .with_task(task.id.clone());
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
        envelope.channel_type = self.config.publish_channel.clone();
        envelope.priority = task_priority(&task);
        envelope.correlation_id = incoming
            .and_then(|incoming| incoming.correlation_id.clone())
            .or_else(|| incoming.map(|incoming| incoming.id.to_string()))
            .or_else(|| Some(task.id.to_string()));
        envelope.causation_id = incoming.map(|incoming| incoming.id.clone());
        envelope.trace = incoming
            .map(|incoming| incoming.trace.clone())
            .unwrap_or_default();
        envelope.trace.push(self.node_id.clone());
        envelope.metadata = routed_envelope_metadata(&reason);

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
    reason: String,
    parent_name: String,
    is_trial_route: bool,
    is_shadow_route: bool,
    suggestion_id: Option<String>,
    routed_genome_id: String,
    trial_mode: Option<String>,
    trial_exposure_basis_points: Option<String>,
    trial_exposure_bucket: Option<String>,
    trial_route_role: Option<String>,
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

fn task_priority(task: &TaskPacket) -> ChannelPriority {
    match &task.risk {
        vsm_core::RiskClass::Low => ChannelPriority::Normal,
        vsm_core::RiskClass::Medium => ChannelPriority::Normal,
        vsm_core::RiskClass::High => ChannelPriority::High,
        vsm_core::RiskClass::Critical => ChannelPriority::Critical,
    }
}

fn routed_envelope_metadata(reason: &str) -> BTreeMap<String, String> {
    let mut metadata = BTreeMap::new();
    metadata.insert("routing_reason".to_string(), reason.to_string());
    metadata
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use vsm_core::{
        GeneSuggestionSource, GenomeId, LeafOperationSpec, OrganizationalGenome,
        OrganizationalGenomePatch, Subscription, System5Policy, TaskId, TaskTrace, Transport,
        TrialMode, ViableNode,
    };
    use vsm_ledger::{InMemoryLedger, StoredTrialRecord, StoredTrialStatus};
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
        assert!(active
            .metadata
            .get("candidate_objectives")
            .is_some_and(|value| value.contains("replay_fit=")));
        assert!(active
            .metadata
            .get("replay_score")
            .and_then(|value| value.parse::<f64>().ok())
            .is_some_and(|score| score > 0.0));
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
