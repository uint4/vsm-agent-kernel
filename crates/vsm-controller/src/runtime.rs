use crate::{
    tag_task_for_trial, trial_decision_key, ControllerError, DirectiveTaskMapper,
    System3StarAuditor, TaskRouter, TrialManager,
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
    TraceWindow,
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
            tag_task_for_trial(
                &mut task,
                &trial_route.suggestion_id,
                &trial_route.genome_id,
            );

            self.publish_routed_task(
                task,
                incoming,
                trial_route.child_id,
                trial_route.reason,
                trial_route.genome_id,
                true,
                Some(trial_route.suggestion_id),
            )
            .await
        } else {
            self.log_trial_fallback_if_approved(&task).await?;

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
            self.publish_routed_task(task, incoming, child_id, reason, genome_id, false, None)
                .await
        }
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
                suggestion_id: suggestion_id.as_ref().map(ToString::to_string),
                routed_genome_id: genome_id.to_string(),
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
    suggestion_id: Option<String>,
    routed_genome_id: String,
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
