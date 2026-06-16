use crate::{ControllerError, DirectiveTaskMapper, RoutingDecision, System3StarAuditor, TaskRouter};
use futures_util::StreamExt;
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::RwLock;
use vsm_core::{
    envelope_for_task, AuditReport, BuiltinPayloadType, ChannelPriority, Directive, MessageEnvelope, NodeId,
    OrganizationalGenome, Subscription, TaskPacket, TaskResult, Transport, VsmChannelType,
};
use vsm_ledger::{EventFilter, Ledger, LedgerEvent, LedgerEventKind, TraceWindow};

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
            config: ControllerConfig::default(),
        }
    }

    pub fn with_config(mut self, config: ControllerConfig) -> Self {
        self.config = config;
        self
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

    pub async fn run_until_results(&self, max_results: usize) -> Result<Vec<TaskResult>, ControllerError> {
        let mut stream = self.subscribe().await?;
        self.append_lifecycle_event(LedgerEventKind::ControllerStarted)
            .await?;

        let mut results = Vec::new();
        while results.len() < max_results {
            let Some(next) = stream.next().await else {
                break;
            };
            let envelope = next?;
            if let ControllerHandleOutcome::ReceivedTaskResult(result) = self.handle_envelope(envelope).await? {
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
    pub async fn run_system_3_star_audit<A>(&self, auditor: &A) -> Result<AuditReport, ControllerError>
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
                Ok(ControllerHandleOutcome::ReceivedTaskResult(result))
            }
            payload if payload == BuiltinPayloadType::AlgedonicSignal.as_str() => {
                let event = LedgerEvent::for_message(LedgerEventKind::AlgedonicSignalReceived, &envelope)?;
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

        let event = LedgerEvent::new(
            LedgerEventKind::TaskRouted,
            &TaskRoutedPayload {
                parent_node_id: self.node_id.clone(),
                child_node_id: decision.child_id.clone(),
                task_id: task.id.clone(),
                task_title: task.title.clone(),
                reason: decision.reason.clone(),
                parent_name: parent.name.clone(),
            },
        )?
        .with_genome(genome_id)
        .with_node(self.node_id.clone())
        .with_task(task.id.clone());
        self.ledger.append_event(event).await?;

        let mut envelope = envelope_for_task(&task)?.with_route(
            Some(self.node_id.clone()),
            Some(decision.child_id.clone()),
        );
        envelope.channel_type = self.config.publish_channel.clone();
        envelope.priority = task_priority(&task);
        envelope.correlation_id = incoming
            .and_then(|incoming| incoming.correlation_id.clone())
            .or_else(|| incoming.map(|incoming| incoming.id.to_string()))
            .or_else(|| Some(task.id.to_string()));
        envelope.causation_id = incoming.map(|incoming| incoming.id.clone());
        envelope.trace = incoming.map(|incoming| incoming.trace.clone()).unwrap_or_default();
        envelope.trace.push(self.node_id.clone());
        envelope.metadata = routed_envelope_metadata(&decision);

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
            child_id: decision.child_id,
            reason: decision.reason,
        })
    }

    async fn append_lifecycle_event(&self, kind: LedgerEventKind) -> Result<(), ControllerError> {
        let genome_id = { self.genome.read().await.id.clone() };
        let event = LedgerEvent::new(kind, serde_json::json!({
            "node_id": self.node_id.to_string(),
            "role": "controller"
        }))?
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
}

fn task_priority(task: &TaskPacket) -> ChannelPriority {
    match &task.risk {
        vsm_core::RiskClass::Low => ChannelPriority::Normal,
        vsm_core::RiskClass::Medium => ChannelPriority::Normal,
        vsm_core::RiskClass::High => ChannelPriority::High,
        vsm_core::RiskClass::Critical => ChannelPriority::Critical,
    }
}

fn routed_envelope_metadata(decision: &RoutingDecision) -> BTreeMap<String, String> {
    let mut metadata = BTreeMap::new();
    metadata.insert("routing_reason".to_string(), decision.reason.clone());
    metadata
}
