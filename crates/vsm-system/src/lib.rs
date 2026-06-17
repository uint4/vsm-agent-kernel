//! Local end-to-end orchestration for a VSM agent system.
//!
//! This crate composes the controller, worker harness, transport, and ledger
//! crates into a runnable local system surface. It is meant for deterministic
//! end-to-end runs and integration tests; production transports/providers can
//! use the same lower-level controller and worker APIs.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use thiserror::Error;
use tokio::sync::RwLock;
use vsm_controller::{
    ControllerRuntime, EvolutionPolicy, RuleBasedSystem3StarAuditor, System3StarAuditor,
};
use vsm_core::{
    envelope_for_directive, Directive, FitnessWeights, GenomeId, LeafOperationSpec, NodeId,
    OrganizationalGenome, Subscription, TaskResult, TaskTrace, Transport, ViableNode,
    VsmChannelType,
};
use vsm_ledger::{
    EvolutionGenerationRecord, Ledger, LedgerError, LedgerEvent, SqliteLedger, TraceWindow,
};
use vsm_runtime::{InMemoryTransport, TrialConfig};
use vsm_worker::{LedgerTraceSink, ModelProvider, WorkerError, WorkerHarness};

pub type SharedGenome = Arc<RwLock<OrganizationalGenome>>;

#[derive(Clone, Debug)]
pub struct LocalSystemConfig {
    pub transport_buffer: usize,
    pub subscription_settle: Duration,
    pub directive_timeout: Duration,
    pub trial_config: TrialConfig,
    pub fitness_weights: FitnessWeights,
}

impl Default for LocalSystemConfig {
    fn default() -> Self {
        Self {
            transport_buffer: 512,
            subscription_settle: Duration::from_millis(50),
            directive_timeout: Duration::from_secs(10),
            trial_config: TrialConfig {
                min_tasks_before_decision: 1,
                promote_margin: 0.1,
                prune_below: -5.0,
            },
            fitness_weights: FitnessWeights::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct DirectiveRunReport {
    pub results: Vec<TaskResult>,
    pub traces: Vec<TaskTrace>,
    pub events: Vec<LedgerEvent>,
}

pub struct LocalVsmSystem {
    root_node_id: NodeId,
    genome: SharedGenome,
    transport: Arc<InMemoryTransport>,
    ledger: Arc<dyn Ledger>,
    controller: Arc<ControllerRuntime>,
    default_provider: Arc<dyn ModelProvider>,
    providers_by_node: Arc<RwLock<BTreeMap<NodeId, Arc<dyn ModelProvider>>>>,
    config: LocalSystemConfig,
}

impl LocalVsmSystem {
    pub async fn with_in_memory_sqlite_ledger(
        genome: OrganizationalGenome,
        default_provider: Arc<dyn ModelProvider>,
    ) -> Result<Self, SystemRunError> {
        Self::new(
            genome,
            default_provider,
            Arc::new(SqliteLedger::in_memory().await?),
            LocalSystemConfig::default(),
        )
    }

    pub fn new(
        genome: OrganizationalGenome,
        default_provider: Arc<dyn ModelProvider>,
        ledger: Arc<dyn Ledger>,
        config: LocalSystemConfig,
    ) -> Result<Self, SystemRunError> {
        let root_node_id = genome.root_node_id.clone();
        let genome = Arc::new(RwLock::new(genome));
        let transport = Arc::new(InMemoryTransport::new(config.transport_buffer));
        let transport_for_controller: Arc<dyn Transport> = transport.clone();
        let controller = ControllerRuntime::new(
            root_node_id.clone(),
            genome.clone(),
            transport_for_controller,
            ledger.clone(),
        )
        .with_trial_config(config.trial_config.clone(), config.fitness_weights.clone());

        Ok(Self {
            root_node_id,
            genome,
            transport,
            ledger,
            controller: Arc::new(controller),
            default_provider,
            providers_by_node: Arc::new(RwLock::new(BTreeMap::new())),
            config,
        })
    }

    pub fn single_coder_genome() -> OrganizationalGenome {
        let root = ViableNode::new_metasystem("root-codebase-system");
        let root_id = root.id.clone();
        let mut genome = OrganizationalGenome::new(root);
        let mut coder = ViableNode::new_leaf("primary-code-service", LeafOperationSpec::coding());
        coder.system_5.identity = "General coding leaf under the root VSM.".to_string();
        coder.model.provider = "local".to_string();
        coder.model.model = "local-worker".to_string();
        genome
            .add_child(&root_id, coder)
            .expect("fresh root should accept primary coder child");
        genome
    }

    pub fn nested_coder_genome() -> OrganizationalGenome {
        let root = ViableNode::new_metasystem("root-codebase-system");
        let root_id = root.id.clone();
        let mut genome = OrganizationalGenome::new(root);

        let mut backend = ViableNode::new_metasystem("backend-system");
        backend.system_5.identity = "Backend viable subsystem.".to_string();
        let backend_id = genome
            .add_child(&root_id, backend)
            .expect("fresh root should accept backend subsystem");

        let mut coder = ViableNode::new_leaf("backend-coder", LeafOperationSpec::coding());
        coder.system_5.identity = "Backend coding leaf.".to_string();
        coder.model.provider = "local".to_string();
        coder.model.model = "local-worker".to_string();
        genome
            .add_child(&backend_id, coder)
            .expect("backend subsystem should accept coder child");

        genome
    }

    pub fn root_node_id(&self) -> NodeId {
        self.root_node_id.clone()
    }

    pub fn shared_genome(&self) -> SharedGenome {
        self.genome.clone()
    }

    pub fn ledger(&self) -> Arc<dyn Ledger> {
        self.ledger.clone()
    }

    pub async fn set_provider_for_node(&self, node_id: NodeId, provider: Arc<dyn ModelProvider>) {
        self.providers_by_node
            .write()
            .await
            .insert(node_id, provider);
    }

    pub async fn run_directive(
        &self,
        directive: Directive,
    ) -> Result<DirectiveRunReport, SystemRunError> {
        self.run_directive_for_results(directive, 1).await
    }

    pub async fn run_directive_for_results(
        &self,
        directive: Directive,
        expected_results: usize,
    ) -> Result<DirectiveRunReport, SystemRunError> {
        let mut controllers = Vec::new();
        for controller in self.current_child_controllers().await? {
            controllers.push(tokio::spawn(async move { controller.run_forever().await }));
        }

        let mut workers = Vec::new();
        for worker in self.current_worker_harnesses().await? {
            workers.push(tokio::spawn(async move { worker.run_forever().await }));
        }

        let controller = self.controller.clone();
        let controller_task =
            tokio::spawn(async move { controller.run_until_results(expected_results).await });
        tokio::time::sleep(self.config.subscription_settle).await;

        let envelope =
            envelope_for_directive(&directive)?.with_route(None, Some(self.root_node_id.clone()));
        self.transport.publish(envelope).await?;

        let results = tokio::time::timeout(self.config.directive_timeout, controller_task)
            .await
            .map_err(|_| SystemRunError::Timeout("controller result wait timed out".to_string()))?
            .map_err(|err| SystemRunError::Join(err.to_string()))??;

        for worker in workers {
            worker.abort();
            let _ = worker.await;
        }
        for controller in controllers {
            controller.abort();
            let _ = controller.await;
        }

        let traces = self
            .ledger
            .recent_task_traces(TraceWindow::default())
            .await?;
        let events = self
            .ledger
            .recent_events(vsm_ledger::EventFilter::default())
            .await?;

        Ok(DirectiveRunReport {
            results,
            traces,
            events,
        })
    }

    pub async fn run_system_3_star_audit<A>(
        &self,
        auditor: &A,
    ) -> Result<vsm_core::AuditReport, SystemRunError>
    where
        A: System3StarAuditor,
    {
        Ok(self.controller.run_system_3_star_audit(auditor).await?)
    }

    pub async fn run_default_system_3_star_audit(
        &self,
    ) -> Result<vsm_core::AuditReport, SystemRunError> {
        self.run_system_3_star_audit(&RuleBasedSystem3StarAuditor::default())
            .await
    }

    pub async fn run_evolution_generation(
        &self,
        policy: EvolutionPolicy,
    ) -> Result<Option<EvolutionGenerationRecord>, SystemRunError> {
        Ok(self.controller.run_evolution_generation(policy).await?)
    }

    pub async fn start_next_trial_and_register_candidate_workers(
        &self,
    ) -> Result<Option<GenomeId>, SystemRunError> {
        let Some(candidate_genome_id) = self.controller.start_next_queued_trial().await? else {
            return Ok(None);
        };
        let Some(candidate_genome) = self.controller.active_candidate_genome().await else {
            return Ok(Some(candidate_genome_id));
        };
        let champion = self.genome.read().await.clone();
        let candidate_leaf_ids =
            candidate_only_direct_leaf_ids(&champion, &candidate_genome, &self.root_node_id)?;
        for node_id in candidate_leaf_ids {
            self.controller
                .register_trial_worker(node_id.clone())
                .await?;
            self.providers_by_node
                .write()
                .await
                .entry(node_id)
                .or_insert_with(|| self.default_provider.clone());
        }
        Ok(Some(candidate_genome_id))
    }

    async fn current_worker_harnesses(&self) -> Result<Vec<WorkerHarness>, SystemRunError> {
        let champion = self.genome.read().await.clone();
        let mut workers = worker_harnesses_for_genome(
            &champion,
            self.genome.clone(),
            self.transport.clone(),
            self.ledger.clone(),
            self.default_provider.clone(),
            self.providers_by_node.clone(),
        )
        .await?;

        if let Some(candidate_genome) = self.controller.active_candidate_genome().await {
            let candidate_leaf_ids =
                candidate_only_direct_leaf_ids(&champion, &candidate_genome, &self.root_node_id)?;
            if !candidate_leaf_ids.is_empty() {
                let candidate_genome = Arc::new(RwLock::new(candidate_genome));
                let candidate_workers = worker_harnesses_for_leaf_ids(
                    &candidate_leaf_ids,
                    candidate_genome,
                    self.transport.clone(),
                    self.ledger.clone(),
                    self.default_provider.clone(),
                    self.providers_by_node.clone(),
                )
                .await;
                workers.extend(candidate_workers);
            }
        }

        Ok(workers)
    }

    async fn current_child_controllers(&self) -> Result<Vec<ControllerRuntime>, SystemRunError> {
        let champion = self.genome.read().await.clone();
        let metasystem_ids = child_metasystem_node_ids(&champion, &self.root_node_id);
        let mut controllers = Vec::new();
        for node_id in metasystem_ids {
            let transport: Arc<dyn Transport> = self.transport.clone();
            controllers.push(
                ControllerRuntime::new(
                    node_id,
                    self.genome.clone(),
                    transport,
                    self.ledger.clone(),
                )
                .with_trial_config(
                    self.config.trial_config.clone(),
                    self.config.fitness_weights.clone(),
                ),
            );
        }
        Ok(controllers)
    }
}

async fn worker_harnesses_for_genome(
    genome: &OrganizationalGenome,
    shared_genome: SharedGenome,
    transport: Arc<InMemoryTransport>,
    ledger: Arc<dyn Ledger>,
    default_provider: Arc<dyn ModelProvider>,
    providers_by_node: Arc<RwLock<BTreeMap<NodeId, Arc<dyn ModelProvider>>>>,
) -> Result<Vec<WorkerHarness>, SystemRunError> {
    let node_ids = genome
        .nodes
        .values()
        .filter(|child| child.is_leaf() && child.status != vsm_core::NodeLifecycleStatus::Retired)
        .map(|child| child.id.clone())
        .collect::<Vec<_>>();
    Ok(worker_harnesses_for_leaf_ids(
        &node_ids,
        shared_genome,
        transport,
        ledger,
        default_provider,
        providers_by_node,
    )
    .await)
}

async fn worker_harnesses_for_leaf_ids(
    node_ids: &[NodeId],
    shared_genome: SharedGenome,
    transport: Arc<InMemoryTransport>,
    ledger: Arc<dyn Ledger>,
    default_provider: Arc<dyn ModelProvider>,
    providers_by_node: Arc<RwLock<BTreeMap<NodeId, Arc<dyn ModelProvider>>>>,
) -> Vec<WorkerHarness> {
    let providers = providers_by_node.read().await;
    node_ids
        .iter()
        .map(|node_id| {
            let provider = providers
                .get(node_id)
                .cloned()
                .unwrap_or_else(|| default_provider.clone());
            let transport: Arc<dyn Transport> = transport.clone();
            WorkerHarness::new(
                node_id.clone(),
                shared_genome.clone(),
                transport,
                provider,
                Arc::new(LedgerTraceSink::new(ledger.clone())),
            )
        })
        .collect()
}

fn candidate_only_direct_leaf_ids(
    champion: &OrganizationalGenome,
    candidate: &OrganizationalGenome,
    parent_node_id: &NodeId,
) -> Result<Vec<NodeId>, SystemRunError> {
    let candidate_parent = candidate.get_node(parent_node_id)?;
    let mut ids = Vec::new();
    for child_id in &candidate_parent.children {
        if champion.nodes.contains_key(child_id) {
            continue;
        }
        let child = candidate.get_node(child_id)?;
        if child.is_leaf() && child.status != vsm_core::NodeLifecycleStatus::Retired {
            ids.push(child.id.clone());
        }
    }
    Ok(ids)
}

fn child_metasystem_node_ids(genome: &OrganizationalGenome, root_node_id: &NodeId) -> Vec<NodeId> {
    genome
        .nodes
        .values()
        .filter(|node| {
            &node.id != root_node_id
                && node.is_metasystem()
                && node.status != vsm_core::NodeLifecycleStatus::Retired
        })
        .map(|node| node.id.clone())
        .collect()
}

#[derive(Debug, Error)]
pub enum SystemRunError {
    #[error(transparent)]
    Controller(#[from] vsm_controller::ControllerError),

    #[error(transparent)]
    Worker(#[from] WorkerError),

    #[error(transparent)]
    Ledger(#[from] LedgerError),

    #[error(transparent)]
    Transport(#[from] vsm_core::TransportError),

    #[error(transparent)]
    Serialization(#[from] serde_json::Error),

    #[error(transparent)]
    Genome(#[from] vsm_core::GenomeError),

    #[error("system run timed out: {0}")]
    Timeout(String),

    #[error("system task join failed: {0}")]
    Join(String),
}

#[allow(dead_code)]
fn _subscription_type_reference(_subscription: Subscription, _channel: VsmChannelType) {}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use vsm_controller::suggestion_operator;
    use vsm_core::{TaskId, TaskTrace};
    use vsm_worker::{
        EchoModelProvider, ModelProviderError, ModelRequest, ModelResponse, ModelUsage,
    };

    use super::*;

    #[tokio::test]
    async fn local_system_runs_directive_through_controller_worker_and_ledger() {
        let system = LocalVsmSystem::with_in_memory_sqlite_ledger(
            LocalVsmSystem::single_coder_genome(),
            Arc::new(EchoModelProvider::default()),
        )
        .await
        .expect("system");

        let mut directive = Directive::new(
            "user",
            "Implement local orchestration",
            "Route this task through the controller and primary worker.",
        );
        directive
            .metadata
            .insert("requires_code_write".to_string(), "true".to_string());

        let report = system
            .run_directive(directive)
            .await
            .expect("directive run");

        assert_eq!(report.results.len(), 1);
        assert_eq!(report.traces.len(), 1);
        assert_eq!(
            report.traces[0].assigned_node_id,
            system
                .shared_genome()
                .read()
                .await
                .get_node(&system.root_node_id())
                .expect("root")
                .children[0]
        );
        assert!(report
            .events
            .iter()
            .any(|event| event.kind == vsm_ledger::LedgerEventKind::TaskRouted));
        assert!(report
            .events
            .iter()
            .any(|event| event.kind == vsm_ledger::LedgerEventKind::TraceWritten));
    }

    #[tokio::test]
    async fn local_system_runs_nested_metasystem_controller_path() {
        let system = LocalVsmSystem::with_in_memory_sqlite_ledger(
            LocalVsmSystem::nested_coder_genome(),
            Arc::new(EchoModelProvider::default()),
        )
        .await
        .expect("system");

        let mut directive = Directive::new(
            "user",
            "Implement backend change",
            "Route through a backend metasystem before reaching a coding leaf.",
        );
        directive
            .metadata
            .insert("requires_code_write".to_string(), "true".to_string());

        let report = system
            .run_directive(directive)
            .await
            .expect("nested directive run");

        assert_eq!(report.results.len(), 1);
        assert_eq!(report.traces.len(), 1);

        let genome = system.shared_genome().read().await.clone();
        let root_id = genome.root_node_id.clone();
        let backend_id = genome
            .get_node(&root_id)
            .expect("root")
            .children
            .first()
            .expect("backend")
            .clone();
        let coder_id = genome
            .get_node(&backend_id)
            .expect("backend")
            .children
            .first()
            .expect("coder")
            .clone();

        assert_eq!(report.traces[0].assigned_node_id, coder_id);
        assert!(report.traces[0]
            .responsible_ancestor_ids
            .iter()
            .any(|ancestor| ancestor == &backend_id));
        assert!(report.traces[0]
            .responsible_ancestor_ids
            .iter()
            .any(|ancestor| ancestor == &root_id));

        let routed_parent_ids = report
            .events
            .iter()
            .filter(|event| event.kind == vsm_ledger::LedgerEventKind::TaskRouted)
            .filter_map(|event| event.payload["parent_node_id"].as_str())
            .collect::<Vec<_>>();
        assert!(routed_parent_ids.contains(&root_id.as_str()));
        assert!(routed_parent_ids.contains(&backend_id.as_str()));

        let root_results = report
            .events
            .iter()
            .filter(|event| event.kind == vsm_ledger::LedgerEventKind::TaskResultReceived)
            .filter(|event| event.node_id.as_ref() == Some(&root_id))
            .count();
        assert_eq!(root_results, 1);
    }

    #[tokio::test]
    async fn local_system_runs_audit_suggested_trial_and_promotion() {
        let system = LocalVsmSystem::with_in_memory_sqlite_ledger(
            LocalVsmSystem::single_coder_genome(),
            Arc::new(NodeSelectiveProvider),
        )
        .await
        .expect("system");

        let mut failing = Directive::new(
            "user",
            "Force a coding failure",
            "The primary coding leaf should fail so System 3 sees pressure.",
        );
        failing
            .metadata
            .insert("requires_code_write".to_string(), "true".to_string());
        let failed_report = system
            .run_directive(failing)
            .await
            .expect("failed directive still returns result");
        assert_eq!(failed_report.results.len(), 1);
        assert_eq!(failed_report.traces[0].merged, Some(false));

        let auditor = RuleBasedSystem3StarAuditor {
            min_traces_for_review_suggestion: 1,
            failed_task_ratio_threshold: 0.1,
        };
        let report = system
            .run_system_3_star_audit(&auditor)
            .await
            .expect("audit report");
        assert_eq!(report.suggested_patches.len(), 1);

        let candidate_genome_id = system
            .start_next_trial_and_register_candidate_workers()
            .await
            .expect("start trial")
            .expect("candidate genome");
        let active = system
            .ledger()
            .get_active_trial_record(&system.root_node_id())
            .await
            .expect("active trial")
            .expect("active trial exists");
        let trial_id = active.trial_id.clone();

        let mut review = Directive::new(
            "user",
            "Review candidate change",
            "Route this review task through the candidate reviewer trial.",
        );
        review
            .metadata
            .insert("required_capability".to_string(), "review".to_string());
        review
            .metadata
            .insert("requires_code_write".to_string(), "false".to_string());
        review
            .metadata
            .insert("trial_approved".to_string(), trial_id.to_string());
        let review_report = system
            .run_directive(review)
            .await
            .expect("review directive");

        assert_eq!(review_report.results.len(), 1);
        assert!(review_report.traces.iter().any(|trace| {
            trace
                .related_suggestion_ids
                .iter()
                .any(|suggestion_id| suggestion_id == &trial_id)
        }));
        assert_eq!(system.shared_genome().read().await.id, candidate_genome_id);
        assert!(system
            .ledger()
            .get_champion_genome_id(&system.root_node_id())
            .await
            .expect("champion id")
            .is_some_and(|genome_id| genome_id == candidate_genome_id));

        let record = system
            .ledger()
            .get_trial_record(&trial_id)
            .await
            .expect("trial record")
            .expect("trial exists");
        assert_eq!(record.status, vsm_ledger::StoredTrialStatus::Promoted);
        assert_eq!(suggestion_operator(&record.suggestion), "unknown");
        assert!(matches!(
            record.suggestion.source,
            vsm_core::GeneSuggestionSource::System3StarAudit
        ));
    }

    #[derive(Clone, Debug)]
    struct NodeSelectiveProvider;

    #[async_trait]
    impl vsm_worker::ModelProvider for NodeSelectiveProvider {
        async fn complete(
            &self,
            request: ModelRequest,
        ) -> Result<ModelResponse, ModelProviderError> {
            if request.metadata.get("node_name").map(String::as_str) == Some("primary-code-service")
            {
                return Err(ModelProviderError::Request(
                    "forced primary coding failure".to_string(),
                ));
            }

            Ok(ModelResponse {
                output_text: "candidate reviewer accepted task".to_string(),
                usage: Some(ModelUsage {
                    input_tokens: 20,
                    output_tokens: 5,
                }),
                raw: None,
            })
        }
    }

    #[test]
    fn candidate_only_leaf_detection_ignores_champion_leaves() {
        let champion = LocalVsmSystem::single_coder_genome();
        let root_id = champion.root_node_id.clone();
        let mut candidate = champion.clone();
        candidate
            .add_child(
                &root_id,
                ViableNode::new_leaf("candidate-reviewer", LeafOperationSpec::reviewer()),
            )
            .expect("candidate child");
        let ids =
            candidate_only_direct_leaf_ids(&champion, &candidate, &root_id).expect("candidate ids");
        assert_eq!(ids.len(), 1);
    }

    #[test]
    fn trace_constructor_stays_available_for_external_evals() {
        let genome = LocalVsmSystem::single_coder_genome();
        let child_id = genome
            .get_node(&genome.root_node_id)
            .expect("root")
            .children[0]
            .clone();
        let trace = TaskTrace::started(TaskId::new(), genome.id, child_id);
        assert_eq!(trace.token_total(), 0);
    }
}
