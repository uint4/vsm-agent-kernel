use crate::{ModelProvider, TaskPromptBuilder, TraceSink, WorkerError};
use chrono::Utc;
use futures_util::StreamExt;
use std::sync::Arc;
use tokio::sync::RwLock;
use vsm_core::{
    envelope_for_task_result, BuiltinPayloadType, CapabilitySet, MessageEnvelope, NodeId,
    OrganizationalGenome, Subscription, SuggestionId, TaskArtifact, TaskPacket, TaskResult,
    TaskTrace, Transport, VsmChannelType,
};

pub type SharedGenome = Arc<RwLock<OrganizationalGenome>>;

#[derive(Clone, Debug)]
pub struct WorkerHarnessConfig {
    pub queue_name: Option<String>,
    pub durable_subscription: bool,
    pub subscription_channels: Vec<VsmChannelType>,
    pub publish_results: bool,
}

impl Default for WorkerHarnessConfig {
    fn default() -> Self {
        Self {
            queue_name: None,
            durable_subscription: false,
            subscription_channels: vec![
                VsmChannelType::ResourceBargaining,
                VsmChannelType::Command,
                VsmChannelType::System2Coordination,
                VsmChannelType::ManagementToOperation,
                VsmChannelType::OperationToOperation,
                VsmChannelType::Audit,
            ],
            publish_results: true,
        }
    }
}

pub struct WorkerHarness {
    node_id: NodeId,
    genome: SharedGenome,
    transport: Arc<dyn Transport>,
    model_provider: Arc<dyn ModelProvider>,
    trace_sink: Arc<dyn TraceSink>,
    prompt_builder: TaskPromptBuilder,
    config: WorkerHarnessConfig,
}

impl WorkerHarness {
    pub fn new(
        node_id: NodeId,
        genome: SharedGenome,
        transport: Arc<dyn Transport>,
        model_provider: Arc<dyn ModelProvider>,
        trace_sink: Arc<dyn TraceSink>,
    ) -> Self {
        Self {
            node_id,
            genome,
            transport,
            model_provider,
            trace_sink,
            prompt_builder: TaskPromptBuilder::default(),
            config: WorkerHarnessConfig::default(),
        }
    }

    pub fn with_config(mut self, config: WorkerHarnessConfig) -> Self {
        self.config = config;
        self
    }

    pub async fn run_forever(&self) -> Result<(), WorkerError> {
        let mut stream = self.subscribe().await?;

        while let Some(next) = stream.next().await {
            let envelope = next?;
            let _ = self.handle_envelope(envelope).await?;
        }

        Ok(())
    }

    pub async fn run_until_tasks(&self, max_tasks: usize) -> Result<Vec<TaskResult>, WorkerError> {
        let mut stream = self.subscribe().await?;
        let mut handled = vec![];

        while handled.len() < max_tasks {
            let Some(next) = stream.next().await else {
                break;
            };
            let envelope = next?;
            if let Some(result) = self.handle_envelope(envelope).await? {
                handled.push(result);
            }
        }

        Ok(handled)
    }

    async fn subscribe(&self) -> Result<vsm_core::EnvelopeStream, WorkerError> {
        let subscription = Subscription {
            channel_types: self.config.subscription_channels.clone(),
            target_node_id: Some(self.node_id.to_string()),
            queue_name: self.config.queue_name.clone(),
            durable: self.config.durable_subscription,
        };

        Ok(self.transport.subscribe(subscription).await?)
    }

    async fn handle_envelope(
        &self,
        envelope: MessageEnvelope,
    ) -> Result<Option<TaskResult>, WorkerError> {
        if envelope.payload_type != BuiltinPayloadType::TaskPacket.as_str() {
            return Ok(None);
        }

        let task: TaskPacket = envelope.payload_as()?;
        if let Some(assigned_to) = &task.assigned_to {
            if assigned_to != &self.node_id {
                return Ok(None);
            }
        }

        let result = self.execute_task(task, &envelope).await?;
        Ok(Some(result))
    }

    async fn execute_task(
        &self,
        task: TaskPacket,
        incoming: &MessageEnvelope,
    ) -> Result<TaskResult, WorkerError> {
        let (node, genome_id, ancestors, parent_id) = {
            let genome = self.genome.read().await;
            let node = genome.get_node(&self.node_id)?.clone();
            let ancestors = genome.ancestor_ids(&self.node_id)?;
            let parent_id = node.parent_id.clone();
            (node, genome.id.clone(), ancestors, parent_id)
        };

        let mut trace = TaskTrace::started(task.id.clone(), genome_id, self.node_id.clone());
        trace.responsible_ancestor_ids = ancestors;

        let started_at = Utc::now();
        let execution = async {
            ensure_executable_leaf(&node, &task)?;
            let request = self.prompt_builder.build(&node, &task);
            let response = self.model_provider.complete(request).await?;

            let usage = response.usage.clone();
            let mut result = TaskResult::completed(
                task.id.clone(),
                self.node_id.clone(),
                response.output_text.clone(),
            );
            result
                .artifacts
                .push(TaskArtifact::inline("model_output", response.output_text));
            harvest_provider_artifacts(&response.raw, &mut result);
            result
                .metadata
                .insert("provider".to_string(), node.model.provider.clone());
            result
                .metadata
                .insert("model".to_string(), node.model.model.clone());

            Ok::<_, WorkerError>((result, usage))
        }
        .await;

        let (mut result, usage) = match execution {
            Ok((result, usage)) => {
                trace.outcome_score = 1.0;
                trace.merged = None;
                trace.review_passed = None;
                trace.tests_passed = None;
                (result, usage)
            }
            Err(err) => {
                trace.outcome_score = -1.0;
                trace.merged = Some(false);
                let result = TaskResult::failed(
                    task.id.clone(),
                    self.node_id.clone(),
                    "worker failed to execute task",
                    err.to_string(),
                );
                (result, None)
            }
        };

        copy_trial_metadata(&task, &mut result, &mut trace);
        copy_replay_metadata(&task, &mut trace);

        if let Some(usage) = usage {
            trace.input_tokens = usage.input_tokens;
            trace.output_tokens = usage.output_tokens;
        }

        let completed_at = Utc::now();
        trace.completed_at = Some(completed_at);
        trace.latency_ms = (completed_at - started_at).num_milliseconds().max(0) as u64;
        trace.files_touched = result.files_touched.clone();
        trace.tests_run = result.tests_run.clone();
        trace
            .metadata
            .insert("status".to_string(), format!("{:?}", result.status));

        self.trace_sink.record(trace).await?;

        if self.config.publish_results {
            self.publish_result(&result, incoming, parent_id).await?;
        }

        Ok(result)
    }

    async fn publish_result(
        &self,
        result: &TaskResult,
        incoming: &MessageEnvelope,
        parent_id: Option<NodeId>,
    ) -> Result<(), WorkerError> {
        let target = incoming.source_node_id.clone().or(parent_id);
        let mut envelope =
            envelope_for_task_result(result)?.with_route(Some(self.node_id.clone()), target);
        envelope.correlation_id = incoming
            .correlation_id
            .clone()
            .or_else(|| Some(incoming.id.to_string()));
        envelope.causation_id = Some(incoming.id.clone());
        envelope.trace = incoming.trace.clone();
        envelope.trace.push(self.node_id.clone());

        self.transport.publish(envelope).await?;
        Ok(())
    }
}

fn harvest_provider_artifacts(raw: &Option<serde_json::Value>, result: &mut TaskResult) {
    let Some(raw) = raw else {
        return;
    };

    if let Some(files) = raw
        .get("files_touched")
        .and_then(serde_json::Value::as_array)
    {
        for file in files.iter().filter_map(serde_json::Value::as_str) {
            if !result.files_touched.iter().any(|existing| existing == file) {
                result.files_touched.push(file.to_string());
            }
        }
    }

    if let Some(artifacts) = raw.get("artifacts").and_then(serde_json::Value::as_array) {
        for artifact in artifacts {
            result.artifacts.push(TaskArtifact::inline(
                "provider_artifact",
                artifact.to_string(),
            ));
        }
    }
}

fn copy_trial_metadata(task: &TaskPacket, result: &mut TaskResult, trace: &mut TaskTrace) {
    for key in [
        "trial_id",
        "related_suggestion_id",
        "candidate_genome_id",
        "trial_mode",
        "trial_route_role",
        "trial_shadow",
        "trial_exposure_basis_points",
        "trial_exposure_bucket",
    ] {
        if let Some(value) = task.metadata.get(key) {
            result.metadata.insert(key.to_string(), value.clone());
            trace.metadata.insert(key.to_string(), value.clone());
        }
    }

    if let Some(value) = task
        .metadata
        .get("related_suggestion_id")
        .or_else(|| task.metadata.get("trial_id"))
    {
        let suggestion_id = SuggestionId::from_string(value.clone());
        if !trace
            .related_suggestion_ids
            .iter()
            .any(|existing| existing == &suggestion_id)
        {
            trace.related_suggestion_ids.push(suggestion_id);
        }
    }
}

fn copy_replay_metadata(task: &TaskPacket, trace: &mut TaskTrace) {
    trace
        .metadata
        .insert("task_title".to_string(), task.title.clone());
    trace
        .metadata
        .insert("task_goal".to_string(), task.goal.clone());
    trace
        .metadata
        .insert("task_risk".to_string(), format!("{:?}", task.risk));

    if !task.scope.is_empty() {
        trace
            .metadata
            .insert("task_scope".to_string(), task.scope.join("\n"));
    }
    if !task.static_predicates.languages.is_empty() {
        trace.metadata.insert(
            "task_languages".to_string(),
            task.static_predicates.languages.join(","),
        );
    }
    if !task.static_predicates.modules.is_empty() {
        trace.metadata.insert(
            "task_modules".to_string(),
            task.static_predicates.modules.join(","),
        );
    }
    if !task.static_predicates.tags.is_empty() {
        trace.metadata.insert(
            "task_tags".to_string(),
            task.static_predicates.tags.join(","),
        );
    }

    for (key, value) in &task.metadata {
        trace
            .metadata
            .insert(format!("task_metadata.{key}"), value.clone());
    }
}

fn ensure_executable_leaf(
    node: &vsm_core::ViableNode,
    task: &TaskPacket,
) -> Result<(), WorkerError> {
    if !node.is_leaf() {
        return Err(WorkerError::NotExecutableLeaf(node.id.clone()));
    }

    let capabilities = node.capabilities();

    if task
        .metadata
        .get("requires_code_write")
        .map(|v| v == "true")
        .unwrap_or(false)
    {
        require_capability(&node.id, &capabilities, "write_code")?;
    }

    if let Some(required) = task.metadata.get("required_capability") {
        require_capability(&node.id, &capabilities, required)?;
    }

    Ok(())
}

fn require_capability(
    node_id: &NodeId,
    capabilities: &CapabilitySet,
    capability: &str,
) -> Result<(), WorkerError> {
    let allowed = match capability {
        "write_code" => capabilities.can_write_code,
        "run_tests" => capabilities.can_run_tests,
        "review" => capabilities.can_review,
        "research" => capabilities.can_research,
        "integrate" => capabilities.can_integrate,
        "read_filesystem" => capabilities.can_read_filesystem,
        "write_filesystem" => capabilities.can_write_filesystem,
        _ => true,
    };

    if allowed {
        Ok(())
    } else {
        Err(WorkerError::CapabilityDenied {
            node_id: node_id.clone(),
            capability: capability.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use vsm_core::{
        GenomeId, NodeId, SuggestionId, TaskId, TaskPacket, TaskResult, TaskTrace, VsmChannelType,
    };

    use super::{copy_replay_metadata, copy_trial_metadata, WorkerHarnessConfig};

    #[test]
    fn worker_default_subscribes_to_executable_vsm_channels() {
        let config = WorkerHarnessConfig::default();
        for channel in [
            VsmChannelType::ResourceBargaining,
            VsmChannelType::Command,
            VsmChannelType::System2Coordination,
            VsmChannelType::ManagementToOperation,
            VsmChannelType::OperationToOperation,
            VsmChannelType::Audit,
        ] {
            assert!(config.subscription_channels.contains(&channel));
        }
    }

    #[test]
    fn trial_metadata_is_copied_to_result_and_trace() {
        let suggestion_id = SuggestionId::new();
        let candidate_genome_id = GenomeId::new();
        let node_id = NodeId::new();
        let mut task = TaskPacket::new("trial task", "exercise trial metadata");
        task.metadata
            .insert("trial_id".to_string(), suggestion_id.to_string());
        task.metadata.insert(
            "related_suggestion_id".to_string(),
            suggestion_id.to_string(),
        );
        task.metadata.insert(
            "candidate_genome_id".to_string(),
            candidate_genome_id.to_string(),
        );
        task.metadata
            .insert("trial_mode".to_string(), "canary".to_string());
        task.metadata
            .insert("trial_route_role".to_string(), "shadow".to_string());
        task.metadata
            .insert("trial_shadow".to_string(), "true".to_string());
        task.metadata
            .insert("trial_exposure_basis_points".to_string(), "250".to_string());
        task.metadata
            .insert("trial_exposure_bucket".to_string(), "42".to_string());

        let mut result = TaskResult::completed(TaskId::new(), node_id.clone(), "ok");
        let mut trace = TaskTrace::started(TaskId::new(), candidate_genome_id.clone(), node_id);

        copy_trial_metadata(&task, &mut result, &mut trace);

        assert_eq!(
            result.metadata.get("trial_id").map(String::as_str),
            Some(suggestion_id.as_str())
        );
        assert_eq!(
            result
                .metadata
                .get("related_suggestion_id")
                .map(String::as_str),
            Some(suggestion_id.as_str())
        );
        assert!(trace
            .related_suggestion_ids
            .iter()
            .any(|related| related == &suggestion_id));
        assert_eq!(
            result.metadata.get("trial_mode").map(String::as_str),
            Some("canary")
        );
        assert_eq!(
            result.metadata.get("trial_route_role").map(String::as_str),
            Some("shadow")
        );
        assert_eq!(
            trace.metadata.get("trial_shadow").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            trace
                .metadata
                .get("trial_exposure_basis_points")
                .map(String::as_str),
            Some("250")
        );
        assert_eq!(
            trace
                .metadata
                .get("trial_exposure_bucket")
                .map(String::as_str),
            Some("42")
        );
    }

    #[test]
    fn replay_metadata_preserves_task_shape_on_trace() {
        let genome_id = GenomeId::new();
        let node_id = NodeId::new();
        let mut task = TaskPacket::new("Review change", "review a risky patch");
        task.risk = vsm_core::RiskClass::High;
        task.scope.push("crates/example/src/lib.rs".to_string());
        task.static_predicates.languages.push("rust".to_string());
        task.static_predicates.modules.push("example".to_string());
        task.static_predicates.tags.push("review".to_string());
        task.metadata
            .insert("required_capability".to_string(), "review".to_string());
        let mut trace = TaskTrace::started(TaskId::new(), genome_id, node_id);

        copy_replay_metadata(&task, &mut trace);

        assert_eq!(
            trace.metadata.get("task_title").map(String::as_str),
            Some("Review change")
        );
        assert_eq!(
            trace.metadata.get("task_risk").map(String::as_str),
            Some("High")
        );
        assert_eq!(
            trace.metadata.get("task_languages").map(String::as_str),
            Some("rust")
        );
        assert_eq!(
            trace
                .metadata
                .get("task_metadata.required_capability")
                .map(String::as_str),
            Some("review")
        );
    }
}
