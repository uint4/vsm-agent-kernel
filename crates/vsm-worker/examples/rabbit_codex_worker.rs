//! RabbitMQ-backed Codex worker process.
//!
//! Send `vsm.task_packet` envelopes to the configured `VSM_WORKER_NODE_ID` over
//! `VsmChannelType::ResourceBargaining`; this process executes them as an
//! operational leaf and publishes `vsm.task_result` back to the source node, or
//! to `VSM_PARENT_NODE_ID` when no source is present.

use std::{env, path::PathBuf, sync::Arc};
use tokio::sync::RwLock;
use vsm_amqp::{RabbitMqConfig, RabbitMqTransport};
use vsm_core::{LeafOperationSpec, NodeId, OrganizationalGenome, ViableNode};
use vsm_worker::{
    CodexCliApprovalMode, CodexCliConfig, CodexCliProvider, CodexCliSandbox, InMemoryTraceSink,
    WorkerHarness, WorkerHarnessConfig,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let worker_id = env::var("VSM_WORKER_NODE_ID")
        .map(NodeId::from_string)
        .unwrap_or_else(|_| NodeId::new());
    let parent_id = env::var("VSM_PARENT_NODE_ID")
        .map(NodeId::from_string)
        .unwrap_or_else(|_| NodeId::new());
    let workspace = env::var("CODEX_WORKSPACE").unwrap_or_else(|_| ".".to_string());

    let mut root = ViableNode::new_metasystem("root-codebase-system");
    root.id = parent_id.clone();

    let mut coder = ViableNode::new_leaf("rabbit-codex-worker", LeafOperationSpec::coding());
    coder.id = worker_id.clone();
    coder.system_5.identity = "RabbitMQ-addressable Codex-backed System 1 coding leaf.".to_string();
    coder.model.provider = "codex_cli".to_string();
    coder.model.model = "configured-by-codex".to_string();
    coder.permissions.allowed_paths = vec![".".to_string()];

    let mut genome = OrganizationalGenome::new(root);
    genome.add_child(&parent_id, coder)?;

    let rabbit = RabbitMqTransport::connect(RabbitMqConfig {
        uri: env::var("VSM_RABBIT_URI")
            .or_else(|_| env::var("RABBITMQ_URI"))
            .unwrap_or_else(|_| "amqp://guest:guest@localhost:5672/%2f".to_string()),
        exchange: env::var("VSM_RABBIT_EXCHANGE")
            .or_else(|_| env::var("VSM_RABBITMQ_EXCHANGE"))
            .unwrap_or_else(|_| "vsm.events".to_string()),
        consumer_tag: format!("vsm-worker-{worker_id}"),
    })
    .await?;

    let provider = Arc::new(CodexCliProvider::new(CodexCliConfig {
        workspace_root: PathBuf::from(workspace),
        sandbox: CodexCliSandbox::WorkspaceWrite,
        approval: CodexCliApprovalMode::Never,
        skip_git_repo_check: true,
        ..CodexCliConfig::default()
    }));

    let mut config = WorkerHarnessConfig::default();
    config.queue_name = Some(format!("vsm.worker.{worker_id}"));
    config.durable_subscription = true;

    eprintln!("starting RabbitMQ Codex worker node_id={worker_id} parent_id={parent_id}");

    let harness = WorkerHarness::new(
        worker_id,
        Arc::new(RwLock::new(genome)),
        Arc::new(rabbit),
        provider,
        Arc::new(InMemoryTraceSink::new()),
    )
    .with_config(config);

    harness.run_forever().await?;
    Ok(())
}
