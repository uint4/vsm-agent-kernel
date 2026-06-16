use futures_util::StreamExt;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use vsm_core::{
    envelope_for_task, BuiltinPayloadType, LeafOperationSpec, MessageEnvelope, OrganizationalGenome,
    Subscription, TaskPacket, Transport, VsmChannelType, ViableNode,
};
use vsm_runtime::InMemoryTransport;
use vsm_worker::{
    CodexCliApprovalMode, CodexCliConfig, CodexCliProvider, CodexCliSandbox, InMemoryTraceSink,
    WorkerHarness,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace = std::env::args().nth(1).unwrap_or_else(|| ".".to_string());

    let root = ViableNode::new_metasystem("root-codebase-system");
    let root_id = root.id.clone();
    let mut genome = OrganizationalGenome::new(root);

    let mut coder = ViableNode::new_leaf("codex-primary-code-service", LeafOperationSpec::coding());
    coder.system_5.identity = "Codex-backed System 1 coding leaf.".to_string();
    coder.system_5.non_negotiable_constraints = vec![
        "Keep changes minimal and bounded to the TaskPacket.".to_string(),
        "Report blockers instead of broadening scope silently.".to_string(),
    ];
    coder.model.provider = "codex_cli".to_string();
    coder.model.model = "configured-by-codex".to_string();
    coder.permissions.allowed_paths = vec![".".to_string()];
    let coder_id = genome.add_child(&root_id, coder)?;

    let genome = Arc::new(RwLock::new(genome));
    let transport = Arc::new(InMemoryTransport::new(128));
    let trace_sink = Arc::new(InMemoryTraceSink::new());

    let provider = Arc::new(CodexCliProvider::new(CodexCliConfig {
        workspace_root: PathBuf::from(workspace),
        sandbox: CodexCliSandbox::WorkspaceWrite,
        approval: CodexCliApprovalMode::Never,
        skip_git_repo_check: true,
        ..CodexCliConfig::default()
    }));

    let harness = WorkerHarness::new(
        coder_id.clone(),
        genome,
        transport.clone(),
        provider,
        trace_sink.clone(),
    );

    let root_subscription = Subscription {
        channel_types: vec![VsmChannelType::ManagementToOperation],
        target_node_id: Some(root_id.to_string()),
        queue_name: None,
        durable: false,
    };
    let mut root_results = transport.subscribe(root_subscription).await?;
    let worker_task = tokio::spawn(async move { harness.run_until_tasks(1).await });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut task = TaskPacket::new(
        "Codex CLI smoke task",
        "Inspect the repository and either make one minimal safe improvement or explain why no safe change is appropriate.",
    );
    task.assigned_to = Some(coder_id.clone());
    task.scope = vec!["repository root".to_string()];
    task.constraints = vec![
        "Do not add production dependencies.".to_string(),
        "Keep the diff small.".to_string(),
    ];
    task.metadata.insert("requires_code_write".to_string(), "true".to_string());

    let envelope = envelope_for_task(&task)?.with_route(Some(root_id.clone()), Some(coder_id));
    transport.publish(envelope).await?;

    let result_envelope: MessageEnvelope = root_results
        .next()
        .await
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "root result stream closed"))??;
    assert_eq!(result_envelope.payload_type, BuiltinPayloadType::TaskResult.as_str());
    println!("root received result: {}", result_envelope.payload);

    let handled = worker_task.await??;
    println!("worker handled {} task(s)", handled.len());
    println!("trace count: {}", trace_sink.traces().await.len());

    Ok(())
}
