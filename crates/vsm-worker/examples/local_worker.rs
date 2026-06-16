use std::sync::Arc;
use tokio::{sync::RwLock, time::{sleep, Duration}};
use vsm_core::{
    envelope_for_task, LeafOperationSpec, ModelSpec, OrganizationalGenome, TaskPacket, Transport,
    ViableNode,
};
use vsm_runtime::InMemoryTransport;
use vsm_worker::{EchoModelProvider, InMemoryTraceSink, WorkerHarness};

#[tokio::main]
async fn main() -> anyhow_free::Result<()> {
    let mut root = ViableNode::new_metasystem("root-controller");
    root.system_5.identity = "Autonomous coding organization root".to_string();

    let mut code_service = ViableNode::new_leaf("primary-code-service", LeafOperationSpec::coding());
    code_service.system_5.identity = "Generalist coding leaf".to_string();
    code_service.model = ModelSpec {
        provider: "echo".to_string(),
        model: "local-echo".to_string(),
        effort: None,
        max_context_tokens: Some(16_000),
    };

    let mut genome = OrganizationalGenome::new(root);
    let root_id = genome.root_node_id.clone();
    let code_id = genome.add_child(&root_id, code_service)?;

    let genome = Arc::new(RwLock::new(genome));
    let transport = Arc::new(InMemoryTransport::new(64));
    let traces = Arc::new(InMemoryTraceSink::new());
    let model = Arc::new(EchoModelProvider::default());

    let harness = WorkerHarness::new(
        code_id.clone(),
        genome.clone(),
        transport.clone(),
        model,
        traces.clone(),
    );

    let harness_task = tokio::spawn(async move { harness.run_until_tasks(1).await });

    // Give the worker time to subscribe to the in-memory broadcast channel.
    sleep(Duration::from_millis(50)).await;

    let mut task = TaskPacket::new(
        "Add a health-check endpoint",
        "Produce a concise implementation plan for a low-risk health-check endpoint.",
    );
    task.assigned_to = Some(code_id.clone());
    task.metadata
        .insert("requires_code_write".to_string(), "true".to_string());

    let envelope = envelope_for_task(&task)?.with_route(Some(root_id), Some(code_id));
    transport.publish(envelope).await?;

    let results = harness_task.await??;
    println!("results: {:#?}", results);
    println!("traces: {:#?}", traces.traces().await);

    Ok(())
}

// Avoid adding anyhow as a dependency just for this example.
mod anyhow_free {
    pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    pub type Result<T> = std::result::Result<T, Error>;
}
