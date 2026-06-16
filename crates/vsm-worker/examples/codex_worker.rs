use std::sync::Arc;
use tokio::{sync::RwLock, time::{sleep, Duration}};
use vsm_core::{
    envelope_for_task, LeafOperationSpec, ModelSpec, OrganizationalGenome, TaskPacket, Transport,
    ViableNode,
};
use vsm_runtime::InMemoryTransport;
use vsm_worker::{InMemoryTraceSink, OpenAiCodexProvider, WorkerHarness};

#[tokio::main]
async fn main() -> anyhow_free::Result<()> {
    let mut root = ViableNode::new_metasystem("root-controller");
    root.system_5.identity = "Autonomous coding organization root".to_string();

    let mut code_service = ViableNode::new_leaf("primary-code-service", LeafOperationSpec::coding());
    code_service.system_5.identity = "Generalist Codex coding leaf".to_string();
    code_service.system_5.non_negotiable_constraints = vec![
        "Do not claim files were modified unless a code-writing tool actually modified them.".to_string(),
        "Prefer small, easily reviewed patches over broad rewrites.".to_string(),
    ];
    code_service.model = ModelSpec {
        provider: "openai".to_string(),
        model: std::env::var("OPENAI_CODEX_MODEL").unwrap_or_else(|_| "gpt-5.5".to_string()),
        effort: Some(std::env::var("OPENAI_CODEX_EFFORT").unwrap_or_else(|_| "medium".to_string())),
        max_context_tokens: None,
    };

    let mut genome = OrganizationalGenome::new(root);
    let root_id = genome.root_node_id.clone();
    let code_id = genome.add_child(&root_id, code_service)?;

    let genome = Arc::new(RwLock::new(genome));
    let transport = Arc::new(InMemoryTransport::new(64));
    let traces = Arc::new(InMemoryTraceSink::new());
    let model = Arc::new(OpenAiCodexProvider::from_env()?);

    let harness = WorkerHarness::new(
        code_id.clone(),
        genome.clone(),
        transport.clone(),
        model,
        traces.clone(),
    );

    let harness_task = tokio::spawn(async move { harness.run_until_tasks(1).await });
    sleep(Duration::from_millis(50)).await;

    let mut task = TaskPacket::new(
        "Plan the first filesystem tool",
        "Design a minimal Rust trait for a future filesystem patch tool. Do not edit files; return the trait shape and safety checks.",
    );
    task.assigned_to = Some(code_id.clone());
    task.metadata
        .insert("required_capability".to_string(), "research".to_string());

    let envelope = envelope_for_task(&task)?.with_route(Some(root_id), Some(code_id));
    transport.publish(envelope).await?;

    let results = harness_task.await??;
    println!("results: {:#?}", results);
    println!("traces: {:#?}", traces.traces().await);

    Ok(())
}

mod anyhow_free {
    pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    pub type Result<T> = std::result::Result<T, Error>;
}
