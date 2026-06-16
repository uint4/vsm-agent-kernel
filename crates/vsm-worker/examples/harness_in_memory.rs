use futures_util::StreamExt;
use std::{sync::Arc, time::Duration};
use tokio::sync::RwLock;
use vsm_core::{
    envelope_for_task, BuiltinPayloadType, LeafOperationSpec, MessageEnvelope,
    OrganizationalGenome, Subscription, TaskPacket, Transport, ViableNode, VsmChannelType,
};
use vsm_runtime::InMemoryTransport;
use vsm_worker::{EchoModelProvider, InMemoryTraceSink, WorkerHarness};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = ViableNode::new_metasystem("root-codebase-system");
    let root_id = root.id.clone();
    let mut genome = OrganizationalGenome::new(root);

    let mut coder = ViableNode::new_leaf("primary-code-service", LeafOperationSpec::coding());
    coder.system_5.identity = "General coding leaf under the root VSM.".to_string();
    coder.permissions.allowed_paths = vec![
        "src".to_string(),
        "crates".to_string(),
        "examples".to_string(),
    ];
    coder.model.provider = "echo".to_string();
    coder.model.model = "echo-local".to_string();
    let coder_id = genome.add_child(&root_id, coder)?;

    let genome = Arc::new(RwLock::new(genome));
    let transport = Arc::new(InMemoryTransport::new(128));
    let trace_sink = Arc::new(InMemoryTraceSink::new());
    let provider = Arc::new(EchoModelProvider::default());

    let harness = WorkerHarness::new(
        coder_id.clone(),
        genome.clone(),
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
        "Exercise the primary code service",
        "Return a model-backed TaskResult and record a trace.",
    );
    task.assigned_to = Some(coder_id.clone());
    task.metadata
        .insert("requires_code_write".to_string(), "true".to_string());

    let envelope =
        envelope_for_task(&task)?.with_route(Some(root_id.clone()), Some(coder_id.clone()));
    transport.publish(envelope).await?;

    let result_envelope: MessageEnvelope = root_results.next().await.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "root result stream closed",
        )
    })??;
    assert_eq!(
        result_envelope.payload_type,
        BuiltinPayloadType::TaskResult.as_str()
    );
    println!("root received result: {}", result_envelope.payload);

    let handled = worker_task.await??;
    println!("worker handled {} task(s)", handled.len());

    let traces = trace_sink.traces().await;
    println!("trace count: {}", traces.len());
    for trace in traces {
        println!(
            "trace task={} node={} score={}",
            trace.task_id, trace.assigned_node_id, trace.outcome_score
        );
    }

    Ok(())
}
