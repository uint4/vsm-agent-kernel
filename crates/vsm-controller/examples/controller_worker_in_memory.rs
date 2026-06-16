use std::{sync::Arc, time::Duration};
use tokio::sync::RwLock;
use vsm_controller::ControllerRuntime;
use vsm_core::{
    envelope_for_directive, Directive, LeafOperationSpec, OrganizationalGenome, Transport,
    ViableNode,
};
use vsm_ledger::{Ledger, SqliteLedger, TraceWindow};
use vsm_runtime::InMemoryTransport;
use vsm_worker::{EchoModelProvider, LedgerTraceSink, WorkerHarness};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = ViableNode::new_metasystem("root-codebase-system");
    let root_id = root.id.clone();
    let mut genome = OrganizationalGenome::new(root);

    let mut coder = ViableNode::new_leaf("primary-code-service", LeafOperationSpec::coding());
    coder.system_5.identity = "General coding leaf under the root VSM.".to_string();
    coder.permissions.allowed_paths = vec!["src".to_string(), "crates".to_string(), "examples".to_string()];
    coder.model.provider = "echo".to_string();
    coder.model.model = "echo-local".to_string();
    let coder_id = genome.add_child(&root_id, coder)?;

    let genome = Arc::new(RwLock::new(genome));
    let transport = Arc::new(InMemoryTransport::new(256));
    let ledger: Arc<dyn Ledger> = Arc::new(SqliteLedger::in_memory().await?);

    let controller = ControllerRuntime::new(
        root_id.clone(),
        genome.clone(),
        transport.clone(),
        ledger.clone(),
    );

    let worker = WorkerHarness::new(
        coder_id.clone(),
        genome.clone(),
        transport.clone(),
        Arc::new(EchoModelProvider::default()),
        Arc::new(LedgerTraceSink::new(ledger.clone())),
    );

    let controller_task = tokio::spawn(async move { controller.run_until_results(1).await });
    let worker_task = tokio::spawn(async move { worker.run_until_tasks(1).await });

    // Let subscriptions attach to the in-memory broadcast bus before publishing.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut directive = Directive::new(
        "user",
        "Wire a controller to a coding worker",
        "Return a TaskResult through the VSM transport and record a durable trace.",
    );
    directive
        .metadata
        .insert("requires_code_write".to_string(), "true".to_string());

    let envelope = envelope_for_directive(&directive)?.with_route(None, Some(root_id.clone()));
    transport.publish(envelope).await?;

    let controller_joined = tokio::time::timeout(Duration::from_secs(5), controller_task).await?;
    let results = controller_joined??;

    let worker_joined = tokio::time::timeout(Duration::from_secs(5), worker_task).await?;
    let handled = worker_joined??;

    println!("controller observed {} result(s)", results.len());
    println!("worker handled {} task(s)", handled.len());

    let traces = ledger.recent_task_traces(TraceWindow::default()).await?;
    println!("ledger trace count: {}", traces.len());
    for trace in traces {
        println!(
            "trace task={} assigned={} ancestors={:?} score={} tokens={}",
            trace.task_id,
            trace.assigned_node_id,
            trace.responsible_ancestor_ids,
            trace.outcome_score,
            trace.token_total()
        );
    }

    Ok(())
}
