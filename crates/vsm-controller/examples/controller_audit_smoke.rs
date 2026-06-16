use std::sync::Arc;
use tokio::sync::RwLock;
use vsm_controller::{ControllerRuntime, RuleBasedSystem3StarAuditor};
use vsm_core::{LeafOperationSpec, OrganizationalGenome, TaskId, TaskTrace, ViableNode};
use vsm_ledger::{EventFilter, Ledger, SqliteLedger};
use vsm_runtime::InMemoryTransport;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = ViableNode::new_metasystem("root-codebase-system");
    let root_id = root.id.clone();
    let mut genome = OrganizationalGenome::new(root);

    let mut coder = ViableNode::new_leaf("primary-code-service", LeafOperationSpec::coding());
    coder.system_5.identity = "General coding leaf under the root VSM.".to_string();
    let coder_id = genome.add_child(&root_id, coder)?;

    // Seed the ledger with enough failed/reverted traces for the rule-based
    // System 3* auditor to propose a review child mutation.
    let ledger: Arc<dyn Ledger> = Arc::new(SqliteLedger::in_memory().await?);
    for index in 0..5 {
        let mut trace = TaskTrace::started(TaskId::new(), genome.id.clone(), coder_id.clone());
        trace.responsible_ancestor_ids.push(root_id.clone());
        trace.completed_at = Some(chrono::Utc::now());
        trace.merged = Some(false);
        trace.reverted = Some(index % 2 == 0);
        trace.outcome_score = -10.0;
        trace
            .metadata
            .insert("seeded_for_example".to_string(), "true".to_string());
        ledger.write_task_trace(trace).await?;
    }

    let genome = Arc::new(RwLock::new(genome));
    let transport = Arc::new(InMemoryTransport::new(16));

    let controller = ControllerRuntime::new(
        root_id.clone(),
        genome,
        transport,
        ledger.clone(),
    );

    let auditor = RuleBasedSystem3StarAuditor::default();
    let report = controller.run_system_3_star_audit(&auditor).await?;

    println!("audit findings: {}", report.findings.len());
    println!("suggested patches: {}", report.suggested_patches.len());
    for finding in &report.findings {
        println!("finding: {} severity={}", finding.title, finding.severity);
        for evidence in &finding.evidence {
            println!("  evidence: {evidence}");
        }
    }

    let events = ledger.recent_events(EventFilter::default()).await?;
    println!("ledger events: {}", events.len());

    Ok(())
}
