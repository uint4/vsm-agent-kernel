use std::{sync::Arc, time::Duration};

use tokio::sync::RwLock;
use vsm_controller::ControllerRuntime;
use vsm_core::{
    envelope_for_directive, FitnessWeights, GeneSuggestion, GeneSuggestionSource,
    LeafOperationSpec, OrganizationalGenome, OrganizationalGenomePatch, System5Policy, Transport,
    ViableNode,
};
use vsm_ledger::{EventFilter, Ledger, LedgerEventKind, SqliteLedger, TraceWindow};
use vsm_runtime::{InMemoryTransport, TrialConfig};
use vsm_worker::{EchoModelProvider, LedgerTraceSink, WorkerHarness};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = ViableNode::new_metasystem("root-codebase-system");
    let root_id = root.id.clone();
    let mut genome = OrganizationalGenome::new(root);

    let mut coder = ViableNode::new_leaf("primary-code-service", LeafOperationSpec::coding());
    coder.system_5.identity = "Champion general coding leaf.".to_string();
    let _coder_id = genome.add_child(&root_id, coder)?;

    let genome = Arc::new(RwLock::new(genome));
    let transport = Arc::new(InMemoryTransport::new(256));
    let ledger: Arc<dyn Ledger> = Arc::new(SqliteLedger::in_memory().await?);

    let controller = ControllerRuntime::new(
        root_id.clone(),
        genome.clone(),
        transport.clone(),
        ledger.clone(),
    )
    .with_trial_config(
        TrialConfig {
            min_tasks_before_decision: 1,
            promote_margin: 0.1,
            prune_below: -1.0,
        },
        FitnessWeights::default(),
    );

    let mut reviewer = ViableNode::new_leaf("candidate-reviewer", LeafOperationSpec::reviewer());
    reviewer.system_5 = System5Policy {
        identity: "Candidate reviewer leaf for bounded mutation trial.".to_string(),
        values: vec!["review safely before integration".to_string()],
        non_negotiable_constraints: vec!["Do not write code.".to_string()],
        denied_capabilities: vec!["write_code".to_string()],
    };
    reviewer.model.provider = "echo".to_string();
    reviewer.model.model = "echo-local".to_string();
    let reviewer_id = reviewer.id.clone();

    let suggestion = GeneSuggestion::new(
        root_id.clone(),
        root_id.clone(),
        GeneSuggestionSource::System3StarAudit,
        OrganizationalGenomePatch::AddChild {
            parent_id: root_id.clone(),
            child: reviewer,
        },
        "Trial a reviewer leaf for review-capability tasks.",
    );
    let suggestion_id = suggestion.id.clone();

    let queued_genome_id = controller
        .queue_candidate_from_suggestion(suggestion)
        .await?;
    assert!(controller.active_candidate_genome().await.is_none());
    let candidate_genome_id = controller
        .start_next_queued_trial()
        .await?
        .ok_or("queued candidate did not start")?;
    assert_eq!(candidate_genome_id, queued_genome_id);
    controller
        .register_trial_worker(reviewer_id.clone())
        .await?;

    let candidate_genome = controller
        .active_candidate_genome()
        .await
        .ok_or("candidate genome missing")?;

    let active_recovery_genome = Arc::new(RwLock::new(OrganizationalGenome::new(
        ViableNode::new_metasystem("active-recovery-placeholder"),
    )));
    let active_recovery_controller = ControllerRuntime::new(
        root_id.clone(),
        active_recovery_genome,
        transport.clone(),
        ledger.clone(),
    );
    let restored_candidate = active_recovery_controller
        .restore_active_trial_from_ledger()
        .await?;
    assert_eq!(restored_candidate, Some(candidate_genome_id.clone()));

    let candidate_genome = Arc::new(RwLock::new(candidate_genome));

    let worker = WorkerHarness::new(
        reviewer_id.clone(),
        candidate_genome,
        transport.clone(),
        Arc::new(EchoModelProvider::default()),
        Arc::new(LedgerTraceSink::new(ledger.clone())),
    );

    let controller_task = tokio::spawn(async move { controller.run_until_results(1).await });
    let worker_task = tokio::spawn(async move { worker.run_until_tasks(1).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut directive = vsm_core::Directive::new(
        "user",
        "Review a candidate change",
        "Exercise bounded trial routing to the candidate reviewer.",
    );
    directive
        .metadata
        .insert("requires_code_write".to_string(), "false".to_string());
    directive
        .metadata
        .insert("required_capability".to_string(), "review".to_string());
    directive
        .metadata
        .insert("trial_approved".to_string(), suggestion_id.to_string());

    let envelope = envelope_for_directive(&directive)?.with_route(None, Some(root_id.clone()));
    transport.publish(envelope).await?;

    let results = tokio::time::timeout(Duration::from_secs(5), controller_task).await???;
    let handled = tokio::time::timeout(Duration::from_secs(5), worker_task).await???;

    println!("controller observed {} result(s)", results.len());
    println!("candidate worker handled {} task(s)", handled.len());
    println!("candidate genome was {}", candidate_genome_id);

    let promoted = genome.read().await;
    assert_eq!(promoted.id, candidate_genome_id);
    assert!(promoted.nodes.contains_key(&reviewer_id));
    drop(promoted);

    let recovery_genome = Arc::new(RwLock::new(OrganizationalGenome::new(
        ViableNode::new_metasystem("recovery-placeholder"),
    )));
    let recovery_controller = ControllerRuntime::new(
        root_id,
        recovery_genome.clone(),
        transport.clone(),
        ledger.clone(),
    );
    let recovered = recovery_controller.load_persisted_champion().await?;
    assert_eq!(recovered, Some(candidate_genome_id.clone()));
    assert_eq!(recovery_genome.read().await.id, candidate_genome_id);

    let traces = ledger.recent_task_traces(TraceWindow::default()).await?;
    println!("ledger trace count: {}", traces.len());
    for trace in traces {
        println!(
            "trace task={} genome={} suggestions={:?} score={}",
            trace.task_id, trace.genome_id, trace.related_suggestion_ids, trace.outcome_score
        );
    }

    let events = ledger
        .recent_events(EventFilter {
            kinds: vec![
                LedgerEventKind::TrialQueued,
                LedgerEventKind::TrialStarted,
                LedgerEventKind::TrialTaskRouted,
                LedgerEventKind::TrialTraceRecorded,
                LedgerEventKind::TrialDecisionRecorded,
                LedgerEventKind::TrialPromoted,
                LedgerEventKind::GenomePatchApplied,
            ],
            limit: Some(20),
            ..EventFilter::default()
        })
        .await?;
    println!("trial lifecycle events: {}", events.len());
    for event in events {
        println!("event: {:?}", event.kind);
    }

    Ok(())
}
