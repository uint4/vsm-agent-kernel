use std::sync::Arc;

use async_trait::async_trait;
use vsm_controller::RuleBasedSystem3StarAuditor;
use vsm_core::Directive;
use vsm_system::LocalVsmSystem;
use vsm_worker::{ModelProvider, ModelProviderError, ModelRequest, ModelResponse, ModelUsage};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let system = LocalVsmSystem::with_in_memory_sqlite_ledger(
        LocalVsmSystem::single_coder_genome(),
        Arc::new(LocalCycleProvider),
    )
    .await?;

    let mut failing = Directive::new(
        "user",
        "Exercise failing primary service",
        "The primary coding leaf fails this first task so System 3* sees audit pressure.",
    );
    failing
        .metadata
        .insert("requires_code_write".to_string(), "true".to_string());
    let failed = system.run_directive(failing).await?;
    println!(
        "baseline directive: results={} traces={} first_trace_failed={}",
        failed.results.len(),
        failed.traces.len(),
        failed
            .traces
            .first()
            .is_some_and(|trace| trace.merged == Some(false))
    );

    let auditor = RuleBasedSystem3StarAuditor {
        min_traces_for_review_suggestion: 1,
        failed_task_ratio_threshold: 0.1,
    };
    let audit_report = system.run_system_3_star_audit(&auditor).await?;
    println!(
        "audit findings={} suggested_patches={}",
        audit_report.findings.len(),
        audit_report.suggested_patches.len()
    );

    let candidate_genome_id = system
        .start_next_trial_and_register_candidate_workers()
        .await?
        .ok_or("queued candidate did not start")?;
    println!("active candidate genome: {candidate_genome_id}");
    let trial_id = system
        .ledger()
        .get_active_trial_record(&system.root_node_id())
        .await?
        .ok_or("active trial record missing")?
        .trial_id;

    let mut review = Directive::new(
        "user",
        "Review candidate work",
        "The candidate reviewer handles this trial-approved review task.",
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
    let promoted = system.run_directive(review).await?;
    println!(
        "trial directive: results={} traces={} champion_genome={}",
        promoted.results.len(),
        promoted.traces.len(),
        system.shared_genome().read().await.id
    );

    let record = system
        .ledger()
        .get_trial_record(&trial_id)
        .await?
        .ok_or("trial record missing")?;
    println!(
        "trial status={:?} score={} traces={}",
        record.status, record.total_score, record.trace_count
    );

    Ok(())
}

#[derive(Clone, Debug)]
struct LocalCycleProvider;

#[async_trait]
impl ModelProvider for LocalCycleProvider {
    async fn complete(&self, request: ModelRequest) -> Result<ModelResponse, ModelProviderError> {
        if request.metadata.get("node_name").map(String::as_str) == Some("primary-code-service") {
            return Err(ModelProviderError::Request(
                "intentional local-cycle primary failure".to_string(),
            ));
        }

        Ok(ModelResponse {
            output_text: "candidate reviewer accepted local-cycle task".to_string(),
            usage: Some(ModelUsage {
                input_tokens: 20,
                output_tokens: 5,
            }),
            raw: None,
        })
    }
}
