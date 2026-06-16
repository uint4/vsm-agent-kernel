use vsm_core::{
    AuditFinding, AuditReport, GeneSuggestion, GeneSuggestionSource, LeafOperationSpec,
    OrganizationalGenomePatch, System5Policy, TaskTrace, ViableNode,
};
use vsm_ledger::LedgerEvent;

use crate::ControllerError;

#[derive(Clone, Debug)]
pub struct AuditOutcome {
    pub report: AuditReport,
    pub suggestions: Vec<GeneSuggestion>,
}

/// System 3* audit extension point.
///
/// Auditors inspect observed child behavior and generate candidate organizational
/// genes. They do not need to know in advance whether a gene is valuable; the
/// mutation/selection layer should trial suggestions under bounded conditions
/// and keep only empirically useful variants.
pub trait System3StarAuditor: Send + Sync {
    fn audit_children(
        &self,
        parent: &ViableNode,
        traces: Vec<TaskTrace>,
        recent_events: Vec<LedgerEvent>,
    ) -> Result<AuditOutcome, ControllerError>;
}

#[derive(Clone, Debug)]
pub struct RuleBasedSystem3StarAuditor {
    pub min_traces_for_review_suggestion: usize,
    pub failed_task_ratio_threshold: f64,
}

impl Default for RuleBasedSystem3StarAuditor {
    fn default() -> Self {
        Self {
            min_traces_for_review_suggestion: 5,
            failed_task_ratio_threshold: 0.25,
        }
    }
}

impl System3StarAuditor for RuleBasedSystem3StarAuditor {
    fn audit_children(
        &self,
        parent: &ViableNode,
        traces: Vec<TaskTrace>,
        _recent_events: Vec<LedgerEvent>,
    ) -> Result<AuditOutcome, ControllerError> {
        let mut findings = Vec::new();
        let mut suggested_patches = Vec::new();
        let mut suggestions = Vec::new();

        let failed = traces
            .iter()
            .filter(|trace| trace.merged == Some(false) || trace.reverted == Some(true))
            .count();
        let failure_ratio = if traces.is_empty() {
            0.0
        } else {
            failed as f64 / traces.len() as f64
        };

        if traces.len() >= self.min_traces_for_review_suggestion
            && failure_ratio >= self.failed_task_ratio_threshold
        {
            let finding = AuditFinding {
                title: "High child failure ratio".to_string(),
                evidence: vec![format!(
                    "{} failed/reverted traces across {} observed traces",
                    failed,
                    traces.len()
                )],
                severity: 6,
                related_nodes: parent.children.clone(),
                related_tasks: traces.iter().map(|trace| trace.task_id.clone()).collect(),
            };
            findings.push(finding);

            let mut reviewer =
                ViableNode::new_leaf("review-probation-leaf", LeafOperationSpec::reviewer());
            reviewer.system_5 = System5Policy {
                identity: "Probationary review leaf suggested by System 3* audit.".to_string(),
                values: vec!["catch regressions before integration".to_string()],
                non_negotiable_constraints: vec![
                    "Do not write code; review only according to leaf capabilities.".to_string(),
                ],
                denied_capabilities: vec!["write_code".to_string()],
            };
            reviewer.status = vsm_core::NodeLifecycleStatus::Probation;
            reviewer
                .metadata
                .insert("suggested_by".to_string(), "system_3_star".to_string());
            reviewer
                .metadata
                .insert("task_tag".to_string(), "review".to_string());

            let patch = OrganizationalGenomePatch::AddChild {
                parent_id: parent.id.clone(),
                child: reviewer,
            };
            suggested_patches.push(patch.clone());

            let mut suggestion = GeneSuggestion::new(
                parent.id.clone(),
                parent.id.clone(),
                GeneSuggestionSource::System3StarAudit,
                patch,
                "Observed failure ratio suggests trialing a bounded review child.",
            );
            suggestion.evidence.push(format!(
                "failure_ratio={:.3}, failed={}, traces={}",
                failure_ratio,
                failed,
                traces.len()
            ));
            suggestion.safety_limits.max_tasks = Some(10);
            suggestion.safety_limits.max_token_budget = Some(100_000);
            suggestion.safety_limits.requires_approval = true;
            suggestions.push(suggestion);
        }

        Ok(AuditOutcome {
            report: AuditReport {
                target_node_id: parent.id.clone(),
                findings,
                suggested_patches,
            },
            suggestions,
        })
    }
}
