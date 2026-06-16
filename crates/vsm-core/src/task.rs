use crate::{DirectiveId, NodeId, TaskId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RiskClass {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StaticTaskPredicates {
    pub languages: Vec<String>,
    pub likely_files: Vec<String>,
    pub modules: Vec<String>,
    pub dependencies: Vec<String>,
    pub test_targets: Vec<String>,
    pub public_api_touched: bool,
    pub database_touched: bool,
    pub auth_touched: bool,
    pub config_touched: bool,
    pub migration_touched: bool,
    pub security_sensitive: bool,
    pub estimated_blast_radius: Option<RiskClass>,
    pub tags: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptanceCriterion {
    pub name: String,
    pub description: String,
    pub verifier: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskPacket {
    pub id: TaskId,
    pub directive_id: Option<DirectiveId>,
    pub parent_task_id: Option<TaskId>,
    pub title: String,
    pub goal: String,
    pub target_state: Option<String>,
    pub scope: Vec<String>,
    pub constraints: Vec<String>,
    pub context_refs: Vec<String>,
    pub authority_refs: Vec<String>,
    pub dependencies: Vec<TaskId>,
    pub acceptance: Vec<AcceptanceCriterion>,
    pub risk: RiskClass,
    pub static_predicates: StaticTaskPredicates,
    pub assigned_to: Option<NodeId>,
    pub metadata: BTreeMap<String, String>,
}

impl TaskPacket {
    pub fn new(title: impl Into<String>, goal: impl Into<String>) -> Self {
        Self {
            id: TaskId::new(),
            directive_id: None,
            parent_task_id: None,
            title: title.into(),
            goal: goal.into(),
            target_state: None,
            scope: vec![],
            constraints: vec![],
            context_refs: vec![],
            authority_refs: vec![],
            dependencies: vec![],
            acceptance: vec![],
            risk: RiskClass::Medium,
            static_predicates: StaticTaskPredicates::default(),
            assigned_to: None,
            metadata: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskOutcomeStatus {
    Completed,
    Failed,
    Rejected,
    NeedsHuman,
    Noop,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskArtifact {
    pub kind: String,
    pub uri: Option<String>,
    pub content: Option<String>,
    pub metadata: BTreeMap<String, String>,
}

impl TaskArtifact {
    pub fn inline(kind: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            uri: None,
            content: Some(content.into()),
            metadata: BTreeMap::new(),
        }
    }
}

/// Result emitted by a leaf operation after attempting a task.
///
/// The result is intentionally transport-safe and model-provider agnostic. A
/// coding harness may include a patch/diff as an inline artifact at first, and
/// later replace it with a repository URI, branch name, or build artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskResult {
    pub task_id: TaskId,
    pub produced_by: NodeId,
    pub status: TaskOutcomeStatus,
    pub summary: String,
    pub artifacts: Vec<TaskArtifact>,
    pub files_touched: Vec<String>,
    pub tests_run: Vec<String>,
    pub error: Option<String>,
    pub metadata: BTreeMap<String, String>,
}

impl TaskResult {
    pub fn completed(task_id: TaskId, produced_by: NodeId, summary: impl Into<String>) -> Self {
        Self {
            task_id,
            produced_by,
            status: TaskOutcomeStatus::Completed,
            summary: summary.into(),
            artifacts: vec![],
            files_touched: vec![],
            tests_run: vec![],
            error: None,
            metadata: BTreeMap::new(),
        }
    }

    pub fn failed(
        task_id: TaskId,
        produced_by: NodeId,
        summary: impl Into<String>,
        error: impl Into<String>,
    ) -> Self {
        Self {
            task_id,
            produced_by,
            status: TaskOutcomeStatus::Failed,
            summary: summary.into(),
            artifacts: vec![],
            files_touched: vec![],
            tests_run: vec![],
            error: Some(error.into()),
            metadata: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Directive {
    pub id: DirectiveId,
    pub origin: String,
    pub title: String,
    pub body: String,
    pub constraints: Vec<String>,
    pub desired_state: Option<String>,
    pub risk: RiskClass,
    pub metadata: BTreeMap<String, String>,
}

impl Directive {
    pub fn new(
        origin: impl Into<String>,
        title: impl Into<String>,
        body: impl Into<String>,
    ) -> Self {
        Self {
            id: DirectiveId::new(),
            origin: origin.into(),
            title: title.into(),
            body: body.into(),
            constraints: vec![],
            desired_state: None,
            risk: RiskClass::Medium,
            metadata: BTreeMap::new(),
        }
    }
}
