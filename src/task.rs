use crate::ids::{DirectiveId, NodeId, TaskId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Directive {
    pub id: DirectiveId,
    pub origin: DirectiveOrigin,
    pub body: String,
    pub constraints: Vec<String>,
    pub priority: Priority,
}

impl Directive {
    pub fn user(body: impl Into<String>) -> Self {
        Self {
            id: DirectiveId::new(),
            origin: DirectiveOrigin::UserEnvironment,
            body: body.into(),
            constraints: Vec::new(),
            priority: Priority::Normal,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DirectiveOrigin {
    UserEnvironment,
    System5Policy,
    System4FutureProbe,
    System3Command,
    AlgedonicSignal,
    ExternalEnvironment,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Priority {
    Low,
    #[default]
    Normal,
    High,
    Interrupt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskPacket {
    pub id: TaskId,
    pub directive_id: DirectiveId,
    pub parent_task_id: Option<TaskId>,
    pub assigned_node_id: Option<NodeId>,
    pub goal: String,
    pub target_state: String,
    pub scope: TaskScope,
    pub constraints: Vec<String>,
    pub static_predicates: StaticTaskPredicates,
    pub authority: AuthoritySpec,
    pub dependencies: Vec<TaskId>,
    pub acceptance: AcceptanceSpec,
    pub risk: RiskClass,
    pub status: TaskStatus,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskScope {
    pub likely_files: Vec<String>,
    pub modules: Vec<String>,
    pub languages: Vec<String>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
    pub estimated_blast_radius: BlastRadius,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub enum BlastRadius {
    #[default]
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AuthoritySpec {
    pub allowed_paths: Vec<String>,
    pub denied_paths: Vec<String>,
    pub allowed_tools: Vec<String>,
    pub can_modify_code: bool,
    pub requires_review_before_merge: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AcceptanceSpec {
    pub checks: Vec<String>,
    pub tests: Vec<String>,
    pub human_review_required: bool,
    pub rollback_condition: Option<String>,
    pub escalation_condition: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub enum RiskClass {
    #[default]
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub enum TaskStatus {
    #[default]
    Proposed,
    Assigned,
    Running,
    Blocked,
    Succeeded,
    Failed,
    Rejected,
    Merged,
    Reverted,
}

/// Simple predicate used by routing and relation genes.
/// This is intentionally basic for the MVP. Replace with a richer expression
/// language later if needed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskPredicate {
    Always,
    HasLanguage(String),
    TouchesModule(String),
    TouchesDependency(String),
    HasLabel { key: String, value: String },
    DatabaseTouched,
    AuthTouched,
    SecuritySensitive,
    BlastRadiusAtLeast(BlastRadius),
    All(Vec<TaskPredicate>),
    Any(Vec<TaskPredicate>),
    Not(Box<TaskPredicate>),
}

impl TaskPredicate {
    pub fn matches(&self, p: &StaticTaskPredicates) -> bool {
        match self {
            TaskPredicate::Always => true,
            TaskPredicate::HasLanguage(lang) => p.languages.iter().any(|x| x == lang),
            TaskPredicate::TouchesModule(module) => p.modules.iter().any(|x| x == module),
            TaskPredicate::TouchesDependency(dep) => p.dependencies.iter().any(|x| x == dep),
            TaskPredicate::HasLabel { key, value } => p.labels.get(key) == Some(value),
            TaskPredicate::DatabaseTouched => p.database_touched,
            TaskPredicate::AuthTouched => p.auth_touched,
            TaskPredicate::SecuritySensitive => p.security_sensitive,
            TaskPredicate::BlastRadiusAtLeast(radius) => p.estimated_blast_radius as u8 >= *radius as u8,
            TaskPredicate::All(items) => items.iter().all(|x| x.matches(p)),
            TaskPredicate::Any(items) => items.iter().any(|x| x.matches(p)),
            TaskPredicate::Not(inner) => !inner.matches(p),
        }
    }
}
