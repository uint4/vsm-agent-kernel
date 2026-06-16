use serde::{Deserialize, Serialize};

/// Operation capability for an operational leaf. A node with children is a
/// metasystem and must not receive code-writing tools, regardless of prompt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LeafOperationKind {
    Coding,
    Reviewing,
    Testing,
    Research,
    Integration,
    Documentation,
    Planning,
    Custom(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LeafOperationSpec {
    pub kind: Option<LeafOperationKind>,
    pub allows_code_write: bool,
    pub allows_test_execution: bool,
    pub allows_review: bool,
    pub allows_research: bool,
    pub allows_integration: bool,
    pub allows_filesystem_read: bool,
    pub allows_filesystem_write: bool,
}

impl LeafOperationSpec {
    pub fn coding() -> Self {
        Self {
            kind: Some(LeafOperationKind::Coding),
            allows_code_write: true,
            allows_test_execution: true,
            allows_review: false,
            allows_research: true,
            allows_integration: false,
            allows_filesystem_read: true,
            allows_filesystem_write: true,
        }
    }

    pub fn reviewer() -> Self {
        Self {
            kind: Some(LeafOperationKind::Reviewing),
            allows_code_write: false,
            allows_test_execution: true,
            allows_review: true,
            allows_research: true,
            allows_integration: false,
            allows_filesystem_read: true,
            allows_filesystem_write: false,
        }
    }

    pub fn tester() -> Self {
        Self {
            kind: Some(LeafOperationKind::Testing),
            allows_code_write: false,
            allows_test_execution: true,
            allows_review: false,
            allows_research: false,
            allows_integration: false,
            allows_filesystem_read: true,
            allows_filesystem_write: false,
        }
    }
}

/// Runtime-enforced capabilities derived from node state. Do not derive these
/// from the prompt. The prompt is advisory; tools and permissions are hard gates.
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct CapabilitySet {
    pub can_write_code: bool,
    pub can_run_tests: bool,
    pub can_review: bool,
    pub can_research: bool,
    pub can_integrate: bool,
    pub can_read_filesystem: bool,
    pub can_write_filesystem: bool,

    pub can_delegate: bool,
    pub can_audit_children: bool,
    pub can_mutate_child_topology: bool,
    pub can_issue_command: bool,
    pub can_coordinate: bool,
    pub can_allocate_resources: bool,
    pub can_run_future_probes: bool,
}

impl CapabilitySet {
    pub fn for_leaf(operation: &LeafOperationSpec) -> Self {
        Self {
            can_write_code: operation.allows_code_write,
            can_run_tests: operation.allows_test_execution,
            can_review: operation.allows_review,
            can_research: operation.allows_research,
            can_integrate: operation.allows_integration,
            can_read_filesystem: operation.allows_filesystem_read,
            can_write_filesystem: operation.allows_filesystem_write,
            can_delegate: false,
            can_audit_children: false,
            can_mutate_child_topology: false,
            can_issue_command: false,
            can_coordinate: false,
            can_allocate_resources: false,
            can_run_future_probes: false,
        }
    }

    pub fn for_metasystem() -> Self {
        Self {
            can_write_code: false,
            can_run_tests: false,
            can_review: false,
            can_research: true,
            can_integrate: true,
            can_read_filesystem: true,
            can_write_filesystem: false,
            can_delegate: true,
            can_audit_children: true,
            can_mutate_child_topology: true,
            can_issue_command: true,
            can_coordinate: true,
            can_allocate_resources: true,
            can_run_future_probes: true,
        }
    }
}
