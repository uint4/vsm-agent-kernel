use crate::ControllerError;
use vsm_core::{NodeId, OrganizationalGenome, TaskPacket, ViableNode};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RoutingStrategy {
    /// Prefer explicit assignment or metadata target, then first eligible direct
    /// child. This keeps VSM recursion intact: a parent routes to its System 1
    /// children, not arbitrary descendants.
    DirectChildFirstEligible,
}

impl Default for RoutingStrategy {
    fn default() -> Self {
        Self::DirectChildFirstEligible
    }
}

#[derive(Clone, Debug)]
pub struct RoutingDecision {
    pub child_id: NodeId,
    pub reason: String,
}

#[derive(Clone, Debug, Default)]
pub struct TaskRouter {
    pub strategy: RoutingStrategy,
}

impl TaskRouter {
    pub fn choose_child(
        &self,
        genome: &OrganizationalGenome,
        parent: &ViableNode,
        task: &TaskPacket,
    ) -> Result<RoutingDecision, ControllerError> {
        match self.strategy {
            RoutingStrategy::DirectChildFirstEligible => choose_direct_child(genome, parent, task),
        }
    }
}

fn choose_direct_child(
    genome: &OrganizationalGenome,
    parent: &ViableNode,
    task: &TaskPacket,
) -> Result<RoutingDecision, ControllerError> {
    if parent.children.is_empty() {
        return Err(ControllerError::NotMetasystem(parent.id.clone()));
    }

    if let Some(assigned_to) = &task.assigned_to {
        if parent
            .children
            .iter()
            .any(|child_id| child_id == assigned_to)
        {
            return Ok(RoutingDecision {
                child_id: assigned_to.clone(),
                reason: "task already assigned to direct child".to_string(),
            });
        }
    }

    if let Some(target_child) = task.metadata.get("target_child") {
        for child_id in &parent.children {
            let child = genome.get_node(child_id)?;
            if child.id.as_str() == target_child || child.name == *target_child {
                return Ok(RoutingDecision {
                    child_id: child.id.clone(),
                    reason: format!("matched target_child metadata: {target_child}"),
                });
            }
        }
    }

    let requires_code_write = task
        .metadata
        .get("requires_code_write")
        .map(|value| value == "true")
        .unwrap_or(false);
    let required_capability = task.metadata.get("required_capability").map(String::as_str);

    for child_id in &parent.children {
        let child = genome.get_node(child_id)?;
        if child.status == vsm_core::NodeLifecycleStatus::Retired {
            continue;
        }
        if child.is_metasystem() {
            return Ok(RoutingDecision {
                child_id: child.id.clone(),
                reason: "direct child is a metasystem capable of further delegation".to_string(),
            });
        }

        let capabilities = child.capabilities();
        let code_ok = !requires_code_write || capabilities.can_write_code;
        let required_ok = required_capability
            .map(|capability| capability_allowed(capability, &capabilities))
            .unwrap_or(true);

        if code_ok && required_ok {
            return Ok(RoutingDecision {
                child_id: child.id.clone(),
                reason: "first direct leaf child with required capabilities".to_string(),
            });
        }
    }

    Err(ControllerError::NoRouteableChild {
        node_id: parent.id.clone(),
        task_title: task.title.clone(),
    })
}

fn capability_allowed(capability: &str, capabilities: &vsm_core::CapabilitySet) -> bool {
    match capability {
        "write_code" => capabilities.can_write_code,
        "run_tests" => capabilities.can_run_tests,
        "review" => capabilities.can_review,
        "research" => capabilities.can_research,
        "integrate" => capabilities.can_integrate,
        "read_filesystem" => capabilities.can_read_filesystem,
        "write_filesystem" => capabilities.can_write_filesystem,
        _ => true,
    }
}
