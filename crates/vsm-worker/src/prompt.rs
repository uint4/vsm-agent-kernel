use crate::ModelRequest;
use std::collections::BTreeMap;
use vsm_core::{PromptComponent, TaskPacket, ViableNode};

#[derive(Clone, Debug, Default)]
pub struct TaskPromptBuilder;

impl TaskPromptBuilder {
    pub fn build(&self, node: &ViableNode, task: &TaskPacket) -> ModelRequest {
        let instructions = build_instructions(node);
        let input = build_task_input(task);

        let mut metadata = BTreeMap::new();
        metadata.insert("node_id".to_string(), node.id.to_string());
        metadata.insert("node_name".to_string(), node.name.clone());
        metadata.insert("task_id".to_string(), task.id.to_string());

        ModelRequest {
            model: node.model.clone(),
            instructions,
            input,
            metadata,
        }
    }
}

fn build_instructions(node: &ViableNode) -> String {
    let mut out = String::new();
    out.push_str(
        "You are an operational leaf in a recursive Viable System Model coding organization.\n",
    );
    out.push_str("You must operate only within the runtime capabilities and permissions granted to this node.\n\n");

    out.push_str("# Node identity\n");
    out.push_str(&format!("Node name: {}\n", node.name));
    out.push_str(&format!("System 5 identity: {}\n", node.system_5.identity));

    if !node.system_5.values.is_empty() {
        out.push_str("Values:\n");
        for value in &node.system_5.values {
            out.push_str(&format!("- {value}\n"));
        }
    }

    if !node.system_5.non_negotiable_constraints.is_empty() {
        out.push_str("\nNon-negotiable constraints:\n");
        for constraint in &node.system_5.non_negotiable_constraints {
            out.push_str(&format!("- {constraint}\n"));
        }
    }

    out.push_str("\n# Runtime capabilities\n");
    out.push_str(&format!("{:?}\n", node.capabilities()));

    out.push_str("\n# Permissions\n");
    out.push_str(&format!(
        "Allowed paths: {:?}\n",
        node.permissions.allowed_paths
    ));
    out.push_str(&format!(
        "Denied paths: {:?}\n",
        node.permissions.denied_paths
    ));
    out.push_str(&format!(
        "Allowed tools: {:?}\n",
        node.permissions.allowed_tools
    ));
    out.push_str(&format!(
        "Denied tools: {:?}\n",
        node.permissions.denied_tools
    ));

    append_section(&mut out, "Behavior rules", &node.prompt.behavior_rules);
    append_section(&mut out, "Domain hints", &node.prompt.domain_hints);
    append_section(
        &mut out,
        "Codebase conventions",
        &node.prompt.codebase_conventions,
    );
    append_section(
        &mut out,
        "Negative constraints",
        &node.prompt.negative_constraints,
    );

    if let Some(component) = node.prompt.output_contract.as_ref().filter(|c| c.active) {
        out.push_str("\n# Output contract\n");
        out.push_str(&component.text);
        out.push('\n');
    } else {
        out.push_str("\n# Output contract\n");
        out.push_str("Return a concise execution summary. If you produce a patch, include it as a unified diff in the response.\n");
    }

    out
}

fn append_section(out: &mut String, title: &str, components: &[PromptComponent]) {
    let active: Vec<&PromptComponent> = components.iter().filter(|c| c.active).collect();
    if active.is_empty() {
        return;
    }

    out.push_str(&format!("\n# {title}\n"));
    for component in active {
        out.push_str(&format!("- {}\n", component.text));
    }
}

fn build_task_input(task: &TaskPacket) -> String {
    serde_json::to_string_pretty(task).unwrap_or_else(|_| {
        format!(
            "Task: {}\nGoal: {}\nScope: {:?}\nConstraints: {:?}",
            task.title, task.goal, task.scope, task.constraints
        )
    })
}
