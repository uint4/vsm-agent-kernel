use vsm_core::{Directive, RiskClass, TaskPacket};

#[derive(Clone, Debug)]
pub struct DirectiveTaskMapper {
    pub default_requires_code_write: bool,
}

impl Default for DirectiveTaskMapper {
    fn default() -> Self {
        Self {
            default_requires_code_write: true,
        }
    }
}

impl DirectiveTaskMapper {
    pub fn map(&self, directive: &Directive) -> TaskPacket {
        let mut task = TaskPacket::new(directive.title.clone(), directive.body.clone());
        task.directive_id = Some(directive.id.clone());
        task.target_state = directive.desired_state.clone();
        task.constraints = directive.constraints.clone();
        task.risk = directive.risk.clone();
        task.metadata
            .insert("origin".to_string(), directive.origin.clone());
        task.metadata
            .insert("mapped_from_directive".to_string(), directive.id.to_string());

        let requires_code_write = directive
            .metadata
            .get("requires_code_write")
            .map(|value| value == "true")
            .unwrap_or(self.default_requires_code_write);

        task.metadata.insert(
            "requires_code_write".to_string(),
            requires_code_write.to_string(),
        );

        for key in ["required_capability", "target_child", "requires_review"] {
            if let Some(value) = directive.metadata.get(key) {
                task.metadata.insert(key.to_string(), value.clone());
            }
        }

        for (key, value) in &directive.metadata {
            if let Some(stripped) = key.strip_prefix("task.metadata.") {
                task.metadata.insert(stripped.to_string(), value.clone());
            }
        }

        if matches!(directive.risk, RiskClass::High | RiskClass::Critical) {
            task.metadata
                .insert("requires_parent_review".to_string(), "true".to_string());
        }

        task
    }
}
