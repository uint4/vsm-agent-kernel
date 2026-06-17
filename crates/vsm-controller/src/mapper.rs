use std::collections::BTreeSet;
use vsm_core::{Directive, RiskClass, StaticTaskPredicates, TaskPacket};

#[derive(Clone, Debug)]
pub struct DirectiveTaskMapper {
    pub default_requires_code_write: bool,
    pub repository_files: Vec<String>,
    pub domain_keyword_hints: Vec<DomainKeywordHint>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DomainKeywordHint {
    pub marker: String,
    pub keywords: Vec<String>,
}

impl Default for DirectiveTaskMapper {
    fn default() -> Self {
        Self {
            default_requires_code_write: true,
            repository_files: vec![],
            domain_keyword_hints: vec![],
        }
    }
}

impl DirectiveTaskMapper {
    pub fn with_repository_files(
        mut self,
        files: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.repository_files = files.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_domain_keyword_hints<I, K, S>(mut self, hints: I) -> Self
    where
        I: IntoIterator<Item = (K, Vec<S>)>,
        K: Into<String>,
        S: Into<String>,
    {
        self.domain_keyword_hints = hints
            .into_iter()
            .map(|(marker, keywords)| DomainKeywordHint {
                marker: marker.into(),
                keywords: keywords.into_iter().map(Into::into).collect(),
            })
            .collect();
        self
    }

    pub fn map(&self, directive: &Directive) -> TaskPacket {
        let mut task = TaskPacket::new(directive.title.clone(), directive.body.clone());
        task.directive_id = Some(directive.id.clone());
        task.target_state = directive.desired_state.clone();
        task.constraints = directive.constraints.clone();
        task.risk = directive.risk.clone();
        task.static_predicates = extract_static_task_predicates(
            directive,
            &self.repository_files,
            &self.domain_keyword_hints,
        );
        if task.scope.is_empty() && !task.static_predicates.likely_files.is_empty() {
            task.scope = task.static_predicates.likely_files.clone();
        }
        task.metadata
            .insert("origin".to_string(), directive.origin.clone());
        task.metadata.insert(
            "mapped_from_directive".to_string(),
            directive.id.to_string(),
        );

        let requires_code_write = directive
            .metadata
            .get("requires_code_write")
            .map(|value| value == "true")
            .unwrap_or(self.default_requires_code_write);

        task.metadata.insert(
            "requires_code_write".to_string(),
            requires_code_write.to_string(),
        );
        if !task.static_predicates.likely_files.is_empty() {
            task.metadata
                .entry("estimated_files_touched".to_string())
                .or_insert_with(|| task.static_predicates.likely_files.len().to_string());
        }

        for key in [
            "required_capability",
            "target_child",
            "requires_review",
            "trial_approved",
            "task_class",
            "estimated_files_touched",
        ] {
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

fn extract_static_task_predicates(
    directive: &Directive,
    repository_files: &[String],
    domain_keyword_hints: &[DomainKeywordHint],
) -> StaticTaskPredicates {
    let mut predicates = StaticTaskPredicates::default();
    let corpus = directive_corpus(directive);
    let corpus_lower = corpus.to_lowercase();

    for token in path_like_tokens(&corpus) {
        insert_unique(&mut predicates.likely_files, token);
    }

    for file in repository_files {
        if repository_file_matches(file, &corpus_lower) {
            insert_unique(&mut predicates.likely_files, file.clone());
        }
    }

    if mentions_any(
        &corpus_lower,
        &["cargo", "crate", "dependency", "dependencies"],
    ) {
        for file in repository_files {
            if file == "Cargo.toml" || file.ends_with("/Cargo.toml") {
                insert_unique(&mut predicates.likely_files, file.clone());
            }
        }
    }

    infer_languages(&mut predicates, &corpus_lower);
    for file in &predicates.likely_files.clone() {
        infer_file_predicates(&mut predicates, file);
    }
    infer_text_predicates(&mut predicates, &corpus_lower);
    infer_configured_domain_markers(&mut predicates, &corpus_lower, domain_keyword_hints);
    merge_metadata_predicates(&mut predicates, directive);

    if predicates.estimated_blast_radius.is_none() {
        predicates.estimated_blast_radius = inferred_blast_radius(&predicates);
    }

    normalize_predicates(&mut predicates);
    predicates
}

fn directive_corpus(directive: &Directive) -> String {
    let mut parts = vec![directive.title.clone(), directive.body.clone()];
    if let Some(desired_state) = &directive.desired_state {
        parts.push(desired_state.clone());
    }
    parts.extend(directive.constraints.iter().cloned());
    parts.extend(directive.metadata.values().cloned());
    parts.join("\n")
}

fn path_like_tokens(text: &str) -> Vec<String> {
    text.split_whitespace()
        .filter_map(|token| {
            let token = token.trim_matches(|ch: char| {
                matches!(
                    ch,
                    '`' | '\'' | '"' | ',' | ';' | ':' | '(' | ')' | '[' | ']' | '{' | '}'
                )
            });
            let token = token.trim_end_matches('.');
            if token.contains('/') || token_has_known_extension(token) {
                Some(token.to_string())
            } else {
                None
            }
        })
        .collect()
}

fn token_has_known_extension(token: &str) -> bool {
    let lower = token.to_lowercase();
    [
        ".rs", ".toml", ".md", ".json", ".yaml", ".yml", ".sql", ".ts", ".tsx", ".js", ".jsx",
        ".py", ".go", ".java", ".rb", ".sh",
    ]
    .iter()
    .any(|extension| lower.ends_with(extension))
}

fn repository_file_matches(file: &str, corpus_lower: &str) -> bool {
    let normalized = file.to_lowercase();
    if corpus_lower.contains(&normalized) {
        return true;
    }

    let Some(file_name) = normalized.rsplit('/').next() else {
        return false;
    };
    if file_name.len() >= 5 && contains_wordish(corpus_lower, file_name) {
        return true;
    }

    let stem = file_name.split('.').next().unwrap_or(file_name);
    stem.len() >= 4 && contains_wordish(corpus_lower, stem)
}

fn contains_wordish(haystack: &str, needle: &str) -> bool {
    haystack
        .split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' || ch == '.'))
        .any(|part| part == needle)
}

fn infer_languages(predicates: &mut StaticTaskPredicates, corpus_lower: &str) {
    for (language, hints) in [
        ("rust", &["rust", "cargo", "crate", ".rs"][..]),
        ("typescript", &["typescript", ".ts", ".tsx"][..]),
        ("javascript", &["javascript", ".js", ".jsx"][..]),
        ("python", &["python", ".py"][..]),
        ("sql", &["sql", ".sql"][..]),
        ("markdown", &["markdown", ".md", "readme"][..]),
        ("toml", &["toml", "cargo.toml"][..]),
    ] {
        if hints.iter().any(|hint| corpus_lower.contains(hint)) {
            insert_unique(&mut predicates.languages, language.to_string());
        }
    }
}

fn infer_file_predicates(predicates: &mut StaticTaskPredicates, file: &str) {
    let lower = file.to_lowercase();
    if lower.ends_with(".rs") {
        insert_unique(&mut predicates.languages, "rust".to_string());
    } else if lower.ends_with(".toml") {
        insert_unique(&mut predicates.languages, "toml".to_string());
        predicates.config_touched = true;
    } else if lower.ends_with(".md") {
        insert_unique(&mut predicates.languages, "markdown".to_string());
    } else if lower.ends_with(".sql") {
        insert_unique(&mut predicates.languages, "sql".to_string());
        predicates.database_touched = true;
        insert_domain_marker(predicates, "data_store", "sql");
    } else if lower.ends_with(".ts") || lower.ends_with(".tsx") {
        insert_unique(&mut predicates.languages, "typescript".to_string());
    } else if lower.ends_with(".js") || lower.ends_with(".jsx") {
        insert_unique(&mut predicates.languages, "javascript".to_string());
    } else if lower.ends_with(".py") {
        insert_unique(&mut predicates.languages, "python".to_string());
    }

    if lower.contains("migrations/") || lower.contains("/migration") {
        predicates.migration_touched = true;
        predicates.database_touched = true;
        insert_domain_marker(predicates, "migration", "true");
    }
    if lower.contains("auth") || lower.contains("security") {
        predicates.auth_touched = true;
        predicates.security_sensitive = true;
        if lower.contains("auth") {
            insert_domain_marker(predicates, "auth", "true");
        }
        if lower.contains("security") {
            insert_domain_marker(predicates, "security", "true");
        }
    }
    if lower.contains("api") || lower.contains("route") {
        predicates.public_api_touched = true;
        insert_domain_marker(predicates, "public_interface", "true");
    }
    if lower.ends_with("cargo.toml")
        || lower.ends_with(".json")
        || lower.ends_with(".yaml")
        || lower.ends_with(".yml")
    {
        predicates.config_touched = true;
        insert_domain_marker(predicates, "configuration", "true");
    }

    if lower.contains("_test.") || lower.contains(".test.") || lower.contains("/tests/") {
        insert_unique(&mut predicates.test_targets, file.to_string());
    }

    for module in modules_from_path(file) {
        insert_unique(&mut predicates.modules, module);
    }
}

fn infer_text_predicates(predicates: &mut StaticTaskPredicates, corpus_lower: &str) {
    if mentions_any(
        corpus_lower,
        &["security", "secret", "credential", "vulnerability"],
    ) {
        predicates.security_sensitive = true;
        insert_domain_marker(predicates, "security", "true");
    }
    if mentions_any(
        corpus_lower,
        &["public api", "endpoint", "route", "interface"],
    ) {
        predicates.public_api_touched = true;
    }
    if mentions_any(corpus_lower, &["config", "configuration", "settings"]) {
        predicates.config_touched = true;
    }
    if mentions_any(corpus_lower, &["test", "tests", "verify", "validation"]) {
        insert_unique(&mut predicates.tags, "verification".to_string());
    }
    if corpus_lower.contains("cargo test") {
        insert_unique(&mut predicates.test_targets, "cargo test".to_string());
    }
}

fn infer_configured_domain_markers(
    predicates: &mut StaticTaskPredicates,
    corpus_lower: &str,
    hints: &[DomainKeywordHint],
) {
    for hint in hints {
        if hint
            .keywords
            .iter()
            .any(|keyword| corpus_lower.contains(&keyword.to_lowercase()))
        {
            insert_domain_marker(predicates, &hint.marker, "true");
        }
    }
}

fn mentions_any(text: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| text.contains(needle))
}

fn modules_from_path(file: &str) -> Vec<String> {
    let mut modules = Vec::new();
    let parts = file.split('/').collect::<Vec<_>>();
    for window in parts.windows(2) {
        if window[0] == "crates" {
            modules.push(window[1].to_string());
        }
    }
    if let Some(file_name) = parts.last() {
        if let Some(stem) = file_name.split('.').next() {
            if stem.len() >= 3 {
                modules.push(stem.to_string());
            }
        }
    }
    modules
}

fn merge_metadata_predicates(predicates: &mut StaticTaskPredicates, directive: &Directive) {
    for value in metadata_values(directive, "languages") {
        extend_unique(&mut predicates.languages, split_metadata_list(&value));
    }
    for value in metadata_values(directive, "likely_files") {
        extend_unique(&mut predicates.likely_files, split_metadata_list(&value));
    }
    for value in metadata_values(directive, "modules") {
        extend_unique(&mut predicates.modules, split_metadata_list(&value));
    }
    for value in metadata_values(directive, "dependencies") {
        extend_unique(&mut predicates.dependencies, split_metadata_list(&value));
    }
    for value in metadata_values(directive, "test_targets") {
        extend_unique(&mut predicates.test_targets, split_metadata_list(&value));
    }
    for value in metadata_values(directive, "tags") {
        extend_unique(&mut predicates.tags, split_metadata_list(&value));
    }
    for value in metadata_values(directive, "domain_markers") {
        merge_domain_marker_list(predicates, &value);
    }
    for (key, value) in &directive.metadata {
        for prefix in [
            "domain_marker.",
            "static_predicates.domain_marker.",
            "task.static_predicates.domain_marker.",
        ] {
            if let Some(marker) = key.strip_prefix(prefix) {
                insert_domain_marker(predicates, marker, value);
            }
        }
    }

    if metadata_bool(directive, "public_api_touched").unwrap_or(false) {
        predicates.public_api_touched = true;
    }
    if metadata_bool(directive, "database_touched").unwrap_or(false) {
        predicates.database_touched = true;
    }
    if metadata_bool(directive, "auth_touched").unwrap_or(false) {
        predicates.auth_touched = true;
    }
    if metadata_bool(directive, "config_touched").unwrap_or(false) {
        predicates.config_touched = true;
    }
    if metadata_bool(directive, "migration_touched").unwrap_or(false) {
        predicates.migration_touched = true;
    }
    if metadata_bool(directive, "security_sensitive").unwrap_or(false) {
        predicates.security_sensitive = true;
    }

    if let Some(value) = metadata_values(directive, "estimated_blast_radius").last() {
        predicates.estimated_blast_radius = parse_risk_class(value);
    }
}

fn metadata_values(directive: &Directive, key: &str) -> Vec<String> {
    [
        key.to_string(),
        format!("task.{key}"),
        format!("task.metadata.{key}"),
        format!("static_predicates.{key}"),
        format!("task.static_predicates.{key}"),
    ]
    .iter()
    .filter_map(|candidate| directive.metadata.get(candidate).cloned())
    .collect()
}

fn metadata_bool(directive: &Directive, key: &str) -> Option<bool> {
    metadata_values(directive, key)
        .last()
        .map(|value| value == "true" || value == "1" || value.eq_ignore_ascii_case("yes"))
}

fn split_metadata_list(value: &str) -> Vec<String> {
    value
        .split([',', ';', '|', '\n'])
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn inferred_blast_radius(predicates: &StaticTaskPredicates) -> Option<RiskClass> {
    if predicates.security_sensitive
        || predicates.auth_touched
        || predicates.database_touched
        || predicates.migration_touched
    {
        Some(RiskClass::High)
    } else if predicates.public_api_touched || predicates.config_touched {
        Some(RiskClass::Medium)
    } else if !predicates.likely_files.is_empty() {
        Some(RiskClass::Low)
    } else {
        None
    }
}

fn parse_risk_class(value: &str) -> Option<RiskClass> {
    match value.to_lowercase().as_str() {
        "low" => Some(RiskClass::Low),
        "medium" => Some(RiskClass::Medium),
        "high" => Some(RiskClass::High),
        "critical" => Some(RiskClass::Critical),
        _ => None,
    }
}

fn normalize_predicates(predicates: &mut StaticTaskPredicates) {
    dedupe_sort(&mut predicates.languages);
    dedupe_sort(&mut predicates.likely_files);
    dedupe_sort(&mut predicates.modules);
    dedupe_sort(&mut predicates.dependencies);
    dedupe_sort(&mut predicates.test_targets);
    dedupe_sort(&mut predicates.tags);
}

fn merge_domain_marker_list(predicates: &mut StaticTaskPredicates, value: &str) {
    for marker in split_metadata_list(value) {
        if let Some((key, value)) = marker.split_once('=') {
            insert_domain_marker(predicates, key.trim(), value.trim());
        } else {
            insert_domain_marker(predicates, marker.trim(), "true");
        }
    }
}

fn insert_unique(values: &mut Vec<String>, value: String) {
    if !value.is_empty() && !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

fn insert_domain_marker(predicates: &mut StaticTaskPredicates, key: &str, value: &str) {
    let key = key.trim();
    let value = value.trim();
    if !key.is_empty() && !value.is_empty() {
        predicates
            .domain_markers
            .insert(key.to_string(), value.to_string());
    }
}

fn extend_unique(values: &mut Vec<String>, additions: Vec<String>) {
    for value in additions {
        insert_unique(values, value);
    }
}

fn dedupe_sort(values: &mut Vec<String>) {
    let set = values.drain(..).collect::<BTreeSet<_>>();
    values.extend(set);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapper_extracts_static_predicates_from_repository_inventory() {
        let mapper = DirectiveTaskMapper::default()
            .with_repository_files([
                "Cargo.toml",
                "crates/vsm-controller/src/mapper.rs",
                "crates/vsm-controller/src/runtime.rs",
                "crates/vsm-core/src/task.rs",
                "crates/vsm-worker/src/harness.rs",
            ])
            .with_domain_keyword_hints([(
                "predicate_extraction",
                vec!["predicate", "predicates", "static predicates"],
            )]);
        let directive = Directive::new(
            "user",
            "Improve mapper predicates",
            "Update mapper.rs so TaskPacket static predicates identify Cargo.toml dependency changes. Run cargo test -p vsm-controller.",
        );

        let task = mapper.map(&directive);

        assert!(task
            .static_predicates
            .likely_files
            .iter()
            .any(|file| file == "crates/vsm-controller/src/mapper.rs"));
        assert!(task
            .static_predicates
            .likely_files
            .iter()
            .any(|file| file == "Cargo.toml"));
        assert!(task
            .static_predicates
            .languages
            .iter()
            .any(|language| language == "rust"));
        assert!(task
            .static_predicates
            .languages
            .iter()
            .any(|language| language == "toml"));
        assert!(task
            .static_predicates
            .modules
            .iter()
            .any(|module| module == "vsm-controller"));
        assert!(task.static_predicates.config_touched);
        assert_eq!(
            task.static_predicates
                .domain_markers
                .get("predicate_extraction")
                .map(String::as_str),
            Some("true")
        );
        assert!(task
            .static_predicates
            .test_targets
            .iter()
            .any(|target| target == "cargo test"));
        assert_eq!(task.scope, task.static_predicates.likely_files);
        let expected_file_count = task.static_predicates.likely_files.len().to_string();
        assert_eq!(
            task.metadata.get("estimated_files_touched"),
            Some(&expected_file_count)
        );
    }

    #[test]
    fn mapper_merges_explicit_static_predicate_metadata() {
        let mut directive = Directive::new(
            "user",
            "Patch ledger persistence",
            "update the ledger persistence layer",
        );
        directive.metadata.insert(
            "static_predicates.likely_files".to_string(),
            "crates/vsm-core/src/task.rs, README.md".to_string(),
        );
        directive.metadata.insert(
            "static_predicates.languages".to_string(),
            "rust,markdown".to_string(),
        );
        directive.metadata.insert(
            "static_predicates.dependencies".to_string(),
            "serde_json".to_string(),
        );
        directive.metadata.insert(
            "static_predicates.domain_markers".to_string(),
            "storage=sqlite,ledger=true".to_string(),
        );
        directive.metadata.insert(
            "static_predicates.domain_marker.persistence".to_string(),
            "durable".to_string(),
        );
        directive
            .metadata
            .insert("estimated_blast_radius".to_string(), "critical".to_string());

        let task = DirectiveTaskMapper::default().map(&directive);

        assert_eq!(
            task.static_predicates.estimated_blast_radius,
            Some(RiskClass::Critical)
        );
        assert_eq!(
            task.static_predicates
                .domain_markers
                .get("storage")
                .map(String::as_str),
            Some("sqlite")
        );
        assert_eq!(
            task.static_predicates
                .domain_markers
                .get("ledger")
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            task.static_predicates
                .domain_markers
                .get("persistence")
                .map(String::as_str),
            Some("durable")
        );
        assert!(!task.static_predicates.auth_touched);
        assert!(!task.static_predicates.database_touched);
        assert!(task
            .static_predicates
            .likely_files
            .iter()
            .any(|file| file == "README.md"));
        assert!(task
            .static_predicates
            .dependencies
            .iter()
            .any(|dependency| dependency == "serde_json"));
        assert!(task
            .static_predicates
            .languages
            .iter()
            .any(|language| language == "markdown"));
    }
}
