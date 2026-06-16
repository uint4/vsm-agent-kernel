use crate::{ModelProvider, ModelProviderError, ModelRequest, ModelResponse, ModelUsage};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::env;

#[derive(Clone, Debug)]
pub struct OpenAiCodexConfig {
    pub api_key: String,
    pub base_url: String,
    pub default_model: String,
}

impl OpenAiCodexConfig {
    pub fn from_env() -> Result<Self, ModelProviderError> {
        let api_key = env::var("OPENAI_API_KEY")
            .map_err(|_| ModelProviderError::MissingConfig("OPENAI_API_KEY".to_string()))?;

        let base_url =
            env::var("OPENAI_BASE_URL").unwrap_or_else(|_| "https://api.openai.com/v1".to_string());

        let default_model =
            env::var("OPENAI_CODEX_MODEL").unwrap_or_else(|_| "gpt-5.5".to_string());

        Ok(Self {
            api_key,
            base_url,
            default_model,
        })
    }
}

#[derive(Clone)]
pub struct OpenAiCodexProvider {
    client: reqwest::Client,
    config: OpenAiCodexConfig,
}

impl OpenAiCodexProvider {
    pub fn new(config: OpenAiCodexConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            config,
        }
    }

    pub fn from_env() -> Result<Self, ModelProviderError> {
        Ok(Self::new(OpenAiCodexConfig::from_env()?))
    }

    fn endpoint(&self) -> String {
        format!("{}/responses", self.config.base_url.trim_end_matches('/'))
    }
}

#[async_trait]
impl ModelProvider for OpenAiCodexProvider {
    async fn complete(&self, request: ModelRequest) -> Result<ModelResponse, ModelProviderError> {
        let model = if request.model.model == "unbound" || request.model.model.trim().is_empty() {
            self.config.default_model.clone()
        } else {
            request.model.model.clone()
        };

        let mut body = json!({
            "model": model,
            "instructions": request.instructions,
            "input": request.input,
        });

        if let Some(effort) = request
            .model
            .effort
            .as_ref()
            .filter(|s| !s.trim().is_empty())
        {
            body.as_object_mut()
                .expect("json object")
                .insert("reasoning".to_string(), json!({ "effort": effort }));
        }

        let response = self
            .client
            .post(self.endpoint())
            .bearer_auth(&self.config.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| ModelProviderError::Request(e.to_string()))?;

        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|e| ModelProviderError::Request(e.to_string()))?;

        if !status.is_success() {
            return Err(ModelProviderError::Request(format!(
                "OpenAI Responses API returned {status}: {text}"
            )));
        }

        let raw: Value = serde_json::from_str(&text)
            .map_err(|e| ModelProviderError::InvalidResponse(e.to_string()))?;

        let output_text = extract_output_text(&raw).ok_or_else(|| {
            ModelProviderError::InvalidResponse("missing output_text in response".to_string())
        })?;

        let usage = extract_usage(&raw);

        Ok(ModelResponse {
            output_text,
            usage,
            raw: Some(raw),
        })
    }
}

fn extract_output_text(raw: &Value) -> Option<String> {
    if let Some(text) = raw.get("output_text").and_then(Value::as_str) {
        return Some(text.to_string());
    }

    let mut parts = vec![];
    let output = raw.get("output")?.as_array()?;
    for item in output {
        let Some(content) = item.get("content").and_then(Value::as_array) else {
            continue;
        };

        for part in content {
            if let Some(text) = part.get("text").and_then(Value::as_str) {
                parts.push(text.to_string());
            }
        }
    }

    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n"))
    }
}

fn extract_usage(raw: &Value) -> Option<ModelUsage> {
    let usage = raw.get("usage")?;
    Some(ModelUsage {
        input_tokens: usage
            .get("input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        output_tokens: usage
            .get("output_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
    })
}

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

#[derive(Clone, Debug)]
pub struct CodexCliConfig {
    pub binary: String,
    pub workspace_root: PathBuf,
    pub model: Option<String>,
    pub profile: Option<String>,
    pub sandbox: CodexCliSandbox,
    pub approval: CodexCliApprovalMode,
    pub ephemeral: bool,
    pub json_events: bool,
    pub skip_git_repo_check: bool,
    pub codex_api_key: Option<String>,
    pub extra_args: Vec<String>,
}

impl Default for CodexCliConfig {
    fn default() -> Self {
        Self {
            binary: "codex".to_string(),
            workspace_root: PathBuf::from("."),
            model: None,
            profile: None,
            sandbox: CodexCliSandbox::WorkspaceWrite,
            approval: CodexCliApprovalMode::Never,
            ephemeral: true,
            json_events: true,
            skip_git_repo_check: false,
            codex_api_key: std::env::var("CODEX_API_KEY").ok(),
            extra_args: Vec::new(),
        }
    }
}

impl CodexCliConfig {
    pub fn from_env() -> Self {
        let workspace_root = std::env::var("VSM_WORKSPACE_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("."));

        Self {
            binary: std::env::var("CODEX_BIN").unwrap_or_else(|_| "codex".to_string()),
            workspace_root,
            model: std::env::var("CODEX_MODEL")
                .ok()
                .or_else(|| std::env::var("OPENAI_CODEX_MODEL").ok()),
            profile: std::env::var("CODEX_PROFILE").ok(),
            sandbox: std::env::var("CODEX_SANDBOX")
                .ok()
                .and_then(|value| CodexCliSandbox::parse(&value))
                .unwrap_or(CodexCliSandbox::WorkspaceWrite),
            approval: std::env::var("CODEX_APPROVAL")
                .ok()
                .and_then(|value| CodexCliApprovalMode::parse(&value))
                .unwrap_or(CodexCliApprovalMode::Never),
            ephemeral: std::env::var("CODEX_EPHEMERAL")
                .map(|value| value != "0" && value != "false")
                .unwrap_or(true),
            json_events: std::env::var("CODEX_JSON")
                .map(|value| value != "0" && value != "false")
                .unwrap_or(true),
            skip_git_repo_check: std::env::var("CODEX_SKIP_GIT_REPO_CHECK")
                .map(|value| value == "1" || value == "true")
                .unwrap_or(false),
            codex_api_key: std::env::var("CODEX_API_KEY").ok(),
            extra_args: Vec::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CodexCliSandbox {
    ReadOnly,
    WorkspaceWrite,
    DangerFullAccess,
}

impl CodexCliSandbox {
    fn as_arg(self) -> &'static str {
        match self {
            Self::ReadOnly => "read-only",
            Self::WorkspaceWrite => "workspace-write",
            Self::DangerFullAccess => "danger-full-access",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "read-only" | "readonly" => Some(Self::ReadOnly),
            "workspace-write" | "workspace_write" => Some(Self::WorkspaceWrite),
            "danger-full-access" | "danger_full_access" => Some(Self::DangerFullAccess),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CodexCliApprovalMode {
    Untrusted,
    OnRequest,
    Never,
}

impl CodexCliApprovalMode {
    fn as_arg(self) -> &'static str {
        match self {
            Self::Untrusted => "untrusted",
            Self::OnRequest => "on-request",
            Self::Never => "never",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "untrusted" => Some(Self::Untrusted),
            "on-request" | "on_request" => Some(Self::OnRequest),
            "never" => Some(Self::Never),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CodexCliProvider {
    pub config: CodexCliConfig,
}

impl CodexCliProvider {
    pub fn new(config: CodexCliConfig) -> Self {
        Self { config }
    }

    pub fn from_env() -> Self {
        Self::new(CodexCliConfig::from_env())
    }
}

#[async_trait]
impl ModelProvider for CodexCliProvider {
    async fn complete(&self, request: ModelRequest) -> Result<ModelResponse, ModelProviderError> {
        let provider = self.clone();
        tokio::task::spawn_blocking(move || provider.complete_blocking(request))
            .await
            .map_err(|e| ModelProviderError::Request(format!("codex CLI join error: {e}")))?
    }
}

impl CodexCliProvider {
    fn complete_blocking(
        &self,
        request: ModelRequest,
    ) -> Result<ModelResponse, ModelProviderError> {
        let prompt = render_codex_prompt(&request);
        let fallback_input_tokens = crate::rough_token_estimate(&prompt);

        let mut command = Command::new(&self.config.binary);
        command
            .arg("exec")
            .arg("--cd")
            .arg(&self.config.workspace_root)
            .arg("--sandbox")
            .arg(self.config.sandbox.as_arg())
            .arg("--ask-for-approval")
            .arg(self.config.approval.as_arg())
            .arg("--color")
            .arg("never");

        if self.config.ephemeral {
            command.arg("--ephemeral");
        }
        if self.config.json_events {
            command.arg("--json");
        }
        if self.config.skip_git_repo_check {
            command.arg("--skip-git-repo-check");
        }
        if let Some(model) = &self.config.model {
            command.arg("--model").arg(model);
        }
        if let Some(profile) = &self.config.profile {
            command.arg("--profile").arg(profile);
        }
        if let Some(api_key) = &self.config.codex_api_key {
            command.env("CODEX_API_KEY", api_key);
        }
        for arg in &self.config.extra_args {
            command.arg(arg);
        }
        command.arg("-");

        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| ModelProviderError::Request(format!("failed to spawn codex CLI: {e}")))?;

        if let Some(stdin) = child.stdin.as_mut() {
            stdin.write_all(prompt.as_bytes()).map_err(|e| {
                ModelProviderError::Request(format!("failed to write codex prompt: {e}"))
            })?;
        }

        let output = child.wait_with_output().map_err(|e| {
            ModelProviderError::Request(format!("failed to wait for codex CLI: {e}"))
        })?;

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let parsed = if self.config.json_events {
            parse_codex_jsonl(&stdout)
        } else {
            CodexJsonlSummary {
                final_message: Some(stdout.trim().to_string()),
                ..CodexJsonlSummary::default()
            }
        };

        if !output.status.success() || parsed.turn_failed {
            return Err(ModelProviderError::Request(format!(
                "codex CLI failed; status={}; message={}; stderr_tail={}",
                output.status,
                parsed
                    .final_message
                    .as_deref()
                    .unwrap_or("no final message"),
                tail_chars(&stderr, 2000),
            )));
        }

        let output_text = parsed
            .final_message
            .clone()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| stdout.clone());

        let usage = Some(ModelUsage {
            input_tokens: parsed.input_tokens.unwrap_or(fallback_input_tokens),
            output_tokens: parsed
                .output_tokens
                .unwrap_or_else(|| crate::rough_token_estimate(&output_text)),
        });

        Ok(ModelResponse {
            usage,
            output_text,
            raw: Some(json!({
                "provider": "codex_cli",
                "exit_status": output.status.code(),
                "stderr_tail": tail_chars(&stderr, 4000),
                "json_events": self.config.json_events,
                "event_count": parsed.event_count,
                "turn_failed": parsed.turn_failed,
                "usage": {
                    "input_tokens": parsed.input_tokens,
                    "cached_input_tokens": parsed.cached_input_tokens,
                    "output_tokens": parsed.output_tokens,
                    "reasoning_output_tokens": parsed.reasoning_output_tokens,
                },
                "files_touched": parsed.files_touched,
                "artifacts": parsed.artifacts,
            })),
        })
    }
}

#[derive(Clone, Debug, Default)]
struct CodexJsonlSummary {
    final_message: Option<String>,
    input_tokens: Option<u64>,
    cached_input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    reasoning_output_tokens: Option<u64>,
    turn_failed: bool,
    event_count: u64,
    artifacts: Vec<String>,
    files_touched: Vec<String>,
}

fn parse_codex_jsonl(stdout: &str) -> CodexJsonlSummary {
    let mut summary = CodexJsonlSummary::default();

    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };

        summary.event_count += 1;
        let event_type = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();

        match event_type {
            "turn.completed" => {
                if let Some(usage) = value.get("usage") {
                    summary.input_tokens = usage.get("input_tokens").and_then(Value::as_u64);
                    summary.cached_input_tokens =
                        usage.get("cached_input_tokens").and_then(Value::as_u64);
                    summary.output_tokens = usage.get("output_tokens").and_then(Value::as_u64);
                    summary.reasoning_output_tokens =
                        usage.get("reasoning_output_tokens").and_then(Value::as_u64);
                }
            }
            "turn.failed" | "error" => {
                summary.turn_failed = true;
                if let Some(message) = value.get("message").and_then(Value::as_str) {
                    summary.final_message = Some(message.to_string());
                }
            }
            "item.completed" => {
                if let Some(item) = value.get("item") {
                    let item_type = item.get("type").and_then(Value::as_str).unwrap_or_default();
                    match item_type {
                        "agent_message" => {
                            if let Some(text) = item.get("text").and_then(Value::as_str) {
                                summary.final_message = Some(text.to_string());
                            }
                        }
                        other => {
                            if other.contains("file")
                                || other.contains("patch")
                                || other.contains("change")
                            {
                                summary.artifacts.push(item.to_string());
                                collect_file_paths(item, &mut summary.files_touched);
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    summary.files_touched.sort();
    summary.files_touched.dedup();
    summary
}

fn collect_file_paths(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if matches!(key.as_str(), "path" | "file" | "filename") {
                    if let Some(path) = child.as_str() {
                        out.push(path.to_string());
                    }
                }
                collect_file_paths(child, out);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_file_paths(item, out);
            }
        }
        _ => {}
    }
}

fn tail_chars(value: &str, max_chars: usize) -> String {
    let len = value.chars().count();
    if len <= max_chars {
        return value.to_string();
    }
    value.chars().skip(len - max_chars).collect()
}

fn render_codex_prompt(request: &ModelRequest) -> String {
    let mut out = String::new();
    out.push_str(&request.instructions);
    out.push_str("\n\n--- TASK INPUT ---\n");
    out.push_str(&request.input);

    if !request.metadata.is_empty() {
        out.push_str("\n\n--- HARNESS METADATA ---\n");
        for (key, value) in &request.metadata {
            out.push_str(key);
            out.push_str(": ");
            out.push_str(value);
            out.push('\n');
        }
    }

    out
}
