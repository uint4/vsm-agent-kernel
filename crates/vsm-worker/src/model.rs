use crate::ModelProviderError;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use vsm_core::ModelSpec;

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ModelUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}

impl ModelUsage {
    pub fn total_tokens(&self) -> u64 {
        self.input_tokens + self.output_tokens
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelRequest {
    pub model: ModelSpec,
    pub instructions: String,
    pub input: String,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelResponse {
    pub output_text: String,
    pub usage: Option<ModelUsage>,
    pub raw: Option<Value>,
}

#[async_trait]
pub trait ModelProvider: Send + Sync {
    async fn complete(&self, request: ModelRequest) -> Result<ModelResponse, ModelProviderError>;
}

/// Deterministic provider used for local wiring tests and transport simulation.
#[derive(Clone, Debug)]
pub struct EchoModelProvider {
    pub prefix: String,
}

impl Default for EchoModelProvider {
    fn default() -> Self {
        Self {
            prefix: "echo".to_string(),
        }
    }
}

#[async_trait]
impl ModelProvider for EchoModelProvider {
    async fn complete(&self, request: ModelRequest) -> Result<ModelResponse, ModelProviderError> {
        let output_text = format!(
            "{}: accepted task.\n\nInstructions:\n{}\n\nInput:\n{}",
            self.prefix, request.instructions, request.input
        );

        Ok(ModelResponse {
            usage: Some(ModelUsage {
                input_tokens: rough_token_estimate(&request.instructions)
                    + rough_token_estimate(&request.input),
                output_tokens: rough_token_estimate(&output_text),
            }),
            output_text,
            raw: None,
        })
    }
}

pub fn rough_token_estimate(text: &str) -> u64 {
    // Cheap fallback for local providers and tests. Real providers should return
    // usage from the API response when available.
    ((text.len() as f64) / 4.0).ceil() as u64
}
