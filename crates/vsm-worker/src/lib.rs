//! Worker harness for operational VSM leaves.
//!
//! This crate wires a `ViableNode` leaf to a transport subscription, model
//! provider, and trace sink. The harness deliberately depends on core traits
//! instead of RabbitMQ, Codex/OpenAI, or any specific code-editing tool.

pub mod codex;
pub mod error;
pub mod harness;
pub mod model;
pub mod prompt;
pub mod trace_sink;

pub use codex::*;
pub use error::*;
pub use harness::*;
pub use model::*;
pub use prompt::*;
pub use trace_sink::*;
