//! Core domain model for a recursive Viable System Model (VSM) agent organization.
//!
//! This crate intentionally contains no AMQP/RabbitMQ, HTTP, model-provider, or
//! filesystem code. It defines the organizational genome, VSM channels, typed
//! messages, state-derived capabilities, task traces, and transport traits.

pub mod capability;
pub mod channel;
pub mod fitness;
pub mod genome;
pub mod id;
pub mod message;
pub mod node;
pub mod patch;
pub mod suggestion;
pub mod task;
pub mod trace;
pub mod transport;

pub use capability::*;
pub use channel::*;
pub use fitness::*;
pub use genome::*;
pub use id::*;
pub use message::*;
pub use node::*;
pub use patch::*;
pub use suggestion::*;
pub use task::*;
pub use trace::*;
pub use transport::*;
