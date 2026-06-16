//! Core kernel for a recursive VSM-based autonomous coding organization.
//!
//! This crate intentionally separates the organizational model from transport.
//! RabbitMQ, in-memory queues, HTTP, NATS, gRPC, or any other broker can be
//! mounted behind the [`transport::Transport`] trait.

pub mod audit;
pub mod channel;
pub mod error;
pub mod genome;
pub mod ids;
pub mod message;
pub mod node;
pub mod runtime;
pub mod selection;
pub mod task;
pub mod trace;
pub mod transport;

pub use audit::*;
pub use channel::*;
pub use error::*;
pub use genome::*;
pub use ids::*;
pub use message::*;
pub use node::*;
pub use runtime::*;
pub use selection::*;
pub use task::*;
pub use trace::*;
