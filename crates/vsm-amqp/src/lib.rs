//! AMQP/RabbitMQ transport adapter for `vsm-core`.
//!
//! The core VSM model only depends on the `Transport` trait. This adapter maps
//! typed `MessageEnvelope`s to AMQP messages. It can be replaced with NATS,
//! Kafka, Redis streams, local IPC, HTTP, or any other transport implementing
//! the same trait.

pub mod rabbit;

pub use rabbit::*;
