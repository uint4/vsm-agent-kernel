use crate::{MessageEnvelope, VsmChannelType};
use async_trait::async_trait;
use futures_core::Stream;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use thiserror::Error;

pub type EnvelopeStream = Pin<Box<dyn Stream<Item = Result<MessageEnvelope, TransportError>> + Send>>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Subscription {
    pub channel_types: Vec<VsmChannelType>,
    pub target_node_id: Option<String>,
    pub queue_name: Option<String>,
    pub durable: bool,
}

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("transport unavailable: {0}")]
    Unavailable(String),

    #[error("transport operation failed: {0}")]
    Operation(String),
}

#[async_trait]
pub trait Transport: Send + Sync {
    async fn publish(&self, envelope: MessageEnvelope) -> Result<(), TransportError>;

    async fn subscribe(&self, subscription: Subscription) -> Result<EnvelopeStream, TransportError>;
}

pub trait ChannelRouter: Send + Sync {
    fn routing_key(&self, envelope: &MessageEnvelope) -> String;
}

#[derive(Clone, Debug, Default)]
pub struct DefaultChannelRouter;

impl ChannelRouter for DefaultChannelRouter {
    fn routing_key(&self, envelope: &MessageEnvelope) -> String {
        let channel = format!("{:?}", envelope.channel_type).to_lowercase();
        let target = envelope
            .target_node_id
            .as_ref()
            .map(|n| n.as_str())
            .unwrap_or("broadcast");
        format!("vsm.{channel}.{target}")
    }
}
