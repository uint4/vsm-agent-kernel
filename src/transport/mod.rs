use crate::error::Result;
use crate::ids::NodeId;
use crate::message::Envelope;
use async_trait::async_trait;
use tokio::sync::mpsc;

pub mod in_memory;

#[cfg(feature = "rabbitmq")]
pub mod rabbitmq;

#[async_trait]
pub trait Transport: Send + Sync {
    async fn publish(&self, envelope: Envelope) -> Result<()>;
    async fn subscribe(&self, node_id: NodeId) -> Result<Subscription>;
}

pub struct Subscription {
    pub node_id: NodeId,
    receiver: mpsc::Receiver<Envelope>,
}

impl Subscription {
    pub fn new(node_id: NodeId, receiver: mpsc::Receiver<Envelope>) -> Self {
        Self { node_id, receiver }
    }

    pub async fn recv(&mut self) -> Option<Envelope> {
        self.receiver.recv().await
    }
}
