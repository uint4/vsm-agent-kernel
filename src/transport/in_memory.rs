use crate::error::{KernelError, Result};
use crate::ids::NodeId;
use crate::message::Envelope;
use crate::transport::{Subscription, Transport};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

#[derive(Clone, Default)]
pub struct InMemoryTransport {
    subscribers: Arc<RwLock<BTreeMap<NodeId, mpsc::Sender<Envelope>>>>,
}

impl InMemoryTransport {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Transport for InMemoryTransport {
    async fn publish(&self, envelope: Envelope) -> Result<()> {
        let Some(target) = envelope.target_node_id.clone() else {
            return Err(KernelError::Transport(
                "in-memory transport requires target_node_id".to_string(),
            ));
        };

        let subscribers = self.subscribers.read().await;
        let Some(sender) = subscribers.get(&target) else {
            return Err(KernelError::Transport(format!(
                "no subscriber registered for target node {}",
                target
            )));
        };

        sender
            .send(envelope)
            .await
            .map_err(|e| KernelError::Transport(e.to_string()))
    }

    async fn subscribe(&self, node_id: NodeId) -> Result<Subscription> {
        let (tx, rx) = mpsc::channel(128);
        self.subscribers.write().await.insert(node_id.clone(), tx);
        Ok(Subscription::new(node_id, rx))
    }
}
