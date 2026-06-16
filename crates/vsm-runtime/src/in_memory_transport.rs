use async_trait::async_trait;
use futures_core::Stream;
use std::{pin::Pin, sync::Arc};
use tokio::sync::broadcast;
use vsm_core::{
    EnvelopeStream, MessageEnvelope, Subscription, Transport, TransportError, VsmChannelType,
};

#[derive(Clone)]
pub struct InMemoryTransport {
    sender: broadcast::Sender<MessageEnvelope>,
}

impl InMemoryTransport {
    pub fn new(buffer: usize) -> Self {
        let (sender, _) = broadcast::channel(buffer);
        Self { sender }
    }
}

#[async_trait]
impl Transport for InMemoryTransport {
    async fn publish(&self, envelope: MessageEnvelope) -> Result<(), TransportError> {
        self.sender
            .send(envelope)
            .map(|_| ())
            .map_err(|e| TransportError::Operation(e.to_string()))
    }

    async fn subscribe(
        &self,
        subscription: Subscription,
    ) -> Result<EnvelopeStream, TransportError> {
        let mut receiver = self.sender.subscribe();
        let subscription = Arc::new(subscription);

        let stream = async_stream::try_stream! {
            loop {
                let envelope = receiver
                    .recv()
                    .await
                    .map_err(|e| TransportError::Operation(e.to_string()))?;

                if matches_subscription(&subscription, &envelope) {
                    yield envelope;
                }
            }
        };

        Ok(Box::pin(stream)
            as Pin<
                Box<dyn Stream<Item = Result<MessageEnvelope, TransportError>> + Send>,
            >)
    }
}

fn matches_subscription(subscription: &Subscription, envelope: &MessageEnvelope) -> bool {
    let channel_ok = subscription.channel_types.is_empty()
        || subscription.channel_types.contains(&envelope.channel_type);

    let target_ok = match (&subscription.target_node_id, &envelope.target_node_id) {
        (None, _) => true,
        (Some(target), Some(node_id)) => target == node_id.as_str(),
        (Some(_), None) => false,
    };

    channel_ok && target_ok
}

#[allow(dead_code)]
fn _keep_channel_type_import(_value: VsmChannelType) {}
