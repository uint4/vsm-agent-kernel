//! RabbitMQ transport adapter.
//!
//! Feature-gated behind `rabbitmq` so the core crate remains broker-agnostic.
//! Routing keys are intentionally simple for the MVP:
//!
//! ```text
//! node.<target_node_id>
//! ```

use crate::error::{KernelError, Result};
use crate::ids::NodeId;
use crate::message::Envelope;
use crate::transport::{Subscription, Transport};
use async_trait::async_trait;
use futures_util::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, ExchangeDeclareOptions,
    QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind};
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct RabbitMqTransport {
    channel: Channel,
    exchange: String,
}

impl RabbitMqTransport {
    pub async fn connect(uri: &str, exchange: impl Into<String>) -> Result<Self> {
        let exchange = exchange.into();
        let conn = Connection::connect(uri, ConnectionProperties::default())
            .await
            .map_err(|e| KernelError::Transport(e.to_string()))?;
        let channel = conn
            .create_channel()
            .await
            .map_err(|e| KernelError::Transport(e.to_string()))?;

        channel
            .exchange_declare(
                &exchange,
                ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    auto_delete: false,
                    internal: false,
                    nowait: false,
                    passive: false,
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| KernelError::Transport(e.to_string()))?;

        Ok(Self { channel, exchange })
    }

    fn routing_key(node_id: &NodeId) -> String {
        format!("node.{}", sanitize_node_id(node_id.as_str()))
    }
}

#[async_trait]
impl Transport for RabbitMqTransport {
    async fn publish(&self, envelope: Envelope) -> Result<()> {
        let Some(target) = envelope.target_node_id.clone() else {
            return Err(KernelError::Transport(
                "RabbitMQ transport requires target_node_id".to_string(),
            ));
        };

        let payload = serde_json::to_vec(&envelope)
            .map_err(|e| KernelError::Serialization(e.to_string()))?;
        let routing_key = Self::routing_key(&target);

        self.channel
            .basic_publish(
                &self.exchange,
                &routing_key,
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default().with_content_type("application/json".into()),
            )
            .await
            .map_err(|e| KernelError::Transport(e.to_string()))?
            .await
            .map_err(|e| KernelError::Transport(e.to_string()))?;

        Ok(())
    }

    async fn subscribe(&self, node_id: NodeId) -> Result<Subscription> {
        let queue_name = format!("vsm-agent-kernel.{}", sanitize_node_id(node_id.as_str()));
        let routing_key = Self::routing_key(&node_id);

        self.channel
            .queue_declare(
                &queue_name,
                QueueDeclareOptions {
                    durable: true,
                    exclusive: false,
                    auto_delete: false,
                    nowait: false,
                    passive: false,
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| KernelError::Transport(e.to_string()))?;

        self.channel
            .queue_bind(
                &queue_name,
                &self.exchange,
                &routing_key,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| KernelError::Transport(e.to_string()))?;

        let mut consumer = self
            .channel
            .basic_consume(
                &queue_name,
                "vsm-agent-kernel",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| KernelError::Transport(e.to_string()))?;

        let (tx, rx) = mpsc::channel(128);
        let node_id_for_task = node_id.clone();

        tokio::spawn(async move {
            while let Some(delivery) = consumer.next().await {
                match delivery {
                    Ok(delivery) => {
                        let parsed = serde_json::from_slice::<Envelope>(&delivery.data);
                        if let Ok(envelope) = parsed {
                            let _ = tx.send(envelope).await;
                        }
                        let _ = delivery.ack(BasicAckOptions::default()).await;
                    }
                    Err(err) => {
                        tracing::warn!(node_id = %node_id_for_task, error = %err, "RabbitMQ consume error");
                    }
                }
            }
        });

        Ok(Subscription::new(node_id, rx))
    }
}

fn sanitize_node_id(id: &str) -> String {
    id.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect()
}
