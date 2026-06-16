use async_trait::async_trait;
use futures_core::Stream;
use futures_util::StreamExt;
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, ExchangeDeclareOptions,
        QueueBindOptions, QueueDeclareOptions,
    },
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
};
use std::{pin::Pin, sync::Arc};
use thiserror::Error;
use vsm_core::{
    ChannelRouter, DefaultChannelRouter, EnvelopeStream, MessageEnvelope, Subscription, Transport,
    TransportError, VsmChannelType,
};

#[derive(Clone, Debug)]
pub struct RabbitMqConfig {
    pub uri: String,
    pub exchange: String,
    pub consumer_tag: String,
}

impl RabbitMqConfig {
    pub fn local_default() -> Self {
        Self {
            uri: "amqp://guest:guest@localhost:5672/%2f".to_string(),
            exchange: "vsm.events".to_string(),
            consumer_tag: "vsm-consumer".to_string(),
        }
    }
}

#[derive(Debug, Error)]
pub enum RabbitTransportError {
    #[error(transparent)]
    Lapin(#[from] lapin::Error),

    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}

pub struct RabbitMqTransport<R = DefaultChannelRouter> {
    config: RabbitMqConfig,
    #[allow(dead_code)]
    connection: Connection,
    channel: Channel,
    router: Arc<R>,
}

impl RabbitMqTransport<DefaultChannelRouter> {
    pub async fn connect(config: RabbitMqConfig) -> Result<Self, RabbitTransportError> {
        Self::connect_with_router(config, DefaultChannelRouter).await
    }
}

impl<R> RabbitMqTransport<R>
where
    R: ChannelRouter + 'static,
{
    pub async fn connect_with_router(
        config: RabbitMqConfig,
        router: R,
    ) -> Result<Self, RabbitTransportError> {
        let connection = Connection::connect(&config.uri, ConnectionProperties::default()).await?;
        let channel = connection.create_channel().await?;

        channel
            .exchange_declare(
                &config.exchange,
                ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    ..ExchangeDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await?;

        Ok(Self {
            config,
            connection,
            channel,
            router: Arc::new(router),
        })
    }
}

#[async_trait]
impl<R> Transport for RabbitMqTransport<R>
where
    R: ChannelRouter + Send + Sync + 'static,
{
    async fn publish(&self, envelope: MessageEnvelope) -> Result<(), TransportError> {
        let routing_key = self.router.routing_key(&envelope);
        let payload = serde_json::to_vec(&envelope)
            .map_err(|e| TransportError::Serialization(e.to_string()))?;

        self.channel
            .basic_publish(
                &self.config.exchange,
                &routing_key,
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default()
                    .with_content_type("application/json".into())
                    .with_message_id(envelope.id.to_string().into())
                    .with_type(envelope.payload_type.clone().into()),
            )
            .await
            .map_err(|e| TransportError::Operation(e.to_string()))?
            .await
            .map_err(|e| TransportError::Operation(e.to_string()))?;

        Ok(())
    }

    async fn subscribe(
        &self,
        subscription: Subscription,
    ) -> Result<EnvelopeStream, TransportError> {
        let queue_name = subscription
            .queue_name
            .clone()
            .unwrap_or_else(|| "vsm.generated".to_string());

        let queue = self
            .channel
            .queue_declare(
                &queue_name,
                QueueDeclareOptions {
                    durable: subscription.durable,
                    exclusive: false,
                    auto_delete: !subscription.durable,
                    ..QueueDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| TransportError::Operation(e.to_string()))?;

        let routing_keys = routing_keys_for_subscription(&subscription);
        for key in routing_keys {
            self.channel
                .queue_bind(
                    queue.name().as_str(),
                    &self.config.exchange,
                    &key,
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await
                .map_err(|e| TransportError::Operation(e.to_string()))?;
        }

        let consumer = self
            .channel
            .basic_consume(
                queue.name().as_str(),
                &self.config.consumer_tag,
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| TransportError::Operation(e.to_string()))?;

        let stream = consumer.filter_map(|delivery_result| async {
            match delivery_result {
                Ok(delivery) => {
                    let parsed = serde_json::from_slice::<MessageEnvelope>(&delivery.data)
                        .map_err(|e| TransportError::Serialization(e.to_string()));

                    let ack_result = delivery.ack(BasicAckOptions::default()).await;
                    if let Err(err) = ack_result {
                        return Some(Err(TransportError::Operation(err.to_string())));
                    }

                    Some(parsed)
                }
                Err(err) => Some(Err(TransportError::Operation(err.to_string()))),
            }
        });

        Ok(Box::pin(stream)
            as Pin<
                Box<dyn Stream<Item = Result<MessageEnvelope, TransportError>> + Send>,
            >)
    }
}

fn routing_keys_for_subscription(subscription: &Subscription) -> Vec<String> {
    if subscription.channel_types.is_empty() {
        return vec!["vsm.#".to_string()];
    }

    subscription
        .channel_types
        .iter()
        .map(|channel_type| {
            let channel = format!("{:?}", channel_type).to_lowercase();
            match &subscription.target_node_id {
                Some(target) => format!("vsm.{channel}.{target}"),
                None => format!("vsm.{channel}.*"),
            }
        })
        .collect()
}

#[allow(dead_code)]
fn _keep_channel_type_import(_value: VsmChannelType) {}
