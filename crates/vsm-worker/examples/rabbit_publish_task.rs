use std::{env, sync::Arc};
use vsm_amqp::{RabbitMqConfig, RabbitMqTransport};
use vsm_core::{envelope_for_task, NodeId, TaskPacket, Transport};

#[tokio::main]
async fn main() -> anyhow_free::Result<()> {
    let root_id = NodeId::from_string(env::var("VSM_ROOT_NODE_ID").unwrap_or_else(|_| "root-controller".to_string()));
    let worker_id = NodeId::from_string(env::var("VSM_WORKER_NODE_ID").unwrap_or_else(|_| "primary-code-service".to_string()));

    let mut rabbit_config = RabbitMqConfig::local_default();
    if let Ok(uri) = env::var("RABBITMQ_URI") {
        rabbit_config.uri = uri;
    }
    if let Ok(exchange) = env::var("VSM_RABBITMQ_EXCHANGE") {
        rabbit_config.exchange = exchange;
    }
    rabbit_config.consumer_tag = "vsm-task-publisher".to_string();

    let transport = Arc::new(RabbitMqTransport::connect(rabbit_config).await?);

    let mut task = TaskPacket::new(
        env::var("VSM_TASK_TITLE").unwrap_or_else(|_| "Smoke-test the worker harness".to_string()),
        env::var("VSM_TASK_GOAL").unwrap_or_else(|_| {
            "Return a concise plan proving that the worker harness received and processed this task.".to_string()
        }),
    );
    task.assigned_to = Some(worker_id.clone());
    task.metadata
        .insert("required_capability".to_string(), "research".to_string());

    let envelope = envelope_for_task(&task)?.with_route(Some(root_id), Some(worker_id.clone()));
    transport.publish(envelope).await?;
    println!("published task {} to {worker_id}", task.id);

    Ok(())
}

mod anyhow_free {
    pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    pub type Result<T> = std::result::Result<T, Error>;
}
