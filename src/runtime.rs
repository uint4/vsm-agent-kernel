use crate::channel::ChannelType;
use crate::error::Result;
use crate::ids::NodeId;
use crate::message::{Envelope, MessagePayload};
use crate::task::Directive;
use crate::trace::TraceLedger;
use crate::transport::Transport;
use crate::OrganizationalGenome;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct Kernel<T: Transport> {
    pub genome: Arc<RwLock<OrganizationalGenome>>,
    pub transport: T,
    pub trace_ledger: Arc<RwLock<TraceLedger>>,
}

impl<T: Transport> Kernel<T> {
    pub fn new(genome: OrganizationalGenome, transport: T) -> Self {
        Self {
            genome: Arc::new(RwLock::new(genome)),
            transport,
            trace_ledger: Arc::new(RwLock::new(TraceLedger::default())),
        }
    }

    /// Injects a directive from the environment into a target node.
    /// Usually this target is the root node.
    pub async fn inject_directive(&self, target_node_id: NodeId, directive: Directive) -> Result<()> {
        let envelope = Envelope::new(ChannelType::OperationToEnvironment, MessagePayload::Directive(directive))
            .addressed_to(target_node_id);
        self.transport.publish(envelope).await
    }

    /// Returns true if this node is currently allowed to write code according to
    /// the genome, not according to its prompt.
    pub async fn can_node_write_code(&self, node_id: &NodeId) -> Result<bool> {
        let genome = self.genome.read().await;
        let node = genome.get_node(node_id)?;
        Ok(node.capabilities().can_write_code)
    }
}
