use crate::{
    EventFilter, GenomeSnapshot, GenomeSnapshotRole, LedgerError, LedgerEvent,
    PopulationArchiveRecord, StoredTrialRecord, TraceWindow,
};
use async_trait::async_trait;
use vsm_core::{GenomeId, NodeId, OrganizationalGenome, SuggestionId, TaskTrace, TraceId};

#[async_trait]
pub trait Ledger: Send + Sync {
    async fn append_event(&self, event: LedgerEvent) -> Result<(), LedgerError>;

    async fn write_task_trace(&self, trace: TaskTrace) -> Result<(), LedgerError>;

    async fn get_trace(&self, trace_id: &TraceId) -> Result<Option<TaskTrace>, LedgerError>;

    async fn recent_events(&self, filter: EventFilter) -> Result<Vec<LedgerEvent>, LedgerError>;

    async fn recent_task_traces(&self, window: TraceWindow) -> Result<Vec<TaskTrace>, LedgerError>;

    async fn save_genome_snapshot(&self, snapshot: GenomeSnapshot) -> Result<(), LedgerError>;

    async fn get_genome_snapshot(
        &self,
        genome_id: &GenomeId,
    ) -> Result<Option<GenomeSnapshot>, LedgerError>;

    async fn set_champion_genome(
        &self,
        controller_node_id: &NodeId,
        genome: OrganizationalGenome,
    ) -> Result<(), LedgerError> {
        self.save_genome_snapshot(GenomeSnapshot::new(
            genome.clone(),
            GenomeSnapshotRole::Champion,
        ))
        .await?;
        self.set_champion_genome_id(controller_node_id, &genome.id)
            .await
    }

    async fn set_champion_genome_id(
        &self,
        controller_node_id: &NodeId,
        genome_id: &GenomeId,
    ) -> Result<(), LedgerError>;

    async fn get_champion_genome(
        &self,
        controller_node_id: &NodeId,
    ) -> Result<Option<OrganizationalGenome>, LedgerError> {
        let Some(genome_id) = self.get_champion_genome_id(controller_node_id).await? else {
            return Ok(None);
        };
        Ok(self
            .get_genome_snapshot(&genome_id)
            .await?
            .map(|snapshot| snapshot.genome))
    }

    async fn get_champion_genome_id(
        &self,
        controller_node_id: &NodeId,
    ) -> Result<Option<GenomeId>, LedgerError>;

    async fn write_trial_record(&self, record: StoredTrialRecord) -> Result<(), LedgerError>;

    async fn get_trial_record(
        &self,
        trial_id: &SuggestionId,
    ) -> Result<Option<StoredTrialRecord>, LedgerError>;

    async fn get_active_trial_record(
        &self,
        controller_node_id: &NodeId,
    ) -> Result<Option<StoredTrialRecord>, LedgerError>;

    async fn queued_trial_records(
        &self,
        controller_node_id: &NodeId,
        limit: usize,
    ) -> Result<Vec<StoredTrialRecord>, LedgerError>;

    async fn completed_trial_records(
        &self,
        controller_node_id: &NodeId,
        limit: usize,
    ) -> Result<Vec<StoredTrialRecord>, LedgerError>;

    async fn write_population_archive_record(
        &self,
        record: PopulationArchiveRecord,
    ) -> Result<(), LedgerError>;

    async fn get_population_archive_record(
        &self,
        trial_id: &SuggestionId,
    ) -> Result<Option<PopulationArchiveRecord>, LedgerError>;

    async fn population_archive_records(
        &self,
        controller_node_id: &NodeId,
        limit: usize,
    ) -> Result<Vec<PopulationArchiveRecord>, LedgerError>;

    async fn pareto_archive_records(
        &self,
        controller_node_id: &NodeId,
        limit: usize,
    ) -> Result<Vec<PopulationArchiveRecord>, LedgerError>;

    async fn subtree_task_traces(
        &self,
        node_id: &NodeId,
        window: TraceWindow,
    ) -> Result<Vec<TaskTrace>, LedgerError> {
        let traces = self.recent_task_traces(window).await?;
        Ok(traces
            .into_iter()
            .filter(|trace| {
                &trace.assigned_node_id == node_id
                    || trace
                        .responsible_ancestor_ids
                        .iter()
                        .any(|ancestor| ancestor == node_id)
            })
            .collect())
    }
}
