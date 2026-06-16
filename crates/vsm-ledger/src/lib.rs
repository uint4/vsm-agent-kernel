//! Durable empirical ledger for the VSM agent organization.
//!
//! This crate is intentionally model-provider and transport agnostic. It stores
//! what happened: messages, directives, routing decisions, task results, traces,
//! algedonic signals, audit findings, gene suggestions, and genome patches.
//! Genetic selection should read from this crate rather than guessing latent
//! costs and benefits in advance.

pub mod error;
pub mod event;
pub mod ledger;
pub mod memory;
pub mod sqlite;
pub mod state;

pub use error::*;
pub use event::*;
pub use ledger::*;
pub use memory::*;
pub use sqlite::*;
pub use state::*;

#[cfg(test)]
mod tests {
    use vsm_core::{
        GeneSuggestion, GeneSuggestionSource, LeafOperationSpec, OrganizationalGenome,
        OrganizationalGenomePatch, ViableNode,
    };

    use super::*;

    fn genome() -> OrganizationalGenome {
        let root = ViableNode::new_metasystem("root");
        let root_id = root.id.clone();
        let mut genome = OrganizationalGenome::new(root);
        let coder = ViableNode::new_leaf("coder", LeafOperationSpec::coding());
        genome.add_child(&root_id, coder).expect("child");
        genome
    }

    fn trial_record(
        controller: vsm_core::NodeId,
        genome: &OrganizationalGenome,
    ) -> StoredTrialRecord {
        let reviewer = ViableNode::new_leaf("reviewer", LeafOperationSpec::reviewer());
        let suggestion = GeneSuggestion::new(
            controller.clone(),
            controller.clone(),
            GeneSuggestionSource::System3StarAudit,
            OrganizationalGenomePatch::AddChild {
                parent_id: controller.clone(),
                child: reviewer,
            },
            "persist trial",
        );
        StoredTrialRecord::active(
            controller,
            genome.id.clone(),
            vsm_core::GenomeId::new(),
            suggestion,
        )
    }

    fn queued_trial_record(
        controller: vsm_core::NodeId,
        genome: &OrganizationalGenome,
    ) -> StoredTrialRecord {
        let reviewer = ViableNode::new_leaf("queued-reviewer", LeafOperationSpec::reviewer());
        let suggestion = GeneSuggestion::new(
            controller.clone(),
            controller.clone(),
            GeneSuggestionSource::System3StarAudit,
            OrganizationalGenomePatch::AddChild {
                parent_id: controller.clone(),
                child: reviewer,
            },
            "queue trial",
        );
        StoredTrialRecord::queued(
            controller,
            genome.id.clone(),
            vsm_core::GenomeId::new(),
            suggestion,
        )
    }

    async fn assert_state_roundtrip(ledger: &dyn Ledger) {
        let genome = genome();
        let controller = genome.root_node_id.clone();
        ledger
            .set_champion_genome(&controller, genome.clone())
            .await
            .expect("champion");

        let loaded = ledger
            .get_champion_genome(&controller)
            .await
            .expect("load champion")
            .expect("champion exists");
        assert_eq!(loaded.id, genome.id);

        let record = trial_record(controller.clone(), &genome);
        let trial_id = record.trial_id.clone();
        ledger
            .write_trial_record(record.clone())
            .await
            .expect("trial");

        let loaded_record = ledger
            .get_trial_record(&trial_id)
            .await
            .expect("load trial")
            .expect("trial exists");
        assert_eq!(loaded_record.trial_id, trial_id);
        assert_eq!(loaded_record.status, StoredTrialStatus::Active);

        let active = ledger
            .get_active_trial_record(&controller)
            .await
            .expect("active trial")
            .expect("active exists");
        assert_eq!(active.trial_id, trial_id);

        let queued_record = queued_trial_record(controller.clone(), &genome);
        let queued_id = queued_record.trial_id.clone();
        ledger
            .write_trial_record(queued_record)
            .await
            .expect("queued trial");

        let queued = ledger
            .queued_trial_records(&controller, 10)
            .await
            .expect("queued trials");
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].trial_id, queued_id);
        assert_eq!(queued[0].status, StoredTrialStatus::Queued);

        let mut completed = record.clone();
        completed.status = StoredTrialStatus::Promoted;
        completed.updated_at = chrono::Utc::now();
        completed.completed_at = Some(completed.updated_at);
        completed.trace_count = 3;
        completed.total_score = 12.0;
        ledger
            .write_trial_record(completed.clone())
            .await
            .expect("completed trial");

        let completed_records = ledger
            .completed_trial_records(&controller, 10)
            .await
            .expect("completed trials");
        assert_eq!(completed_records.len(), 1);
        assert_eq!(completed_records[0].trial_id, completed.trial_id);
        assert_eq!(completed_records[0].status, StoredTrialStatus::Promoted);
    }

    #[tokio::test]
    async fn in_memory_state_roundtrips() {
        let ledger = InMemoryLedger::new();
        assert_state_roundtrip(&ledger).await;
    }

    #[tokio::test]
    async fn sqlite_state_roundtrips() {
        let ledger = SqliteLedger::in_memory().await.expect("sqlite");
        assert_state_roundtrip(&ledger).await;
    }
}
