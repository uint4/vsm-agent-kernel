use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use vsm_core::{GeneSuggestion, GenomeId, NodeId, OrganizationalGenome, SuggestionId};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GenomeSnapshotRole {
    Champion,
    Candidate,
    Archived,
    Other(String),
}

impl GenomeSnapshotRole {
    pub fn as_storage_key(&self) -> String {
        match self {
            Self::Champion => "champion".to_string(),
            Self::Candidate => "candidate".to_string(),
            Self::Archived => "archived".to_string(),
            Self::Other(value) => format!("other:{value}"),
        }
    }

    pub fn from_storage_key(value: &str) -> Self {
        match value {
            "champion" => Self::Champion,
            "candidate" => Self::Candidate,
            "archived" => Self::Archived,
            other if other.starts_with("other:") => {
                Self::Other(other["other:".len()..].to_string())
            }
            other => Self::Other(other.to_string()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GenomeSnapshot {
    pub genome_id: GenomeId,
    pub role: GenomeSnapshotRole,
    pub saved_at: DateTime<Utc>,
    pub genome: OrganizationalGenome,
    pub metadata: BTreeMap<String, String>,
}

impl GenomeSnapshot {
    pub fn new(genome: OrganizationalGenome, role: GenomeSnapshotRole) -> Self {
        Self {
            genome_id: genome.id.clone(),
            role,
            saved_at: Utc::now(),
            genome,
            metadata: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoredTrialStatus {
    Queued,
    Active,
    Promoted,
    Pruned,
    Rejected,
    Archived,
}

impl StoredTrialStatus {
    pub fn as_storage_key(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Active => "active",
            Self::Promoted => "promoted",
            Self::Pruned => "pruned",
            Self::Rejected => "rejected",
            Self::Archived => "archived",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoredTrialDecision {
    Continue,
    Promote,
    Prune,
}

impl StoredTrialDecision {
    pub fn as_storage_key(&self) -> &'static str {
        match self {
            Self::Continue => "continue",
            Self::Promote => "promote",
            Self::Prune => "prune",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StoredTrialRecord {
    pub trial_id: SuggestionId,
    pub controller_node_id: NodeId,
    pub base_genome_id: GenomeId,
    pub candidate_genome_id: GenomeId,
    pub suggestion: GeneSuggestion,
    pub status: StoredTrialStatus,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub routed_tasks: u64,
    pub consumed_tokens: u64,
    pub trace_count: u64,
    pub total_score: f64,
    pub decision: Option<StoredTrialDecision>,
    pub registered_candidate_workers: Vec<NodeId>,
    pub metadata: BTreeMap<String, String>,
}

impl StoredTrialRecord {
    pub fn queued(
        controller_node_id: NodeId,
        base_genome_id: GenomeId,
        candidate_genome_id: GenomeId,
        suggestion: GeneSuggestion,
    ) -> Self {
        let mut record = Self::active(
            controller_node_id,
            base_genome_id,
            candidate_genome_id,
            suggestion,
        );
        record.status = StoredTrialStatus::Queued;
        record
    }

    pub fn active(
        controller_node_id: NodeId,
        base_genome_id: GenomeId,
        candidate_genome_id: GenomeId,
        suggestion: GeneSuggestion,
    ) -> Self {
        let now = Utc::now();
        Self {
            trial_id: suggestion.id.clone(),
            controller_node_id,
            base_genome_id,
            candidate_genome_id,
            suggestion,
            status: StoredTrialStatus::Active,
            started_at: now,
            updated_at: now,
            completed_at: None,
            routed_tasks: 0,
            consumed_tokens: 0,
            trace_count: 0,
            total_score: 0.0,
            decision: None,
            registered_candidate_workers: vec![],
            metadata: BTreeMap::new(),
        }
    }

    pub fn mark_active(&mut self) {
        let now = Utc::now();
        self.status = StoredTrialStatus::Active;
        self.started_at = now;
        self.updated_at = now;
        self.completed_at = None;
        self.decision = None;
    }

    pub fn mark_rejected(&mut self, reason: impl Into<String>) {
        let now = Utc::now();
        self.status = StoredTrialStatus::Rejected;
        self.updated_at = now;
        self.completed_at = Some(now);
        self.decision = None;
        self.metadata
            .insert("rejection_reason".to_string(), reason.into());
    }
}
