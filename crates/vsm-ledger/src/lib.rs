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

pub use error::*;
pub use event::*;
pub use ledger::*;
pub use memory::*;
pub use sqlite::*;
