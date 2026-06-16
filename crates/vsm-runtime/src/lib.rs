//! Minimal runtime utilities for the VSM core.
//!
//! This crate provides an in-memory transport and a small mutation trial manager.
//! It is intentionally simple so the first implementation can collect traces and
//! evolve organizational genomes before any provider-specific agent code exists.

pub mod in_memory_transport;
pub mod trial;

pub use in_memory_transport::*;
pub use trial::*;
