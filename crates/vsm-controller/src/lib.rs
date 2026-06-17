//! Metasystem runtime for non-leaf VSM nodes.
//!
//! A controller node cannot write code. It accepts directives or task packets,
//! maps directives into task packets, routes work to direct System 1 children,
//! records routing events in the ledger, and observes task results.

pub mod audit;
pub mod error;
pub mod evolution;
pub mod mapper;
pub mod router;
pub mod runtime;
pub mod trial;

pub use audit::*;
pub use error::*;
pub use evolution::*;
pub use mapper::*;
pub use router::*;
pub use runtime::*;
pub use trial::*;
