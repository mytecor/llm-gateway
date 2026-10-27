//! Native structural extraction with ast-grep and conservative, single-batch healing.
//! Inputs are borrowed for the parse future; outputs and immutable rule snapshots are owned.
mod core;
mod error;
mod flights;
mod healing;
mod jev;
mod parser;
mod store;
mod types;
pub use error::{Error, Result};
pub use healing::Candidate;
pub use jev::{
    ChoiceAnswer, ChoiceQuestion, DecisionRequest, DecisionResponse, HttpJevClient, JevClient,
};
pub use parser::{Parser, ParserBuilder};
pub use store::{MemoryRuleStore, RuleSet, RuleStore};
pub use types::*;
