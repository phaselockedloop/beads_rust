//! JSON storage layer for `beads_rust`.
//!
//! This module provides the persistence layer using a JSON Lines (JSONL) file.
//! All issue data (labels, dependencies, comments) is stored directly in the
//! JSONL file — no separate database is required.

pub mod json;

pub use json::{IssueUpdate, JsonStorage, ListFilters, ReadyFilters, ReadySortPolicy};

