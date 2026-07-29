//! CRDT manifest: an OR-Set of `path -> blob_hash` entries with hybrid-logical-clock
//! last-writer-wins, falling back to conflict-copy retention on true concurrency.
//! See ../../ARCHITECTURE.md §2 for the full data model rationale.

pub mod hlc;
pub mod manifest;
pub mod lease;
