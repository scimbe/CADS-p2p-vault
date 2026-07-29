//! CRDT manifest: an OR-Set of `path -> entry` (entry = ordered content-defined
//! chunk hashes) with hybrid-logical-clock last-writer-wins, falling back to
//! conflict-copy retention on true concurrency. See ../../ARCHITECTURE.md §2
//! for the full data model rationale.

pub mod chunk;
pub mod hlc;
pub mod manifest;
pub mod lease;
