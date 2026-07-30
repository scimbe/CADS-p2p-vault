//! Shared modules between the `vault-agent` main-loop binary and the
//! `gossip-handler` binary (invoked fresh per call by a persistent
//! `ct-agent channel accept` process — see scripts/serve-vault-gossip.sh).
//! The two are separate OS processes, not threads, so they only share state
//! through disk (`store::Store::{load,save}_manifest`) and this wire format.

pub mod store;
pub mod wire;
