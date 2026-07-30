//! Gossip wire protocol: one request -> one response per exchange, self-
//! describing via a `type` tag so a single handler invocation (see
//! `bin/gossip_handler.rs`) can tell which of the two exchange kinds it's
//! being asked to do — required once a real CADS-Tunnel Agent-Fabric channel
//! call is a fresh, stateless subprocess per dial (ARCHITECTURE.md §7 step 2),
//! not just two lines read off a persistent stream (the original plain-TCP
//! transport this replaces still uses the same types, just still framed as
//! two JSON lines over one connection).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use vault_manifest::manifest::Manifest;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GossipRequest {
    Manifest { manifest: Manifest },
    GetChunks { hashes: Vec<String> },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GossipResponse {
    Manifest { manifest: Manifest },
    /// hash (hex) -> base64-encoded chunk bytes. Only chunks the responder
    /// actually has are included; a requester that still has gaps after this
    /// will just try again next tick (possibly against a different peer).
    Chunks { chunks: HashMap<String, String> },
}
