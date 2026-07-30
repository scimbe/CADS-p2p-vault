//! Gossip wire protocol: one exchange per peer per tick, three line-delimited
//! JSON messages over a plain TCP stream (real Agent-Fabric channel transport
//! is a follow-up — see ARCHITECTURE.md §3/§7 step 2). Always paired (the
//! client always sends all three message kinds, even with an empty chunk
//! request list) so the server side never has to guess whether a message is
//! coming.

use serde::{Deserialize, Serialize};
use vault_manifest::manifest::Manifest;

#[derive(Debug, Serialize, Deserialize)]
pub struct ManifestMsg {
    pub manifest: Manifest,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetChunksMsg {
    pub hashes: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChunksMsg {
    /// hash (hex) -> base64-encoded chunk bytes. Only chunks the responder
    /// actually has are included; a requester that still has gaps after this
    /// will just try again next tick (possibly against a different peer).
    pub chunks: std::collections::HashMap<String, String>,
}
