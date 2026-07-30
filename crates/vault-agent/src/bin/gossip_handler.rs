//! gossip-handler: single-shot request/response handler for the vault's
//! `vault_gossip` channel service. `ct-agent channel accept` spawns this
//! fresh, once per incoming call, as `CT_AGENT_SERVICE_HANDLER_CMD` (see
//! scripts/serve-vault-gossip.sh — the same pattern the crew demos already
//! use for their safety_check/text_generation handlers). Reads one
//! `GossipRequest` on stdin, applies it against the on-disk manifest + chunk
//! store the long-lived `vault-agent` process in the same `--dir` also
//! reads/writes, writes one `GossipResponse` to stdout, exits. No state
//! survives between invocations here — disk is the only thing shared with
//! the agent process (see `vault_agent::store::Store::{load,save}_manifest`).

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::path::PathBuf;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;

use vault_agent::store::Store;
use vault_agent::wire::{GossipRequest, GossipResponse};

fn to_io_err<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

fn parse_dir() -> PathBuf {
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        if flag == "--dir" {
            return PathBuf::from(args.next().expect("--dir needs a value"));
        }
    }
    panic!("usage: gossip-handler --dir <vault-dir>");
}

fn main() -> io::Result<()> {
    let dir = parse_dir();

    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;
    let req: GossipRequest = serde_json::from_str(input.trim()).map_err(to_io_err)?;

    let store = Store::open(dir)?;
    let response = match req {
        GossipRequest::Manifest { manifest: incoming } => {
            let mut current = store.load_manifest()?;
            current.merge(&incoming);
            store.save_manifest(&current)?;
            GossipResponse::Manifest { manifest: current }
        }
        GossipRequest::GetChunks { hashes } => {
            let mut chunks: HashMap<String, String> = HashMap::new();
            for hash in &hashes {
                if let Ok(bytes) = store.read_chunk(hash) {
                    chunks.insert(hash.clone(), B64.encode(bytes));
                }
            }
            GossipResponse::Chunks { chunks }
        }
    };

    let out = serde_json::to_string(&response).map_err(to_io_err)?;
    io::stdout().write_all(out.as_bytes())?;
    io::stdout().write_all(b"\n")
}
