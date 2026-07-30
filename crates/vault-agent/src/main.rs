//! vault-agent: a real, running peer for the CADS-p2p-vault demo.
//!
//! Watches a local directory (the "shared folder"), turns it into
//! content-defined chunks + a CRDT manifest (both from `vault_manifest`),
//! and gossips with configured peers over plain TCP every tick — full
//! manifest state + on-demand chunk fetch, so file bytes actually cross the
//! wire, not just simulated state (see `tests/local_simulation.rs` in
//! `vault-manifest` for the earlier in-process-only version this replaces
//! for the "is it running" bar). Wiring this transport to real CADS-Tunnel
//! Agent-Fabric channels is a follow-up (ARCHITECTURE.md §7 step 2) — this
//! is the local single-host milestone (§7 step 1).

mod store;
mod wire;

use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use vault_manifest::hlc::Hlc;
use vault_manifest::manifest::Manifest;

use store::Store;
use wire::{ChunksMsg, GetChunksMsg, ManifestMsg};

struct Args {
    id: String,
    dir: PathBuf,
    listen: String,
    peers: Vec<String>,
    tick_ms: u64,
}

fn parse_args() -> Args {
    let mut id = None;
    let mut dir = None;
    let mut listen = None;
    let mut peers = Vec::new();
    let mut tick_ms = 2000u64;

    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let val = it.next().unwrap_or_else(|| panic!("{flag} needs a value"));
        match flag.as_str() {
            "--id" => id = Some(val),
            "--dir" => dir = Some(PathBuf::from(val)),
            "--listen" => listen = Some(val),
            "--peers" => {
                peers = val.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
            }
            "--tick-ms" => tick_ms = val.parse().expect("--tick-ms must be a number"),
            other => panic!("unknown flag {other}"),
        }
    }

    Args {
        id: id.expect("--id is required"),
        dir: dir.expect("--dir is required"),
        listen: listen.expect("--listen is required (host:port)"),
        peers,
        tick_ms,
    }
}

fn to_io_err<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

fn write_line<T: serde::Serialize>(stream: &mut TcpStream, msg: &T) -> io::Result<()> {
    let s = serde_json::to_string(msg).map_err(to_io_err)?;
    stream.write_all(s.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.flush()
}

fn read_line<R: BufRead>(reader: &mut R) -> io::Result<String> {
    let mut line = String::new();
    let n = reader.read_line(&mut line)?;
    if n == 0 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "peer closed the connection"));
    }
    Ok(line)
}

/// One gossip exchange, initiated by us against `peer_addr`: send our
/// manifest, merge theirs, then fetch whatever chunk bytes we're still
/// missing after the merge.
fn gossip_once(peer_addr: &str, manifest: &Mutex<Manifest>, store: &Store) -> io::Result<()> {
    let mut stream = TcpStream::connect(peer_addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    let local_snapshot = manifest.lock().unwrap().clone();
    write_line(&mut stream, &ManifestMsg { manifest: local_snapshot })?;

    let mut reader = BufReader::new(stream.try_clone()?);
    let line = read_line(&mut reader)?;
    let peer_msg: ManifestMsg = serde_json::from_str(&line).map_err(to_io_err)?;
    manifest.lock().unwrap().merge(&peer_msg.manifest);

    let missing: Vec<String> = {
        let m = manifest.lock().unwrap();
        let mut set = std::collections::HashSet::new();
        for (_, entry) in m.iter() {
            for h in &entry.chunk_hashes {
                if !store.has_chunk(h) {
                    set.insert(h.clone());
                }
            }
        }
        set.into_iter().collect()
    };
    write_line(&mut stream, &GetChunksMsg { hashes: missing })?;

    let line = read_line(&mut reader)?;
    let chunks_msg: ChunksMsg = serde_json::from_str(&line).map_err(to_io_err)?;
    for (hash, b64) in chunks_msg.chunks {
        let bytes = B64.decode(b64.as_bytes()).map_err(to_io_err)?;
        store.write_chunk(&hash, &bytes)?;
    }
    Ok(())
}

fn handle_incoming(mut stream: TcpStream, manifest: &Mutex<Manifest>, store: &Store) -> io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let mut reader = BufReader::new(stream.try_clone()?);

    let line = read_line(&mut reader)?;
    let peer_msg: ManifestMsg = serde_json::from_str(&line).map_err(to_io_err)?;
    manifest.lock().unwrap().merge(&peer_msg.manifest);

    let reply_snapshot = manifest.lock().unwrap().clone();
    write_line(&mut stream, &ManifestMsg { manifest: reply_snapshot })?;

    let line = read_line(&mut reader)?;
    let req: GetChunksMsg = serde_json::from_str(&line).map_err(to_io_err)?;
    let mut chunks = HashMap::new();
    for hash in &req.hashes {
        if let Ok(bytes) = store.read_chunk(hash) {
            chunks.insert(hash.clone(), B64.encode(bytes));
        }
    }
    write_line(&mut stream, &ChunksMsg { chunks })?;
    Ok(())
}

fn main() -> io::Result<()> {
    let args = parse_args();
    println!(
        "vault-agent[{}]: dir={} listen={} peers={:?} tick={}ms",
        args.id,
        args.dir.display(),
        args.listen,
        args.peers,
        args.tick_ms
    );

    let store = Arc::new(Store::open(args.dir.clone())?);
    let manifest = Arc::new(Mutex::new(Manifest::new()));
    let clock = Arc::new(Mutex::new(Hlc::zero()));
    let known_local: Arc<store::KnownLocal> = Arc::new(Mutex::new(HashMap::new()));

    // Server: accept peer gossip connections.
    {
        let manifest = Arc::clone(&manifest);
        let store = Arc::clone(&store);
        let listener = TcpListener::bind(&args.listen)?;
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                match conn {
                    Ok(stream) => {
                        let manifest = Arc::clone(&manifest);
                        let store = Arc::clone(&store);
                        std::thread::spawn(move || {
                            if let Err(e) = handle_incoming(stream, &manifest, &store) {
                                eprintln!("vault-agent: inbound gossip error: {e}");
                            }
                        });
                    }
                    Err(e) => eprintln!("vault-agent: accept error: {e}"),
                }
            }
        });
    }

    // Main loop: scan local changes, gossip with every peer, materialize.
    let id = args.id.clone();
    loop {
        match store::scan_local(&store, &manifest, &clock, &known_local, &id) {
            Ok(changed) if !changed.is_empty() => println!("vault-agent[{id}]: local change(s): {changed:?}"),
            Ok(_) => {}
            Err(e) => eprintln!("vault-agent[{id}]: scan error: {e}"),
        }

        for peer in &args.peers {
            if let Err(e) = gossip_once(peer, &manifest, &store) {
                eprintln!("vault-agent[{id}]: gossip with {peer} failed: {e}");
            }
        }

        match store::materialize_remote(&store, &manifest, &id, &known_local) {
            Ok(applied) if !applied.is_empty() => println!("vault-agent[{id}]: materialized: {applied:?}"),
            Ok(_) => {}
            Err(e) => eprintln!("vault-agent[{id}]: materialize error: {e}"),
        }

        std::thread::sleep(Duration::from_millis(args.tick_ms));
    }
}
