//! vault-agent: a real, running peer for the CADS-p2p-vault demo.
//!
//! Watches a local directory (the "shared folder"), turns it into
//! content-defined chunks + a CRDT manifest (both from `vault_manifest`),
//! and gossips with configured peers every tick — full manifest state +
//! on-demand chunk fetch, so file bytes actually cross the wire, not just
//! simulated state (see `vault-manifest/tests/local_simulation.rs` for the
//! earlier in-process-only version this replaces for the "is it running"
//! bar). Two peer transports, selectable per peer:
//!
//! - `--peers host:port,...`: plain TCP (the original local-host proof —
//!   still fully supported, used by the 3-agent convergence test).
//! - `--peer-cmd '<shell command>'` (repeatable): a real CADS-Tunnel
//!   Agent-Fabric channel dial, e.g. `env CT_CHANNEL_ROLE=initiate
//!   CT_CHANNEL_CALL_SERVICE=vault_gossip ... ct-agent channel` — exactly the
//!   same one-shot dial-per-call shape the existing crew demos already use
//!   for CREW_PHYSICS_CMD etc (see CADS-flappy-demo/bridge/server.lib.js's
//!   `runCmd`), so no new core capability is needed, only a new demo-side
//!   handler (`gossip-handler`, invoked on the accept side by
//!   scripts/serve-vault-gossip.sh). This is ARCHITECTURE.md §7 step 2.

use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use vault_manifest::hlc::Hlc;
use vault_manifest::manifest::Manifest;

use vault_agent::store::{self, Store};
use vault_agent::wire::{self, GossipRequest, GossipResponse};

struct Args {
    id: String,
    dir: PathBuf,
    listen: Option<String>,
    peers: Vec<String>,
    peer_cmds: Vec<String>,
    tick_ms: u64,
    dial_timeout: Duration,
}

fn parse_args() -> Args {
    let mut id = None;
    let mut dir = None;
    let mut listen = None;
    let mut peers = Vec::new();
    let mut peer_cmds = Vec::new();
    let mut tick_ms = 2000u64;
    let mut dial_timeout_secs = 20u64;

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
            "--peer-cmd" => peer_cmds.push(val),
            "--tick-ms" => tick_ms = val.parse().expect("--tick-ms must be a number"),
            "--dial-timeout-secs" => dial_timeout_secs = val.parse().expect("--dial-timeout-secs must be a number"),
            other => panic!("unknown flag {other}"),
        }
    }

    Args {
        id: id.expect("--id is required"),
        dir: dir.expect("--dir is required"),
        listen,
        peers,
        peer_cmds,
        tick_ms,
        dial_timeout: Duration::from_secs(dial_timeout_secs),
    }
}

fn to_io_err<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

// --- TCP transport (unchanged behavior, local-host proof) ---

fn write_line<T: serde::Serialize>(stream: &mut TcpStream, msg: &T) -> io::Result<()> {
    let s = serde_json::to_string(msg).map_err(to_io_err)?;
    stream.write_all(s.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.flush()
}

fn read_line<R: BufRead>(reader: &mut R) -> io::Result<String> {
    // Bounded (wire::read_bounded_line): plain TCP has no auth at all on this
    // transport, so an unbounded `read_line` would let any reachable peer grow this
    // process's memory without limit just by never sending a newline.
    let line = wire::read_bounded_line(reader)?;
    if line.is_empty() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "peer closed the connection"));
    }
    Ok(line)
}

fn missing_chunk_hashes(manifest: &Mutex<Manifest>, store: &Store) -> Vec<String> {
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
}

fn apply_chunks(store: &Store, chunks: HashMap<String, String>) -> io::Result<()> {
    for (hash, b64) in chunks {
        let bytes = B64.decode(b64.as_bytes()).map_err(to_io_err)?;
        store.write_chunk(&hash, &bytes)?;
    }
    Ok(())
}

/// One gossip exchange over plain TCP, initiated by us against `peer_addr`.
fn gossip_once_tcp(peer_addr: &str, manifest: &Mutex<Manifest>, store: &Store) -> io::Result<()> {
    let mut stream = TcpStream::connect(peer_addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    let local_snapshot = manifest.lock().unwrap().clone();
    write_line(&mut stream, &GossipRequest::Manifest { manifest: local_snapshot })?;

    let mut reader = BufReader::new(stream.try_clone()?);
    let line = read_line(&mut reader)?;
    let resp: GossipResponse = serde_json::from_str(&line).map_err(to_io_err)?;
    let GossipResponse::Manifest { manifest: peer_manifest } = resp else {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected a manifest response"));
    };
    manifest.lock().unwrap().merge(&peer_manifest);

    let missing = missing_chunk_hashes(manifest, store);
    write_line(&mut stream, &GossipRequest::GetChunks { hashes: missing })?;

    let line = read_line(&mut reader)?;
    let resp: GossipResponse = serde_json::from_str(&line).map_err(to_io_err)?;
    let GossipResponse::Chunks { chunks } = resp else {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected a chunks response"));
    };
    apply_chunks(store, chunks)
}

fn handle_incoming_tcp(mut stream: TcpStream, manifest: &Mutex<Manifest>, store: &Store) -> io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let mut reader = BufReader::new(stream.try_clone()?);

    let line = read_line(&mut reader)?;
    let req: GossipRequest = serde_json::from_str(&line).map_err(to_io_err)?;
    let GossipRequest::Manifest { manifest: peer_manifest } = req else {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected a manifest request first"));
    };
    manifest.lock().unwrap().merge(&peer_manifest);

    let reply_snapshot = manifest.lock().unwrap().clone();
    write_line(&mut stream, &GossipResponse::Manifest { manifest: reply_snapshot })?;

    let line = read_line(&mut reader)?;
    let req: GossipRequest = serde_json::from_str(&line).map_err(to_io_err)?;
    let GossipRequest::GetChunks { hashes } = req else {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected a get_chunks request second"));
    };
    let mut chunks = HashMap::new();
    for hash in &hashes {
        if let Ok(bytes) = store.read_chunk(hash) {
            chunks.insert(hash.clone(), B64.encode(bytes));
        }
    }
    write_line(&mut stream, &GossipResponse::Chunks { chunks })
}

// --- Real-channel transport: one `sh -c <cmd>` subprocess per message,
// stdin=request/stdout=response, exactly like the crew bridge's `runCmd` ---

fn run_dial_cmd(cmd: &str, input: &str, timeout: Duration) -> io::Result<String> {
    let mut child = Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    if let Some(mut stdin) = child.stdin.take() {
        // Best-effort write, same as the crew bridge: a peer that answers
        // before draining stdin makes this fail with EPIPE — deliberately
        // ignored, the exit status + stdout decide the outcome.
        let _ = stdin.write_all(input.as_bytes());
    }

    // Drain stdout/stderr on their own threads, concurrently with waiting for
    // exit below. The OS pipe buffer is typically 64KiB; a gossip response
    // larger than that (routine for a real manifest snapshot or a chunk-fetch
    // batch -- even one max-size 64KiB chunk is ~87KB once base64-encoded)
    // fills the pipe and blocks the child's write() before it can exit, so a
    // reader that only runs *after* `try_wait` sees `Some` would deadlock
    // every such call until `timeout` kills it. Bounded via
    // wire::read_bounded_to_string for the same reason as every other
    // network-facing read in this crate: the bytes on the other end of
    // stdout are a peer's gossip response.
    let mut out = child.stdout.take().expect("stdout was piped");
    let stdout_thread = std::thread::spawn(move || wire::read_bounded_to_string(&mut out));
    let mut err = child.stderr.take().expect("stderr was piped");
    let stderr_thread = std::thread::spawn(move || wire::read_bounded_to_string(&mut err));

    let start = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if start.elapsed() > timeout {
            let _ = child.kill();
            let _ = child.wait();
            return Err(io::Error::new(io::ErrorKind::TimedOut, format!("dial command timed out after {timeout:?}")));
        }
        std::thread::sleep(Duration::from_millis(50));
    };

    let stdout = stdout_thread.join().unwrap_or_else(|_| Ok(String::new())).unwrap_or_default();
    if !status.success() {
        let stderr = stderr_thread.join().unwrap_or_else(|_| Ok(String::new())).unwrap_or_default();
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("dial command exited {:?}: {}", status.code(), stderr.trim()),
        ));
    }
    if stdout.trim().is_empty() {
        return Err(io::Error::new(io::ErrorKind::Other, "dial command produced no output"));
    }
    Ok(stdout)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_dial_cmd_does_not_deadlock_on_a_response_larger_than_one_pipe_buffer() {
        // 64KiB is the typical Linux pipe buffer; write comfortably more than
        // that to prove the reader drains concurrently with the child
        // running instead of only after it exits.
        let out = run_dial_cmd("yes x | head -c 200000", "", Duration::from_secs(10))
            .expect("a large response must not time out or deadlock");
        assert_eq!(out.len(), 200000);
    }

    #[test]
    fn run_dial_cmd_still_reports_a_failing_command() {
        let err = run_dial_cmd("echo boom >&2; exit 1", "", Duration::from_secs(5))
            .expect_err("a nonzero exit must be reported as an error");
        assert!(err.to_string().contains("boom"), "stderr is included: {err}");
    }
}

/// One gossip exchange over a real channel dial: two subprocess calls
/// (manifest exchange, then chunk fetch if anything's missing), each its own
/// fresh `ct-agent channel initiate` dial — matching how the existing crew
/// roles are called (see compose.flappy-demo.yml's CREW_PHYSICS_CMD).
fn gossip_once_cmd(cmd: &str, manifest: &Mutex<Manifest>, store: &Store, timeout: Duration) -> io::Result<()> {
    let local_snapshot = manifest.lock().unwrap().clone();
    let req = serde_json::to_string(&GossipRequest::Manifest { manifest: local_snapshot }).map_err(to_io_err)?;
    let raw = run_dial_cmd(cmd, &req, timeout)?;
    let resp: GossipResponse = serde_json::from_str(raw.trim()).map_err(to_io_err)?;
    let GossipResponse::Manifest { manifest: peer_manifest } = resp else {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected a manifest response"));
    };
    manifest.lock().unwrap().merge(&peer_manifest);

    let missing = missing_chunk_hashes(manifest, store);
    if missing.is_empty() {
        return Ok(());
    }

    let req2 = serde_json::to_string(&GossipRequest::GetChunks { hashes: missing }).map_err(to_io_err)?;
    let raw2 = run_dial_cmd(cmd, &req2, timeout)?;
    let resp2: GossipResponse = serde_json::from_str(raw2.trim()).map_err(to_io_err)?;
    let GossipResponse::Chunks { chunks } = resp2 else {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected a chunks response"));
    };
    apply_chunks(store, chunks)
}

fn main() -> io::Result<()> {
    let args = parse_args();
    println!(
        "vault-agent[{}]: dir={} listen={:?} peers={:?} peer_cmds={} tick={}ms",
        args.id,
        args.dir.display(),
        args.listen,
        args.peers,
        args.peer_cmds.len(),
        args.tick_ms
    );

    let store = Arc::new(Store::open(args.dir.clone())?);
    // Load whatever's already on disk (e.g. written by `gossip-handler`
    // invocations from an incoming channel call that happened before this
    // process started, or from a previous run of this same process) rather
    // than starting from an empty manifest — otherwise this tick's first
    // `save_manifest` would silently clobber that state. Local-file
    // authorship bookkeeping (`known_local`) still starts fresh each run —
    // a known, pre-existing limitation (see docs/TESTPLAN.md), unchanged by
    // adding disk persistence here.
    let manifest = Arc::new(Mutex::new(store.load_manifest()?));
    let clock = Arc::new(Mutex::new(Hlc::zero()));
    let known_local: Arc<store::KnownLocal> = Arc::new(Mutex::new(HashMap::new()));

    // TCP accept side (only if --listen was given — real-channel peers are
    // accepted by an external, persistent `ct-agent channel accept` process
    // instead; see scripts/serve-vault-gossip.sh).
    if let Some(listen) = &args.listen {
        let manifest = Arc::clone(&manifest);
        let store = Arc::clone(&store);
        let listener = TcpListener::bind(listen)?;
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                match conn {
                    Ok(stream) => {
                        let manifest = Arc::clone(&manifest);
                        let store = Arc::clone(&store);
                        std::thread::spawn(move || {
                            if let Err(e) = handle_incoming_tcp(stream, &manifest, &store) {
                                eprintln!("vault-agent: inbound gossip error: {e}");
                            }
                        });
                    }
                    Err(e) => eprintln!("vault-agent: accept error: {e}"),
                }
            }
        });
    }

    // Main loop: scan local changes, gossip with every peer (both
    // transports), materialize, persist.
    let id = args.id.clone();
    loop {
        match store::scan_local(&store, &manifest, &clock, &known_local, &id) {
            Ok(changed) if !changed.is_empty() => println!("vault-agent[{id}]: local change(s): {changed:?}"),
            Ok(_) => {}
            Err(e) => eprintln!("vault-agent[{id}]: scan error: {e}"),
        }

        for peer in &args.peers {
            if let Err(e) = gossip_once_tcp(peer, &manifest, &store) {
                eprintln!("vault-agent[{id}]: gossip (tcp) with {peer} failed: {e}");
            }
        }
        for (i, cmd) in args.peer_cmds.iter().enumerate() {
            if let Err(e) = gossip_once_cmd(cmd, &manifest, &store, args.dial_timeout) {
                eprintln!("vault-agent[{id}]: gossip (channel) with peer_cmd[{i}] failed: {e}");
            }
        }

        match store::materialize_remote(&store, &manifest, &id, &known_local) {
            Ok(applied) if !applied.is_empty() => println!("vault-agent[{id}]: materialized: {applied:?}"),
            Ok(_) => {}
            Err(e) => eprintln!("vault-agent[{id}]: materialize error: {e}"),
        }

        if let Err(e) = store.save_manifest(&manifest.lock().unwrap()) {
            eprintln!("vault-agent[{id}]: failed to persist manifest: {e}");
        }

        std::thread::sleep(Duration::from_millis(args.tick_ms));
    }
}
