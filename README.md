# CADS-p2p-vault

Encrypted, CRDT-versioned, gossip-converging shared file storage for
CADS-Tunnel-connected agents. Core coordinates discovery only — it never
transfers or sees file content. See [ARCHITECTURE.md](ARCHITECTURE.md) for the
full design, prior-art survey, and test plan.

**Status: running locally.** `vault-agent` is a real binary that gossips full
manifest state + on-demand chunk bytes over plain TCP and materializes files
to disk — verified with 3 local instances converging on creates, edits,
deletes, and near-concurrent writes (see `docs/TESTPLAN.md`, cycle 3). Not
yet wired to real CADS-Tunnel Agent-Fabric channels (still plain TCP — see
ARCHITECTURE.md §7 step 2) and manifest state does not persist across a
restart yet.

## Layout

- `crates/vault-manifest` — the CRDT manifest (OR-Set + HLC) and provider-lease
  logic, transport-agnostic.
- `crates/vault-agent` — the running agent binary: scans a local directory
  into content-defined chunks + the CRDT manifest, gossips with configured
  peers over TCP (full manifest exchange + chunk fetch each tick), and
  materializes remote entries (writes/deletes) back to disk. Transport is
  plain TCP for now; swapping in real CADS-Tunnel Agent-Fabric channels is a
  follow-up, not a redesign — see ARCHITECTURE.md §7 step 2.
- `android/` — Android client scaffolding (Kotlin/Compose), shares the Rust
  manifest core via JNI bindings once built. Not started (no Android SDK on
  this host yet).
- `docs/` — design notes, test-plan logs.

## Running it locally

```
cargo build --release -p vault-agent
./target/release/vault-agent --id alice --dir ./alice-vault \
  --listen 127.0.0.1:9401 --peers 127.0.0.1:9402,127.0.0.1:9403
```

Run one instance per agent with a distinct `--dir`/`--listen`/`--peers` set;
drop files into any instance's `--dir` and watch them appear in the others.
Any agent may create, edit, or delete any file — deletes and edits from a
non-author correctly propagate (see the two real bugs documented inline in
`crates/vault-agent/src/store.rs` and in the test-plan log, both found by
running actual multi-agent local traffic, not by inspection).
