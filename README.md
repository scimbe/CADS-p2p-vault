# CADS-p2p-vault

Encrypted, CRDT-versioned, gossip-converging shared file storage for
CADS-Tunnel-connected agents. Core coordinates discovery only — it never
transfers or sees file content. See [ARCHITECTURE.md](ARCHITECTURE.md) for the
full design, prior-art survey, and test plan.

**Status: early scaffolding, multi-cycle build in progress.**

## Layout

- `crates/vault-manifest` — the CRDT manifest (OR-Set + HLC) and provider-lease
  logic, transport-agnostic.
- `crates/vault-agent` — the bare-host/desktop agent: gossip transport over
  CADS-Tunnel Agent-Fabric channels, local blob store.
- `android/` — Android client scaffolding (Kotlin/Compose), shares the Rust
  manifest core via JNI bindings once built.
- `docs/` — design notes, test-plan logs.
