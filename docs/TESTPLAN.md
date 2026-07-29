# Local-first test plan

See ARCHITECTURE.md §7. Log of runs goes here as cycles progress.

## Run log

(none yet — scaffolding cycle only)

## 2026-07-29 — cycle 1

- Implemented HLC, manifest OR-Set CRDT (LWW + conflict-copy retention), and
  provider-lease merge logic in `crates/vault-manifest`.
- Local in-process simulation (`tests/local_simulation.rs`, no real networking
  yet): 3-agent gossip convergence with concurrent writes to different paths,
  true-concurrent writes to the *same* path (both preserved, deterministic
  winner), and provider-lease takeover converging to the best-connected
  survivor regardless of merge order. **11/11 tests pass.**
- Filed scimbe/CADS-Tunnel#230 proposing the one small coordination-only
  registry endpoint this needs from core.
- Next cycle: wire `vault-agent` to real CADS-Tunnel Agent-Fabric channels
  (reusing `ct-agent channel`) for the two-host test (ARCHITECTURE.md §7 step 2);
  Android SDK setup is still pending (large download, not started yet).
