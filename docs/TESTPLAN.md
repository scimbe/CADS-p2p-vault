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

## 2026-07-29 — cycle 2 (rigor pass, per operator's instruction to use genuinely best-in-class research)

- Web research pulled in: FastCDC (USENIX ATC'16) confirms content-defined
  chunking is the established best practice over fixed-size blocks for
  large-file dedup/delta-sync; independent sources confirm full-content CRDTs
  (Automerge/Yjs) carry real 2-3x metadata overhead in production, validating
  the decision to keep the CRDT scope to the manifest only; confirmed
  Syncthing (closest production analog) also skips CRDT-merging file content
  and falls back to conflict copies, same as this design's manifest-conflict
  path.
- **Replaced the whole-file-blob data model with content-defined chunking**
  (buzhash rolling hash) — `crates/vault-manifest/src/chunk.rs`. Two real bugs
  found and fixed by the test suite, not by inspection:
  1. An add-based "Gear hash" first attempt doesn't cleanly forget old bytes
     (carry propagation has no fixed lifetime) — silently broke resync after
     an edit. Fixed by switching to buzhash (XOR + rotate, exact cancellation).
  2. The first version of the resync/dedup tests used uniform/periodic test
     data, which mostly exercises the MAX_CHUNK_SIZE forced-cut fallback and
     would have hidden bug (1) either way. Replaced with a seeded PRNG
     (splitmix64) to generate realistic test content.
- 16/16 tests pass after the fix, including: chunk-hash dedup across two
  otherwise-different 50KB-shared-block files, and >70% chunk-hash survival
  after a 3-byte insertion near the start of a 300KB pseudo-random file.
- Updated ARCHITECTURE.md §1/§2 with citations and the corrected data model.
