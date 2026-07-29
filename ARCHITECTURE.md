# CADS-p2p-vault — architecture & plan

Encrypted, versioned, conflict-free shared storage for CADS-Tunnel-connected agents.
Core (`bunsenbrenner.org`) coordinates *who can find whom*; it never sees or relays a
single byte of file content. Agents exchange data directly over already-established
Agent-Fabric channels (Noise_IK, the same primitive `ct-agent channel` uses today).

## 1. Prior art surveyed

| Concern | Existing systems | What we take from them |
|---|---|---|
| Conflict-free multi-writer data | Automerge, Yjs, diamond-types | Deliberately **not** used for file content: real-world reports put full-document-CRDT metadata at 2-3x the data size, with Automerge specifically costing 40-60% overhead vs. raw text and tombstones accumulating into the tens of thousands on heavily-edited documents ([tonsky.me](https://tonsky.me/blog/crdt-filesync/), [Zylos Research](https://zylos.ai/research/2026-01-29-crdt-real-time-collaboration/)). We only apply CRDT semantics to the **manifest** (path → chunk-list), a much smaller state space — file bytes themselves are immutable content-addressed chunks, never CRDT-merged. |
| Gossip-based convergence & membership | Scuttlebutt (SSB), SWIM, HyParView/Plumtree | SSB is the closest prior art: signed, append-only per-peer logs, gossiped pairwise, fully offline-capable, no central server required for data — only for peer discovery ("pubs"). We mirror that split exactly: core = SSB "pub" (discovery only), agents = SSB "feeds." SWIM-style heartbeats for failure detection driving provider failover. |
| P2P file sync w/ a coordination-only server | **Syncthing** (introducer/relay/discovery server never touches file bytes when direct P2P is possible), BitTorrent+DHT, Resilio Sync | Directly validates the requested shape: a lightweight rendezvous service + direct device-to-device block exchange. Confirmed: **Syncthing does not use CRDTs either** — on a true conflict it just keeps both files as `sync-conflict` copies rather than merging ([Syncthing docs](https://docs.syncthing.net/users/syncing.html)); we adopt that same conflict-copy fallback for the manifest's genuinely-concurrent case (§2), but add a deterministic CRDT merge rule on top so *every* replica converges to the same choice without Syncthing's ad hoc "which neighbor has the latest state" heuristic. |
| Chunking large files for efficient sync/dedup | Fixed-size blocks vs. **content-defined chunking** (rsync rolling checksum, FastCDC, restic/Borg/Perkeep) | Fixed-size chunking has the "boundary-shift" problem: one inserted byte near the start invalidates every following block. Content-defined chunking (CDC) places boundaries based on a rolling hash of the content itself, so an edit only perturbs the chunks around it. FastCDC is the current widely-adopted approach, using dual-mask boundary normalization for a tighter chunk-size distribution ([USENIX ATC '16](https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf)). We use a single-mask **buzhash** variant (§2) — simpler, keeps CDC's essential resync property, dual-mask normalization is a possible follow-up, not a correctness requirement. |
| Content-addressed storage + Merkle history | Git, IPFS/libp2p, Hypercore (Dat/Holepunch) | Chunks keyed by BLAKE3 hash; a file is an ordered chunk-hash list (a flat Merkle list), same shape as git's own content-addressing, generalized the way IPFS/Hypercore chunk large objects. |
| NAT traversal without a known public address | WebRTC ICE/STUN/TURN, libp2p AutoNAT + DCUtR + relay/hole-punch | Not reinvented — **CADS-Tunnel's own Agent-Fabric channel broker/relay already solves this** (agents dial the broker, get relayed or upgraded to direct). The vault reuses that channel transport as-is. |
| Leaderless failover / lease election | Raft leader election (too heavyweight — needs a coordinator), Bully algorithm, SWIM-style suspicion | We use a **CRDT lease record** (see §4) instead of a consensus protocol — converges the same way the manifest does, no separate election protocol to implement. |

**Conclusion driving the design:** nothing here is genuinely novel — it's Scuttlebutt's
gossip/offline-first model + Syncthing's coordination-server-that-never-touches-bytes
shape + FastCDC-style content-defined chunking (not whole-file hashing — see §2) +
git/Hypercore's content-addressing, wired through CADS-Tunnel's *existing*
Agent-Fabric channel transport (so the core extension needed is small — see §5).

## 2. Data model

- **Chunk**: an immutable, content-defined slice of a file's bytes, addressed by
  `blake3(chunk_bytes)`. Boundaries are placed by a **buzhash** rolling hash (a true
  bounded sliding-window hash: XOR + rotate, so a byte's contribution is exactly
  cancelled out once it's `WINDOW` bytes in the past) rather than at fixed offsets —
  editing part of a large file only changes the chunks around the edit, so peers only
  need to transfer/store those chunks (implemented in `crates/vault-manifest/src/chunk.rs`,
  verified by tests: same content anywhere dedups to the same chunk hash, and a small
  edit near the start of a 300KB file leaves >70% of chunk hashes unchanged).
  **Implementation note, kept here deliberately as a warning:** the first version
  of this used an *add*-based "Gear hash" (`hash = (hash << 1) + table[byte]`),
  which looks equivalent to buzhash but isn't — `wrapping_add`'s carries propagate
  upward with no fixed lifetime, so it does *not* cleanly forget old bytes the way
  XOR-based cancellation does, and it silently failed the resync/dedup tests. It
  was caught only because the tests used realistic (pseudo-random) data instead of
  uniform/periodic bytes — degenerate test input mostly exercises the
  `MAX_CHUNK_SIZE` forced-cut fallback and would have hidden the bug either way.
  Chunks are encrypted at rest with the vault's symmetric content key (§6) before
  being written to any agent's local disk — an agent storing a chunk it doesn't
  otherwise have permission to read still cannot read it.
- **Manifest entry**: `{path, chunk_hashes: [String], size, hlc_timestamp,
  author_pubkey, tombstone: bool}` — a file's content is the ordered concatenation
  of its chunks, not a single whole-file hash. The manifest is an **OR-Set CRDT**
  keyed by `path`, using a Hybrid Logical Clock (HLC) for last-writer-wins per path
  *and* keeping both sides as `path.conflict-<author_short>` when two writes to the
  same path are truly concurrent (unordered by HLC) — same conflict-preserving
  behavior as Syncthing, never silently drops data.
- **Vault log**: each agent keeps its own append-only, self-signed log of manifest
  operations (create/update/delete) — an SSB-style feed. Peers gossip **deltas**
  (missing log entries), not full state, once initial sync completes.

## 3. Membership & transport

A vault is a CADS-Tunnel Agent-Fabric **channel group**, not a new transport:

- Vault creation mints a `vault_id` + a channel operator key (reusing
  `ct-agent channel operator-init`).
- Joining a vault = being granted membership in that channel group (reusing
  `ct-agent channel grant` / the existing self-service channel-provisioning flow
  this session already built for the crew roles).
- Once granted, every member pair dials each other directly via
  `ct-agent channel` (Noise_IK, broker/relay-assisted NAT traversal) — **exactly**
  the transport the 7 crew-serve roles use today. No new wire protocol needed for
  the data plane.

## 4. Provider failover (no consensus protocol required)

The "provider" is whichever agent currently holds the durable/canonical copy for
agents that can't afford full local replication (e.g., a phone). Tracked as a CRDT
register, not elected via Raft/Paxos:

```
provider_lease = {
  holder_pubkey, epoch: u64, connectivity_score: f32, renewed_at: hlc_timestamp
}
```

- The holder renews its own lease every N seconds by gossiping a fresh
  `provider_lease` with the same `epoch`.
- Any member sees a stale lease (no renewal within `2N` seconds, SWIM-style
  suspicion) and gossips a **takeover proposal**: `{epoch: epoch+1, holder_pubkey:
  self, connectivity_score: self.score()}`.
- If multiple members propose in the same window (network partition edge case),
  the CRDT merge rule is deterministic: **highest `connectivity_score` wins;
  ties broken by lowest `holder_pubkey`** — every node computes the same winner
  independently from the same gossiped set, no voting round needed.
- `connectivity_score` = a cheap local heuristic: `(directly-reachable peer count)
  * 10 - (round-trip ms to the CADS-Tunnel edge)`. Good enough for "prefer the
  best-connected agent" without needing real network topology awareness.

## 5. What this needs from core (proposed via GitHub issue, not built directly)

Per this session's standing rule, core changes are **proposed, not implemented,
by workflow-pipelines** — filed as an issue on `scimbe/CADS-Tunnel` (see linked
issue). The ask is deliberately small given §3 already reuses existing channel
infra:

1. A **vault-membership registry** endpoint — `GET /registry/vaults/:vault_id` →
   list of member `holder_pubkey`s + their last-seen channel-broker address hint,
   so a newly-joined or reconnecting agent can find its peers without needing
   every other member's address out of band. This is the *only* new coordination
   surface; it never carries file content, only pubkeys + reachability hints
   (same shape as the existing `/registry/agents` from #226).
2. Confirmation that Agent-Fabric channels support **N-way group membership**
   for a stable `vault_id` (today's crew channels are pairwise/role-based) rather
   than a new primitive — if pairwise-only, we'd instead open `N*(N-1)/2` pairwise
   channels per vault, which works but doesn't scale past small groups; core's
   input on whether a native group-channel primitive is worth adding.

## 6. Encryption

- Transport: already end-to-end via each agent's existing Noise_IK channel
  keypair (holder + noise key) — nothing new.
- At rest / content confidentiality: one symmetric **vault content key**,
  generated at vault creation, distributed to each member only over their
  already-encrypted channel at admission time, rotated (re-encrypt future blobs,
  old blobs stay under the old key + a key-version tag) whenever a member is
  removed. No new key-exchange protocol — piggybacks on channel admission.

## 7. MVP test plan (local first, per the operator's instruction)

1. **Single-host simulation**: N bare-host agent processes on this same demo
   host (same pattern as the 7 crew-serve roles already running), each with its
   own local vault directory, joined to one test `vault_id`. Verify: concurrent
   writes to the same path converge to the same manifest state on all N agents;
   killing the "provider" process triggers a clean takeover within one gossip
   round; a late-joining agent backfills via delta gossip instead of full resync.
2. **Two-host** (this host + the existing second core instance stood up earlier
   in this session) to validate the broker/relay-assisted NAT traversal path,
   not just loopback.
3. Only after (1)+(2) pass: real multi-device testing, including the Android
   client (§8).

## 8. Android app (scaffolding — not a full build yet)

No Android SDK is present on this host yet (large download, next cycle). Plan:
Kotlin, minimal Jetpack Compose UI (file list + conflict indicator), the sync
engine as a foreground service reusing the same manifest-CRDT + gossip logic
(shared Rust core via UniFFI/JNI bindings, so the merge logic isn't
reimplemented per-platform — same blob format, same manifest format as the
desktop/bare-host agent). Scaffolding only in this pass; SDK setup + a real
build are a follow-up cycle.

## Status

This is a multi-cycle build (explicitly acknowledged — this is not a
single-session deliverable). Progress tracked in this repo's issues; core-facing
asks tracked on `scimbe/CADS-Tunnel`.
