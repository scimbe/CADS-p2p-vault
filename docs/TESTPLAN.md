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

## 2026-07-30 — cycle 3 (`vault-agent` built and run for real — ARCHITECTURE.md §7 step 1)

Operator asked for the demo to actually be up and running, not just
unit-tested. Built `crates/vault-agent`, a real binary: scans a directory
into chunks + the CRDT manifest, gossips full manifest state + on-demand
chunk fetch with configured peers over plain TCP each tick (line-delimited
JSON — real CADS-Tunnel channel transport is still a follow-up, §7 step 2),
and materializes remote entries (write or delete) to disk. Ran 3 instances
(alice/bob/carol) as real local processes with separate directories/ports and
put real traffic through them. **17/17 `vault-manifest` unit/simulation
tests still pass**, plus the live run below.

Three real bugs found by running actual multi-agent traffic — none of them
would have been caught by the in-process simulation alone, because that
simulation only ever exercised each agent's *own* authored writes, never a
disk that lags behind or disagrees with the manifest:

1. **Spurious conflict from materializing your own peer's file.** First
   version compared a locally-scanned file against the manifest entry only
   if `existing.author_pubkey == self_id` before treating it as "unchanged."
   The moment an agent materialized a peer's file to disk, its own next scan
   saw *a file whose manifest entry it didn't author* and re-authored it
   under its own id with a fresh HLC — and when two agents did this in the
   same tick, it manufactured a byte-identical "conflict copy" purely from
   local disk churn. Fixed by comparing content only, regardless of author.

2. **Deletes only worked for the original author.** The "did a file vanish
   locally" check only considered entries this agent itself had authored, so
   a *different* agent deleting a file it didn't create did nothing — no
   tombstone, no propagation — violating the "all agents can CRUD any file"
   requirement. Fixed by tracking `known_local`: whatever this agent last
   confirmed was correctly reflected on its own disk, whether self-authored
   or applied from a peer, and tombstoning any of those paths that
   disappear.

3. **Deletes (and remote edits) got silently reverted by the original
   holder.** Even after fix (2), a tombstone or edit from another agent kept
   getting undone: the *original* holder's own scan compared its local disk
   against the manifest's current (now-tombstoned/edited) entry using
   `Manifest::get()`, which hides tombstones — so the holder's own untouched,
   still-present file looked "new" the instant it learned of someone else's
   delete or edit, and got re-authored with a fresh (always-winning) HLC.
   The real fix wasn't `get()` vs. a tombstone-aware `get_any()` (added to
   `vault-manifest` with a regression test) — it was recognizing that a local
   scan must only ever compare against `known_local` (what *this* agent last
   confirmed matches), never against the manifest's global state, which may
   simply be ahead of this replica's own materialize step.

Verified live, with 3 real local processes on 1-second ticks:

- Create on alice → converges (byte-identical) on bob and carol.
- Edit by **carol** to a file **alice** created → converges everywhere,
  including back onto alice's own disk, and stays stable over repeated
  ticks (no flapping).
- Delete by **bob** of a file **alice** created → file disappears on alice,
  bob, and carol, and stays gone (no resurrection).
- Two agents (alice, carol) writing to the same new path within the same
  second → all three replicas converge to the identical winning content (no
  split-brain); triggering the exact-HLC-tie conflict-copy path deterministically
  in a live run needs finer-grained clock control than shell timing gives —
  that path is already covered by `vault-manifest`'s own unit tests.
- 8s idle settle period after all of the above: zero errors, zero further
  changes in any log — confirms the system reaches a quiescent fixed point
  rather than oscillating.

**Known limitations, deliberately deferred, not silently dropped:**
manifest state is in-memory only and does not survive an agent restart yet
(a restarted agent starts empty and re-derives only its own local files,
temporarily "orphaning" its authorship of anything a peer gave it until the
next gossip round refills it); transport is plain TCP, not yet the real
CADS-Tunnel Agent-Fabric channel (§7 step 2); the Android client is still
unstarted (§8).

## 2026-07-30 — cycle 4 (real channel transport — ARCHITECTURE.md §7 step 2)

Operator asked to wire `vault-agent` to real CADS-Tunnel Agent-Fabric
channels. Investigated the exact `ct-agent channel` accept/initiate env-var
protocol by reading the already-working crew-bridge code (`serve-role.sh`,
`server.lib.js`, `compose.flappy-demo.yml`) rather than guessing, so this
reuses the same mechanism the flappy/cookbook demos already depend on — no
new core capability needed, matching the "small ask to core, reuse
everything else" plan in ARCHITECTURE.md §3.

A real channel call is a fresh, stateless subprocess dial per message (unlike
the old persistent TCP stream), which forced two real design changes, not
just a new transport option:

1. **Wire format**: replaced the order-dependent `ManifestMsg` →
   `GetChunksMsg` → `ChunksMsg` sequence with self-describing tagged enums
   (`GossipRequest`/`GossipResponse`, `crates/vault-agent/src/wire.rs`) — a
   one-shot handler invocation has no "which step" context, so every message
   now carries its own type tag.
2. **Manifest persistence**: the long-lived `vault-agent` process and the
   new short-lived `gossip-handler` binary (invoked fresh per incoming call
   by `ct-agent channel accept`) are separate OS processes with no shared
   memory, so the manifest now persists to `.vault/manifest.json` (atomic
   write-via-temp-then-rename) as the only channel between them. This also
   incidentally closes half of the "manifest does not survive a restart"
   limitation from cycle 3 — `vault-agent` now loads the on-disk manifest at
   startup instead of an empty one (needed anyway, so a restart does not
   clobber whatever `gossip-handler` wrote while it was down). `known_local`
   (per-agent local-authorship bookkeeping) still starts empty on restart —
   deliberately not also fixed here, to keep this change scoped; a restarted
   agent may still spuriously re-author its own already-synced files under a
   fresh HLC on its first post-restart scan.

`main.rs` gained a new `gossip_once_cmd` path alongside the existing,
already-proven `gossip_once_tcp`: spawns `sh -c <peer's dial command>`
per message, with a manual poll-based timeout (`Child` has no native one),
mirroring the crew bridge's own `runCmd`. Peers are now given as full dial
command strings via repeatable `--peer-cmd`, distinct from the TCP `--peers`
list; a run can mix both. `--listen` is now optional — channel-only mode
needs no in-process TCP listener, since the accept side is an external
`ct-agent channel accept` process.

**Regression check**: re-ran the exact 3-agent (alice/bob/carol) local TCP
convergence test from cycle 3 after the rewrite — create/edit/delete all
still converge correctly, byte-identical, zero errors. The wire-format
rewrite did not regress the already-proven transport.

**New, channel-shaped path proven end-to-end** (without real channels, which
are still blocked on credentials — see below): ran two full `vault-agent`
processes (dave/erin), each with `--peer-cmd` pointed directly at the
other's `gossip-handler --dir <their-vault>` — i.e. the exact request/
response subprocess shape a real `ct-agent channel initiate` dial would
produce, minus the channel plumbing itself. A file created on erin appeared
byte-identical on dave, and vice versa, both directions, no errors — proving
the new wire format + disk-persistence + subprocess-dial design is sound.

Added `scripts/serve-vault-gossip.sh` (mirrors `serve-role.sh` exactly),
`scripts/vault-gossip-handler.sh` (the `CT_AGENT_SERVICE_HANDLER_CMD` target,
since `gossip-handler` itself needs a `--dir` argument ct-agent has no way to
pass), and `scripts/provision-vault-gossip-channel.sh` (thin wrapper around
CADS-Tunnel's own self-service `provision-link-channel.sh`, fixed to
`SERVICE=vault_gossip` semantics). All three ship a `--selftest` mode
(no network calls) — all three pass, once `CT_AGENT` points at a real
`ct-agent` binary (not on `PATH` on this maintenance host by default, but
present at `.demo-checkouts/bin/ct-agent`).

One real bug found and fixed while writing these: a `${VAR:?message}`
default-value message containing an apostrophe (e.g. "CADS-Tunnel's own")
corrupted bash's own quote-tracking for that parameter-expansion construct —
even though the whole thing was inside double quotes — silently swallowing
everything from that line to the next stray matching quote later in the
file as dead text, with no syntax error (`bash -n` passes) and no visible
symptom beyond "unbound variable" errors referencing lines nowhere near the
real cause. Fixed by rewording all `:?`/`:-` default messages to avoid
apostrophes; confirmed via `bash -n` plus each script's `--selftest`.

**Still genuinely blocked**: provisioning the real `vault_gossip` channel for
the two-host test needs a live OIDC bearer token (Keycloak username/password)
and an operator keypair (`ct-agent channel operator-init`) — neither exists
anywhere in this environment (confirmed by grepping `shared.env`'s variable
names). This is architecturally self-service, not a core-admin restriction,
but is a real credential gap only the operator can close.

## 2026-07-31 — cycle 5 (core maintainer: real Docker-container validation)

Operator asked for the demo to be built and tested with real Docker
containers specifically, not just bare host processes — a genuinely new bar,
since every prior cycle's live testing ran `vault-agent` as plain processes
on one host's loopback, which can't catch bugs that only show up under real
network-namespace isolation (e.g. an accidental `127.0.0.1` hardcode, a
`--listen` bind that only works when the peer is literally the same kernel
network stack).

Added `Dockerfile` (multi-stage: `rust:1-slim-bookworm` builder →
`debian:bookworm-slim` runtime, matching the *same* Debian base in both
stages deliberately — ct-agent's own `docker/Dockerfile` shipped with a real
GLIBC-version mismatch bug from cross-stage base drift, found and fixed
earlier this session; building from source with matching bases here avoids
that class of bug entirely rather than needing the same fix later) and
`docker-compose.test.yml` (three services, `alice`/`bob`/`carol`, each its
own container + bind-mounted vault dir, `--peers` addressed by Docker's
built-in service-name DNS instead of `127.0.0.1:<port>`).

Built and ran all three containers for real. Re-verified every scenario
cycle 3 proved on bare processes, this time across genuinely separate
containers on a real Docker bridge network:

- Create on alice → converges byte-identical to bob and carol.
- Edit by **carol** (non-author) → converges everywhere, including back onto
  alice's own container, stable over repeated ticks.
- Delete by **bob** (non-author) → file disappears on all three, stays gone
  after 3 further ticks (~6s), no resurrection.
- Concurrent create on alice and carol to the same new path → all three
  converge to the identical winning content, no split-brain, no spurious
  conflict-copy file.
- Container logs: only transient `Connection refused` lines during the very
  first tick (peers not all listening yet — expected, self-heals same tick
  cadence as bare-process mode), zero errors afterward, zero further changes
  during an 8s idle-settle window — same quiescent-fixed-point bar cycle 3
  set, now met under real container isolation too.

No new bugs found — the wire-format/persistence work from cycle 4 and the
three materialize-logic fixes from cycle 3 hold up unchanged under real
network-namespace separation, not just loopback.

One test-harness note, not a vault-agent bug: `vault-agent` runs as root
inside the container (no `USER` directive — deliberately deferred, this is a
test harness, not a hardening pass), so materialized files in the bind-mounted
`test-run/*` directories end up root-owned on the host and can't be edited by
a plain host-user redirect (`echo x > test-run/bob/f.txt` → `Permission
denied`). Worked around by driving edits through `docker exec <container>
sh -c '...'` instead, which is arguably more realistic anyway (an edit "from
inside" the peer, not a host-level side channel) — noting this so a future
non-root `USER` line in the Dockerfile is a deliberate follow-up, not a
forgotten one.

**Still genuinely blocked, same root cause as cycle 4**: the real two-container
channel-transport test (the actual §7 step 2 milestone, `--peer-cmd` dialing
real `ct-agent channel` instead of `--peers` TCP) needs the same OIDC bearer
token + operator keypair cycle 4 was blocked on. Checked whether a lower-risk
path exists — the realm has no pre-seeded test user and no service-account-
enabled client (`ct-portal`'s `serviceAccountsEnabled`/`directAccessGrantsEnabled`
are both `false`; only Keycloak's own built-in `admin-cli` supports the
password grant `mint-oidc-token.sh` uses) — so genuinely closing this gap
means creating a new account in the **production** `ct-demo` realm backing
bunsenbrenner.org, which also auto-provisions a Standard-tier tunnel per the
platform's own onboarding design. Flagged to the operator as a real decision
rather than done unilaterally.

## 2026-07-31 — cycle 6 (non-root container hardening, while still blocked on cycle 5's credential decision)

Closed the test-harness gap cycle 5 deliberately deferred rather than leaving
it flagged-but-untouched: `Dockerfile` now builds a `vault` user from two new
build args, `VAULT_UID`/`VAULT_GID` (default `1000`, matching common single-
user-host convention), `USER vault` before `ENTRYPOINT`, `docker-compose.test.yml`
passes them through from `HOST_UID`/`HOST_GID` shell env vars so a build run
as `HOST_UID=$(id -u) HOST_GID=$(id -g) docker compose ... up --build`
produces host-owned, not root-owned, materialized files.

Re-ran the full cycle-5 scenario set against the rebuilt image, this time
using plain host-side redirects throughout instead of cycle 5's `docker exec`
workaround (no longer needed):

- Create on alice (`echo ... > test-run/alice/greeting.txt`, no `docker exec`)
  → converges byte-identical to bob and carol; the file's owner on all three
  bind mounts is the invoking host user (`becke:docker`, uid 1001 in this
  run), confirmed via `stat`, not root.
- Edit by **carol** (non-author, plain host redirect) → converges to alice
  and bob.
- Delete by **bob** (non-author, plain `rm`) → propagates to alice and carol,
  stays gone.
- Container logs: zero errors/panics; 8s idle-settle window shows zero further
  log lines — same quiescent-fixed-point bar prior cycles set.

No regressions from the non-root change, no new bugs. `README.md` updated to
drop the `docker exec`/`docker run --rm ... rm -rf` workaround instructions
and document `HOST_UID`/`HOST_GID`. Cycle 5's original root-cause note above
is left as-is (historical record of what was true then); this entry is the
fix.

**Still genuinely blocked**: same OIDC bearer token + operator keypair gap as
cycles 4-5 — this cycle deliberately picked a task that didn't need it, not a
resolution of it.
