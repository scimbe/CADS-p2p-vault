# CADS-p2p-vault

Encrypted, CRDT-versioned, gossip-converging shared file storage for
CADS-Tunnel-connected agents. Core coordinates discovery only — it never
transfers or sees file content. See [ARCHITECTURE.md](ARCHITECTURE.md) for the
full design, prior-art survey, and test plan.

**Status: running locally, in real Docker containers, channel transport
wired.** `vault-agent` gossips full manifest state + on-demand chunk bytes
each tick over either plain TCP or a real CADS-Tunnel Agent-Fabric channel
dial (ARCHITECTURE.md §7 step 2), and materializes files to disk. Manifest
state now persists to `.vault/manifest.json` (atomic write) so it survives
an agent restart and can be shared with the separate `gossip-handler`
process. Verified: the 3-instance TCP convergence test (creates/edits/deletes)
now passes across three genuinely separate Docker containers on a real
bridge network (`Dockerfile` + `docker-compose.test.yml`, cycle 5), not just
same-host processes as in cycle 3, and a 2-agent run through `gossip-handler`
subprocess calls (the same request/response shape a real `ct-agent channel`
dial uses) converges files in both directions (cycle 4). Still blocked on
real channels specifically: provisioning a `vault_gossip` channel needs OIDC
+ operator credentials this checkout does not hold — see `docs/TESTPLAN.md`.

## Layout

- `crates/vault-manifest` — the CRDT manifest (OR-Set + HLC) and provider-lease
  logic, transport-agnostic.
- `crates/vault-agent` — the running agent binary: scans a local directory
  into content-defined chunks + the CRDT manifest, gossips with configured
  peers (full manifest exchange + chunk fetch each tick) over TCP or a real
  channel dial, and materializes remote entries (writes/deletes) back to
  disk. Also builds `gossip-handler`, a single-shot binary invoked fresh per
  incoming call by a `ct-agent channel accept` process on the serve side (see
  `scripts/serve-vault-gossip.sh`) — the two binaries share manifest state
  through `.vault/manifest.json` on disk, since they are separate OS
  processes.
- `scripts/` — `serve-vault-gossip.sh` (accept-side wrapper, mirrors the
  existing crew `serve-role.sh`), `vault-gossip-handler.sh` (the handler
  script `ct-agent` invokes), and `provision-vault-gossip-channel.sh`
  (self-service channel provisioning for the two-host test, delegates to
  CADS-Tunnel's `provision-link-channel.sh`). All three have a `--selftest`
  mode that checks plumbing with no network calls.
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

## Running it in Docker (three real containers)

```
mkdir -p test-run/alice test-run/bob test-run/carol
docker compose -f docker-compose.test.yml up -d --build
echo hello > test-run/alice/greeting.txt   # then watch it appear in bob/carol
docker compose -f docker-compose.test.yml down
```

Three genuinely separate containers on a real Docker bridge network, peers
addressed by Docker's own service-name DNS (`bob:9401`, not `127.0.0.1:...`)
— see `docs/TESTPLAN.md` cycle 5 for what this catches that same-host
processes can't. `vault-agent` runs as root in the container (no `USER` line
yet), so materialized files in `test-run/*` end up root-owned on the host;
edit through `docker exec <container> sh -c '...'` rather than a host-side
redirect, or clean up with `docker run --rm -v "$PWD/test-run":/c debian:bookworm-slim rm -rf /c`.

## Running it over a real CADS-Tunnel channel (two hosts)

Each host runs `vault-agent` with no `--listen`/`--peers` (the accept side is
an external process, not vault-agent itself) and one `--peer-cmd` per remote
peer — a full `ct-agent channel initiate ...` dial command string, the same
shape the crew demos already use for `CREW_PHYSICS_CMD` etc:

```
./target/release/vault-agent --id alice --dir ./alice-vault \
  --peer-cmd 'env CT_CHANNEL_ROLE=initiate CT_CHANNEL_CALL_SERVICE=vault_gossip \
    CT_CHANNEL_BROKER=$CT_AGENT_EDGE_BROKER CT_CHANNEL_RELAY=$CT_AGENT_EDGE_RELAY \
    CT_CHANNEL_LISTEN=0.0.0.0:0 CT_CHANNEL_GRANT=$ALICE_INITIATE_GRANT \
    CT_CHANNEL_HOLDER_KEY=$ALICE_HOLDER_KEY CT_CHANNEL_NOISE_KEY=$ALICE_NOISE_KEY \
    ct-agent channel'
```

And each host also runs `scripts/serve-vault-gossip.sh` so its peer can dial
*it* back (bidirectional gossip needs both sides serving and dialing).
`scripts/provision-vault-gossip-channel.sh` provisions the pairwise channel +
prints the exact `--peer-cmd` / `serve-vault-gossip.sh` invocations for both
sides — see that script's usage comment. Provisioning is self-service (any
OIDC-authenticated user can register their own channels) but needs a live
OIDC login + operator keypair this checkout does not currently hold.
