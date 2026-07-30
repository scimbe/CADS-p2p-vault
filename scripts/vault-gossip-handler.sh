#!/usr/bin/env bash
# vault-gossip-handler.sh — the single executable path handed to `ct-agent`
# as CT_AGENT_SERVICE_HANDLER_CMD (see serve-vault-gossip.sh). ct-agent execs
# this fresh, once per incoming channel call, with the request on stdin and
# expects the response on stdout — same contract as every other crew handler
# in this workspace (see e.g. serve-handlers/flappy/physics-handler.sh), but
# `gossip-handler` itself needs a `--dir <vault-dir>` argument that ct-agent
# has no way to pass, so this wrapper supplies it from VAULT_DIR.
set -euo pipefail

die() { printf 'vault-gossip-handler: %s\n' "$*" >&2; exit 1; }

GOSSIP_HANDLER_BIN="${GOSSIP_HANDLER_BIN:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/crates/vault-agent/target/release/gossip-handler}"
: "${VAULT_DIR:?set VAULT_DIR (the same --dir this host vault-agent process runs against)}"

[ -x "$GOSSIP_HANDLER_BIN" ] || die "GOSSIP_HANDLER_BIN=$GOSSIP_HANDLER_BIN not found or not executable (build it: docker run --rm -v \$PWD:/work -w /work/crates/vault-agent rust:1-slim cargo build --release)"

exec "$GOSSIP_HANDLER_BIN" --dir "$VAULT_DIR"
