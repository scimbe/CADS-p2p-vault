#!/usr/bin/env bash
# serve-vault-gossip.sh — bring up a long-lived `ct-agent channel` accept/serve
# process for this host's `vault_gossip` service, wired to
# vault-gossip-handler.sh. Mirrors the existing crew serve-role.sh exactly
# (same env vars, same ct-agent invocation shape — see
# ../../bin/serve-role.sh) so this reuses infrastructure the demos already
# depend on rather than inventing a new pattern. ARCHITECTURE.md §7 step 2.
#
# Runs in the foreground; the caller backgrounds it (nohup/&, systemd, docker,
# etc. — this script itself does none of that).
#
# Usage:
#   CT_AGENT_EDGE_BROKER=bunsenbrenner.org:4433 \
#   CT_AGENT_EDGE_RELAY=bunsenbrenner.org:4433 \
#   HOLDER_KEY=<64-hex priv> NOISE_KEY=<64-hex priv> GRANT=<hex signed grant, accept direction> \
#   VAULT_DIR=/path/to/this/hosts/vault \
#     ./serve-vault-gossip.sh
#
#   ./serve-vault-gossip.sh --selftest   # verify ct-agent + handler + gossip-handler binary
#                                        # are resolvable, no network
set -euo pipefail

die() { printf 'serve-vault-gossip: %s\n' "$*" >&2; exit 1; }

CT_AGENT="${CT_AGENT:-ct-agent}"
HANDLER_CMD="${HANDLER_CMD:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/vault-gossip-handler.sh}"

if [ "${1:-}" = "--selftest" ]; then
  command -v "$(printf '%s' "$CT_AGENT" | awk '{print $1}')" >/dev/null 2>&1 || die "ct-agent not resolvable (CT_AGENT=$CT_AGENT)"
  [ -x "$HANDLER_CMD" ] || die "HANDLER_CMD=$HANDLER_CMD not executable"
  : "${VAULT_DIR:?--selftest still needs VAULT_DIR set, so the handler wrapper can check it exists}"
  GOSSIP_HANDLER_BIN="${GOSSIP_HANDLER_BIN:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/crates/vault-agent/target/release/gossip-handler}"
  [ -x "$GOSSIP_HANDLER_BIN" ] || die "gossip-handler binary not built at $GOSSIP_HANDLER_BIN — run: docker run --rm -v \$PWD:/work -w /work/crates/vault-agent rust:1-slim cargo build --release"
  echo "serve-vault-gossip: ct-agent + handler + gossip-handler binary all resolvable — selftest passed (no CP/edge calls made)"
  exit 0
fi

: "${CT_AGENT_EDGE_BROKER:?set CT_AGENT_EDGE_BROKER (edge rendezvous host:port)}"
: "${CT_AGENT_EDGE_RELAY:?set CT_AGENT_EDGE_RELAY (edge relay host:port, often same as broker)}"
: "${HOLDER_KEY:?set HOLDER_KEY (64-hex, the serving holder PRIVATE key)}"
: "${NOISE_KEY:?set NOISE_KEY (64-hex, the serving noise PRIVATE key)}"
: "${GRANT:?set GRANT (hex signed grant for vault_gossip, accept direction)}"
: "${VAULT_DIR:?set VAULT_DIR (this host vault directory, same as vault-agent own --dir)}"
export VAULT_DIR

[ -x "$HANDLER_CMD" ] || die "HANDLER_CMD=$HANDLER_CMD not found or not executable"

echo "serve-vault-gossip: starting SERVICE=vault_gossip VAULT_DIR=$VAULT_DIR via broker=$CT_AGENT_EDGE_BROKER" >&2
exec env \
  CT_CHANNEL_ROLE=accept \
  CT_CHANNEL_SERVE=1 \
  CT_CHANNEL_RELAY_ONLY=1 \
  CT_CHANNEL_BROKER="$CT_AGENT_EDGE_BROKER" \
  CT_CHANNEL_RELAY="$CT_AGENT_EDGE_RELAY" \
  CT_CHANNEL_HOLDER_KEY="$HOLDER_KEY" \
  CT_CHANNEL_NOISE_KEY="$NOISE_KEY" \
  CT_CHANNEL_GRANT="$GRANT" \
  CT_AGENT_SERVICE_HANDLER_CMD="$HANDLER_CMD" \
  CT_AGENT_SERVICES=vault_gossip \
  VAULT_DIR="$VAULT_DIR" \
  $CT_AGENT channel
