#!/usr/bin/env bash
# provision-vault-gossip-channel.sh — provision the one pairwise `vault_gossip`
# Agent-Fabric channel needed for the two-host test (ARCHITECTURE.md §7 step
# 2), by delegating to CADS-Tunnel's own self-service
# scripts/channel-ops/provision-link-channel.sh (channel provisioning is
# self-service — any OIDC-authenticated user can register/own their own
# channels; no core admin token is needed or used here).
#
# This is a thin naming wrapper, not a reimplementation: it just fixes
# SERVICE=vault_gossip semantics and prints the exact --peer-cmd /
# serve-vault-gossip.sh invocations each of the two hosts needs, wired from
# the grants provision-link-channel.sh hands back.
#
# Needs (same credentials as any self-service channel, see
# CADS-Tunnel/docs/agent-onboarding.md §B):
#   - an OIDC bearer token (mint via CADS-Tunnel/scripts/channel-ops/mint-oidc-token.sh)
#   - an operator keypair (from `ct-agent channel operator-init`)
#   - a holder+noise keypair for EACH of the two hosts (from `ct-agent channel init`)
#
# Usage:
#   PROVISION_LINK_CHANNEL=/path/to/CADS-Tunnel/scripts/channel-ops/provision-link-channel.sh \
#   CT_AGENT_CP_URL=https://bunsenbrenner.org \
#   OIDC_TOKEN=... OPERATOR_KEY=... OPERATOR_PUBKEY=... \
#   HOST_A_NAME=alice HOST_A_HOLDER_KEY=... HOST_A_NOISE_KEY=... HOST_A_NOISE_PUBKEY=... \
#   HOST_B_NAME=bob   HOST_B_HOLDER_KEY=... HOST_B_NOISE_KEY=... HOST_B_NOISE_PUBKEY=... \
#     ./provision-vault-gossip-channel.sh
#
#   ./provision-vault-gossip-channel.sh --selftest   # arg-parsing/plumbing only, no network
set -euo pipefail

die() { printf 'provision-vault-gossip-channel: %s\n' "$*" >&2; exit 1; }

PROVISION_LINK_CHANNEL="${PROVISION_LINK_CHANNEL:?set PROVISION_LINK_CHANNEL (path to CADS-Tunnel scripts/channel-ops/provision-link-channel.sh)}"

if [ "${1:-}" = "--selftest" ]; then
  [ -x "$PROVISION_LINK_CHANNEL" ] || die "PROVISION_LINK_CHANNEL=$PROVISION_LINK_CHANNEL not found or not executable"
  SIDE_A_NAME=x SIDE_A_HOLDER_KEY=0 SIDE_A_NOISE_KEY=0 \
  SIDE_B_NAME=y SIDE_B_HOLDER_KEY=0 SIDE_B_NOISE_KEY=0 \
    "$PROVISION_LINK_CHANNEL" --selftest || die "underlying provision-link-channel.sh --selftest failed"
  echo "provision-vault-gossip-channel: underlying script resolvable — selftest passed (no CP/edge calls made)"
  exit 0
fi

: "${HOST_A_NAME:?set HOST_A_NAME (e.g. alice)}"
: "${HOST_A_HOLDER_KEY:?set HOST_A_HOLDER_KEY (64-hex priv, from: ct-agent channel init)}"
: "${HOST_A_NOISE_KEY:?set HOST_A_NOISE_KEY (64-hex priv, from: ct-agent channel init)}"
: "${HOST_A_NOISE_PUBKEY:?set HOST_A_NOISE_PUBKEY (64-hex pub, printed alongside HOST_A_NOISE_KEY by ct-agent channel init)}"
: "${HOST_B_NAME:?set HOST_B_NAME (e.g. bob)}"
: "${HOST_B_HOLDER_KEY:?set HOST_B_HOLDER_KEY (64-hex priv, from: ct-agent channel init)}"
: "${HOST_B_NOISE_KEY:?set HOST_B_NOISE_KEY (64-hex priv, from: ct-agent channel init)}"
: "${HOST_B_NOISE_PUBKEY:?set HOST_B_NOISE_PUBKEY (64-hex pub, printed alongside HOST_B_NOISE_KEY by ct-agent channel init)}"

# provision-link-channel.sh itself validates CT_AGENT_CP_URL / OIDC_TOKEN /
# OPERATOR_KEY / OPERATOR_PUBKEY — no need to duplicate those checks here.
RESULT="$(
  SIDE_A_NAME="$HOST_A_NAME" SIDE_A_HOLDER_KEY="$HOST_A_HOLDER_KEY" SIDE_A_NOISE_KEY="$HOST_A_NOISE_KEY" SIDE_A_NOISE_PUBKEY="$HOST_A_NOISE_PUBKEY" \
  SIDE_B_NAME="$HOST_B_NAME" SIDE_B_HOLDER_KEY="$HOST_B_HOLDER_KEY" SIDE_B_NOISE_KEY="$HOST_B_NOISE_KEY" SIDE_B_NOISE_PUBKEY="$HOST_B_NOISE_PUBKEY" \
    "$PROVISION_LINK_CHANNEL"
)" || die "provision-link-channel.sh failed — see its stderr above"

CHANNEL_ID="$(printf '%s\n' "$RESULT" | awk -F= '$1=="CHANNEL_ID"{print $2}')"
GRANT_A="$(printf '%s\n' "$RESULT" | awk -F= -v k="${HOST_A_NAME}_GRANT" '$1==k{print $2}')"
GRANT_B="$(printf '%s\n' "$RESULT" | awk -F= -v k="${HOST_B_NAME}_GRANT" '$1==k{print $2}')"
[ -n "$CHANNEL_ID" ] && [ -n "$GRANT_A" ] && [ -n "$GRANT_B" ] || die "couldn't parse channel_id/grants out of provision-link-channel.sh's output: $RESULT"

echo "provision-vault-gossip-channel: channel_id=$CHANNEL_ID" >&2
cat <<EOF

# --- on $HOST_A_NAME: dial $HOST_B_NAME's vault_gossip service ---
# pass this whole line as vault-agent's --peer-cmd
$HOST_A_NAME peer-cmd:
  env CT_CHANNEL_ROLE=initiate CT_CHANNEL_CALL_SERVICE=vault_gossip CT_CHANNEL_BROKER=\$CT_AGENT_EDGE_BROKER CT_CHANNEL_RELAY=\$CT_AGENT_EDGE_RELAY CT_CHANNEL_LISTEN=0.0.0.0:0 CT_CHANNEL_GRANT=$GRANT_A CT_CHANNEL_HOLDER_KEY=$HOST_A_HOLDER_KEY CT_CHANNEL_NOISE_KEY=$HOST_A_NOISE_KEY ct-agent channel

# --- on $HOST_B_NAME: serve vault_gossip for $HOST_A_NAME to dial ---
$HOST_B_NAME serve (run via scripts/serve-vault-gossip.sh):
  HOLDER_KEY=$HOST_B_HOLDER_KEY NOISE_KEY=$HOST_B_NOISE_KEY GRANT=$GRANT_B VAULT_DIR=<host b's vault dir> \\
  CT_AGENT_EDGE_BROKER=\$CT_AGENT_EDGE_BROKER CT_AGENT_EDGE_RELAY=\$CT_AGENT_EDGE_RELAY \\
    ./serve-vault-gossip.sh

# For bidirectional gossip (both hosts dial each other, matching main.rs's
# --peer-cmd design), repeat this script with HOST_A/HOST_B swapped to get
# the second directional grant pair, then each host runs BOTH its own
# serve-vault-gossip.sh (accept) AND vault-agent --peer-cmd (initiate)
# simultaneously.
EOF
