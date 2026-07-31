# Builds vault-agent + gossip-handler from source. Multi-stage so the shipped
# image carries no Rust toolchain, matching ct-agent's own docker/Dockerfile
# convention in the sibling CADS-Tunnel repos.
FROM rust:1-slim-bookworm AS builder
WORKDIR /work
COPY crates/vault-manifest crates/vault-manifest
COPY crates/vault-agent crates/vault-agent
RUN cd crates/vault-agent && cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /work/crates/vault-agent/target/release/vault-agent /usr/local/bin/vault-agent
COPY --from=builder /work/crates/vault-agent/target/release/gossip-handler /usr/local/bin/gossip-handler
# Non-root by default (cycle 5's TESTPLAN.md follow-up note): UID/GID are build
# args, not hardcoded, so docker-compose.test.yml can pass the *host* user's
# ids and get host-writable bind-mounted files instead of a fixed guess that
# may not match whoever runs the build.
ARG VAULT_UID=1000
ARG VAULT_GID=1000
RUN groupadd -g "${VAULT_GID}" vault \
    && useradd -u "${VAULT_UID}" -g "${VAULT_GID}" -M -s /usr/sbin/nologin vault \
    && mkdir -p /vault && chown vault:vault /vault
USER vault
ENTRYPOINT ["/usr/local/bin/vault-agent"]
