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
ENTRYPOINT ["/usr/local/bin/vault-agent"]
