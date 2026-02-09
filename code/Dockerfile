# Multi-stage build for the Malachite app binary.
# Used by quake to build testnet Docker images.
#
# Usage (via quake):
#   cargo run -p quake -- -f <manifest> build
#
# Manual usage:
#   security find-certificate -a -p /System/Library/Keychains/SystemRootCertificates.keychain \
#     /Library/Keychains/System.keychain > .quake/ca-certificates.crt
#   GITHUB_TOKEN=<token> docker build \
#     --secret id=GITHUB_TOKEN,env=GITHUB_TOKEN \
#     -t malachite:latest .

# --- Builder stage ---
FROM rust:1-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates pkg-config libssl-dev protobuf-compiler git \
    && rm -rf /var/lib/apt/lists/*

# Copy host CA certs (including corporate CAs exported from macOS Keychain)
COPY .quake/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

ENV CARGO_HOME=/usr/local/cargo
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
ENV CARGO_HTTP_CAINFO=/etc/ssl/certs/ca-certificates.crt

WORKDIR /build

COPY . .

ARG APP_BIN=informalsystems-malachitebft-starknet-app

RUN --mount=type=cache,id=malachite-cargo-registry,sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,id=malachite-cargo-git,sharing=locked,target=/usr/local/cargo/git \
    --mount=type=cache,id=malachite-cargo-target,sharing=locked,target=/build/target \
    --mount=type=secret,id=GITHUB_TOKEN \
    git config --global url."https://$(cat /run/secrets/GITHUB_TOKEN)@github.com/".insteadOf "https://github.com/" && \
    cargo build --release -p "$APP_BIN" && \
    cp target/release/"$APP_BIN" /usr/local/bin/malachite-app && \
    git config --global --unset url."https://$(cat /run/secrets/GITHUB_TOKEN)@github.com/".insteadOf

# --- Runtime stage ---
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates iproute2 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/malachite-app /usr/local/bin/malachite-app

ENTRYPOINT ["malachite-app"]
CMD ["start", "--home", "/data"]
