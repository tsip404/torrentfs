# Stage 1: Build
FROM rust:1.97-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    libtorrent-rasterbar-dev \
    libssl-dev \
    pkg-config \
    clang \
    libclang-dev \
    libfuse-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release && \
    cp target/release/torrentfs /torrentfs

# Stage 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libtorrent-rasterbar2.0 \
    libssl3 \
    libfuse2 \
    fuse3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd --create-home --shell /bin/bash torrentfs

COPY --from=builder /torrentfs /usr/local/bin/torrentfs

USER torrentfs
WORKDIR /home/torrentfs

ENTRYPOINT ["torrentfs"]
