# Stage 1: Build
FROM rust:1.97-bookworm AS builder

# Add Debian testing repos for libtorrent >=2.1.0 (not in bookworm)
RUN echo 'deb http://deb.debian.org/debian testing main' > /etc/apt/sources.list.d/testing.list && \
    echo 'Package: libtorrent-rasterbar*' > /etc/apt/preferences.d/libtorrent && \
    echo 'Pin: release a=testing' >> /etc/apt/preferences.d/libtorrent && \
    echo 'Pin-Priority: 500' >> /etc/apt/preferences.d/libtorrent

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

# Stage 2: Runtime — Debian testing for libtorrent >=2.1.0
FROM debian:testing-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libtorrent-rasterbar \
    libssl3t64 \
    libfuse2t64 \
    fuse3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd --create-home --shell /bin/bash torrentfs

COPY --from=builder /torrentfs /usr/local/bin/torrentfs

USER torrentfs
WORKDIR /home/torrentfs

ENTRYPOINT ["torrentfs"]
