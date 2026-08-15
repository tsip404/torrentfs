# Stage 1: Build
# debian:sid-slim matches CI container (.github/workflows/ci.yml:17)
FROM debian:sid-slim AS builder

# Install Rust toolchain (matches CI's dtolnay/rust-toolchain@stable)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates build-essential pkg-config \
    && rm -rf /var/lib/apt/lists/* \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"

# Pull latest libtorrent from Debian experimental (matches CI .github/workflows/ci.yml)
RUN apt-get update && \
    echo "deb http://deb.debian.org/debian experimental main" >> /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    -t experimental libtorrent-rasterbar-dev \
    libssl-dev \
    clang \
    libclang-dev \
    libfuse-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY . .

RUN --mount=type=cache,target=/root/.cargo/registry \
    cargo build --release && \
    cp target/release/torrentfs /torrentfs

# Stage 2: Runtime
# debian:sid-slim — must match builder base for experimental libtorrent deps
FROM debian:sid-slim

# Pull latest libtorrent runtime from Debian experimental (matches CI)
RUN apt-get update && \
    echo "deb http://deb.debian.org/debian experimental main" >> /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    -t experimental libtorrent-rasterbar2.1 \
    libfuse2 \
    fuse3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*
# Enable allow_other so non-root users can access the shared mount.
# fuse3 ships /etc/fuse.conf with `#user_allow_other` commented out; uncomment
# it (falling back to appending) so torrentfs detects user_allow_other at startup.
RUN touch /etc/fuse.conf \
    && sed -i 's/^#\s*user_allow_other\s*$/user_allow_other/' /etc/fuse.conf \
    && (grep -q '^user_allow_other$' /etc/fuse.conf || echo 'user_allow_other' >> /etc/fuse.conf)

COPY --from=builder /torrentfs /usr/local/bin/torrentfs
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["/mnt"]
