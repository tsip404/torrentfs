#!/bin/bash
# torrentfs container entrypoint — handles FUSE device setup and mount visibility.
#
# Key behaviors:
#   1. Detects container + FUSE availability
#   2. Attempts to create /dev/fuse when running as root
#   3. Provides actionable diagnostics when FUSE is unavailable
#   4. Two-stage FUSE mount: mounts on internal path (/mnt-inner), then
#      bind-mounts to the container-facing path for host visibility via
#      shared mount propagation (rshared).
#      See: https://docs.docker.com/engine/storage/bind-mounts/#configure-bind-propagation

set -euo pipefail

# ── helpers ──────────────────────────────────────────────────────────────────

# Commands that do not require a FUSE mount — pass these straight through.
needs_fuse() {
    for arg in "$@"; do
        case "$arg" in
            --help|-h|help|--version|-V)
                return 1  # does NOT need FUSE
                ;;
        esac
    done
    return 0  # needs FUSE
}

in_container() {
    # Heuristics: cgroup mount, /.dockerenv, /run/.containerenv (podman)
    grep -q ':/docker/' /proc/1/cgroup 2>/dev/null && return 0
    grep -q ':/libpod-' /proc/1/cgroup 2>/dev/null && return 0
    test -f /.dockerenv && return 0
    test -f /run/.containerenv && return 0
    return 1
}

is_root() {
    test "$(id -u)" -eq 0
}

fuse_device_exists() {
    test -c /dev/fuse
}

ensure_fuse_device() {
    if fuse_device_exists; then
        return 0
    fi

    if is_root; then
        echo "[entrypoint] /dev/fuse missing — attempting to create device node" >&2
        if mknod /dev/fuse c 10 229 2>/dev/null; then
            echo "[entrypoint] /dev/fuse created successfully" >&2
            return 0
        fi
        echo "[entrypoint] mknod /dev/fuse failed" >&2
    fi

    return 1
}

# ── main ─────────────────────────────────────────────────────────────────────

echo "[entrypoint] torrentfs container startup" >&2

if in_container; then
    echo "[entrypoint] detected container environment" >&2
else
    echo "[entrypoint] bare-metal or VM environment" >&2
fi

if ! needs_fuse "$@"; then
    # Help, version, and other diagnostic commands don't need FUSE.
    echo "[entrypoint] skipping FUSE check for diagnostic command — starting torrentfs" >&2
    exec torrentfs "$@"
fi

if ! ensure_fuse_device; then
    cat >&2 <<'DIAG'
╔══════════════════════════════════════════════════════════════════════╗
║  FUSE kernel device (/dev/fuse) is not available in this container.  ║
║  torrentfs requires FUSE to mount the virtual filesystem.            ║
║                                                                      ║
║  Solutions:                                                          ║
║                                                                      ║
║  1. podman (recommended):                                            ║
║     podman run --device /dev/fuse --cap-add SYS_ADMIN ...            ║
║                                                                      ║
║  2. podman (alternative — full privileges):                          ║
║     podman run --privileged ...                                      ║
║                                                                      ║
║  3. docker:                                                          ║
║     docker run --device /dev/fuse --cap-add SYS_ADMIN ...            ║
║                                                                      ║
║  4. docker compose:                                                  ║
║     services:                                                        ║
║       torrentfs:                                                     ║
║         devices:                                                     ║
║           - /dev/fuse:/dev/fuse                                      ║
║         cap_add:                                                     ║
║           - SYS_ADMIN                                                ║
║                                                                      ║
║  If you cannot grant these privileges, torrentfs cannot mount its    ║
║  FUSE filesystem inside a container. Run torrentfs on the host or    ║
║  in a privileged container instead.                                  ║
╚══════════════════════════════════════════════════════════════════════╝
DIAG
    exit 100
fi

# ── FUSE mount with host visibility ──────────────────────────────────────────
# Runs torrentfs on an internal mount point, then bind-mounts to the
# container-facing path (typically a shared bind mount). This allows
# the FUSE filesystem to propagate to the host via shared mount propagation.
# For the host to see the mount, the container must be started with:
#   --mount type=bind,source=<host-path>,target=<container-path>,bind-propagation=rshared
# and the host source directory must itself be a shared mount:
#   mount --bind <host-path> <host-path> && mount --make-shared <host-path>
start_torrentfs() {
    local mountpoint="$1"
    shift
    local internal_mnt="/mnt-inner"

    mkdir -p "$internal_mnt"
    mkdir -p "$mountpoint"

    echo "[entrypoint] starting torrentfs on internal mount $internal_mnt" >&2

    # Forward termination signals to torrentfs (we are PID 1 in container)
    local torrentfs_pid=""
    cleanup() {
        echo "[entrypoint] shutting down" >&2
        if [ -n "$torrentfs_pid" ]; then
            kill "$torrentfs_pid" 2>/dev/null || true
            wait "$torrentfs_pid" 2>/dev/null || true
        fi
        umount "$mountpoint" 2>/dev/null || true
    }
    trap cleanup EXIT INT TERM

    # Start torrentfs on internal path with remaining args
    torrentfs "$internal_mnt" "$@" &
    torrentfs_pid=$!

    # Wait for FUSE mount to become ready (up to 30s)
    local ready=0
    for i in $(seq 1 60); do
        if mountpoint -q "$internal_mnt" 2>/dev/null; then
            ready=1
            break
        fi
        sleep 0.5
    done

    if [ "$ready" -eq 0 ]; then
        echo "[entrypoint] FUSE mount did not become ready within 30s" >&2
        kill "$torrentfs_pid" 2>/dev/null || true
        wait "$torrentfs_pid" 2>/dev/null || true
        exit 1
    fi

    echo "[entrypoint] FUSE mount ready — publishing to $mountpoint" >&2
    mount --bind "$internal_mnt" "$mountpoint"

    echo "[entrypoint] torrentfs running (pid=$torrentfs_pid), available at $mountpoint" >&2

    # Wait for torrentfs to exit
    wait "$torrentfs_pid" || true
}

echo "[entrypoint] /dev/fuse is available" >&2
start_torrentfs "$@"
