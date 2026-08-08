#!/bin/bash
# torrentfs container entrypoint — handles rootless container FUSE requirements.
#
# In rootless containers (podman without --privileged, Docker with limited caps),
# /dev/fuse is typically absent. This script:
#   1. Detects container + FUSE availability
#   2. Attempts to create /dev/fuse when running as root
#   3. Provides actionable diagnostics when FUSE is unavailable
#   4. Falls through to the torrentfs binary when everything is ready

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

echo "[entrypoint] /dev/fuse is available — starting torrentfs" >&2

exec torrentfs "$@"
