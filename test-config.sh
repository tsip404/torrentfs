#!/bin/bash
set -euo pipefail

echo "=== Config Test Container: Testing --config parameter (scenario 10) ==="

CONFIG_FILE="/home/torrentfs/test-config.toml"
MOUNT_POINT="/mnt"

# Verify config file exists and is readable
if [ ! -f "$CONFIG_FILE" ]; then
    echo "FAIL: Config file not found at $CONFIG_FILE"
    exit 1
fi
echo "PASS: Config file exists at $CONFIG_FILE"

# Verify config file is valid TOML with expected content
if ! grep -q "max_connections = 100" "$CONFIG_FILE"; then
    echo "FAIL: Config file missing expected content"
    exit 1
fi
echo "PASS: Config file has expected content"

# Mount torrentfs with --config
echo "Mounting torrentfs with --config..."
torrentfs --config "$CONFIG_FILE" \
    --db /home/torrentfs/test.db \
    --cache /home/torrentfs/cache \
    "$MOUNT_POINT" &
TORRENTFS_PID=$!

# Wait for mount to become ready
sleep 3

# Verify mount is active
if ! mountpoint -q "$MOUNT_POINT"; then
    echo "FAIL: Mount point $MOUNT_POINT is not active"
    kill $TORRENTFS_PID 2>/dev/null || true
    exit 1
fi
echo "PASS: Mount point is active"

# Verify the filesystem structure exists
if [ -d "$MOUNT_POINT/metadata" ]; then
    echo "PASS: metadata/ directory found"
else
    echo "FAIL: metadata/ directory not found"
    fusermount -u "$MOUNT_POINT" 2>/dev/null || true
    exit 1
fi

if [ -d "$MOUNT_POINT/data" ]; then
    echo "PASS: data/ directory found"
else
    echo "FAIL: data/ directory not found"
    fusermount -u "$MOUNT_POINT" 2>/dev/null || true
    exit 1
fi

# Verify .stats file exists
if [ -f "$MOUNT_POINT/.stats" ]; then
    echo "PASS: .stats file found"
else
    echo "FAIL: .stats file not found"
    fusermount -u "$MOUNT_POINT" 2>/dev/null || true
    exit 1
fi

# Unmount cleanly
echo "Unmounting..."
fusermount -u "$MOUNT_POINT"

# Wait for process to exit
wait $TORRENTFS_PID 2>/dev/null || true

echo "=== All config tests passed! ==="
