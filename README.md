# torrentfs

A FUSE-based virtual filesystem for BitTorrent management. Mount `.torrent` files, browse their structure, and read file contents on-demand via the BitTorrent network.

## Architecture

```
main → fuse → services → domain/infrastructure
```

| Layer | Role |
|-------|------|
| `main` | Entry point: CLI args, FUSE mount, bootstrap |
| `fuse` | FUSE protocol adapter: `Filesystem` trait impl + inode management. No DB/download/seeding logic |
| `services` | Orchestration: `TorrentService` (torrent lifecycle), `DownloadService` (piece download), `SeedingService` (seeding management) |
| `domain` | Pure data models and repository traits (`Torrent`, `TorrentFile`, `TorrentRepository`) |
| `infrastructure` | Concrete implementations: `db` (SQLite), `download` (libtorrent session), `cache` (LRU piece cache), `config` (TOML), `metadata` (.torrent parsing) |

### Key modules

- `src/fuse/` — FUSE protocol (`mod.rs`), inode management (`inodes.rs`), data resolution (`lookup.rs`), stats generation (`stats.rs`)
- `src/services/` — `torrent.rs` (DB delegation for torrent CRUD), `download.rs` (piece download orchestration), `seeding.rs` (seeding lifecycle)
- `src/domain/` — `types.rs` (data models), `repository.rs` (traits), `error.rs` (error types)
- `src/infrastructure/` — `db/` (SQLite persistence), `download/` (libtorrent session + piece management), `cache/` (LRU cache), `config/` (TOML config), `metadata/` (.torrent parsing)
- `src/seeding.rs` — `SeedingManager` (peer seeding with cache eviction callbacks)
- `src/error.rs` — re-exports from `domain::error`

Dependency direction: `domain` has no dependency on `infrastructure`; `infrastructure` implements `domain` traits.

## Container Deployment

torrentfs ships a Docker image (`ghcr.io/tsip404/torrentfs`) with a smart entrypoint that handles FUSE device setup and mount visibility.

### Quick Start

```bash
# Docker (rootful) — host-visible FUSE mount via shared propagation
docker run --rm \
  --device /dev/fuse \
  --cap-add SYS_ADMIN \
  --mount type=bind,source=/host/torrentfs,target=/mnt,bind-propagation=rshared \
  ghcr.io/tsip404/torrentfs
```

On the host, prepare the shared mount first:
```bash
mkdir -p /host/torrentfs
mount --bind /host/torrentfs /host/torrentfs
mount --make-shared /host/torrentfs
```

### Rootless podman

Rootless podman **does not support shared mount propagation** (`shared`/`rshared`). This is a fundamental limitation of user namespaces — the container runs in a private mount namespace and cannot create mount events that propagate to the host. `--privileged` does **not** change this: in rootless mode it only grants capabilities inside the user namespace, which still cannot touch the host mount namespace.

**What works**: torrentfs mounts and operates correctly inside the container. Use `podman exec` to access the filesystem:

```bash
podman run -d --name torrentfs \
  --device /dev/fuse \
  --cap-add SYS_ADMIN \
  ghcr.io/tsip404/torrentfs

podman exec torrentfs ls /mnt/metadata/
```

**What does not work**: the host cannot access the FUSE mount (including `data/`) through a bind-mounted directory — a `-v /host/dir:/mnt:shared` (or `:rshared`) volume has no effect on the host side under rootless podman. For host-visible FUSE mounts, use a rootful runtime:

```bash
# Rootful podman (Docker uses the same flags)
sudo podman run -d --name torrentfs \
  --device /dev/fuse \
  --cap-add SYS_ADMIN \
  --mount type=bind,source=/host/torrentfs,target=/mnt,bind-propagation=rshared \
  ghcr.io/tsip404/torrentfs
```

Prepare `/host/torrentfs` as a shared mount first (see Quick Start above), or run torrentfs directly on the host without a container.

The entrypoint automatically detects rootless podman and runs in container-only mode, skipping the unsupported bind mount step.