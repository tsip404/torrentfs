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