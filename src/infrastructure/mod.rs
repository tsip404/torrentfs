//! Infrastructure layer — concrete implementations of domain traits and external adapters.
//!
//! Sub-modules:
//! - `db` — SQLite-backed persistence, implements domain Repository traits
//! - `download` — libtorrent session management and piece download orchestration
//! - `cache` — LRU piece cache (CacheManager)
//! - `config` — TOML configuration management (TorrentfsConfig)
//! - `metadata` — .torrent file parsing (TorrentInfo)

pub mod alert;
pub mod cache;
pub mod config;
pub mod db;
pub mod download;
pub mod metadata;

pub use cache::{CacheManager, PieceMetadata};
pub use config::TorrentfsConfig;
pub use db::{
    Database, DbError, FileEntry, InsertTorrentResult, Torrent, TorrentDirectory, TorrentFile,
    TorrentStatus,
};
pub use download::{
    DownloadManager, FilePieceInfo, Session, TorrentHandle, TorrentState,
    TorrentStatus as DownloadTorrentStatus,
};
pub use metadata::TorrentInfo;
