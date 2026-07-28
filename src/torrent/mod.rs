//! Torrent domain types — re-exported from the db module.
//!
//! With the TSI-1987 split, db.rs was decomposed into:
//! - `db::types` — data types (Torrent, TorrentFile, TorrentDirectory, etc.)
//! - `db::database` — Database struct + connection management + migrations
//! - `db::torrent_ops` — torrent CRUD operations
//! - `db::file_ops` — file/directory query operations
//! - `db::metadata_ops` — metadata directory operations
//!
//! This module will eventually hold the TorrentRepository trait and SQLite implementation.

pub use crate::db::{
    Database, DbError, FileEntry, InsertTorrentResult, Torrent, TorrentDirectory, TorrentFile,
    TorrentStatus,
};
