//! Database module — SQLite-backed persistence layer for torrent metadata.
//!
//! Split from the original monolithic `db.rs` (2277 lines) into focused sub-modules:
//! - `types` — data types and error types
//! - `database` — Database struct, connection management, migrations
//! - `torrent_ops` — torrent CRUD operations
//! - `file_ops` — file and directory query operations
//! - `metadata_ops` — metadata directory management operations

mod database;
mod file_ops;
mod metadata_ops;
#[cfg(test)]
mod tests;
mod torrent_ops;
mod types;

pub use database::Database;
pub use types::{
    DbError, FileEntry, InsertTorrentResult, Torrent, TorrentDirectory, TorrentFile, TorrentStatus,
};
