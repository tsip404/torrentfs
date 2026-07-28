//! Torrent domain types — re-exported from the db module.
//! This module will eventually hold the TorrentRepository trait and SQLite implementation.

pub use crate::db::{
    Database, DbError, FileEntry, InsertTorrentResult, Torrent, TorrentDirectory, TorrentFile,
    TorrentStatus,
};
