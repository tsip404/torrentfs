//! Domain layer — core domain types, errors, and repository traits.
//!
//! This module contains the pure domain model with no infrastructure dependencies.
//! Sub-modules:
//! - `types` — domain data types (Torrent, TorrentFile, etc.)
//! - `error` — domain error types (TorrentError, TorrentResult)
//! - `repository` — repository traits (TorrentRepository, FileRepository)

pub mod error;
pub mod repository;
pub mod types;

pub use error::{TorrentError, TorrentResult};
pub use repository::{FileRepository, TorrentRepository};
pub use types::{
    DbError, FileEntry, InsertTorrentResult, Torrent, TorrentDirectory, TorrentFile, TorrentStatus,
};
