//! Domain layer — core domain types, errors, and repository traits.
//!
//! This module contains the pure domain model with no infrastructure dependencies.
//! Sub-modules:
//! - `types` — domain data types (Torrent, TorrentFile, etc.)
//! - `error` — domain error types (TorrentError, TorrentResult)
//! - `repository` — repository traits (TorrentRepository, FileRepository)
//! - `pieces_manager` — piece priority lifecycle management (PiecesManager)

pub mod error;
pub mod fs_error;
pub mod pieces_manager;
pub mod repository;
pub mod types;

pub use error::{TorrentError, TorrentResult};
pub use fs_error::{FsError, FsResult};
pub use pieces_manager::{PiecePriorityConfig, PiecesManager};
pub use repository::{FileRepository, TorrentRepository};
pub use types::{
    DbError, FileEntry, InsertTorrentResult, Torrent, TorrentDirectory, TorrentFile, TorrentStatus,
};
