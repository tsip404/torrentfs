//! Download module — libtorrent session management and piece download orchestration.
//!
//! Split from the original monolithic `download.rs` (1219 lines) into focused sub-modules:
//! - `types` — data types (SessionStats, TorrentStatus, TorrentState, FilePieceInfo)
//! - `session` — Session and TorrentHandle wrappers around libtorrent FFI
//! - `manager` — DownloadManager orchestrating piece downloads with caching

mod manager;
mod session;
mod types;

pub use manager::DownloadManager;
pub use session::{Session, TorrentHandle};
pub use types::{FilePieceInfo, SessionStats, TorrentState, TorrentStatus};
