//! Network layer — Session and TorrentHandle types.
//!
//! With the TSI-1987 split, download.rs was decomposed into:
//! - `download::types` — data types (SessionStats, TorrentStatus, TorrentState, FilePieceInfo)
//! - `download::session` — Session and TorrentHandle FFI wrappers
//! - `download::manager` — DownloadManager orchestration
//!
//! Re-exported from the download module for forward compatibility.

pub use crate::download::{Session, TorrentHandle, TorrentState};
