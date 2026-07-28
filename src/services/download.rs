//! DownloadManager service — re-exported from the download module.
//!
//! With the TSI-1987 split, DownloadManager now lives in `download::manager`
//! alongside the Session/TorrentHandle in `download::session`.
//!
//! This module will eventually host a higher-level download orchestration service.

pub use crate::download::{
    DownloadManager, FilePieceInfo, Session, TorrentHandle, TorrentState,
    TorrentStatus as DownloadTorrentStatus,
};
