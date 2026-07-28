//! DownloadManager service — re-exported from the download module.
//! This will eventually hold the DownloadManager directly after step 6 split.

pub use crate::download::{
    DownloadManager, FilePieceInfo, Session, TorrentHandle, TorrentState,
    TorrentStatus as DownloadTorrentStatus,
};
