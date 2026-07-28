pub mod cache;
pub mod config;
pub mod db;
pub mod download;
pub mod error;
pub mod fuse;
pub mod metadata;
pub mod network;
pub mod seeding;
pub mod services;
pub mod storage;
pub mod torrent;

// Re-exports for backward compatibility
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
pub use error::{TorrentError, TorrentResult};
pub use fuse::TorrentFs;
pub use metadata::TorrentInfo;
pub use seeding::{SeedingInfo, SeedingManager, SeedingState};
