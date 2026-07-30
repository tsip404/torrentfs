pub mod domain;
pub mod error;
pub mod fuse;
pub mod infrastructure;
pub mod network;
pub mod seeding;
pub mod services;
pub mod storage;
pub mod torrent;

// Re-exports for backward compatibility — old paths still work
// infrastructure layer
pub use infrastructure::{
    cache, config, db, download, metadata, CacheManager, Database, DownloadManager, FilePieceInfo,
    PieceMetadata, Session, TorrentHandle, TorrentInfo, TorrentState, TorrentfsConfig,
    DownloadTorrentStatus,
};
pub use infrastructure::db::{
    DbError, FileEntry, InsertTorrentResult, Torrent, TorrentDirectory, TorrentFile, TorrentStatus,
};

// domain layer re-exports
pub use domain::{
    DbError as DomainDbError, FileEntry as DomainFileEntry, FileRepository,
    InsertTorrentResult as DomainInsertResult, Torrent as DomainTorrent,
    TorrentDirectory as DomainTorrentDirectory, TorrentFile as DomainTorrentFile,
    TorrentRepository, TorrentStatus as DomainTorrentStatus,
};

// other re-exports
pub use error::{TorrentError, TorrentResult};
pub use fuse::TorrentFs;
pub use seeding::{SeedingInfo, SeedingManager, SeedingState};
