pub mod domain;
pub mod error;
pub mod fuse;
pub mod infrastructure;
pub mod seeding;
pub mod services;

// Re-exports for backward compatibility — old paths still work
// infrastructure layer
pub use infrastructure::db::{
    DbError, FileEntry, InsertTorrentResult, Torrent, TorrentDirectory, TorrentFile, TorrentStatus,
};
pub use infrastructure::{
    cache, config, db, download, metadata, CacheManager, Database, DownloadManager,
    DownloadTorrentStatus, FilePieceInfo, PieceMetadata, Session, TorrentHandle, TorrentInfo,
    TorrentState, TorrentfsConfig,
};

// domain layer re-exports
pub use domain::{
    DbError as DomainDbError, FileEntry as DomainFileEntry, FileRepository,
    InsertTorrentResult as DomainInsertResult, PiecePriorityConfig, PiecesManager,
    Torrent as DomainTorrent,
    TorrentDirectory as DomainTorrentDirectory, TorrentFile as DomainTorrentFile,
    TorrentRepository, TorrentStatus as DomainTorrentStatus,
};

// other re-exports
pub use error::{is_transient_read_error, TorrentError, TorrentResult};
pub use fuse::TorrentFs;
pub use seeding::{SeedingInfo, SeedingManager, SeedingState};
pub use services::download::DownloadService;
pub use services::seeding::SeedingService;
