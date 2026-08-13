//! Single errno mapping exit point: `FsError` → `libc::c_int`.
//!
//! This is the ONLY place in the codebase that translates a domain `FsError`
//! into an errno. See the architecture design doc §7.2 for the full table.

use libc::{
    EACCES, EEXIST, EFBIG, EINVAL, EIO, EISDIR, ENAMETOOLONG, ENOENT, ENOMEM, ENOSPC, ENOTDIR,
    ENOTEMPTY, EPERM, EROFS, ESTALE,
};

use crate::domain::fs_error::FsError;

impl From<FsError> for libc::c_int {
    fn from(e: FsError) -> Self {
        match e {
            // ── path / namespace ──
            FsError::NotFound => ENOENT,
            FsError::NotDirectory => ENOTDIR,
            FsError::IsDirectory => EISDIR,
            FsError::AlreadyExists => EEXIST,
            FsError::DirectoryNotEmpty => ENOTEMPTY,
            FsError::StaleHandle => ESTALE,
            // ── permission ──
            FsError::PermissionDenied => EACCES,
            FsError::ReadOnlyFileSystem => EROFS,
            FsError::NotPermitted => EPERM,
            // ── argument / input ──
            FsError::InvalidArgument => EINVAL,
            FsError::FileTooLarge(_) => EFBIG,
            FsError::NameTooLong => ENAMETOOLONG,
            // ── data integrity ──
            FsError::CorruptTorrent(_) => EINVAL,
            FsError::CorruptPiece(_) => EIO,
            // ── persistence ──
            FsError::Database(_) | FsError::Migration(_) => EIO,
            // ── remote / network ──
            FsError::NoPeers(_)
            | FsError::PieceNotReady(_)
            | FsError::DownloadTimeout(_)
            | FsError::DownloadFailed(_) => EIO,
            // ── internal / system ──
            FsError::Io(_) => EIO,
            FsError::NoSpace(_) => ENOSPC,
            FsError::OutOfMemory(_) => ENOMEM,
            FsError::Internal(_) | FsError::LockPoisoned => EIO,
        }
    }
}
