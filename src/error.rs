use std::ffi::CStr;
use thiserror::Error;

#[derive(Error, Debug)]
#[allow(dead_code)]
pub enum TorrentError {
    #[error("Invalid torrent file: {0}")]
    InvalidFile(String),

    #[error("Failed to parse torrent: {0}")]
    ParseError(String),

    #[error("IO error: {0}")]
    IoError(String),

    #[error("Null pointer encountered")]
    NullPointer,

    #[error("No peers available: {0}. Tracker returned 0 peers and 0 seeds. Check tracker health or try again later.")]
    NoPeers(String),

    #[error("Unknown error: code {code}, message: {message}")]
    Unknown { code: i32, message: String },
}

impl From<std::io::Error> for TorrentError {
    fn from(err: std::io::Error) -> Self {
        TorrentError::IoError(err.to_string())
    }
}

pub type TorrentResult<T> = Result<T, TorrentError>;

/// Convert a C `lt_error_t` pointer into a `TorrentError`.
///
/// # Safety
///
/// `error` must be either null or a valid, aligned pointer to an `lt_error_t`.
pub(crate) unsafe fn error_from_c(error: *const libtorrent_sys::lt_error_t) -> TorrentError {
    if error.is_null() {
        return TorrentError::Unknown {
            code: -1,
            message: "Unknown error".to_string(),
        };
    }

    // SAFETY: `error` was checked for null above; if non-null it is a valid
    // pointer from libtorrent that remains valid for the duration of this call.
    let error_ref = &*error;
    let message = if error_ref.message.is_null() {
        "Unknown error".to_string()
    } else {
        // SAFETY: `message` was checked for non-null above; the C string is
        // NUL-terminated per libtorrent's contract and remains valid here.
        CStr::from_ptr(error_ref.message)
            .to_string_lossy()
            .into_owned()
    };

    if message.contains("bdecode") || message.contains("parse") || message.contains("invalid") {
        TorrentError::ParseError(message)
    } else if message.contains("file") || message.contains("path") || message.contains("not found")
    {
        TorrentError::InvalidFile(message)
    } else {
        TorrentError::Unknown {
            code: error_ref.code,
            message,
        }
    }
}
