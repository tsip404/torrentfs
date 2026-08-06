//! Domain error types — core error enum and conversion utilities.

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

    #[error("Piece not ready: {0}")]
    PieceNotReady(String),

    #[error("Operation timed out: {0}")]
    Timeout(String),

    #[error("Unknown error: code {code}, message: {message}")]
    Unknown { code: i32, message: String },
}

impl TorrentError {
    /// Returns `true` if this error represents a transient condition that
    /// may succeed on retry (e.g., timeout, no peers yet, piece not ready).
    ///
    /// Permanent errors (invalid file, parse errors, null pointers) return
    /// `false`.  `IoError` is conservatively treated as non-transient since
    /// I/O errors may indicate disk-full or permission problems.
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            TorrentError::NoPeers(_) | TorrentError::PieceNotReady(_) | TorrentError::Timeout(_)
        )
    }
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

    let lower = message.to_lowercase();
    if lower.contains("bdecode") || lower.contains("parse") || lower.contains("invalid") {
        TorrentError::ParseError(message)
    } else if lower.contains("file") || lower.contains("path") || lower.contains("not found") {
        TorrentError::InvalidFile(message)
    } else if lower.contains("timed out") || lower.contains("timeout") {
        TorrentError::Timeout(message)
    } else {
        TorrentError::Unknown {
            code: error_ref.code,
            message,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── is_transient() ──────────────────────────────────────────────────

    #[test]
    fn test_is_transient_true_for_no_peers() {
        assert!(TorrentError::NoPeers("no peers".into()).is_transient());
    }

    #[test]
    fn test_is_transient_true_for_piece_not_ready() {
        assert!(TorrentError::PieceNotReady("piece 0".into()).is_transient());
    }

    #[test]
    fn test_is_transient_true_for_timeout() {
        assert!(TorrentError::Timeout("timed out".into()).is_transient());
    }

    #[test]
    fn test_is_transient_false_for_invalid_file() {
        assert!(!TorrentError::InvalidFile("bad file".into()).is_transient());
    }

    #[test]
    fn test_is_transient_false_for_parse_error() {
        assert!(!TorrentError::ParseError("bad parse".into()).is_transient());
    }

    #[test]
    fn test_is_transient_false_for_io_error() {
        assert!(!TorrentError::IoError("disk full".into()).is_transient());
    }

    #[test]
    fn test_is_transient_false_for_null_pointer() {
        assert!(!TorrentError::NullPointer.is_transient());
    }

    #[test]
    fn test_is_transient_false_for_unknown() {
        assert!(!TorrentError::Unknown {
            code: -1,
            message: "unknown".into()
        }
        .is_transient());
    }

    // ── error_from_c ────────────────────────────────────────────────────

    /// Helper: simulate an lt_error_t with the given message string.
    unsafe fn make_lt_error(msg: &str) -> libtorrent_sys::lt_error_t {
        use std::ffi::CString;
        let c_msg = CString::new(msg).unwrap();
        libtorrent_sys::lt_error_t {
            message: c_msg.into_raw(),
            code: 0,
        }
    }

    #[test]
    fn test_error_from_c_null_returns_unknown() {
        let result = unsafe { error_from_c(std::ptr::null()) };
        assert!(matches!(result, TorrentError::Unknown { .. }));
    }

    #[test]
    fn test_error_from_c_timeout_lowercase() {
        let err = unsafe { make_lt_error("operation timed out") };
        let result = unsafe { error_from_c(&err) };
        assert!(matches!(result, TorrentError::Timeout(_)));
    }

    #[test]
    fn test_error_from_c_timeout_titlecase() {
        let err = unsafe { make_lt_error("Timed out waiting for peers") };
        let result = unsafe { error_from_c(&err) };
        assert!(matches!(result, TorrentError::Timeout(_)));
    }

    #[test]
    fn test_error_from_c_timeout_uppercase() {
        let err = unsafe { make_lt_error("TIMEOUT: connection failed") };
        let result = unsafe { error_from_c(&err) };
        assert!(matches!(result, TorrentError::Timeout(_)));
    }

    #[test]
    fn test_error_from_c_timeout_as_substring() {
        let err = unsafe { make_lt_error("request timed out after 30s") };
        let result = unsafe { error_from_c(&err) };
        assert!(matches!(result, TorrentError::Timeout(_)));
    }

    #[test]
    fn test_error_from_c_bdecode_maps_to_parse_error() {
        let err = unsafe { make_lt_error("bdecode error: invalid encoding") };
        let result = unsafe { error_from_c(&err) };
        assert!(matches!(result, TorrentError::ParseError(_)));
    }

    #[test]
    fn test_error_from_c_invalid_maps_to_parse_error() {
        let err = unsafe { make_lt_error("invalid torrent file") };
        let result = unsafe { error_from_c(&err) };
        assert!(matches!(result, TorrentError::ParseError(_)));
    }

    #[test]
    fn test_error_from_c_file_maps_to_invalid_file() {
        let err = unsafe { make_lt_error("file not found: /path/to/torrent") };
        let result = unsafe { error_from_c(&err) };
        assert!(matches!(result, TorrentError::InvalidFile(_)));
    }

    #[test]
    fn test_error_from_c_unclassified_maps_to_unknown() {
        let err = unsafe { make_lt_error("some random error text") };
        let result = unsafe { error_from_c(&err) };
        assert!(matches!(result, TorrentError::Unknown { .. }));
    }
}
