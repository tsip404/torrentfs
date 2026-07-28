use std::io::Write;
use torrentfs::{TorrentError, TorrentInfo};

#[test]
fn test_parse_invalid_file_returns_error() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    write!(file, "this is not a valid torrent file").unwrap();

    let result = TorrentInfo::from_file(file.path());
    assert!(result.is_err());

    match result {
        Err(TorrentError::ParseError(_)) => (),
        Err(e) => panic!("Expected ParseError, got {:?}", e),
        Ok(_) => panic!("Expected error, got success"),
    }
}

#[test]
fn test_parse_nonexistent_file_returns_error() {
    let result = TorrentInfo::from_file("/nonexistent/path/to/file.torrent");
    assert!(result.is_err());
}

#[test]
fn test_parse_empty_file_returns_error() {
    let file = tempfile::NamedTempFile::new().unwrap();

    let result = TorrentInfo::from_file(file.path());
    assert!(result.is_err());
}
