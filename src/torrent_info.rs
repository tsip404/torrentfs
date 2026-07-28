use std::ffi::{CStr, CString};
use std::path::Path;
use std::ptr;

use crate::error::{error_from_c, TorrentError, TorrentResult};

pub struct TorrentInfo {
    pub(crate) inner: libtorrent_sys::lt_torrent_info_t,
}

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: String,
    pub size: u64,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TorrentMetadata {
    pub name: String,
    pub total_size: u64,
    pub piece_length: u32,
    pub num_pieces: u32,
    pub num_files: u32,
    pub files: Vec<FileInfo>,
    pub info_hash: [u8; 20],
}

impl TorrentInfo {
    #[allow(dead_code)]
    pub fn from_file<P: AsRef<Path>>(path: P) -> TorrentResult<Self> {
        let path_str = path
            .as_ref()
            .to_str()
            .ok_or_else(|| TorrentError::InvalidFile("Path contains invalid UTF-8".to_string()))?;

        let c_path = CString::new(path_str)
            .map_err(|_| TorrentError::InvalidFile("Path contains null byte".to_string()))?;

        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };

        let inner = unsafe { libtorrent_sys::lt_torrent_info_create(c_path.as_ptr(), &mut error) };

        if inner.is_null() {
            Err(unsafe { error_from_c(&error) })
        } else {
            Ok(TorrentInfo { inner })
        }
    }

    pub fn from_bytes(data: Vec<u8>) -> TorrentResult<Self> {
        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };

        let inner = unsafe {
            libtorrent_sys::lt_torrent_info_create_from_buffer(
                data.as_ptr(),
                data.len(),
                &mut error,
            )
        };

        if inner.is_null() {
            Err(unsafe { error_from_c(&error) })
        } else {
            Ok(TorrentInfo { inner })
        }
    }

    pub fn name(&self) -> String {
        unsafe {
            let name_ptr = libtorrent_sys::lt_torrent_info_name(self.inner);
            if name_ptr.is_null() {
                String::new()
            } else {
                CStr::from_ptr(name_ptr).to_string_lossy().into_owned()
            }
        }
    }

    pub fn total_size(&self) -> u64 {
        unsafe { libtorrent_sys::lt_torrent_info_total_size(self.inner) }
    }

    pub fn piece_length(&self) -> u32 {
        unsafe { libtorrent_sys::lt_torrent_info_piece_length(self.inner) }
    }

    pub fn num_pieces(&self) -> u32 {
        unsafe { libtorrent_sys::lt_torrent_info_num_pieces(self.inner) }
    }

    pub fn num_files(&self) -> u32 {
        unsafe { libtorrent_sys::lt_torrent_info_num_files(self.inner) }
    }

    pub fn files(&self) -> TorrentResult<Vec<FileInfo>> {
        let mut files_ptr: *mut libtorrent_sys::lt_file_entry_t = ptr::null_mut();
        let mut count: u32 = 0;

        let result = unsafe {
            libtorrent_sys::lt_torrent_info_get_files(self.inner, &mut files_ptr, &mut count)
        };

        if result != 0 {
            return Err(TorrentError::Unknown {
                code: result,
                message: "Failed to get file list".to_string(),
            });
        }

        let files = if count == 0 || files_ptr.is_null() {
            Vec::new()
        } else {
            let slice = unsafe { std::slice::from_raw_parts(files_ptr, count as usize) };
            let result: Vec<FileInfo> = slice
                .iter()
                .map(|entry| FileInfo {
                    path: if entry.path.is_null() {
                        String::new()
                    } else {
                        unsafe { CStr::from_ptr(entry.path) }
                            .to_string_lossy()
                            .into_owned()
                    },
                    size: entry.size,
                })
                .collect();

            unsafe { libtorrent_sys::lt_files_free(files_ptr) };

            result
        };

        Ok(files)
    }

    pub fn info_hash(&self) -> TorrentResult<[u8; 20]> {
        let mut hash = [0u8; 20];
        let result =
            unsafe { libtorrent_sys::lt_torrent_info_get_info_hash(self.inner, hash.as_mut_ptr()) };

        if result != 0 {
            Err(TorrentError::Unknown {
                code: result,
                message: "Failed to get info hash".to_string(),
            })
        } else {
            Ok(hash)
        }
    }

    pub fn metadata(&self) -> TorrentResult<TorrentMetadata> {
        Ok(TorrentMetadata {
            name: self.name(),
            total_size: self.total_size(),
            piece_length: self.piece_length(),
            num_pieces: self.num_pieces(),
            num_files: self.num_files(),
            files: self.files()?,
            info_hash: self.info_hash()?,
        })
    }
}

impl Drop for TorrentInfo {
    fn drop(&mut self) {
        unsafe {
            libtorrent_sys::lt_torrent_info_destroy(self.inner);
        }
    }
}

unsafe impl Send for TorrentInfo {}
unsafe impl Sync for TorrentInfo {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn create_test_torrent() -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write!(file, "this is not a valid torrent file").unwrap();
        file
    }

    #[test]
    fn test_invalid_torrent_returns_error() {
        let tmp = create_test_torrent();
        let result = TorrentInfo::from_file(tmp.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_nonexistent_file_returns_error() {
        let result = TorrentInfo::from_file("/nonexistent/path.torrent");
        assert!(result.is_err());
    }
}
