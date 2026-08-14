use std::ffi::CString;
use std::path::Path;
use std::ptr;

use crate::config::TorrentfsConfig;
use crate::error::{error_from_c, TorrentError, TorrentResult};

use super::types::{FilePieceInfo, SessionStats, TorrentState, TorrentStatus};

pub struct Session {
    pub(crate) inner: libtorrent_sys::lt_session_t,
    /// Settings JSON for applying to the session (via apply_settings or baked
    /// into session_params during session creation).
    settings_json: CString,
}

pub struct TorrentHandle {
    pub(crate) inner: libtorrent_sys::lt_torrent_handle_t,
    pub(crate) info_hash: String,
    #[allow(dead_code)]
    pub(crate) session: libtorrent_sys::lt_session_t,
}

impl Session {
    pub fn new(config: &TorrentfsConfig) -> TorrentResult<Self> {
        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };

        // Create session with default settings (no listen_interface, no custom storage)
        let inner = unsafe { libtorrent_sys::lt_session_create(ptr::null(), &mut error) };

        if inner.is_null() {
            return Err(unsafe { error_from_c(&error) });
        }

        // Save settings JSON for later application
        let settings_json = config.to_settings_json();
        let settings_json_c = CString::new(&settings_json[..]).unwrap_or_default();

        let session = Session {
            inner,
            settings_json: settings_json_c,
        };

        // Apply user configuration via JSON
        if settings_json != "{}" {
            unsafe {
                libtorrent_sys::lt_session_apply_settings(
                    session.inner,
                    session.settings_json.as_ptr(),
                );
            }
        }

        Ok(session)
    }

    /// Create a session with custom piece storage (PieceStorageDiskIO) from the start.
    /// Settings are baked into the session_params on the C++ side, so no post-hoc
    /// apply_settings is needed.
    pub fn new_with_custom_storage(
        config: &TorrentfsConfig,
        piece_cache_dir: &Path,
    ) -> TorrentResult<Self> {
        let settings_json = config.to_settings_json();
        let settings_json_c = CString::new(&settings_json[..]).unwrap_or_default();

        let piece_cache_dir_c = CString::new(piece_cache_dir.to_string_lossy().into_owned())
            .map_err(|_| TorrentError::InvalidFile("Cache dir contains null byte".to_string()))?;

        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };

        let inner = unsafe {
            libtorrent_sys::lt_session_create_with_custom_storage(
                piece_cache_dir_c.as_ptr(),
                if settings_json != "{}" {
                    settings_json_c.as_ptr()
                } else {
                    ptr::null()
                },
                &mut error,
            )
        };

        if inner.is_null() {
            return Err(unsafe { error_from_c(&error) });
        }

        Ok(Session {
            inner,
            settings_json: settings_json_c,
        })
    }

    /// Read a boolean setting from the live libtorrent session.
    pub fn get_bool_setting(&self, key: &str) -> TorrentResult<bool> {
        let key_c = CString::new(key).map_err(|_| TorrentError::Unknown {
            code: -1,
            message: "Setting key contains null byte".to_string(),
        })?;
        let mut out: i32 = 0;
        let result = unsafe {
            libtorrent_sys::lt_session_get_bool_setting(self.inner, key_c.as_ptr(), &mut out)
        };
        if result == 0 {
            Ok(out != 0)
        } else {
            Err(TorrentError::Unknown {
                code: result,
                message: format!("Setting '{}' not found or session unavailable", key),
            })
        }
    }

    /// Re-apply saved settings JSON to the libtorrent session.
    /// No longer needed in normal flow: settings are now baked into session_params
    /// on the C++ side during session rebuild. Kept for testing / manual recovery.
    #[allow(dead_code)]
    fn reapply_settings(&self) {
        if self.settings_json.as_bytes() != b"{}" {
            unsafe {
                libtorrent_sys::lt_session_apply_settings(self.inner, self.settings_json.as_ptr());
            }
        }
    }

    pub fn add_torrent(
        &mut self,
        info: &crate::TorrentInfo,
        save_path: &Path,
    ) -> TorrentResult<TorrentHandle> {
        let save_path_c = CString::new(save_path.to_string_lossy().into_owned())
            .map_err(|_| TorrentError::InvalidFile("Save path contains null byte".to_string()))?;

        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };

        let handle = unsafe {
            libtorrent_sys::lt_session_add_torrent(
                self.inner,
                info.inner,
                save_path_c.as_ptr(),
                &mut error,
            )
        };

        if handle.is_null() {
            Err(unsafe { error_from_c(&error) })
        } else {
            let info_hash = hex::encode(info.info_hash()?);
            Ok(TorrentHandle {
                inner: handle,
                info_hash,
                session: self.inner,
            })
        }
    }

    /// Add a torrent in upload_mode: connects to trackers and peers
    /// but never requests pieces. Use for lightweight status-only handles.
    pub fn add_torrent_upload_mode(
        &mut self,
        info: &crate::TorrentInfo,
        save_path: &Path,
    ) -> TorrentResult<TorrentHandle> {
        let save_path_c = CString::new(save_path.to_string_lossy().into_owned())
            .map_err(|_| TorrentError::InvalidFile("Save path contains null byte".to_string()))?;

        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };

        let handle = unsafe {
            libtorrent_sys::lt_session_add_torrent_upload_mode(
                self.inner,
                info.inner,
                save_path_c.as_ptr(),
                &mut error,
            )
        };

        if handle.is_null() {
            Err(unsafe { error_from_c(&error) })
        } else {
            let info_hash = hex::encode(info.info_hash()?);
            Ok(TorrentHandle {
                inner: handle,
                info_hash,
                session: self.inner,
            })
        }
    }

    #[allow(dead_code)]
    pub fn remove_torrent(&mut self, handle: TorrentHandle, remove_files: bool) {
        unsafe {
            libtorrent_sys::lt_session_remove_torrent(
                self.inner,
                handle.inner,
                if remove_files { 1 } else { 0 },
            );
        }
    }

    pub(crate) fn inner(&self) -> libtorrent_sys::lt_session_t {
        self.inner
    }

    /// Get session-level statistics (rates, connections, DHT nodes).
    pub fn get_stats(&self) -> TorrentResult<SessionStats> {
        let mut stats = libtorrent_sys::lt_session_stats_t {
            download_rate: 0,
            upload_rate: 0,
            total_downloaded: 0,
            total_uploaded: 0,
            dht_nodes: 0,
            peers_connected: 0,
            half_open_connections: 0,
        };
        let mut status: i32 = -1;

        let result =
            unsafe { libtorrent_sys::lt_session_get_stats(self.inner, &mut stats, &mut status) };

        if result != 0 {
            Err(TorrentError::Unknown {
                code: result,
                message: "Failed to get session stats".to_string(),
            })
        } else {
            Ok(SessionStats {
                download_rate: stats.download_rate,
                upload_rate: stats.upload_rate,
                total_downloaded: stats.total_downloaded,
                total_uploaded: stats.total_uploaded,
                dht_nodes: stats.dht_nodes,
                peers_connected: stats.peers_connected,
                half_open_connections: stats.half_open_connections,
            })
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        unsafe {
            libtorrent_sys::lt_session_destroy(self.inner);
        }
    }
}


impl TorrentHandle {
    pub fn is_valid(&self) -> bool {
        unsafe { libtorrent_sys::lt_torrent_handle_is_valid(self.inner) != 0 }
    }

    pub fn status(&self) -> TorrentResult<TorrentStatus> {
        let mut state: i32 = 0;
        let mut progress: f32 = 0.0;
        let mut total_done: u64 = 0;
        let mut total: u64 = 0;
        let mut download_rate: i64 = 0;
        let mut upload_rate: i64 = 0;
        let mut total_download: i64 = 0;
        let mut total_upload: i64 = 0;
        let mut num_peers: i32 = 0;
        let mut num_seeds: i32 = 0;

        let result = unsafe {
            libtorrent_sys::lt_torrent_handle_status(
                self.inner,
                &mut state,
                &mut progress,
                &mut total_done,
                &mut total,
                &mut download_rate,
                &mut upload_rate,
                &mut total_download,
                &mut total_upload,
                &mut num_peers,
                &mut num_seeds,
            )
        };

        if result != 0 {
            Err(TorrentError::Unknown {
                code: result,
                message: "Failed to get torrent status".to_string(),
            })
        } else {
            Ok(TorrentStatus {
                state: TorrentState::from(state),
                progress,
                total_done,
                total,
                download_rate,
                upload_rate,
                total_download,
                total_upload,
                num_peers,
                num_seeds,
            })
        }
    }


    pub fn get_file_piece_info(&self, file_index: i32) -> TorrentResult<FilePieceInfo> {
        let mut first_piece: i64 = 0;
        let mut num_pieces: i64 = 0;
        let mut file_offset: i64 = 0;

        let result = unsafe {
            libtorrent_sys::lt_torrent_handle_get_piece_info(
                self.inner,
                file_index,
                &mut first_piece,
                &mut num_pieces,
                &mut file_offset,
            )
        };

        if result != 0 {
            Err(TorrentError::Unknown {
                code: result,
                message: "Failed to get file piece info".to_string(),
            })
        } else {
            Ok(FilePieceInfo {
                first_piece,
                num_pieces,
                file_offset,
            })
        }
    }

    pub fn get_torrent_info(&self) -> TorrentResult<(i64, i64)> {
        let mut piece_length: i64 = 0;
        let mut num_pieces: i64 = 0;

        let result = unsafe {
            libtorrent_sys::lt_torrent_handle_get_torrent_info(
                self.inner,
                &mut piece_length,
                &mut num_pieces,
            )
        };

        if result != 0 {
            Err(TorrentError::Unknown {
                code: result,
                message: "Failed to get torrent info from handle".to_string(),
            })
        } else {
            Ok((piece_length, num_pieces))
        }
    }

    pub fn have_piece(&self, piece_index: i32) -> bool {
        unsafe { libtorrent_sys::lt_torrent_handle_have_piece(self.inner, piece_index) != 0 }
    }

    /// Set a piece deadline to prioritize downloading this piece.
    pub fn set_piece_deadline(&self, piece_index: i32, deadline_ms: i32) -> bool {
        unsafe {
            libtorrent_sys::lt_torrent_handle_set_piece_deadline(
                self.inner,
                piece_index,
                deadline_ms,
            ) == 0
        }
    }

    /// Set piece priority for seeding/availability announcement.
    pub fn set_piece_priority(&self, piece_index: i32, priority: i32) -> bool {
        unsafe {
            libtorrent_sys::lt_torrent_handle_set_piece_priority(self.inner, piece_index, priority)
                == 0
        }
    }

    pub fn info_hash(&self) -> &str {
        &self.info_hash
    }
}

impl Drop for TorrentHandle {
    fn drop(&mut self) {
        if !self.inner.is_null() {
            unsafe {
                libtorrent_sys::lt_torrent_handle_destroy(self.inner);
            }
        }
    }
}
