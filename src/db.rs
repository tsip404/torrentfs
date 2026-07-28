use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;

#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum DbError {
    #[error("database error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("torrent with source_path already exists: {0}")]
    SourcePathExists(String),
    #[error("migration error: {0}")]
    Migration(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum TorrentStatus {
    Pending,
    Downloading,
    Seeding,
    Error,
}

impl TorrentStatus {
    #[allow(dead_code)]
    pub fn as_str(&self) -> &'static str {
        match self {
            TorrentStatus::Pending => "pending",
            TorrentStatus::Downloading => "downloading",
            TorrentStatus::Seeding => "seeding",
            TorrentStatus::Error => "error",
        }
    }
}

impl From<&str> for TorrentStatus {
    fn from(s: &str) -> Self {
        match s {
            "downloading" => TorrentStatus::Downloading,
            "seeding" => TorrentStatus::Seeding,
            "error" => TorrentStatus::Error,
            _ => TorrentStatus::Pending,
        }
    }
}

impl From<String> for TorrentStatus {
    fn from(s: String) -> Self {
        TorrentStatus::from(s.as_str())
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Torrent {
    pub id: i64,
    pub source_path: String,
    pub name: String,
    pub filename: String,
    pub total_size: i64,
    pub info_hash: String,
    pub file_count: i64,
    pub status: TorrentStatus,
    pub torrent_data: Option<Vec<u8>>,
    pub resume_data: Option<Vec<u8>>,
    pub created_at: String,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TorrentFile {
    pub id: i64,
    pub torrent_id: i64,
    pub directory_id: Option<i64>,
    pub name: String,
    pub path: String,
    pub size: i64,
    pub first_piece: i64,
    pub last_piece: i64,
    pub piece_start: Option<i64>,
    pub piece_end: Option<i64>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TorrentDirectory {
    pub id: i64,
    pub torrent_id: i64,
    pub parent_id: Option<i64>,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InsertTorrentResult {
    Inserted(i64),
    Duplicate(i64),
}

pub struct FileEntry {
    pub path: String,
    pub size: i64,
}

pub struct Database {
    conn: Connection,
}

impl Database {
    pub fn open(path: &Path) -> Result<Self, DbError> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        let mut db = Self { conn };
        db.run_migrations()?;
        Ok(db)
    }

    #[allow(dead_code)]
    pub fn open_in_memory() -> Result<Self, DbError> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
        let mut db = Self { conn };
        db.run_migrations()?;
        Ok(db)
    }

    fn run_migrations(&mut self) -> Result<(), DbError> {
        let tx = self.conn.transaction()?;
        let user_version: i64 = tx
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .optional()?
            .unwrap_or(0);

        if user_version < 1 {
            Self::migrate_v1(&tx)?;
            tx.pragma_update(None, "user_version", 2)?;
        } else if user_version == 1 {
            Self::migrate_v2(&tx)?;
            tx.pragma_update(None, "user_version", 2)?;
        }

        if user_version < 3 {
            Self::migrate_v3(&tx)?;
            tx.pragma_update(None, "user_version", 3)?;
        }

        if user_version < 4 {
            Self::migrate_v4(&tx)?;
            tx.pragma_update(None, "user_version", 4)?;
        }

        tx.commit()?;

        if user_version < 3 {
            let paths: Vec<String> = {
                let mut stmt = self
                    .conn
                    .prepare("SELECT DISTINCT source_path FROM torrents WHERE source_path != ''")?;
                let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
                rows.collect::<Result<Vec<_>, _>>()?
            };

            for path in paths {
                if let Err(e) = self.ensure_metadata_directories(&path) {
                    tracing::warn!("Failed to create metadata directories for {}: {}", path, e);
                }
            }
        }

        Ok(())
    }

    fn migrate_v1(conn: &Connection) -> Result<(), DbError> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS torrents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                info_hash TEXT NOT NULL,
                name TEXT NOT NULL,
                total_size INTEGER NOT NULL,
                file_count INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL DEFAULT 'pending',
                source_path TEXT NOT NULL DEFAULT '',
                torrent_data BLOB,
                resume_data BLOB,
                created_at DATETIME NOT NULL DEFAULT (datetime('now')),
                UNIQUE(info_hash, source_path)
            );

            CREATE INDEX IF NOT EXISTS idx_torrents_info_hash ON torrents(info_hash);
            CREATE INDEX IF NOT EXISTS idx_torrents_status ON torrents(status);
            CREATE INDEX IF NOT EXISTS idx_torrents_info_hash_source_path ON torrents(info_hash, source_path);
            CREATE INDEX IF NOT EXISTS idx_torrents_source_path ON torrents(source_path);

            CREATE TABLE IF NOT EXISTS torrent_directories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                torrent_id INTEGER NOT NULL,
                parent_id INTEGER,
                name TEXT NOT NULL,
                FOREIGN KEY (torrent_id) REFERENCES torrents(id) ON DELETE CASCADE,
                FOREIGN KEY (parent_id) REFERENCES torrent_directories(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_torrent_dirs_torrent_id ON torrent_directories(torrent_id);
            CREATE INDEX IF NOT EXISTS idx_torrent_dirs_parent_id ON torrent_directories(parent_id);

            CREATE TABLE IF NOT EXISTS directory_closure (
                ancestor_id INTEGER NOT NULL,
                descendant_id INTEGER NOT NULL,
                depth INTEGER NOT NULL,
                PRIMARY KEY (ancestor_id, descendant_id),
                FOREIGN KEY (ancestor_id) REFERENCES torrent_directories(id) ON DELETE CASCADE,
                FOREIGN KEY (descendant_id) REFERENCES torrent_directories(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_closure_descendant ON directory_closure(descendant_id);

            CREATE TABLE IF NOT EXISTS torrent_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                torrent_id INTEGER NOT NULL,
                directory_id INTEGER,
                name TEXT NOT NULL,
                path TEXT NOT NULL DEFAULT '',
                size INTEGER NOT NULL,
                first_piece INTEGER NOT NULL DEFAULT 0,
                last_piece INTEGER NOT NULL DEFAULT 0,
                piece_start INTEGER,
                piece_end INTEGER,
                FOREIGN KEY (torrent_id) REFERENCES torrents(id) ON DELETE CASCADE,
                FOREIGN KEY (directory_id) REFERENCES torrent_directories(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_torrent_files_torrent_id ON torrent_files(torrent_id);
            CREATE INDEX IF NOT EXISTS idx_torrent_files_directory_id ON torrent_files(directory_id);
            CREATE INDEX IF NOT EXISTS idx_torrent_files_path ON torrent_files(path);",
        )?;
        Ok(())
    }

    fn migrate_v2(conn: &Connection) -> Result<(), DbError> {
        conn.execute_batch(
            "ALTER TABLE torrents ADD COLUMN file_count INTEGER NOT NULL DEFAULT 1;
             ALTER TABLE torrents ADD COLUMN status TEXT NOT NULL DEFAULT 'pending';
             ALTER TABLE torrents ADD COLUMN torrent_data BLOB;
             ALTER TABLE torrents ADD COLUMN resume_data BLOB;

             CREATE INDEX IF NOT EXISTS idx_torrents_status ON torrents(status);
             CREATE INDEX IF NOT EXISTS idx_torrents_info_hash_source_path ON torrents(info_hash, source_path);

             ALTER TABLE torrent_files ADD COLUMN path TEXT NOT NULL DEFAULT '';
             ALTER TABLE torrent_files ADD COLUMN first_piece INTEGER NOT NULL DEFAULT 0;
             ALTER TABLE torrent_files ADD COLUMN last_piece INTEGER NOT NULL DEFAULT 0;

             CREATE INDEX IF NOT EXISTS idx_torrent_files_path ON torrent_files(path);",
        )?;
        Ok(())
    }

    fn migrate_v3(conn: &Connection) -> Result<(), DbError> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS metadata_directories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                parent_id INTEGER,
                name TEXT NOT NULL,
                path TEXT NOT NULL UNIQUE,
                FOREIGN KEY (parent_id) REFERENCES metadata_directories(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_metadata_dirs_parent_id ON metadata_directories(parent_id);
            CREATE INDEX IF NOT EXISTS idx_metadata_dirs_path ON metadata_directories(path);

            CREATE TABLE IF NOT EXISTS metadata_directory_closure (
                ancestor_id INTEGER NOT NULL,
                descendant_id INTEGER NOT NULL,
                depth INTEGER NOT NULL,
                PRIMARY KEY (ancestor_id, descendant_id),
                FOREIGN KEY (ancestor_id) REFERENCES metadata_directories(id) ON DELETE CASCADE,
                FOREIGN KEY (descendant_id) REFERENCES metadata_directories(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_metadata_closure_descendant ON metadata_directory_closure(descendant_id);",
        )?;
        Ok(())
    }

    fn migrate_v4(conn: &Connection) -> Result<(), DbError> {
        conn.execute_batch(
            "ALTER TABLE torrents ADD COLUMN filename TEXT NOT NULL DEFAULT '';
             UPDATE torrents SET filename = name WHERE filename = '';",
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn rebuild_metadata_directories(&mut self) -> Result<(), DbError> {
        let paths: Vec<String> = {
            let mut stmt = self
                .conn
                .prepare("SELECT DISTINCT source_path FROM torrents WHERE source_path != ''")?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            rows.collect::<Result<Vec<_>, _>>()?
        };

        for path in paths {
            self.ensure_metadata_directories(&path)?;
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub fn insert_torrent(
        &mut self,
        source_path: &str,
        name: &str,
        filename: &str,
        total_size: i64,
        info_hash: &str,
        file_count: i64,
    ) -> Result<InsertTorrentResult, DbError> {
        let existing: Option<i64> = self
            .conn
            .query_row(
                "SELECT id FROM torrents WHERE info_hash = ? AND source_path = ?",
                params![info_hash, source_path],
                |row| row.get(0),
            )
            .optional()?
            .flatten();

        if let Some(id) = existing {
            return Ok(InsertTorrentResult::Duplicate(id));
        }

        self.conn.execute(
            "INSERT INTO torrents (source_path, name, filename, total_size, info_hash, file_count, status) VALUES (?, ?, ?, ?, ?, ?, 'pending')",
            params![source_path, name, filename, total_size, info_hash, file_count],
        )?;

        let id = self.conn.last_insert_rowid();

        if !source_path.is_empty() {
            if let Err(e) = self.ensure_metadata_directories(source_path) {
                tracing::warn!(
                    "Failed to create metadata directories for {}: {}",
                    source_path,
                    e
                );
            }
        }

        Ok(InsertTorrentResult::Inserted(id))
    }

    pub fn ensure_metadata_directories(&mut self, source_path: &str) -> Result<(), DbError> {
        let parts: Vec<&str> = source_path.split('/').filter(|s| !s.is_empty()).collect();
        if parts.is_empty() {
            return Ok(());
        }

        let mut current_path = String::new();
        let mut parent_id: Option<i64> = None;

        for part in parts {
            if current_path.is_empty() {
                current_path = part.to_string();
            } else {
                current_path = format!("{}/{}", current_path, part);
            }

            let existing_id: Option<i64> = self
                .conn
                .query_row(
                    "SELECT id FROM metadata_directories WHERE path = ?",
                    params![&current_path],
                    |row| row.get(0),
                )
                .optional()?
                .flatten();

            if let Some(id) = existing_id {
                parent_id = Some(id);
                continue;
            }

            self.conn.execute(
                "INSERT INTO metadata_directories (parent_id, name, path) VALUES (?, ?, ?)",
                params![parent_id, part, &current_path],
            )?;
            let dir_id = self.conn.last_insert_rowid();

            self.conn.execute(
                "INSERT INTO metadata_directory_closure (ancestor_id, descendant_id, depth) VALUES (?, ?, 0)",
                params![dir_id, dir_id],
            )?;

            if let Some(pid) = parent_id {
                self.conn.execute(
                    "INSERT INTO metadata_directory_closure (ancestor_id, descendant_id, depth)
                     SELECT ancestor_id, ?, depth + 1 FROM metadata_directory_closure WHERE descendant_id = ?",
                    params![dir_id, pid],
                )?;
            }

            parent_id = Some(dir_id);
        }

        Ok(())
    }

    pub fn set_torrent_data(&mut self, torrent_id: i64, data: &[u8]) -> Result<(), DbError> {
        self.conn.execute(
            "UPDATE torrents SET torrent_data = ? WHERE id = ?",
            params![data, torrent_id],
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn set_resume_data(&mut self, torrent_id: i64, data: &[u8]) -> Result<(), DbError> {
        self.conn.execute(
            "UPDATE torrents SET resume_data = ? WHERE id = ?",
            params![data, torrent_id],
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn set_torrent_status(
        &mut self,
        torrent_id: i64,
        status: &TorrentStatus,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "UPDATE torrents SET status = ? WHERE id = ?",
            params![status.as_str(), torrent_id],
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn insert_files(&mut self, torrent_id: i64, files: &[FileEntry]) -> Result<(), DbError> {
        let tx = self.conn.transaction()?;

        let mut dir_cache: std::collections::HashMap<String, i64> =
            std::collections::HashMap::new();

        for file_entry in files {
            let path_parts: Vec<&str> = file_entry.path.split('/').collect();
            if path_parts.is_empty() {
                continue;
            }

            let mut current_parent_id: Option<i64> = None;

            for (i, part) in path_parts.iter().enumerate() {
                let is_file = i == path_parts.len() - 1;
                let current_path = path_parts[..=i].join("/");

                if is_file {
                    tx.execute(
                        "INSERT INTO torrent_files (torrent_id, directory_id, name, path, size) VALUES (?, ?, ?, ?, ?)",
                        params![torrent_id, current_parent_id, part, &file_entry.path, file_entry.size],
                    )?;
                } else {
                    if let Some(&cached_id) = dir_cache.get(&current_path) {
                        current_parent_id = Some(cached_id);
                        continue;
                    }

                    let existing_id: Option<i64> = tx
                        .query_row(
                            "SELECT id FROM torrent_directories WHERE torrent_id = ? AND parent_id IS ? AND name = ?",
                            params![torrent_id, current_parent_id, part],
                            |row| row.get(0),
                        )
                        .optional()?
                        .flatten();

                    if let Some(id) = existing_id {
                        dir_cache.insert(current_path.clone(), id);
                        current_parent_id = Some(id);
                        continue;
                    }

                    tx.execute(
                        "INSERT INTO torrent_directories (torrent_id, parent_id, name) VALUES (?, ?, ?)",
                        params![torrent_id, current_parent_id, part],
                    )?;
                    let dir_id = tx.last_insert_rowid();

                    tx.execute(
                        "INSERT INTO directory_closure (ancestor_id, descendant_id, depth) VALUES (?, ?, 0)",
                        params![dir_id, dir_id],
                    )?;

                    if let Some(parent_id) = current_parent_id {
                        tx.execute(
                            "INSERT INTO directory_closure (ancestor_id, descendant_id, depth)
                             SELECT ancestor_id, ?, depth + 1 FROM directory_closure WHERE descendant_id = ?",
                            params![dir_id, parent_id],
                        )?;
                    }

                    dir_cache.insert(current_path.clone(), dir_id);
                    current_parent_id = Some(dir_id);
                }
            }
        }

        tx.commit()?;
        Ok(())
    }

    /// Insert torrent and its files atomically in a single transaction.
    /// This prevents orphaned torrent records without file entries.
    #[allow(clippy::too_many_arguments)]
    pub fn insert_torrent_with_files(
        &mut self,
        source_path: &str,
        name: &str,
        filename: &str,
        total_size: i64,
        info_hash: &str,
        file_count: i64,
        files: &[FileEntry],
    ) -> Result<InsertTorrentResult, DbError> {
        let tx = self.conn.transaction()?;

        // Check for existing torrent with same info_hash and source_path
        let existing: Option<i64> = tx
            .query_row(
                "SELECT id FROM torrents WHERE info_hash = ? AND source_path = ?",
                params![info_hash, source_path],
                |row| row.get(0),
            )
            .optional()?
            .flatten();

        if let Some(id) = existing {
            return Ok(InsertTorrentResult::Duplicate(id));
        }

        // Insert torrent record
        tx.execute(
            "INSERT INTO torrents (source_path, name, filename, total_size, info_hash, file_count, status) VALUES (?, ?, ?, ?, ?, ?, 'pending')",
            params![source_path, name, filename, total_size, info_hash, file_count],
        )?;
        let torrent_id = tx.last_insert_rowid();

        // Insert files in the same transaction
        let mut dir_cache: std::collections::HashMap<String, i64> =
            std::collections::HashMap::new();

        for file_entry in files {
            let path_parts: Vec<&str> = file_entry.path.split('/').collect();
            if path_parts.is_empty() {
                continue;
            }

            let mut current_parent_id: Option<i64> = None;

            for (i, part) in path_parts.iter().enumerate() {
                let is_file = i == path_parts.len() - 1;
                let current_path = path_parts[..=i].join("/");

                if is_file {
                    tx.execute(
                        "INSERT INTO torrent_files (torrent_id, directory_id, name, size) VALUES (?, ?, ?, ?)",
                        params![torrent_id, current_parent_id, part, file_entry.size],
                    )?;
                } else {
                    if let Some(&cached_id) = dir_cache.get(&current_path) {
                        current_parent_id = Some(cached_id);
                        continue;
                    }

                    let existing_id: Option<i64> = tx
                        .query_row(
                            "SELECT id FROM torrent_directories WHERE torrent_id = ? AND parent_id IS ? AND name = ?",
                            params![torrent_id, current_parent_id, part],
                            |row| row.get(0),
                        )
                        .optional()?
                        .flatten();

                    if let Some(id) = existing_id {
                        dir_cache.insert(current_path.clone(), id);
                        current_parent_id = Some(id);
                        continue;
                    }

                    tx.execute(
                        "INSERT INTO torrent_directories (torrent_id, parent_id, name) VALUES (?, ?, ?)",
                        params![torrent_id, current_parent_id, part],
                    )?;
                    let dir_id = tx.last_insert_rowid();

                    tx.execute(
                        "INSERT INTO directory_closure (ancestor_id, descendant_id, depth) VALUES (?, ?, 0)",
                        params![dir_id, dir_id],
                    )?;

                    if let Some(parent_id) = current_parent_id {
                        tx.execute(
                            "INSERT INTO directory_closure (ancestor_id, descendant_id, depth)
                             SELECT ancestor_id, ?, depth + 1 FROM directory_closure WHERE descendant_id = ?",
                            params![dir_id, parent_id],
                        )?;
                    }

                    dir_cache.insert(current_path.clone(), dir_id);
                    current_parent_id = Some(dir_id);
                }
            }
        }

        tx.commit()?;

        // Ensure metadata directories exist for the source_path
        // This is needed for get_source_path_prefixes to work correctly after remount
        if !source_path.is_empty() {
            if let Err(e) = self.ensure_metadata_directories(source_path) {
                tracing::warn!(
                    "Failed to create metadata directories for {}: {}",
                    source_path,
                    e
                );
            }
        }

        Ok(InsertTorrentResult::Inserted(torrent_id))
    }

    pub fn get_torrent_by_source_path(
        &self,
        source_path: &str,
    ) -> Result<Option<Torrent>, DbError> {
        let result = self
            .conn
            .query_row(
                "SELECT id, source_path, name, filename, total_size, info_hash, file_count, status, torrent_data, resume_data, created_at
                 FROM torrents WHERE source_path = ?",
                params![source_path],
                |row| {
                    Ok(Torrent {
                        id: row.get(0)?,
                        source_path: row.get(1)?,
                        name: row.get(2)?,
                        filename: row.get(3)?,
                        total_size: row.get(4)?,
                        info_hash: row.get(5)?,
                        file_count: row.get(6)?,
                        status: row.get::<_, String>(7)?.into(),
                        torrent_data: row.get(8)?,
                        resume_data: row.get(9)?,
                        created_at: row.get(10)?,
                    })
                },
            )
            .optional()?;

        Ok(result)
    }

    #[allow(dead_code)]
    pub fn get_torrent_by_info_hash(&self, info_hash: &str) -> Result<Option<Torrent>, DbError> {
        let result = self
            .conn
            .query_row(
                "SELECT id, source_path, name, filename, total_size, info_hash, file_count, status, torrent_data, resume_data, created_at
                 FROM torrents WHERE info_hash = ?",
                params![info_hash],
                |row| {
                    Ok(Torrent {
                        id: row.get(0)?,
                        source_path: row.get(1)?,
                        name: row.get(2)?,
                        filename: row.get(3)?,
                        total_size: row.get(4)?,
                        info_hash: row.get(5)?,
                        file_count: row.get(6)?,
                        status: row.get::<_, String>(7)?.into(),
                        torrent_data: row.get(8)?,
                        resume_data: row.get(9)?,
                        created_at: row.get(10)?,
                    })
                },
            )
            .optional()?;

        Ok(result)
    }

    pub fn get_files_by_torrent_id(&self, torrent_id: i64) -> Result<Vec<TorrentFile>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, torrent_id, directory_id, name, path, size, first_piece, last_piece, piece_start, piece_end
             FROM torrent_files WHERE torrent_id = ? ORDER BY id",
        )?;

        let files = stmt
            .query_map(params![torrent_id], |row| {
                Ok(TorrentFile {
                    id: row.get(0)?,
                    torrent_id: row.get(1)?,
                    directory_id: row.get(2)?,
                    name: row.get(3)?,
                    path: row.get(4)?,
                    size: row.get(5)?,
                    first_piece: row.get(6)?,
                    last_piece: row.get(7)?,
                    piece_start: row.get(8)?,
                    piece_end: row.get(9)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(files)
    }

    #[allow(dead_code)]
    pub fn get_subdirectory_ids(&self, parent_id: i64) -> Result<Vec<i64>, DbError> {
        let mut stmt = self
            .conn
            .prepare("SELECT id FROM torrent_directories WHERE parent_id = ?")?;

        let ids = stmt
            .query_map(params![parent_id], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ids)
    }

    pub fn get_files_in_directory(&self, directory_id: i64) -> Result<Vec<TorrentFile>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, torrent_id, directory_id, name, path, size, first_piece, last_piece, piece_start, piece_end
             FROM torrent_files WHERE directory_id = ?",
        )?;

        let files = stmt
            .query_map(params![directory_id], |row| {
                Ok(TorrentFile {
                    id: row.get(0)?,
                    torrent_id: row.get(1)?,
                    directory_id: row.get(2)?,
                    name: row.get(3)?,
                    path: row.get(4)?,
                    size: row.get(5)?,
                    first_piece: row.get(6)?,
                    last_piece: row.get(7)?,
                    piece_start: row.get(8)?,
                    piece_end: row.get(9)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(files)
    }

    pub fn get_root_files(&self, torrent_id: i64) -> Result<Vec<TorrentFile>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, torrent_id, directory_id, name, path, size, first_piece, last_piece, piece_start, piece_end
             FROM torrent_files WHERE torrent_id = ? AND directory_id IS NULL",
        )?;

        let files = stmt
            .query_map(params![torrent_id], |row| {
                Ok(TorrentFile {
                    id: row.get(0)?,
                    torrent_id: row.get(1)?,
                    directory_id: row.get(2)?,
                    name: row.get(3)?,
                    path: row.get(4)?,
                    size: row.get(5)?,
                    first_piece: row.get(6)?,
                    last_piece: row.get(7)?,
                    piece_start: row.get(8)?,
                    piece_end: row.get(9)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(files)
    }

    pub fn get_torrent_directory(
        &self,
        torrent_id: i64,
        parent_id: Option<i64>,
        name: &str,
    ) -> Result<Option<TorrentDirectory>, DbError> {
        let result = self.conn
            .query_row(
                "SELECT id, torrent_id, parent_id, name FROM torrent_directories WHERE torrent_id = ? AND parent_id IS ? AND name = ?",
                params![torrent_id, parent_id, name],
                |row| {
                    Ok(TorrentDirectory {
                        id: row.get(0)?,
                        torrent_id: row.get(1)?,
                        parent_id: row.get(2)?,
                        name: row.get(3)?,
                    })
                },
            )
            .optional()?;

        Ok(result)
    }

    pub fn get_torrent_directory_by_id(
        &self,
        dir_id: i64,
    ) -> Result<Option<TorrentDirectory>, DbError> {
        let result = self
            .conn
            .query_row(
                "SELECT id, torrent_id, parent_id, name FROM torrent_directories WHERE id = ?",
                params![dir_id],
                |row| {
                    Ok(TorrentDirectory {
                        id: row.get(0)?,
                        torrent_id: row.get(1)?,
                        parent_id: row.get(2)?,
                        name: row.get(3)?,
                    })
                },
            )
            .optional()?;

        Ok(result)
    }

    pub fn get_torrent_directories_by_parent(
        &self,
        parent_id: Option<i64>,
        torrent_id: i64,
    ) -> Result<Vec<TorrentDirectory>, DbError> {
        let mut stmt = if parent_id.is_none() {
            self.conn.prepare(
                "SELECT id, torrent_id, parent_id, name FROM torrent_directories WHERE torrent_id = ? AND parent_id IS NULL",
            )?
        } else {
            self.conn.prepare(
                "SELECT id, torrent_id, parent_id, name FROM torrent_directories WHERE torrent_id = ? AND parent_id = ?",
            )?
        };

        let dirs = if parent_id.is_none() {
            stmt.query_map(params![torrent_id], |row| {
                Ok(TorrentDirectory {
                    id: row.get(0)?,
                    torrent_id: row.get(1)?,
                    parent_id: row.get(2)?,
                    name: row.get(3)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
        } else {
            stmt.query_map(params![torrent_id, parent_id], |row| {
                Ok(TorrentDirectory {
                    id: row.get(0)?,
                    torrent_id: row.get(1)?,
                    parent_id: row.get(2)?,
                    name: row.get(3)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
        };

        Ok(dirs)
    }

    #[allow(dead_code)]
    pub fn get_all_files_under_directory(
        &self,
        directory_id: i64,
    ) -> Result<Vec<TorrentFile>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT f.id, f.torrent_id, f.directory_id, f.name, f.path, f.size, f.first_piece, f.last_piece, f.piece_start, f.piece_end
             FROM torrent_files f
             JOIN directory_closure c ON f.directory_id = c.descendant_id
             WHERE c.ancestor_id = ?",
        )?;

        let files = stmt
            .query_map(params![directory_id], |row| {
                Ok(TorrentFile {
                    id: row.get(0)?,
                    torrent_id: row.get(1)?,
                    directory_id: row.get(2)?,
                    name: row.get(3)?,
                    path: row.get(4)?,
                    size: row.get(5)?,
                    first_piece: row.get(6)?,
                    last_piece: row.get(7)?,
                    piece_start: row.get(8)?,
                    piece_end: row.get(9)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(files)
    }

    pub fn delete_torrent(&mut self, torrent_id: i64) -> Result<(), DbError> {
        self.conn
            .execute("DELETE FROM torrents WHERE id = ?", params![torrent_id])?;
        Ok(())
    }

    /// Delete a metadata directory by its path.
    /// This will also delete all child directories due to ON DELETE CASCADE.
    pub fn delete_metadata_directory(&mut self, path: &str) -> Result<(), DbError> {
        self.conn.execute(
            "DELETE FROM metadata_directories WHERE path = ?",
            params![path],
        )?;
        Ok(())
    }

    /// Rename a metadata directory and update all descendant paths and torrent source_paths.
    /// Uses a transaction for atomicity so renames persist across restarts.
    pub fn rename_metadata_directory(
        &mut self,
        old_path: &str,
        new_name: &str,
        new_path: &str,
    ) -> Result<(), DbError> {
        let tx = self.conn.transaction()?;

        // 1. Update the directory itself
        tx.execute(
            "UPDATE metadata_directories SET name = ?, path = ? WHERE path = ?",
            params![new_name, new_path, old_path],
        )?;

        // 2. Update all child directories' paths (replace old_path prefix with new_path)
        let old_prefix = format!("{}/", old_path);
        let new_prefix = format!("{}/", new_path);
        tx.execute(
            "UPDATE metadata_directories SET path = ? || substr(path, ?) WHERE path LIKE ?",
            params![new_prefix, old_prefix.len() + 1, format!("{}%", old_prefix)],
        )?;

        // 3. Update torrents whose source_path matches exactly
        tx.execute(
            "UPDATE torrents SET source_path = ? WHERE source_path = ?",
            params![new_path, old_path],
        )?;

        // 4. Update torrents whose source_path starts with old_path/
        tx.execute(
            "UPDATE torrents SET source_path = ? || substr(source_path, ?) WHERE source_path LIKE ?",
            params![new_prefix, old_prefix.len() + 1, format!("{}%", old_prefix)],
        )?;

        tx.commit()?;
        Ok(())
    }

    pub fn get_all_torrents(&self) -> Result<Vec<Torrent>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, source_path, name, filename, total_size, info_hash, file_count, status, torrent_data, resume_data, created_at
             FROM torrents ORDER BY id",
        )?;

        let torrents = stmt
            .query_map([], |row| {
                Ok(Torrent {
                    id: row.get(0)?,
                    source_path: row.get(1)?,
                    name: row.get(2)?,
                    filename: row.get(3)?,
                    total_size: row.get(4)?,
                    info_hash: row.get(5)?,
                    file_count: row.get(6)?,
                    status: row.get::<_, String>(7)?.into(),
                    torrent_data: row.get(8)?,
                    resume_data: row.get(9)?,
                    created_at: row.get(10)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(torrents)
    }

    #[allow(dead_code)]
    pub fn get_torrents_by_status(&self, status: &TorrentStatus) -> Result<Vec<Torrent>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, source_path, name, filename, total_size, info_hash, file_count, status, torrent_data, resume_data, created_at
             FROM torrents WHERE status = ? ORDER BY id",
        )?;

        let torrents = stmt
            .query_map(params![status.as_str()], |row| {
                Ok(Torrent {
                    id: row.get(0)?,
                    source_path: row.get(1)?,
                    name: row.get(2)?,
                    filename: row.get(3)?,
                    total_size: row.get(4)?,
                    info_hash: row.get(5)?,
                    file_count: row.get(6)?,
                    status: row.get::<_, String>(7)?.into(),
                    torrent_data: row.get(8)?,
                    resume_data: row.get(9)?,
                    created_at: row.get(10)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(torrents)
    }

    pub fn get_torrents_by_source_path(&self, source_path: &str) -> Result<Vec<Torrent>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, source_path, name, filename, total_size, info_hash, file_count, status, torrent_data, resume_data, created_at
             FROM torrents WHERE source_path = ? ORDER BY id",
        )?;

        let torrents = stmt
            .query_map(params![source_path], |row| {
                Ok(Torrent {
                    id: row.get(0)?,
                    source_path: row.get(1)?,
                    name: row.get(2)?,
                    filename: row.get(3)?,
                    total_size: row.get(4)?,
                    info_hash: row.get(5)?,
                    file_count: row.get(6)?,
                    status: row.get::<_, String>(7)?.into(),
                    torrent_data: row.get(8)?,
                    resume_data: row.get(9)?,
                    created_at: row.get(10)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(torrents)
    }

    /// Get counts of torrents grouped by status.
    /// Returns (pending, downloading, seeding, error, total).
    pub fn get_torrent_counts_by_status(&self) -> Result<(i64, i64, i64, i64, i64), DbError> {
        let mut pending: i64 = 0;
        let mut downloading: i64 = 0;
        let mut seeding: i64 = 0;
        let mut error: i64 = 0;

        let mut stmt = self
            .conn
            .prepare("SELECT status, COUNT(*) as cnt FROM torrents GROUP BY status")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;
        for row in rows {
            let (status, cnt) = row?;
            match status.as_str() {
                "pending" => pending = cnt,
                "downloading" => downloading = cnt,
                "seeding" => seeding = cnt,
                "error" => error = cnt,
                _ => {}
            }
        }
        let total = pending + downloading + seeding + error;
        Ok((pending, downloading, seeding, error, total))
    }

    /// Get all torrents that share a given info_hash.
    /// Used to list associated torrents for cache stats.
    pub fn get_torrents_by_infohash(
        &self,
        info_hash: &str,
    ) -> Result<Vec<(i64, String, String, String)>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, filename, source_path FROM torrents WHERE info_hash = ? ORDER BY id",
        )?;
        let rows = stmt.query_map(params![info_hash], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        let mut result = Vec::new();
        for row in rows {
            let (id, name, filename, source_path) = row?;
            result.push((id, name, filename, source_path));
        }
        Ok(result)
    }

    /// Get all metadata directories with their id, parent_id, name, and path.
    /// Used to restore inode cache on filesystem startup.
    #[allow(clippy::type_complexity)]
    pub fn get_all_metadata_directories(
        &self,
    ) -> Result<Vec<(i64, Option<i64>, String, String)>, DbError> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, parent_id, name, path FROM metadata_directories ORDER BY path")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn get_source_path_prefixes(&self, prefix: &str) -> Result<Vec<String>, DbError> {
        let names: Vec<String> = if prefix.is_empty() {
            let mut stmt = self.conn.prepare(
                "SELECT name FROM metadata_directories WHERE parent_id IS NULL ORDER BY name",
            )?;
            let rows = stmt.query_map([], |row| row.get(0))?;
            rows.collect::<Result<Vec<_>, _>>()?
        } else {
            let parent_id: Option<i64> = self
                .conn
                .query_row(
                    "SELECT id FROM metadata_directories WHERE path = ?",
                    params![prefix],
                    |row| row.get(0),
                )
                .optional()?
                .flatten();

            match parent_id {
                Some(pid) => {
                    let mut stmt = self.conn.prepare(
                        "SELECT name FROM metadata_directories WHERE parent_id = ? ORDER BY name",
                    )?;
                    let rows = stmt.query_map(params![pid], |row| row.get(0))?;
                    rows.collect::<Result<Vec<_>, _>>()?
                }
                None => Vec::new(),
            }
        };

        Ok(names)
    }

    #[allow(dead_code)]
    pub fn get_file_by_path(
        &self,
        torrent_id: i64,
        path: &str,
    ) -> Result<Option<TorrentFile>, DbError> {
        let parts: Vec<&str> = path.split('/').collect();
        if parts.is_empty() {
            return Ok(None);
        }

        let file_name = parts.last().unwrap();
        let dir_path_parts: Vec<&str> = parts[..parts.len() - 1].to_vec();

        if dir_path_parts.is_empty() {
            let result = self.conn
                .query_row(
                    "SELECT id, torrent_id, directory_id, name, path, size, first_piece, last_piece, piece_start, piece_end
                     FROM torrent_files WHERE torrent_id = ? AND directory_id IS NULL AND name = ?",
                    params![torrent_id, file_name],
                    |row| {
                        Ok(TorrentFile {
                            id: row.get(0)?,
                            torrent_id: row.get(1)?,
                            directory_id: row.get(2)?,
                            name: row.get(3)?,
                            path: row.get(4)?,
                            size: row.get(5)?,
                            first_piece: row.get(6)?,
                            last_piece: row.get(7)?,
                            piece_start: row.get(8)?,
                            piece_end: row.get(9)?,
                        })
                    },
                )
                .optional()?;

            return Ok(result);
        }

        let dir_id = self.resolve_directory_path(torrent_id, &dir_path_parts)?;

        match dir_id {
            Some(did) => {
                let result = self.conn
                    .query_row(
                        "SELECT id, torrent_id, directory_id, name, path, size, first_piece, last_piece, piece_start, piece_end
                         FROM torrent_files WHERE torrent_id = ? AND directory_id = ? AND name = ?",
                        params![torrent_id, did, file_name],
                        |row| {
                            Ok(TorrentFile {
                                id: row.get(0)?,
                                torrent_id: row.get(1)?,
                                directory_id: row.get(2)?,
                                name: row.get(3)?,
                                path: row.get(4)?,
                                size: row.get(5)?,
                                first_piece: row.get(6)?,
                                last_piece: row.get(7)?,
                                piece_start: row.get(8)?,
                                piece_end: row.get(9)?,
                            })
                        },
                    )
                    .optional()?;
                Ok(result)
            }
            None => Ok(None),
        }
    }

    #[allow(dead_code)]
    fn resolve_directory_path(
        &self,
        torrent_id: i64,
        parts: &[&str],
    ) -> Result<Option<i64>, DbError> {
        let mut current_parent: Option<i64> = None;

        for part in parts {
            let existing_id: Option<i64> = self.conn
                .query_row(
                    "SELECT id FROM torrent_directories WHERE torrent_id = ? AND parent_id IS ? AND name = ?",
                    params![torrent_id, current_parent, part],
                    |row| row.get(0),
                )
                .optional()?
                .flatten();

            match existing_id {
                Some(id) => current_parent = Some(id),
                None => return Ok(None),
            }
        }

        Ok(current_parent)
    }

    #[allow(dead_code)]
    pub fn get_torrent_id_by_name_and_source_path(
        &self,
        name: &str,
        source_path: &str,
    ) -> Result<Option<i64>, DbError> {
        let result = self
            .conn
            .query_row(
                "SELECT id FROM torrents WHERE name = ? AND source_path = ?",
                params![name, source_path],
                |row| row.get(0),
            )
            .optional()?
            .flatten();

        Ok(result)
    }

    pub fn get_torrent_by_id(&self, id: i64) -> Result<Option<Torrent>, DbError> {
        let result = self
            .conn
            .query_row(
                "SELECT id, source_path, name, filename, total_size, info_hash, file_count, status, torrent_data, resume_data, created_at
                 FROM torrents WHERE id = ?",
                params![id],
                |row| {
                    Ok(Torrent {
                        id: row.get(0)?,
                        source_path: row.get(1)?,
                        name: row.get(2)?,
                        filename: row.get(3)?,
                        total_size: row.get(4)?,
                        info_hash: row.get(5)?,
                        file_count: row.get(6)?,
                        status: row.get::<_, String>(7)?.into(),
                        torrent_data: row.get(8)?,
                        resume_data: row.get(9)?,
                        created_at: row.get(10)?,
                    })
                },
            )
            .optional()?;

        Ok(result)
    }

    /// Rename a torrent by updating its name, filename, and source_path fields.
    /// This is used when a torrent file is renamed in the metadata/ directory.
    /// The source_path is updated to reflect the new location when moving across directories.
    pub fn rename_torrent(
        &mut self,
        torrent_id: i64,
        new_name: &str,
        new_filename: &str,
        new_source_path: &str,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "UPDATE torrents SET name = ?, filename = ?, source_path = ? WHERE id = ?",
            params![new_name, new_filename, new_source_path, torrent_id],
        )?;

        // Ensure metadata directories exist for the new source_path
        // This is needed for get_source_path_prefixes to work correctly after remount
        if !new_source_path.is_empty() {
            if let Err(e) = self.ensure_metadata_directories(new_source_path) {
                tracing::warn!(
                    "Failed to create metadata directories for {}: {}",
                    new_source_path,
                    e
                );
            }
        }

        Ok(())
    }

    /// Get a torrent by its filename and source_path.
    /// Used to find a torrent when renaming its file.
    pub fn get_torrent_by_filename_and_source_path(
        &self,
        filename: &str,
        source_path: &str,
    ) -> Result<Option<Torrent>, DbError> {
        let result = self
            .conn
            .query_row(
                "SELECT id, source_path, name, filename, total_size, info_hash, file_count, status, torrent_data, resume_data, created_at
                 FROM torrents WHERE filename = ? AND source_path = ?",
                params![filename, source_path],
                |row| {
                    Ok(Torrent {
                        id: row.get(0)?,
                        source_path: row.get(1)?,
                        name: row.get(2)?,
                        filename: row.get(3)?,
                        total_size: row.get(4)?,
                        info_hash: row.get(5)?,
                        file_count: row.get(6)?,
                        status: row.get::<_, String>(7)?.into(),
                        torrent_data: row.get(8)?,
                        resume_data: row.get(9)?,
                        created_at: row.get(10)?,
                    })
                },
            )
            .optional()?;

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{NamedTempFile, TempDir};

    #[test]
    fn test_open_in_memory() {
        let db = Database::open_in_memory();
        assert!(db.is_ok());
    }

    #[test]
    fn test_insert_and_get_torrent() {
        let mut db = Database::open_in_memory().unwrap();

        let result = db
            .insert_torrent(
                "test/path",
                "Test Torrent",
                "Test Torrent",
                1024,
                "abc123",
                5,
            )
            .unwrap();
        assert_eq!(result, InsertTorrentResult::Inserted(1));

        let torrent = db.get_torrent_by_source_path("test/path").unwrap().unwrap();
        assert_eq!(torrent.name, "Test Torrent");
        assert_eq!(torrent.total_size, 1024);
        assert_eq!(torrent.file_count, 5);
        assert_eq!(torrent.status, TorrentStatus::Pending);
    }

    #[test]
    fn test_same_info_hash_different_source_path() {
        let mut db = Database::open_in_memory().unwrap();

        let result1 = db
            .insert_torrent("path1", "Torrent 1", "Torrent 1", 1024, "hash1", 1)
            .unwrap();
        assert_eq!(result1, InsertTorrentResult::Inserted(1));

        let result2 = db
            .insert_torrent("path2", "Torrent 2", "Torrent 2", 2048, "hash1", 2)
            .unwrap();
        assert_eq!(result2, InsertTorrentResult::Inserted(2));

        let torrent1 = db.get_torrent_by_source_path("path1").unwrap().unwrap();
        let torrent2 = db.get_torrent_by_source_path("path2").unwrap().unwrap();
        assert_eq!(torrent1.info_hash, torrent2.info_hash);
        assert_eq!(torrent1.id, 1);
        assert_eq!(torrent2.id, 2);
    }

    #[test]
    fn test_duplicate_info_hash_and_source_path() {
        let mut db = Database::open_in_memory().unwrap();

        db.insert_torrent("path1", "Torrent 1", "Torrent 1", 1024, "hash1", 1)
            .unwrap();
        let result = db
            .insert_torrent("path1", "Torrent 2", "Torrent 2", 2048, "hash1", 2)
            .unwrap();
        assert_eq!(result, InsertTorrentResult::Duplicate(1));
    }

    #[test]
    fn test_torrent_status() {
        let mut db = Database::open_in_memory().unwrap();

        let torrent_id = match db
            .insert_torrent("path1", "Test", "Test", 1024, "hash1", 1)
            .unwrap()
        {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
        assert_eq!(torrent.status, TorrentStatus::Pending);

        db.set_torrent_status(torrent_id, &TorrentStatus::Downloading)
            .unwrap();
        let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
        assert_eq!(torrent.status, TorrentStatus::Downloading);

        db.set_torrent_status(torrent_id, &TorrentStatus::Seeding)
            .unwrap();
        let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
        assert_eq!(torrent.status, TorrentStatus::Seeding);

        db.set_torrent_status(torrent_id, &TorrentStatus::Error)
            .unwrap();
        let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
        assert_eq!(torrent.status, TorrentStatus::Error);
    }

    #[test]
    fn test_torrent_data() {
        let mut db = Database::open_in_memory().unwrap();

        let torrent_id = match db
            .insert_torrent("path1", "Test", "Test", 1024, "hash1", 1)
            .unwrap()
        {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
        assert!(torrent.torrent_data.is_none());
        assert!(torrent.resume_data.is_none());

        let test_data = vec![1, 2, 3, 4, 5];
        db.set_torrent_data(torrent_id, &test_data).unwrap();
        let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
        assert_eq!(torrent.torrent_data, Some(test_data));

        let resume_data = vec![10, 20, 30];
        db.set_resume_data(torrent_id, &resume_data).unwrap();
        let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
        assert_eq!(torrent.resume_data, Some(resume_data));
    }

    #[test]
    fn test_get_torrents_by_status() {
        let mut db = Database::open_in_memory().unwrap();

        let id1 = match db
            .insert_torrent("path1", "T1", "T1", 100, "hash1", 1)
            .unwrap()
        {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };
        let id2 = match db
            .insert_torrent("path2", "T2", "T2", 200, "hash2", 1)
            .unwrap()
        {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };
        let _id3 = match db
            .insert_torrent("path3", "T3", "T3", 300, "hash3", 1)
            .unwrap()
        {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        db.set_torrent_status(id1, &TorrentStatus::Downloading)
            .unwrap();
        db.set_torrent_status(id2, &TorrentStatus::Seeding).unwrap();

        let pending = db.get_torrents_by_status(&TorrentStatus::Pending).unwrap();
        assert_eq!(pending.len(), 1);

        let downloading = db
            .get_torrents_by_status(&TorrentStatus::Downloading)
            .unwrap();
        assert_eq!(downloading.len(), 1);

        let seeding = db.get_torrents_by_status(&TorrentStatus::Seeding).unwrap();
        assert_eq!(seeding.len(), 1);
    }

    #[test]
    fn test_insert_files() {
        let mut db = Database::open_in_memory().unwrap();

        let torrent_id = match db
            .insert_torrent("path1", "Test", "Test", 1024, "hash1", 3)
            .unwrap()
        {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        let files = vec![
            FileEntry {
                path: "dir1/file1.txt".to_string(),
                size: 100,
            },
            FileEntry {
                path: "dir1/file2.txt".to_string(),
                size: 200,
            },
            FileEntry {
                path: "dir2/file3.txt".to_string(),
                size: 300,
            },
        ];

        db.insert_files(torrent_id, &files).unwrap();

        let all_files = db.get_files_by_torrent_id(torrent_id).unwrap();
        assert_eq!(all_files.len(), 3);
    }

    #[test]
    fn test_file_path_field_populated() {
        let mut db = Database::open_in_memory().unwrap();

        let torrent_id = match db
            .insert_torrent("path1", "Test", "Test.torrent", 1024, "hash1", 3)
            .unwrap()
        {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        let files = vec![
            FileEntry {
                path: "dir1/file1.txt".to_string(),
                size: 100,
            },
            FileEntry {
                path: "file2.txt".to_string(),
                size: 200,
            },
            FileEntry {
                path: "a/b/c/deep.txt".to_string(),
                size: 300,
            },
        ];

        db.insert_files(torrent_id, &files).unwrap();

        let all_files = db.get_files_by_torrent_id(torrent_id).unwrap();
        assert_eq!(all_files.len(), 3);

        // Verify path field is correctly populated
        let file1 = all_files.iter().find(|f| f.name == "file1.txt").unwrap();
        assert_eq!(file1.path, "dir1/file1.txt");

        let file2 = all_files.iter().find(|f| f.name == "file2.txt").unwrap();
        assert_eq!(file2.path, "file2.txt");

        let deep = all_files.iter().find(|f| f.name == "deep.txt").unwrap();
        assert_eq!(deep.path, "a/b/c/deep.txt");
    }

    #[test]
    fn test_get_subdirectory_ids() {
        let mut db = Database::open_in_memory().unwrap();

        let torrent_id = match db
            .insert_torrent("path1", "Test", "Test", 1024, "hash1", 2)
            .unwrap()
        {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        let files = vec![
            FileEntry {
                path: "dir1/file1.txt".to_string(),
                size: 100,
            },
            FileEntry {
                path: "dir2/file2.txt".to_string(),
                size: 200,
            },
        ];

        db.insert_files(torrent_id, &files).unwrap();

        let root_dirs = db
            .get_torrent_directories_by_parent(None, torrent_id)
            .unwrap();
        assert_eq!(root_dirs.len(), 2);
    }

    #[test]
    fn test_delete_torrent_cascade() {
        let mut db = Database::open_in_memory().unwrap();

        let torrent_id = match db
            .insert_torrent("path1", "Test", "Test", 1024, "hash1", 1)
            .unwrap()
        {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        let files = vec![FileEntry {
            path: "file.txt".to_string(),
            size: 100,
        }];
        db.insert_files(torrent_id, &files).unwrap();

        db.delete_torrent(torrent_id).unwrap();

        let torrent = db.get_torrent_by_source_path("path1").unwrap();
        assert!(torrent.is_none());

        let files = db.get_files_by_torrent_id(torrent_id).unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn test_get_files_in_directory() {
        let mut db = Database::open_in_memory().unwrap();

        let torrent_id = match db
            .insert_torrent("path1", "Test", "Test", 1024, "hash1", 3)
            .unwrap()
        {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        let files = vec![
            FileEntry {
                path: "dir1/file1.txt".to_string(),
                size: 100,
            },
            FileEntry {
                path: "dir1/file2.txt".to_string(),
                size: 200,
            },
            FileEntry {
                path: "file3.txt".to_string(),
                size: 300,
            },
        ];

        db.insert_files(torrent_id, &files).unwrap();

        let dirs = db
            .get_torrent_directories_by_parent(None, torrent_id)
            .unwrap();
        let dir1 = dirs.iter().find(|d| d.name == "dir1").unwrap();

        let dir_files = db.get_files_in_directory(dir1.id).unwrap();
        assert_eq!(dir_files.len(), 2);
    }

    #[test]
    fn test_get_all_files_under_directory() {
        let mut db = Database::open_in_memory().unwrap();

        let torrent_id = match db
            .insert_torrent("path1", "Test", "Test", 1024, "hash1", 2)
            .unwrap()
        {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        let files = vec![
            FileEntry {
                path: "dir1/subdir/file1.txt".to_string(),
                size: 100,
            },
            FileEntry {
                path: "dir1/file2.txt".to_string(),
                size: 200,
            },
        ];

        db.insert_files(torrent_id, &files).unwrap();

        let dirs = db
            .get_torrent_directories_by_parent(None, torrent_id)
            .unwrap();
        let dir1 = dirs.iter().find(|d| d.name == "dir1").unwrap();

        let all_files = db.get_all_files_under_directory(dir1.id).unwrap();
        assert_eq!(all_files.len(), 2);
    }

    #[test]
    fn test_persistence() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        {
            let mut db = Database::open(path).unwrap();
            db.insert_torrent("path1", "Test", "Test", 1024, "hash1", 1)
                .unwrap();
        }

        {
            let db = Database::open(path).unwrap();
            let torrent = db.get_torrent_by_source_path("path1").unwrap().unwrap();
            assert_eq!(torrent.name, "Test");
            assert_eq!(torrent.status, TorrentStatus::Pending);
        }
    }

    #[test]
    fn test_get_torrent_by_info_hash() {
        let mut db = Database::open_in_memory().unwrap();

        db.insert_torrent("path1", "Test", "Test", 1024, "abc123", 1)
            .unwrap();

        let torrent = db.get_torrent_by_info_hash("abc123").unwrap().unwrap();
        assert_eq!(torrent.source_path, "path1");
    }

    #[test]
    fn test_get_all_torrents() {
        let mut db = Database::open_in_memory().unwrap();

        db.insert_torrent("path1", "Torrent 1", "Torrent 1", 1024, "hash1", 1)
            .unwrap();
        db.insert_torrent("path2", "Torrent 2", "Torrent 2", 2048, "hash2", 1)
            .unwrap();

        let torrents = db.get_all_torrents().unwrap();
        assert_eq!(torrents.len(), 2);
    }

    #[test]
    fn test_nested_directory_structure() {
        let mut db = Database::open_in_memory().unwrap();

        let torrent_id = match db
            .insert_torrent("path1", "Test", "Test", 1024, "hash1", 1)
            .unwrap()
        {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        let files = vec![FileEntry {
            path: "a/b/c/file.txt".to_string(),
            size: 100,
        }];

        db.insert_files(torrent_id, &files).unwrap();

        let all_files = db.get_files_by_torrent_id(torrent_id).unwrap();
        assert_eq!(all_files.len(), 1);
    }

    #[test]
    fn test_get_torrents_by_source_path() {
        let mut db = Database::open_in_memory().unwrap();

        db.insert_torrent("path1", "Torrent 1", "Torrent 1", 1024, "hash1", 1)
            .unwrap();
        db.insert_torrent("path2", "Torrent 2", "Torrent 2", 2048, "hash2", 1)
            .unwrap();
        db.insert_torrent("other", "Torrent 3", "Torrent 3", 3072, "hash3", 1)
            .unwrap();

        let torrents = db.get_torrents_by_source_path("path1").unwrap();
        assert_eq!(torrents.len(), 1);
        assert_eq!(torrents[0].name, "Torrent 1");

        let torrents = db.get_torrents_by_source_path("nonexistent").unwrap();
        assert_eq!(torrents.len(), 0);
    }

    #[test]
    fn test_get_source_path_prefixes() {
        let mut db = Database::open_in_memory().unwrap();

        db.insert_torrent("a/b", "Torrent 1", "Torrent 1", 1024, "hash1", 1)
            .unwrap();
        db.insert_torrent("a/c", "Torrent 2", "Torrent 2", 2048, "hash2", 1)
            .unwrap();
        db.insert_torrent("d", "Torrent 3", "Torrent 3", 3072, "hash3", 1)
            .unwrap();

        let prefixes = db.get_source_path_prefixes("").unwrap();
        assert!(prefixes.contains(&"a".to_string()));
        assert!(prefixes.contains(&"d".to_string()));

        let prefixes = db.get_source_path_prefixes("a").unwrap();
        assert!(prefixes.contains(&"b".to_string()));
        assert!(prefixes.contains(&"c".to_string()));
    }

    #[test]
    fn test_metadata_directory_structure_preserved() {
        let mut db = Database::open_in_memory().unwrap();

        db.insert_torrent(
            "anime/naruto/season1",
            "Naruto S1",
            "Naruto S1",
            1024,
            "hash1",
            1,
        )
        .unwrap();
        db.insert_torrent(
            "anime/naruto/season2",
            "Naruto S2",
            "Naruto S2",
            2048,
            "hash2",
            1,
        )
        .unwrap();
        db.insert_torrent("anime/onepiece", "One Piece", "One Piece", 3072, "hash3", 1)
            .unwrap();
        db.insert_torrent(
            "movies/scifi",
            "SciFi Movies",
            "SciFi Movies",
            4096,
            "hash4",
            1,
        )
        .unwrap();

        let root = db.get_source_path_prefixes("").unwrap();
        assert_eq!(root.len(), 2);
        assert!(root.contains(&"anime".to_string()));
        assert!(root.contains(&"movies".to_string()));

        let anime = db.get_source_path_prefixes("anime").unwrap();
        assert_eq!(anime.len(), 2);
        assert!(anime.contains(&"naruto".to_string()));
        assert!(anime.contains(&"onepiece".to_string()));

        let naruto = db.get_source_path_prefixes("anime/naruto").unwrap();
        assert_eq!(naruto.len(), 2);
        assert!(naruto.contains(&"season1".to_string()));
        assert!(naruto.contains(&"season2".to_string()));

        let onepiece = db.get_source_path_prefixes("anime/onepiece").unwrap();
        assert_eq!(onepiece.len(), 0);

        let movies = db.get_source_path_prefixes("movies").unwrap();
        assert_eq!(movies.len(), 1);
        assert!(movies.contains(&"scifi".to_string()));
    }

    #[test]
    fn test_delete_metadata_directory() {
        let mut db = Database::open_in_memory().unwrap();

        // Create a nested directory structure
        db.insert_torrent(
            "anime/naruto/season1",
            "Naruto S1",
            "Naruto S1",
            1024,
            "hash1",
            1,
        )
        .unwrap();

        // Verify the directory structure exists
        let root = db.get_source_path_prefixes("").unwrap();
        assert!(root.contains(&"anime".to_string()));

        let anime = db.get_source_path_prefixes("anime").unwrap();
        assert!(anime.contains(&"naruto".to_string()));

        let naruto = db.get_source_path_prefixes("anime/naruto").unwrap();
        assert!(naruto.contains(&"season1".to_string()));

        // Delete a leaf directory
        db.delete_metadata_directory("anime/naruto/season1")
            .unwrap();

        // Verify the leaf directory is gone
        let naruto = db.get_source_path_prefixes("anime/naruto").unwrap();
        assert!(!naruto.contains(&"season1".to_string()));

        // Delete a parent directory - should cascade delete children
        db.delete_metadata_directory("anime").unwrap();

        // Verify parent and children are gone
        let root = db.get_source_path_prefixes("").unwrap();
        assert!(!root.contains(&"anime".to_string()));

        let anime = db.get_source_path_prefixes("anime").unwrap();
        assert!(anime.is_empty());
    }

    #[test]
    fn test_get_root_files() {
        let mut db = Database::open_in_memory().unwrap();

        let torrent_id = match db
            .insert_torrent("path1", "Test", "Test", 1024, "hash1", 3)
            .unwrap()
        {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        let files = vec![
            FileEntry {
                path: "file1.txt".to_string(),
                size: 100,
            },
            FileEntry {
                path: "file2.txt".to_string(),
                size: 200,
            },
            FileEntry {
                path: "dir/file3.txt".to_string(),
                size: 300,
            },
        ];

        db.insert_files(torrent_id, &files).unwrap();

        let root_files = db.get_root_files(torrent_id).unwrap();
        assert_eq!(root_files.len(), 2);
    }

    #[test]
    fn test_get_torrent_directory() {
        let mut db = Database::open_in_memory().unwrap();

        let torrent_id = match db
            .insert_torrent("path1", "Test", "Test", 1024, "hash1", 1)
            .unwrap()
        {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        let files = vec![FileEntry {
            path: "dir1/file.txt".to_string(),
            size: 100,
        }];

        db.insert_files(torrent_id, &files).unwrap();

        let dir = db.get_torrent_directory(torrent_id, None, "dir1").unwrap();
        assert!(dir.is_some());
        assert_eq!(dir.unwrap().name, "dir1");
    }

    #[test]
    fn test_insert_torrent_with_files_atomic() {
        let mut db = Database::open_in_memory().unwrap();

        let files = vec![
            FileEntry {
                path: "dir1/file1.txt".to_string(),
                size: 100,
            },
            FileEntry {
                path: "dir1/file2.txt".to_string(),
                size: 200,
            },
            FileEntry {
                path: "dir2/file3.txt".to_string(),
                size: 300,
            },
        ];

        // Insert torrent with files atomically
        let result = db
            .insert_torrent_with_files(
                "path1",
                "Test Torrent",
                "Test Torrent.torrent",
                600,
                "hash1",
                3,
                &files,
            )
            .unwrap();

        let torrent_id = match result {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        // Verify torrent was inserted
        let torrent = db.get_torrent_by_source_path("path1").unwrap().unwrap();
        assert_eq!(torrent.name, "Test Torrent");
        assert_eq!(torrent.filename, "Test Torrent.torrent");
        assert_eq!(torrent.total_size, 600);

        // Verify files were inserted
        let db_files = db.get_files_by_torrent_id(torrent_id).unwrap();
        assert_eq!(db_files.len(), 3);

        // Verify directories were created
        let root_dirs = db
            .get_torrent_directories_by_parent(None, torrent_id)
            .unwrap();
        assert_eq!(root_dirs.len(), 2);
    }

    #[test]
    fn test_insert_torrent_with_files_duplicate() {
        let mut db = Database::open_in_memory().unwrap();

        let files = vec![FileEntry {
            path: "file1.txt".to_string(),
            size: 100,
        }];

        // First insert
        let result = db
            .insert_torrent_with_files(
                "path1",
                "Test Torrent",
                "Test Torrent.torrent",
                100,
                "hash1",
                1,
                &files,
            )
            .unwrap();
        assert!(matches!(result, InsertTorrentResult::Inserted(_)));

        // Second insert with same info_hash and source_path should return Duplicate
        let result = db
            .insert_torrent_with_files(
                "path1",
                "Test Torrent 2",
                "Test Torrent 2.torrent",
                200,
                "hash1",
                1,
                &files,
            )
            .unwrap();
        assert!(matches!(result, InsertTorrentResult::Duplicate(_)));
    }

    #[test]
    fn test_rename_torrent() {
        let mut db = Database::open_in_memory().unwrap();

        // Insert a torrent
        let torrent_id = match db
            .insert_torrent("path1", "Old Name", "Old Name.torrent", 1024, "hash1", 1)
            .unwrap()
        {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        // Verify initial state
        let torrent = db
            .get_torrent_by_filename_and_source_path("Old Name.torrent", "path1")
            .unwrap()
            .unwrap();
        assert_eq!(torrent.name, "Old Name");
        assert_eq!(torrent.filename, "Old Name.torrent");

        // Rename the torrent
        db.rename_torrent(torrent_id, "New Name", "New Name.torrent", "path1")
            .unwrap();

        // Verify the rename
        let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
        assert_eq!(torrent.name, "New Name");
        assert_eq!(torrent.filename, "New Name.torrent");

        // Old filename lookup should return None
        let old_lookup = db
            .get_torrent_by_filename_and_source_path("Old Name.torrent", "path1")
            .unwrap();
        assert!(old_lookup.is_none());

        // New filename lookup should work
        let new_lookup = db
            .get_torrent_by_filename_and_source_path("New Name.torrent", "path1")
            .unwrap();
        assert!(new_lookup.is_some());
    }

    #[test]
    fn test_rename_torrent_cross_directory() {
        let mut db = Database::open_in_memory().unwrap();

        // Insert a torrent in "path1"
        let torrent_id = match db
            .insert_torrent("path1", "MyTorrent", "MyTorrent.torrent", 1024, "hash1", 1)
            .unwrap()
        {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("Expected Inserted"),
        };

        // Verify initial state
        let torrent = db
            .get_torrent_by_filename_and_source_path("MyTorrent.torrent", "path1")
            .unwrap()
            .unwrap();
        assert_eq!(torrent.source_path, "path1");

        // Rename and move to "path2" (cross-directory rename)
        db.rename_torrent(torrent_id, "Renamed", "Renamed.torrent", "path2")
            .unwrap();

        // Verify the rename and source_path update
        let torrent = db.get_torrent_by_id(torrent_id).unwrap().unwrap();
        assert_eq!(torrent.name, "Renamed");
        assert_eq!(torrent.filename, "Renamed.torrent");
        assert_eq!(torrent.source_path, "path2");

        // Old location lookup should return None
        let old_lookup = db
            .get_torrent_by_filename_and_source_path("MyTorrent.torrent", "path1")
            .unwrap();
        assert!(old_lookup.is_none());

        // New location lookup should work
        let new_lookup = db
            .get_torrent_by_filename_and_source_path("Renamed.torrent", "path2")
            .unwrap();
        assert!(new_lookup.is_some());
    }

    #[test]
    fn test_rename_metadata_directory() {
        let mut db = Database::open_in_memory().unwrap();

        // Create metadata directories: "movies" and "movies/action"
        db.ensure_metadata_directories("movies/action").unwrap();

        // Insert a torrent in "movies/action"
        db.insert_torrent(
            "movies/action",
            "DieHard",
            "DieHard.torrent",
            1024,
            "hash1",
            1,
        )
        .unwrap();

        // Rename "movies" -> "films"
        db.rename_metadata_directory("movies", "films", "films")
            .unwrap();

        // Verify directory was renamed
        let dirs = db.get_all_metadata_directories().unwrap();
        let dir_paths: Vec<&str> = dirs.iter().map(|(_, _, _, p)| p.as_str()).collect();
        assert!(dir_paths.contains(&"films"));
        assert!(dir_paths.contains(&"films/action"));
        assert!(!dir_paths.contains(&"movies"));
        assert!(!dir_paths.contains(&"movies/action"));

        // Verify torrent source_path was updated
        let torrent = db.get_torrent_by_id(1).unwrap().unwrap();
        assert_eq!(torrent.source_path, "films/action");
    }

    #[test]
    fn test_rename_metadata_directory_simple() {
        let mut db = Database::open_in_memory().unwrap();

        // Create metadata directory "old_dir"
        db.ensure_metadata_directories("old_dir").unwrap();

        // Insert a torrent in "old_dir"
        db.insert_torrent("old_dir", "MyTorrent", "MyTorrent.torrent", 512, "hash2", 1)
            .unwrap();

        // Rename "old_dir" -> "new_dir"
        db.rename_metadata_directory("old_dir", "new_dir", "new_dir")
            .unwrap();

        // Verify directory renamed
        let dirs = db.get_all_metadata_directories().unwrap();
        let dir_paths: Vec<&str> = dirs.iter().map(|(_, _, _, p)| p.as_str()).collect();
        assert!(dir_paths.contains(&"new_dir"));
        assert!(!dir_paths.contains(&"old_dir"));

        // Verify torrent source_path updated
        let torrent = db.get_torrent_by_id(1).unwrap().unwrap();
        assert_eq!(torrent.source_path, "new_dir");
    }

    #[test]
    fn test_rename_metadata_directory_persists() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test.db");

        // Create, insert, rename, and close
        {
            let mut db = Database::open(&db_path).unwrap();
            db.ensure_metadata_directories("alpha").unwrap();
            db.insert_torrent("alpha", "Test", "Test.torrent", 100, "h1", 1)
                .unwrap();
            db.rename_metadata_directory("alpha", "beta", "beta")
                .unwrap();
        }

        // Reopen and verify persistence
        {
            let db = Database::open(&db_path).unwrap();
            let dirs = db.get_all_metadata_directories().unwrap();
            let dir_paths: Vec<&str> = dirs.iter().map(|(_, _, _, p)| p.as_str()).collect();
            assert!(dir_paths.contains(&"beta"));
            assert!(!dir_paths.contains(&"alpha"));

            let torrent = db.get_torrent_by_id(1).unwrap().unwrap();
            assert_eq!(torrent.source_path, "beta");
        }
    }

    #[test]
    fn test_rename_metadata_directory_nested_torrents() {
        let mut db = Database::open_in_memory().unwrap();

        // Create "a/b" and "a/c"
        db.ensure_metadata_directories("a/b").unwrap();
        db.ensure_metadata_directories("a/c").unwrap();

        // Insert torrents in different subdirs
        db.insert_torrent("a/b", "T1", "T1.torrent", 100, "h1", 1)
            .unwrap();
        db.insert_torrent("a/c", "T2", "T2.torrent", 200, "h2", 1)
            .unwrap();

        // Rename "a" -> "z"
        db.rename_metadata_directory("a", "z", "z").unwrap();

        // Both torrents should have updated source_paths
        let t1 = db.get_torrent_by_id(1).unwrap().unwrap();
        assert_eq!(t1.source_path, "z/b");
        let t2 = db.get_torrent_by_id(2).unwrap().unwrap();
        assert_eq!(t2.source_path, "z/c");
    }
}
