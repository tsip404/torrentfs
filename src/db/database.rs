use std::path::Path;

use rusqlite::{Connection, OptionalExtension};

use super::types::DbError;

pub struct Database {
    pub(crate) conn: Connection,
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

    pub(crate) fn run_migrations(&mut self) -> Result<(), DbError> {
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
}
