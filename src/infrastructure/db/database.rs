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

        // v5 runs outside any transaction so that PRAGMA foreign_keys = OFF
        // takes effect — inside a transaction it is silently ignored, causing
        // DROP TABLE to cascade-delete child rows.
        if user_version < 5 {
            self.conn.execute_batch("PRAGMA foreign_keys = OFF;")?;
            Self::migrate_v5(&self.conn)?;
            self.conn.execute_batch("PRAGMA foreign_keys = ON;")?;
            self.conn.pragma_update(None, "user_version", 5)?;
        }

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

    pub(crate) fn migrate_v1(conn: &Connection) -> Result<(), DbError> {
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

    pub(crate) fn migrate_v2(conn: &Connection) -> Result<(), DbError> {
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

    pub(crate) fn migrate_v3(conn: &Connection) -> Result<(), DbError> {
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

    pub(crate) fn migrate_v4(conn: &Connection) -> Result<(), DbError> {
        conn.execute_batch(
            "ALTER TABLE torrents ADD COLUMN filename TEXT NOT NULL DEFAULT '';
             UPDATE torrents SET filename = name WHERE filename = '';",
        )?;
        Ok(())
    }

    /// Change UNIQUE constraint from (info_hash, source_path) to (source_path, filename)
    /// so that the same info_hash at different source_paths or with different filenames
    /// produces independent data/ mirrors.
    ///
    /// Caller MUST have disabled foreign_keys before calling this, otherwise
    /// DROP TABLE will cascade-delete child rows in torrent_directories /
    /// directory_closure / torrent_files.
    ///
    /// With foreign_keys OFF, the old UNIQUE(info_hash, source_path) constraint may
    /// have left multiple rows per (source_path, filename). Only MAX(id) per key is
    /// retained; the rest would leave orphaned child rows in torrent_files /
    /// torrent_directories / directory_closure (their torrent_id no longer exists
    /// after DROP TABLE, and re-enabling foreign_keys does not retroactively clean
    /// them). We therefore collect the non-retained ids and explicitly DELETE their
    /// child rows before the DROP. The whole migration runs in a transaction for
    /// crash consistency.
    pub(crate) fn migrate_v5(conn: &Connection) -> Result<(), DbError> {
        let tx = conn.unchecked_transaction()?;

        // Child rows of the non-retained torrents must be removed explicitly;
        // foreign_keys is OFF so DROP TABLE will not cascade.
        tx.execute_batch(
            "CREATE TEMP TABLE _v5_orphans AS
             SELECT id FROM torrents
             WHERE id NOT IN (
                 SELECT MAX(id) FROM torrents GROUP BY source_path, filename
             );",
        )?;

        // directory_closure has no direct torrent_id FK — clean rows whose
        // ancestor/descendant directory ids belong to orphaned torrents.
        // CTE computes the orphan directory id set once.
        tx.execute_batch(
            "WITH orphan_dirs AS (
                 SELECT id FROM torrent_directories
                 WHERE torrent_id IN (SELECT id FROM _v5_orphans)
             )
             DELETE FROM directory_closure
             WHERE ancestor_id IN (SELECT id FROM orphan_dirs)
                OR descendant_id IN (SELECT id FROM orphan_dirs);",
        )?;

        tx.execute_batch(
            "DELETE FROM torrent_directories
             WHERE torrent_id IN (SELECT id FROM _v5_orphans);

             DELETE FROM torrent_files
             WHERE torrent_id IN (SELECT id FROM _v5_orphans);",
        )?;

        tx.execute_batch("DROP TABLE IF EXISTS _v5_orphans;")?;

        tx.execute_batch(
            "CREATE TABLE torrents_v5 (
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
                filename TEXT NOT NULL DEFAULT '',
                UNIQUE(source_path, filename)
            );

            -- Keep the latest row for each (source_path, filename) to satisfy
            -- the new UNIQUE constraint; old UNIQUE(info_hash, source_path) could
            -- have left multiple rows with same source_path+filename.
            INSERT INTO torrents_v5
                (id, info_hash, name, total_size, file_count, status,
                 source_path, torrent_data, resume_data, created_at, filename)
            SELECT id, info_hash, name, total_size, file_count, status,
                   source_path, torrent_data, resume_data, created_at, filename
            FROM torrents
            WHERE id IN (
                SELECT MAX(id) FROM torrents GROUP BY source_path, filename
            );

            DROP TABLE torrents;

            ALTER TABLE torrents_v5 RENAME TO torrents;

            CREATE INDEX IF NOT EXISTS idx_torrents_info_hash ON torrents(info_hash);
            CREATE INDEX IF NOT EXISTS idx_torrents_status ON torrents(status);
            CREATE INDEX IF NOT EXISTS idx_torrents_info_hash_source_path ON torrents(info_hash, source_path);
            CREATE INDEX IF NOT EXISTS idx_torrents_source_path ON torrents(source_path);",
        )?;

        tx.commit()?;
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
