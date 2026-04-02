use rusqlite::Connection;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Manages multiple SQLite connections (global + per-project).
///
/// Thread safety: wrapped in `std::sync::Mutex` because `rusqlite::Connection`
/// is `!Send`. Tauri managed state requires `Send + Sync`, which `Mutex`
/// provides.
pub struct DatabaseManager {
    connections: Mutex<HashMap<String, Connection>>,
    app_data_dir: PathBuf,
}

impl DatabaseManager {
    /// Create a new `DatabaseManager` rooted at `app_data_dir`.
    /// Also ensures the `projects/` subdirectory exists.
    pub fn new(app_data_dir: impl Into<PathBuf>) -> Result<Self, String> {
        let app_data_dir = app_data_dir.into();

        // Ensure the projects/ subdirectory exists
        let projects_dir = app_data_dir.join("projects");
        fs::create_dir_all(&projects_dir).map_err(|e| {
            format!(
                "Failed to create projects directory at {}: {}",
                projects_dir.display(),
                e
            )
        })?;

        Ok(Self {
            connections: Mutex::new(HashMap::new()),
            app_data_dir,
        })
    }

    /// Open the global database (`global.db`) and create the required tables.
    pub fn open_global_db(&self) -> Result<(), String> {
        let db_path = self.app_data_dir.join("global.db");
        let conn = open_connection(&db_path)?;
        create_global_tables(&conn)?;

        let mut conns = self.connections.lock().map_err(|e| format!("Lock poisoned: {}", e))?;
        conns.insert("global".to_string(), conn);
        Ok(())
    }

    /// Open a project database (`projects/{project_id}.db`) on demand.
    pub fn open_project_db(&self, project_id: &str) -> Result<(), String> {
        if project_id == "global" {
            return Err("Use open_global_db() for the global database".to_string());
        }

        let db_path = self
            .app_data_dir
            .join("projects")
            .join(format!("{}.db", project_id));

        let conn = open_connection(&db_path)?;

        let mut conns = self.connections.lock().map_err(|e| format!("Lock poisoned: {}", e))?;
        conns.insert(project_id.to_string(), conn);
        Ok(())
    }

    /// Close a project database connection.
    pub fn close_project_db(&self, project_id: &str) -> Result<(), String> {
        if project_id == "global" {
            return Err("Cannot close the global database".to_string());
        }

        let mut conns = self.connections.lock().map_err(|e| format!("Lock poisoned: {}", e))?;
        // Removing the entry drops the Connection, which closes it.
        conns.remove(project_id);
        Ok(())
    }

    /// Close the connection (if open) and delete the database file.
    pub fn delete_project_db(&self, project_id: &str) -> Result<(), String> {
        if project_id == "global" {
            return Err("Cannot delete the global database".to_string());
        }

        // Close connection first
        {
            let mut conns =
                self.connections.lock().map_err(|e| format!("Lock poisoned: {}", e))?;
            conns.remove(project_id);
        }

        // Delete the database file and WAL/SHM sidecar files
        let db_path = self
            .app_data_dir
            .join("projects")
            .join(format!("{}.db", project_id));

        if db_path.exists() {
            fs::remove_file(&db_path)
                .map_err(|e| format!("Failed to delete {}: {}", db_path.display(), e))?;
        }

        // Clean up WAL and SHM files if they exist
        let wal_path = db_path.with_extension("db-wal");
        if wal_path.exists() {
            let _ = fs::remove_file(&wal_path);
        }
        let shm_path = db_path.with_extension("db-shm");
        if shm_path.exists() {
            let _ = fs::remove_file(&shm_path);
        }

        Ok(())
    }

    /// Get a reference to the inner connections map (locked).
    /// Used by command handlers to execute SQL on a specific connection.
    pub fn with_connection<F, T>(&self, project_id: &str, f: F) -> Result<T, String>
    where
        F: FnOnce(&Connection) -> Result<T, String>,
    {
        let conns = self.connections.lock().map_err(|e| format!("Lock poisoned: {}", e))?;
        let conn = conns
            .get(project_id)
            .ok_or_else(|| format!("Database '{}' is not open", project_id))?;
        f(conn)
    }
}

/// Open a SQLite connection with WAL mode and busy_timeout=5000.
fn open_connection(path: &Path) -> Result<Connection, String> {
    let conn = Connection::open(path)
        .map_err(|e| format!("Failed to open database at {}: {}", path.display(), e))?;

    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(|e| format!("Failed to enable WAL mode: {}", e))?;

    conn.pragma_update(None, "busy_timeout", 5000)
        .map_err(|e| format!("Failed to set busy_timeout: {}", e))?;

    Ok(conn)
}

/// Create the global database tables: project_meta, _migration_history, _sync_checkpoint.
fn create_global_tables(conn: &Connection) -> Result<(), String> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS project_meta (
            _id         TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            projectType TEXT,
            status      INTEGER DEFAULT 0,
            area        REAL DEFAULT 0,
            updatedAt   TEXT NOT NULL,
            db_path     TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS _migration_history (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            from_version TEXT NOT NULL,
            to_version   TEXT NOT NULL,
            migrated_at  TEXT NOT NULL,
            result       TEXT NOT NULL,
            detail       TEXT
        );

        CREATE TABLE IF NOT EXISTS _sync_checkpoint (
            table_name   TEXT NOT NULL,
            sync_mode    TEXT NOT NULL,
            filter_key   TEXT NOT NULL,
            last_sync_at TEXT NOT NULL,
            PRIMARY KEY (table_name, sync_mode, filter_key)
        );
        ",
    )
    .map_err(|e| format!("Failed to create global tables: {}", e))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> PathBuf {
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "offlite_test_{}_{}_{}",
            std::process::id(),
            id,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn test_new_creates_projects_dir() {
        let dir = temp_dir();
        let _mgr = DatabaseManager::new(&dir).unwrap();
        assert!(dir.join("projects").is_dir());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_open_global_db() {
        let dir = temp_dir();
        let mgr = DatabaseManager::new(&dir).unwrap();
        mgr.open_global_db().unwrap();

        // Verify global.db file exists
        assert!(dir.join("global.db").exists());

        // Verify tables were created
        mgr.with_connection("global", |conn| {
            let mut stmt = conn
                .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                .map_err(|e| e.to_string())?;
            let tables: Vec<String> = stmt
                .query_map([], |row| row.get(0))
                .map_err(|e| e.to_string())?
                .filter_map(|r| r.ok())
                .collect();
            assert!(tables.contains(&"project_meta".to_string()));
            assert!(tables.contains(&"_migration_history".to_string()));
            assert!(tables.contains(&"_sync_checkpoint".to_string()));
            Ok(())
        })
        .unwrap();

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_open_and_close_project_db() {
        let dir = temp_dir();
        let mgr = DatabaseManager::new(&dir).unwrap();

        mgr.open_project_db("proj_001").unwrap();
        assert!(dir.join("projects").join("proj_001.db").exists());

        // Should be able to use the connection
        mgr.with_connection("proj_001", |conn| {
            conn.execute_batch("CREATE TABLE IF NOT EXISTS test (id TEXT)")
                .map_err(|e| e.to_string())
        })
        .unwrap();

        // Close it
        mgr.close_project_db("proj_001").unwrap();

        // Connection should no longer be available
        assert!(mgr.with_connection("proj_001", |_| Ok(())).is_err());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_delete_project_db() {
        let dir = temp_dir();
        let mgr = DatabaseManager::new(&dir).unwrap();

        mgr.open_project_db("proj_del").unwrap();
        let db_path = dir.join("projects").join("proj_del.db");
        assert!(db_path.exists());

        mgr.delete_project_db("proj_del").unwrap();
        assert!(!db_path.exists());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_cannot_close_global() {
        let dir = temp_dir();
        let mgr = DatabaseManager::new(&dir).unwrap();
        assert!(mgr.close_project_db("global").is_err());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_cannot_delete_global() {
        let dir = temp_dir();
        let mgr = DatabaseManager::new(&dir).unwrap();
        assert!(mgr.delete_project_db("global").is_err());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_wal_mode_enabled() {
        let dir = temp_dir();
        let mgr = DatabaseManager::new(&dir).unwrap();
        mgr.open_global_db().unwrap();

        mgr.with_connection("global", |conn| {
            let mode: String = conn
                .pragma_query_value(None, "journal_mode", |row| row.get(0))
                .map_err(|e| e.to_string())?;
            assert_eq!(mode.to_lowercase(), "wal");
            Ok(())
        })
        .unwrap();

        let _ = fs::remove_dir_all(&dir);
    }
}
