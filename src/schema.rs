use rusqlite::Connection;
use serde::Deserialize;

/// Describes an additional JSON-extract index on the `data` column.
#[derive(Debug, Clone, Deserialize)]
pub struct JsonIndex {
    /// Index suffix name (used in `idx_{table}_{name}`).
    pub name: String,
    /// JSON path inside the `data` column, e.g. `"properties.地类"`.
    pub json_path: String,
}

/// Schema definition for a single business table.
///
/// The front-end sends this as JSON; Rust deserialises it and creates the
/// table, standard indexes, optional `json_extract` indexes, and the shared
/// `_change_log` table (once per database).
#[derive(Debug, Clone, Deserialize)]
pub struct TableSchema {
    /// Table name (must be a valid SQLite identifier).
    pub name: String,
    /// Optional extra indexes on `json_extract(data, '$.{json_path}')`.
    #[serde(default)]
    pub json_indexes: Vec<JsonIndex>,
}

/// Create a single business table together with its standard indexes and any
/// extra `json_extract` indexes.  Also ensures the `_change_log` table exists
/// (idempotent).
pub fn create_table(conn: &Connection, schema: &TableSchema) -> Result<(), String> {
    // 1. Create the business table with fixed metadata columns
    let create_table_sql = format!(
        "CREATE TABLE IF NOT EXISTS [{table}] (
            _id         TEXT PRIMARY KEY,
            uid         INTEGER,
            companyId   INTEGER,
            p_id        TEXT,
            createdAt   TEXT NOT NULL,
            updatedAt   TEXT NOT NULL,
            _deleted    INTEGER DEFAULT 0,
            _version    INTEGER DEFAULT 1,
            _status     TEXT DEFAULT 'synced',
            data        TEXT NOT NULL
        )",
        table = schema.name,
    );
    conn.execute_batch(&create_table_sql)
        .map_err(|e| format!("Failed to create table '{}': {}", schema.name, e))?;

    // 2. Create standard indexes
    let standard_indexes = [
        ("uid", "uid"),
        ("companyId", "companyId"),
        ("p_id", "p_id"),
        ("updatedAt", "updatedAt"),
        ("deleted", "_deleted"),
        ("status", "_status"),
    ];
    for (suffix, column) in &standard_indexes {
        let sql = format!(
            "CREATE INDEX IF NOT EXISTS [idx_{table}_{suffix}] ON [{table}]({column})",
            table = schema.name,
            suffix = suffix,
            column = column,
        );
        conn.execute_batch(&sql).map_err(|e| {
            format!(
                "Failed to create index idx_{}_{}: {}",
                schema.name, suffix, e
            )
        })?;
    }

    // 3. Create extra json_extract indexes
    for idx in &schema.json_indexes {
        let sql = format!(
            "CREATE INDEX IF NOT EXISTS [idx_{table}_{name}] ON [{table}](json_extract(data, '$.{path}'))",
            table = schema.name,
            name = idx.name,
            path = idx.json_path,
        );
        conn.execute_batch(&sql).map_err(|e| {
            format!(
                "Failed to create json index idx_{}_{}: {}",
                schema.name, idx.name, e
            )
        })?;
    }

    // 4. Ensure the shared _change_log table exists (once per database)
    create_change_log_table(conn)?;

    Ok(())
}

/// Batch-create multiple tables from a list of schemas.
pub fn create_tables(conn: &Connection, schemas: &[TableSchema]) -> Result<(), String> {
    for schema in schemas {
        create_table(conn, schema)?;
    }
    Ok(())
}

/// Create the `_change_log` table and its index (idempotent).
fn create_change_log_table(conn: &Connection) -> Result<(), String> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS _change_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name  TEXT NOT NULL,
            doc_id      TEXT NOT NULL,
            operation   TEXT NOT NULL,
            data        TEXT,
            timestamp   TEXT NOT NULL,
            synced      INTEGER DEFAULT 0,
            sync_error  TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_change_log_synced ON _change_log(synced, timestamp);",
    )
    .map_err(|e| format!("Failed to create _change_log table: {}", e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn memory_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn
    }

    #[test]
    fn test_create_table_basic() {
        let conn = memory_conn();
        let schema = TableSchema {
            name: "planning".to_string(),
            json_indexes: vec![],
        };
        create_table(&conn, &schema).unwrap();

        // Verify table exists
        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='planning'")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        assert_eq!(tables, vec!["planning"]);

        // Verify standard indexes exist
        let indexes: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='planning'")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        assert!(indexes.contains(&"idx_planning_uid".to_string()));
        assert!(indexes.contains(&"idx_planning_companyId".to_string()));
        assert!(indexes.contains(&"idx_planning_p_id".to_string()));
        assert!(indexes.contains(&"idx_planning_updatedAt".to_string()));
        assert!(indexes.contains(&"idx_planning_deleted".to_string()));
    }

    #[test]
    fn test_create_table_with_json_indexes() {
        let conn = memory_conn();
        let schema = TableSchema {
            name: "planning".to_string(),
            json_indexes: vec![
                JsonIndex {
                    name: "dilei".to_string(),
                    json_path: "properties.地类".to_string(),
                },
                JsonIndex {
                    name: "shuzhong".to_string(),
                    json_path: "properties.树种".to_string(),
                },
            ],
        };
        create_table(&conn, &schema).unwrap();

        let indexes: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='planning'")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        assert!(indexes.contains(&"idx_planning_dilei".to_string()));
        assert!(indexes.contains(&"idx_planning_shuzhong".to_string()));
    }

    #[test]
    fn test_change_log_table_created() {
        let conn = memory_conn();
        let schema = TableSchema {
            name: "sample".to_string(),
            json_indexes: vec![],
        };
        create_table(&conn, &schema).unwrap();

        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='_change_log'")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        assert_eq!(tables, vec!["_change_log"]);

        let indexes: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='_change_log'")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        assert!(indexes.contains(&"idx_change_log_synced".to_string()));
    }

    #[test]
    fn test_create_tables_batch() {
        let conn = memory_conn();
        let schemas = vec![
            TableSchema {
                name: "project".to_string(),
                json_indexes: vec![],
            },
            TableSchema {
                name: "planning".to_string(),
                json_indexes: vec![JsonIndex {
                    name: "dilei".to_string(),
                    json_path: "properties.地类".to_string(),
                }],
            },
            TableSchema {
                name: "sample".to_string(),
                json_indexes: vec![],
            },
        ];
        create_tables(&conn, &schemas).unwrap();

        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        assert!(tables.contains(&"project".to_string()));
        assert!(tables.contains(&"planning".to_string()));
        assert!(tables.contains(&"sample".to_string()));
        assert!(tables.contains(&"_change_log".to_string()));
    }

    #[test]
    fn test_idempotent_creation() {
        let conn = memory_conn();
        let schema = TableSchema {
            name: "planning".to_string(),
            json_indexes: vec![],
        };
        // Creating twice should not error
        create_table(&conn, &schema).unwrap();
        create_table(&conn, &schema).unwrap();
    }

    #[test]
    fn test_crud_on_created_table() {
        let conn = memory_conn();
        let schema = TableSchema {
            name: "planning".to_string(),
            json_indexes: vec![],
        };
        create_table(&conn, &schema).unwrap();

        // INSERT
        conn.execute(
            "INSERT INTO planning (_id, uid, companyId, p_id, createdAt, updatedAt, data) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params!["doc_1", 1, 100, "p_001", "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z", r#"{"name":"test"}"#],
        ).unwrap();

        // SELECT
        let data: String = conn
            .query_row("SELECT data FROM planning WHERE _id = ?1", ["doc_1"], |row| row.get(0))
            .unwrap();
        assert_eq!(data, r#"{"name":"test"}"#);

        // UPDATE
        conn.execute(
            "UPDATE planning SET data = ?1, updatedAt = ?2 WHERE _id = ?3",
            rusqlite::params![r#"{"name":"updated"}"#, "2025-01-02T00:00:00Z", "doc_1"],
        ).unwrap();

        let data: String = conn
            .query_row("SELECT data FROM planning WHERE _id = ?1", ["doc_1"], |row| row.get(0))
            .unwrap();
        assert_eq!(data, r#"{"name":"updated"}"#);

        // DELETE (soft)
        conn.execute("UPDATE planning SET _deleted = 1 WHERE _id = ?1", ["doc_1"]).unwrap();
        let deleted: i32 = conn
            .query_row("SELECT _deleted FROM planning WHERE _id = ?1", ["doc_1"], |row| row.get(0))
            .unwrap();
        assert_eq!(deleted, 1);
    }
}
