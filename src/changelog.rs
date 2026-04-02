use chrono::Utc;
use rusqlite::{params, Connection};
use serde::Serialize;

/// Represents a single entry in the `_change_log` table.
#[derive(Debug, Clone, Serialize)]
pub struct ChangeRecord {
    pub id: i64,
    pub table_name: String,
    pub doc_id: String,
    pub operation: String,
    pub data: Option<String>,
    pub timestamp: String,
    pub synced: i32,
    pub sync_error: Option<String>,
}

/// Record a change (INSERT / UPDATE / DELETE) into the `_change_log` table.
///
/// - `data` should be the full document JSON for INSERT/UPDATE, or `None` for DELETE.
/// - The timestamp is automatically set to the current UTC time in ISO 8601 format.
pub fn record_change(
    conn: &Connection,
    table_name: &str,
    doc_id: &str,
    operation: &str,
    data: Option<&str>,
) -> Result<i64, String> {
    let timestamp = Utc::now().to_rfc3339();
    conn.execute(
        "INSERT INTO _change_log (table_name, doc_id, operation, data, timestamp)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![table_name, doc_id, operation, data, timestamp],
    )
    .map_err(|e| format!("Failed to record change: {}", e))?;

    let id = conn.last_insert_rowid();
    Ok(id)
}

/// Return all pending (unsynced) changes for a specific table, ordered by timestamp.
pub fn get_pending_changes(conn: &Connection, table_name: &str) -> Result<Vec<ChangeRecord>, String> {
    let mut stmt = conn
        .prepare(
            "SELECT id, table_name, doc_id, operation, data, timestamp, synced, sync_error
             FROM _change_log
             WHERE synced = 0 AND table_name = ?1
             ORDER BY timestamp ASC",
        )
        .map_err(|e| format!("Failed to prepare get_pending_changes: {}", e))?;

    let rows = stmt
        .query_map(params![table_name], row_to_change_record)
        .map_err(|e| format!("Failed to query pending changes: {}", e))?;

    collect_rows(rows)
}

/// Return all pending (unsynced) changes across all tables, ordered by timestamp.
pub fn get_all_pending_changes(conn: &Connection) -> Result<Vec<ChangeRecord>, String> {
    let mut stmt = conn
        .prepare(
            "SELECT id, table_name, doc_id, operation, data, timestamp, synced, sync_error
             FROM _change_log
             WHERE synced = 0
             ORDER BY timestamp ASC",
        )
        .map_err(|e| format!("Failed to prepare get_all_pending_changes: {}", e))?;

    let rows = stmt
        .query_map([], row_to_change_record)
        .map_err(|e| format!("Failed to query all pending changes: {}", e))?;

    collect_rows(rows)
}

/// Mark the given change IDs as synced (`synced = 1`).
pub fn mark_synced(conn: &Connection, change_ids: &[i64]) -> Result<u64, String> {
    if change_ids.is_empty() {
        return Ok(0);
    }

    // Build a parameterised IN clause: WHERE id IN (?1, ?2, …)
    let placeholders: Vec<String> = (1..=change_ids.len()).map(|i| format!("?{}", i)).collect();
    let sql = format!(
        "UPDATE _change_log SET synced = 1 WHERE id IN ({})",
        placeholders.join(", ")
    );

    let param_values: Vec<Box<dyn rusqlite::types::ToSql>> =
        change_ids.iter().map(|id| Box::new(*id) as Box<dyn rusqlite::types::ToSql>).collect();
    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        param_values.iter().map(|b| b.as_ref()).collect();

    let affected = conn
        .execute(&sql, param_refs.as_slice())
        .map_err(|e| format!("Failed to mark changes as synced: {}", e))?;

    Ok(affected as u64)
}

/// Record a sync error for a specific change entry.
pub fn mark_sync_error(conn: &Connection, change_id: i64, error: &str) -> Result<(), String> {
    conn.execute(
        "UPDATE _change_log SET sync_error = ?1 WHERE id = ?2",
        params![error, change_id],
    )
    .map_err(|e| format!("Failed to mark sync error: {}", e))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn row_to_change_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<ChangeRecord> {
    Ok(ChangeRecord {
        id: row.get(0)?,
        table_name: row.get(1)?,
        doc_id: row.get(2)?,
        operation: row.get(3)?,
        data: row.get(4)?,
        timestamp: row.get(5)?,
        synced: row.get(6)?,
        sync_error: row.get(7)?,
    })
}

fn collect_rows(
    rows: rusqlite::MappedRows<'_, impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<ChangeRecord>>,
) -> Result<Vec<ChangeRecord>, String> {
    let mut results = Vec::new();
    for row in rows {
        results.push(row.map_err(|e| format!("Failed to read change record row: {}", e))?);
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    /// Create an in-memory database with the `_change_log` table.
    fn setup_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
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
        .unwrap();
        conn
    }

    #[test]
    fn test_record_insert_change() {
        let conn = setup_conn();
        let id = record_change(&conn, "planning", "doc_1", "INSERT", Some(r#"{"name":"test"}"#)).unwrap();
        assert!(id > 0);

        let rec: ChangeRecord = conn
            .query_row(
                "SELECT id, table_name, doc_id, operation, data, timestamp, synced, sync_error FROM _change_log WHERE id = ?1",
                params![id],
                row_to_change_record,
            )
            .unwrap();

        assert_eq!(rec.table_name, "planning");
        assert_eq!(rec.doc_id, "doc_1");
        assert_eq!(rec.operation, "INSERT");
        assert_eq!(rec.data.as_deref(), Some(r#"{"name":"test"}"#));
        assert_eq!(rec.synced, 0);
        assert!(rec.sync_error.is_none());
        // Timestamp should be a valid ISO 8601 string
        assert!(rec.timestamp.contains('T'));
    }

    #[test]
    fn test_record_update_change() {
        let conn = setup_conn();
        let id = record_change(&conn, "sample", "doc_2", "UPDATE", Some(r#"{"v":2}"#)).unwrap();

        let rec: ChangeRecord = conn
            .query_row(
                "SELECT id, table_name, doc_id, operation, data, timestamp, synced, sync_error FROM _change_log WHERE id = ?1",
                params![id],
                row_to_change_record,
            )
            .unwrap();

        assert_eq!(rec.operation, "UPDATE");
        assert_eq!(rec.data.as_deref(), Some(r#"{"v":2}"#));
    }

    #[test]
    fn test_record_delete_change() {
        let conn = setup_conn();
        let id = record_change(&conn, "planning", "doc_3", "DELETE", None).unwrap();

        let rec: ChangeRecord = conn
            .query_row(
                "SELECT id, table_name, doc_id, operation, data, timestamp, synced, sync_error FROM _change_log WHERE id = ?1",
                params![id],
                row_to_change_record,
            )
            .unwrap();

        assert_eq!(rec.operation, "DELETE");
        assert!(rec.data.is_none());
    }

    #[test]
    fn test_get_pending_changes_filtered_by_table() {
        let conn = setup_conn();
        record_change(&conn, "planning", "d1", "INSERT", Some("{}")).unwrap();
        record_change(&conn, "sample", "d2", "INSERT", Some("{}")).unwrap();
        record_change(&conn, "planning", "d3", "UPDATE", Some("{}")).unwrap();

        let pending = get_pending_changes(&conn, "planning").unwrap();
        assert_eq!(pending.len(), 2);
        assert!(pending.iter().all(|r| r.table_name == "planning"));
    }

    #[test]
    fn test_get_all_pending_changes() {
        let conn = setup_conn();
        record_change(&conn, "planning", "d1", "INSERT", Some("{}")).unwrap();
        record_change(&conn, "sample", "d2", "INSERT", Some("{}")).unwrap();
        record_change(&conn, "planning", "d3", "DELETE", None).unwrap();

        let pending = get_all_pending_changes(&conn).unwrap();
        assert_eq!(pending.len(), 3);
    }

    #[test]
    fn test_mark_synced() {
        let conn = setup_conn();
        let id1 = record_change(&conn, "planning", "d1", "INSERT", Some("{}")).unwrap();
        let id2 = record_change(&conn, "planning", "d2", "UPDATE", Some("{}")).unwrap();
        let _id3 = record_change(&conn, "planning", "d3", "DELETE", None).unwrap();

        let affected = mark_synced(&conn, &[id1, id2]).unwrap();
        assert_eq!(affected, 2);

        // Only d3 should remain pending
        let pending = get_all_pending_changes(&conn).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].doc_id, "d3");

        // Verify synced records have synced=1
        let synced: i32 = conn
            .query_row("SELECT synced FROM _change_log WHERE id = ?1", params![id1], |row| row.get(0))
            .unwrap();
        assert_eq!(synced, 1);
    }

    #[test]
    fn test_mark_synced_empty_ids() {
        let conn = setup_conn();
        let affected = mark_synced(&conn, &[]).unwrap();
        assert_eq!(affected, 0);
    }

    #[test]
    fn test_mark_sync_error() {
        let conn = setup_conn();
        let id = record_change(&conn, "planning", "d1", "INSERT", Some("{}")).unwrap();

        mark_sync_error(&conn, id, "network timeout").unwrap();

        let error: Option<String> = conn
            .query_row(
                "SELECT sync_error FROM _change_log WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(error.as_deref(), Some("network timeout"));

        // The record should still be pending (synced=0)
        let synced: i32 = conn
            .query_row("SELECT synced FROM _change_log WHERE id = ?1", params![id], |row| row.get(0))
            .unwrap();
        assert_eq!(synced, 0);
    }

    #[test]
    fn test_pending_changes_ordered_by_timestamp() {
        let conn = setup_conn();
        // Insert with explicit timestamps to verify ordering
        conn.execute(
            "INSERT INTO _change_log (table_name, doc_id, operation, data, timestamp) VALUES (?1, ?2, ?3, ?4, ?5)",
            params!["t", "d2", "INSERT", "{}", "2025-01-02T00:00:00Z"],
        ).unwrap();
        conn.execute(
            "INSERT INTO _change_log (table_name, doc_id, operation, data, timestamp) VALUES (?1, ?2, ?3, ?4, ?5)",
            params!["t", "d1", "INSERT", "{}", "2025-01-01T00:00:00Z"],
        ).unwrap();
        conn.execute(
            "INSERT INTO _change_log (table_name, doc_id, operation, data, timestamp) VALUES (?1, ?2, ?3, ?4, ?5)",
            params!["t", "d3", "INSERT", "{}", "2025-01-03T00:00:00Z"],
        ).unwrap();

        let pending = get_pending_changes(&conn, "t").unwrap();
        assert_eq!(pending.len(), 3);
        assert_eq!(pending[0].doc_id, "d1");
        assert_eq!(pending[1].doc_id, "d2");
        assert_eq!(pending[2].doc_id, "d3");
    }
}
