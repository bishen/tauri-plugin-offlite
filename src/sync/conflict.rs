use chrono::Utc;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use super::push::ConflictInfo;

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

/// Which version won the conflict resolution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConflictWinner {
    Local,
    Server,
}

impl std::fmt::Display for ConflictWinner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConflictWinner::Local => write!(f, "local"),
            ConflictWinner::Server => write!(f, "server"),
        }
    }
}

/// Result of resolving a single conflict via LWW.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictResolution {
    pub doc_id: String,
    pub local_updated_at: String,
    pub server_updated_at: String,
    pub winner: ConflictWinner,
    pub resolved_at: String,
}

// ---------------------------------------------------------------------------
// Conflict log table management
// ---------------------------------------------------------------------------

/// Ensure the `_conflict_log` table exists in the given connection.
pub fn ensure_conflict_log_table(conn: &Connection) -> Result<(), String> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS _conflict_log (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id            TEXT NOT NULL,
            table_name        TEXT NOT NULL,
            local_updated_at  TEXT NOT NULL,
            server_updated_at TEXT NOT NULL,
            winner            TEXT NOT NULL,
            resolved_at       TEXT NOT NULL
        );",
    )
    .map_err(|e| format!("Failed to create _conflict_log table: {}", e))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// LWW conflict resolution
// ---------------------------------------------------------------------------

/// Resolve a single conflict using Last-Write-Wins (LWW) strategy.
///
/// - Reads the local document's `updatedAt` from the table.
/// - Compares with the server's `server_updated_at`.
/// - If server is newer (or equal as tiebreaker) → update local doc with server data.
/// - If local is newer → keep local (server will get it on next push).
/// - Returns a `ConflictResolution` describing the outcome.
pub fn resolve_conflict(
    conn: &Connection,
    table_name: &str,
    conflict: &ConflictInfo,
) -> Result<ConflictResolution, String> {
    // Read local document's updatedAt
    let local_updated_at: String = conn
        .query_row(
            &format!(
                "SELECT updatedAt FROM [{}] WHERE _id = ?1",
                table_name
            ),
            params![conflict.doc_id],
            |row| row.get(0),
        )
        .unwrap_or_default(); // If doc doesn't exist locally, treat as empty (server wins)

    let resolved_at = Utc::now().to_rfc3339();

    // LWW comparison: server wins on tie
    let server_wins = local_updated_at.is_empty() || conflict.server_updated_at >= local_updated_at;

    let winner = if server_wins {
        // Update local doc with server data
        conn.execute(
            &format!(
                "INSERT OR REPLACE INTO [{}] (_id, data, updatedAt, _deleted) VALUES (?1, ?2, ?3, 0)",
                table_name
            ),
            params![
                conflict.doc_id,
                conflict.server_data.to_string(),
                conflict.server_updated_at,
            ],
        )
        .map_err(|e| {
            format!(
                "Failed to apply server version for doc '{}': {}",
                conflict.doc_id, e
            )
        })?;
        ConflictWinner::Server
    } else {
        // Local is newer — keep local, server will get it on next push
        ConflictWinner::Local
    };

    Ok(ConflictResolution {
        doc_id: conflict.doc_id.clone(),
        local_updated_at,
        server_updated_at: conflict.server_updated_at.clone(),
        winner,
        resolved_at,
    })
}


/// Resolve multiple conflicts in batch using LWW strategy.
///
/// Returns a `Vec<ConflictResolution>` with one entry per conflict.
pub fn resolve_conflicts(
    conn: &Connection,
    table_name: &str,
    conflicts: &[ConflictInfo],
) -> Result<Vec<ConflictResolution>, String> {
    let mut resolutions = Vec::with_capacity(conflicts.len());
    for conflict in conflicts {
        let resolution = resolve_conflict(conn, table_name, conflict)?;
        resolutions.push(resolution);
    }
    Ok(resolutions)
}

/// Scan the database for potential conflicts and resolve them.
///
/// In the LWW model, conflicts are resolved at push/pull time, so this is
/// mainly a no-op for normal operation. Provided for completeness and edge
/// cases where conflicts might linger (e.g. interrupted sync).
///
/// Currently returns an empty list since LWW conflicts are resolved inline.
pub fn scan_and_resolve_all(
    _conn: &Connection,
    _table_name: &str,
) -> Result<Vec<ConflictResolution>, String> {
    // In the LWW model, conflicts are resolved immediately during push/pull.
    // There is no separate "conflict" state stored in the DB.
    // This function exists as a hook for future conflict strategies.
    Ok(vec![])
}

// ---------------------------------------------------------------------------
// Conflict logging
// ---------------------------------------------------------------------------

/// Record a conflict resolution into the `_conflict_log` table.
///
/// Creates the table if it does not exist.
pub fn log_conflict(
    conn: &Connection,
    table_name: &str,
    resolution: &ConflictResolution,
) -> Result<i64, String> {
    ensure_conflict_log_table(conn)?;

    conn.execute(
        "INSERT INTO _conflict_log (doc_id, table_name, local_updated_at, server_updated_at, winner, resolved_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            resolution.doc_id,
            table_name,
            resolution.local_updated_at,
            resolution.server_updated_at,
            resolution.winner.to_string(),
            resolution.resolved_at,
        ],
    )
    .map_err(|e| format!("Failed to log conflict resolution: {}", e))?;

    Ok(conn.last_insert_rowid())
}

/// Log multiple conflict resolutions in batch.
pub fn log_conflicts(
    conn: &Connection,
    table_name: &str,
    resolutions: &[ConflictResolution],
) -> Result<Vec<i64>, String> {
    let mut ids = Vec::with_capacity(resolutions.len());
    for resolution in resolutions {
        let id = log_conflict(conn, table_name, resolution)?;
        ids.push(id);
    }
    Ok(ids)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    /// Create an in-memory database with a business table for testing.
    fn setup_project_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS planning (
                _id         TEXT PRIMARY KEY,
                uid         INTEGER,
                companyId   INTEGER,
                p_id        TEXT,
                createdAt   TEXT NOT NULL DEFAULT '',
                updatedAt   TEXT NOT NULL DEFAULT '',
                _deleted    INTEGER DEFAULT 0,
                _version    INTEGER DEFAULT 1,
                data        TEXT NOT NULL DEFAULT '{}'
            );",
        )
        .unwrap();
        conn
    }

    /// Insert a local document with a specific updatedAt timestamp.
    fn insert_local_doc(conn: &Connection, doc_id: &str, data: &str, updated_at: &str) {
        conn.execute(
            "INSERT INTO planning (_id, data, updatedAt, createdAt) VALUES (?1, ?2, ?3, ?3)",
            params![doc_id, data, updated_at],
        )
        .unwrap();
    }

    fn make_conflict(doc_id: &str, server_data: Value, server_updated_at: &str) -> ConflictInfo {
        ConflictInfo {
            doc_id: doc_id.to_string(),
            server_data,
            server_updated_at: server_updated_at.to_string(),
        }
    }

    // -- LWW: server newer → server wins --

    #[test]
    fn test_lww_server_newer_wins() {
        let conn = setup_project_db();
        insert_local_doc(&conn, "doc_1", r#"{"v":"local"}"#, "2025-01-01T00:00:00Z");

        let conflict = make_conflict(
            "doc_1",
            serde_json::json!({"v": "server"}),
            "2025-07-01T00:00:00Z",
        );

        let resolution = resolve_conflict(&conn, "planning", &conflict).unwrap();

        assert_eq!(resolution.doc_id, "doc_1");
        assert_eq!(resolution.local_updated_at, "2025-01-01T00:00:00Z");
        assert_eq!(resolution.server_updated_at, "2025-07-01T00:00:00Z");
        assert_eq!(resolution.winner, ConflictWinner::Server);

        // Verify local doc was updated with server data
        let data: String = conn
            .query_row(
                "SELECT data FROM planning WHERE _id = ?1",
                params!["doc_1"],
                |row| row.get(0),
            )
            .unwrap();
        let parsed: Value = serde_json::from_str(&data).unwrap();
        assert_eq!(parsed["v"], "server");
    }

    // -- LWW: local newer → local wins --

    #[test]
    fn test_lww_local_newer_wins() {
        let conn = setup_project_db();
        insert_local_doc(&conn, "doc_2", r#"{"v":"local"}"#, "2025-07-01T00:00:00Z");

        let conflict = make_conflict(
            "doc_2",
            serde_json::json!({"v": "server"}),
            "2025-01-01T00:00:00Z",
        );

        let resolution = resolve_conflict(&conn, "planning", &conflict).unwrap();

        assert_eq!(resolution.winner, ConflictWinner::Local);

        // Verify local doc was NOT changed
        let data: String = conn
            .query_row(
                "SELECT data FROM planning WHERE _id = ?1",
                params!["doc_2"],
                |row| row.get(0),
            )
            .unwrap();
        let parsed: Value = serde_json::from_str(&data).unwrap();
        assert_eq!(parsed["v"], "local");
    }

    // -- LWW: same timestamp → server wins (tiebreaker) --

    #[test]
    fn test_lww_same_timestamp_server_wins() {
        let conn = setup_project_db();
        let ts = "2025-06-15T12:00:00Z";
        insert_local_doc(&conn, "doc_3", r#"{"v":"local"}"#, ts);

        let conflict = make_conflict(
            "doc_3",
            serde_json::json!({"v": "server"}),
            ts,
        );

        let resolution = resolve_conflict(&conn, "planning", &conflict).unwrap();

        assert_eq!(resolution.winner, ConflictWinner::Server);

        // Verify server data was applied
        let data: String = conn
            .query_row(
                "SELECT data FROM planning WHERE _id = ?1",
                params!["doc_3"],
                |row| row.get(0),
            )
            .unwrap();
        let parsed: Value = serde_json::from_str(&data).unwrap();
        assert_eq!(parsed["v"], "server");
    }

    // -- LWW: doc doesn't exist locally → server wins --

    #[test]
    fn test_lww_no_local_doc_server_wins() {
        let conn = setup_project_db();

        let conflict = make_conflict(
            "doc_new",
            serde_json::json!({"v": "server_new"}),
            "2025-07-01T00:00:00Z",
        );

        let resolution = resolve_conflict(&conn, "planning", &conflict).unwrap();

        assert_eq!(resolution.winner, ConflictWinner::Server);
        assert!(resolution.local_updated_at.is_empty());

        // Verify server doc was inserted
        let data: String = conn
            .query_row(
                "SELECT data FROM planning WHERE _id = ?1",
                params!["doc_new"],
                |row| row.get(0),
            )
            .unwrap();
        let parsed: Value = serde_json::from_str(&data).unwrap();
        assert_eq!(parsed["v"], "server_new");
    }

    // -- Batch conflict resolution --

    #[test]
    fn test_resolve_conflicts_batch() {
        let conn = setup_project_db();
        insert_local_doc(&conn, "d1", r#"{"v":"local1"}"#, "2025-01-01T00:00:00Z");
        insert_local_doc(&conn, "d2", r#"{"v":"local2"}"#, "2025-07-01T00:00:00Z");
        insert_local_doc(&conn, "d3", r#"{"v":"local3"}"#, "2025-06-01T00:00:00Z");

        let conflicts = vec![
            make_conflict("d1", serde_json::json!({"v": "s1"}), "2025-06-01T00:00:00Z"), // server newer
            make_conflict("d2", serde_json::json!({"v": "s2"}), "2025-01-01T00:00:00Z"), // local newer
            make_conflict("d3", serde_json::json!({"v": "s3"}), "2025-06-01T00:00:00Z"), // same ts → server
        ];

        let resolutions = resolve_conflicts(&conn, "planning", &conflicts).unwrap();

        assert_eq!(resolutions.len(), 3);
        assert_eq!(resolutions[0].winner, ConflictWinner::Server);
        assert_eq!(resolutions[1].winner, ConflictWinner::Local);
        assert_eq!(resolutions[2].winner, ConflictWinner::Server);
    }

    // -- Conflict logging --

    #[test]
    fn test_log_conflict() {
        let conn = setup_project_db();

        let resolution = ConflictResolution {
            doc_id: "doc_log".to_string(),
            local_updated_at: "2025-01-01T00:00:00Z".to_string(),
            server_updated_at: "2025-07-01T00:00:00Z".to_string(),
            winner: ConflictWinner::Server,
            resolved_at: "2025-07-01T12:00:00Z".to_string(),
        };

        let id = log_conflict(&conn, "planning", &resolution).unwrap();
        assert!(id > 0);

        // Verify the log entry
        let (doc_id, table, winner): (String, String, String) = conn
            .query_row(
                "SELECT doc_id, table_name, winner FROM _conflict_log WHERE id = ?1",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();

        assert_eq!(doc_id, "doc_log");
        assert_eq!(table, "planning");
        assert_eq!(winner, "server");
    }

    #[test]
    fn test_log_conflicts_batch() {
        let conn = setup_project_db();

        let resolutions = vec![
            ConflictResolution {
                doc_id: "d1".to_string(),
                local_updated_at: "2025-01-01T00:00:00Z".to_string(),
                server_updated_at: "2025-07-01T00:00:00Z".to_string(),
                winner: ConflictWinner::Server,
                resolved_at: "2025-07-01T12:00:00Z".to_string(),
            },
            ConflictResolution {
                doc_id: "d2".to_string(),
                local_updated_at: "2025-07-01T00:00:00Z".to_string(),
                server_updated_at: "2025-01-01T00:00:00Z".to_string(),
                winner: ConflictWinner::Local,
                resolved_at: "2025-07-01T12:00:00Z".to_string(),
            },
        ];

        let ids = log_conflicts(&conn, "planning", &resolutions).unwrap();
        assert_eq!(ids.len(), 2);

        // Verify count
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM _conflict_log", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }

    // -- scan_and_resolve_all (no-op for LWW) --

    #[test]
    fn test_scan_and_resolve_all_returns_empty() {
        let conn = setup_project_db();
        let result = scan_and_resolve_all(&conn, "planning").unwrap();
        assert!(result.is_empty());
    }

    // -- ConflictWinner serialization --

    #[test]
    fn test_conflict_winner_serialize() {
        assert_eq!(
            serde_json::to_string(&ConflictWinner::Local).unwrap(),
            r#""local""#
        );
        assert_eq!(
            serde_json::to_string(&ConflictWinner::Server).unwrap(),
            r#""server""#
        );
    }

    #[test]
    fn test_conflict_winner_display() {
        assert_eq!(ConflictWinner::Local.to_string(), "local");
        assert_eq!(ConflictWinner::Server.to_string(), "server");
    }

    // -- ConflictResolution serialization roundtrip --

    #[test]
    fn test_conflict_resolution_json_roundtrip() {
        let resolution = ConflictResolution {
            doc_id: "doc_rt".to_string(),
            local_updated_at: "2025-01-01T00:00:00Z".to_string(),
            server_updated_at: "2025-07-01T00:00:00Z".to_string(),
            winner: ConflictWinner::Server,
            resolved_at: "2025-07-01T12:00:00Z".to_string(),
        };

        let json = serde_json::to_string(&resolution).unwrap();
        let decoded: ConflictResolution = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.doc_id, "doc_rt");
        assert_eq!(decoded.winner, ConflictWinner::Server);
    }
}
