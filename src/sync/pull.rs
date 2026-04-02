use reqwest::Client;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

/// A single change entry received from the server pull endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PullChange {
    pub doc_id: String,
    pub data: Value,
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
    pub deleted: bool,
}

/// Response body from `GET /offlite/sync/{table}/pull`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullResponse {
    pub changes: Vec<PullChange>,
    pub server_time: String,
    pub has_more: bool,
}

/// Summary returned after pulling all changes for a table.
#[derive(Debug, Clone, Serialize)]
pub struct PullSummary {
    pub pulled: usize,
    pub applied: usize,
    pub deleted: usize,
    pub pages: usize,
}

// ---------------------------------------------------------------------------
// Network: pull changes from server
// ---------------------------------------------------------------------------

/// Fetch one page of incremental changes from the server pull endpoint.
///
/// Endpoint: `GET {base_url}/offlite/sync/{table}/pull?since=...&mode=...&filter_key=...`
/// Accept: `application/sjs` (MessagePack)
/// Authorization: `Bearer {token}`
pub async fn pull_changes(
    client: &Client,
    base_url: &str,
    token: &str,
    table_name: &str,
    since: &str,
    mode: &str,
    filter_key: &str,
) -> Result<PullResponse, String> {
    let url = format!(
        "{}/offlite/sync/{}/pull",
        base_url.trim_end_matches('/'),
        table_name,
    );

    let resp = client
        .get(&url)
        .query(&[("since", since), ("mode", mode), ("filter_key", filter_key)])
        .header("Accept", "application/sjs")
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await
        .map_err(|e| format!("Pull request failed: {}", e))?;

    if !resp.status().is_success() {
        return Err(format!(
            "Pull endpoint returned status {}",
            resp.status()
        ));
    }

    let resp_bytes = resp
        .bytes()
        .await
        .map_err(|e| format!("Failed to read pull response body: {}", e))?;

    let pull_resp: PullResponse = rmp_serde::from_slice(&resp_bytes)
        .map_err(|e| format!("Failed to decode pull response MessagePack: {}", e))?;

    Ok(pull_resp)
}

// ---------------------------------------------------------------------------
// Local DB: apply pulled changes
// ---------------------------------------------------------------------------

/// Apply a batch of pulled changes to the local project database.
///
/// For each change:
/// - If `deleted` is true → `UPDATE SET _deleted=1` (soft-delete)
/// - Otherwise → `INSERT OR REPLACE` with full data
///
/// Pulled changes are NOT written to `_change_log` because they originate
/// from the server, not from local writes.
pub fn apply_pull_changes(
    conn: &Connection,
    table_name: &str,
    changes: &[PullChange],
) -> Result<(usize, usize), String> {
    let mut applied = 0usize;
    let mut deleted = 0usize;

    for change in changes {
        if change.deleted {
            // Soft-delete: mark existing row as deleted
            let affected = conn
                .execute(
                    &format!(
                        "UPDATE [{}] SET _deleted = 1, updatedAt = ?1 WHERE _id = ?2",
                        table_name
                    ),
                    params![change.updated_at, change.doc_id],
                )
                .map_err(|e| {
                    format!(
                        "Failed to soft-delete doc '{}' in '{}': {}",
                        change.doc_id, table_name, e
                    )
                })?;
            if affected > 0 {
                deleted += 1;
            }
        } else {
            // Upsert: INSERT OR REPLACE with full document data
            conn.execute(
                &format!(
                    "INSERT OR REPLACE INTO [{}] (_id, data, updatedAt, _deleted) VALUES (?1, ?2, ?3, 0)",
                    table_name
                ),
                params![
                    change.doc_id,
                    change.data.to_string(),
                    change.updated_at,
                ],
            )
            .map_err(|e| {
                format!(
                    "Failed to upsert doc '{}' in '{}': {}",
                    change.doc_id, table_name, e
                )
            })?;
            applied += 1;
        }
    }

    Ok((applied, deleted))
}

// ---------------------------------------------------------------------------
// Checkpoint management (global.db)
// ---------------------------------------------------------------------------

/// Read the last sync checkpoint for a given table + sync_mode + filter_key.
///
/// Returns `None` if no checkpoint exists yet.
pub fn get_checkpoint(
    conn: &Connection,
    table_name: &str,
    sync_mode: &str,
    filter_key: &str,
) -> Result<Option<String>, String> {
    let result = conn.query_row(
        "SELECT last_sync_at FROM _sync_checkpoint
         WHERE table_name = ?1 AND sync_mode = ?2 AND filter_key = ?3",
        params![table_name, sync_mode, filter_key],
        |row| row.get::<_, String>(0),
    );

    match result {
        Ok(val) => Ok(Some(val)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(format!("Failed to read checkpoint: {}", e)),
    }
}

/// Insert or update the sync checkpoint for a given table + sync_mode + filter_key.
pub fn update_checkpoint(
    conn: &Connection,
    table_name: &str,
    sync_mode: &str,
    filter_key: &str,
    last_sync_at: &str,
) -> Result<(), String> {
    conn.execute(
        "INSERT OR REPLACE INTO _sync_checkpoint (table_name, sync_mode, filter_key, last_sync_at)
         VALUES (?1, ?2, ?3, ?4)",
        params![table_name, sync_mode, filter_key, last_sync_at],
    )
    .map_err(|e| format!("Failed to update checkpoint: {}", e))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Orchestration: paginated pull loop
// ---------------------------------------------------------------------------

/// Pull all pending changes for a table, handling pagination (`has_more`).
///
/// 1. Read the checkpoint from `global_conn` (`_sync_checkpoint`).
/// 2. Loop: call `pull_changes`, apply to `conn` (project db), until
///    `has_more` is false.
/// 3. Update the checkpoint in `global_conn` with the latest `server_time`.
pub async fn pull_table_changes(
    client: &Client,
    base_url: &str,
    token: &str,
    conn: &Connection,
    global_conn: &Connection,
    table_name: &str,
    sync_mode: &str,
    filter_key: &str,
) -> Result<PullSummary, String> {
    // 1. Get the last checkpoint
    let mut since = get_checkpoint(global_conn, table_name, sync_mode, filter_key)?
        .unwrap_or_default();

    let mut total_applied = 0usize;
    let mut total_deleted = 0usize;
    let mut pages = 0usize;

    // 2. Paginated pull loop
    loop {
        let resp = pull_changes(client, base_url, token, table_name, &since, sync_mode, filter_key).await?;

        pages += 1;

        if !resp.changes.is_empty() {
            let (applied, deleted) = apply_pull_changes(conn, table_name, &resp.changes)?;
            total_applied += applied;
            total_deleted += deleted;
        }

        let has_more = resp.has_more;
        // Update `since` for the next page (or final checkpoint) to server_time
        since = resp.server_time;

        if !has_more {
            break;
        }
    }

    // 3. Update checkpoint with the latest server_time (stored in `since`)
    if !since.is_empty() {
        update_checkpoint(global_conn, table_name, sync_mode, filter_key, &since)?;
    }

    Ok(PullSummary {
        pulled: total_applied + total_deleted,
        applied: total_applied,
        deleted: total_deleted,
        pages,
    })
}


// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    /// Create an in-memory database with a business table and _change_log.
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
            );
            CREATE TABLE IF NOT EXISTS _change_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name  TEXT NOT NULL,
                doc_id      TEXT NOT NULL,
                operation   TEXT NOT NULL,
                data        TEXT,
                timestamp   TEXT NOT NULL,
                synced      INTEGER DEFAULT 0,
                sync_error  TEXT
            );",
        )
        .unwrap();
        conn
    }

    /// Create an in-memory database with the _sync_checkpoint table.
    fn setup_global_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS _sync_checkpoint (
                table_name   TEXT NOT NULL,
                sync_mode    TEXT NOT NULL,
                filter_key   TEXT NOT NULL,
                last_sync_at TEXT NOT NULL,
                PRIMARY KEY (table_name, sync_mode, filter_key)
            );",
        )
        .unwrap();
        conn
    }

    // -- PullResponse MessagePack roundtrip --

    #[test]
    fn test_pull_response_msgpack_roundtrip() {
        let resp = PullResponse {
            changes: vec![
                PullChange {
                    doc_id: "doc_1".to_string(),
                    data: serde_json::json!({"name": "test", "value": 42}),
                    updated_at: "2025-01-01T00:00:00Z".to_string(),
                    deleted: false,
                },
                PullChange {
                    doc_id: "doc_2".to_string(),
                    data: serde_json::json!({}),
                    updated_at: "2025-01-02T00:00:00Z".to_string(),
                    deleted: true,
                },
            ],
            server_time: "2025-01-02T00:00:00Z".to_string(),
            has_more: false,
        };

        let bytes = rmp_serde::to_vec(&resp).unwrap();
        let decoded: PullResponse = rmp_serde::from_slice(&bytes).unwrap();

        assert_eq!(decoded.changes.len(), 2);
        assert_eq!(decoded.changes[0].doc_id, "doc_1");
        assert_eq!(decoded.changes[0].data, serde_json::json!({"name": "test", "value": 42}));
        assert!(!decoded.changes[0].deleted);
        assert_eq!(decoded.changes[1].doc_id, "doc_2");
        assert!(decoded.changes[1].deleted);
        assert_eq!(decoded.server_time, "2025-01-02T00:00:00Z");
        assert!(!decoded.has_more);
    }

    #[test]
    fn test_pull_response_msgpack_roundtrip_with_has_more() {
        let resp = PullResponse {
            changes: vec![PullChange {
                doc_id: "d1".to_string(),
                data: serde_json::json!({"x": 1}),
                updated_at: "2025-06-01T00:00:00Z".to_string(),
                deleted: false,
            }],
            server_time: "2025-06-01T00:00:00Z".to_string(),
            has_more: true,
        };

        let bytes = rmp_serde::to_vec(&resp).unwrap();
        let decoded: PullResponse = rmp_serde::from_slice(&bytes).unwrap();

        assert!(decoded.has_more);
        assert_eq!(decoded.changes.len(), 1);
    }

    #[test]
    fn test_pull_response_msgpack_roundtrip_empty_changes() {
        let resp = PullResponse {
            changes: vec![],
            server_time: "2025-01-01T00:00:00Z".to_string(),
            has_more: false,
        };

        let bytes = rmp_serde::to_vec(&resp).unwrap();
        let decoded: PullResponse = rmp_serde::from_slice(&bytes).unwrap();

        assert!(decoded.changes.is_empty());
        assert!(!decoded.has_more);
    }

    // -- apply_pull_changes tests --

    #[test]
    fn test_apply_pull_changes_insert_new() {
        let conn = setup_project_db();

        let changes = vec![PullChange {
            doc_id: "doc_new".to_string(),
            data: serde_json::json!({"name": "新文档", "area": 100.5}),
            updated_at: "2025-07-01T00:00:00Z".to_string(),
            deleted: false,
        }];

        let (applied, deleted) = apply_pull_changes(&conn, "planning", &changes).unwrap();
        assert_eq!(applied, 1);
        assert_eq!(deleted, 0);

        // Verify the row was inserted
        let data: String = conn
            .query_row(
                "SELECT data FROM planning WHERE _id = ?1",
                params!["doc_new"],
                |row| row.get(0),
            )
            .unwrap();
        let parsed: Value = serde_json::from_str(&data).unwrap();
        assert_eq!(parsed["name"], "新文档");
        assert_eq!(parsed["area"], 100.5);

        let updated_at: String = conn
            .query_row(
                "SELECT updatedAt FROM planning WHERE _id = ?1",
                params!["doc_new"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(updated_at, "2025-07-01T00:00:00Z");

        let is_deleted: i32 = conn
            .query_row(
                "SELECT _deleted FROM planning WHERE _id = ?1",
                params!["doc_new"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(is_deleted, 0);
    }

    #[test]
    fn test_apply_pull_changes_update_existing() {
        let conn = setup_project_db();

        // Insert an existing document
        conn.execute(
            "INSERT INTO planning (_id, data, updatedAt, createdAt) VALUES (?1, ?2, ?3, ?4)",
            params!["doc_exist", r#"{"name":"old"}"#, "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z"],
        )
        .unwrap();

        // Pull an update for the same doc
        let changes = vec![PullChange {
            doc_id: "doc_exist".to_string(),
            data: serde_json::json!({"name": "updated"}),
            updated_at: "2025-07-01T12:00:00Z".to_string(),
            deleted: false,
        }];

        let (applied, deleted) = apply_pull_changes(&conn, "planning", &changes).unwrap();
        assert_eq!(applied, 1);
        assert_eq!(deleted, 0);

        // Verify the data was updated
        let data: String = conn
            .query_row(
                "SELECT data FROM planning WHERE _id = ?1",
                params!["doc_exist"],
                |row| row.get(0),
            )
            .unwrap();
        let parsed: Value = serde_json::from_str(&data).unwrap();
        assert_eq!(parsed["name"], "updated");
    }

    #[test]
    fn test_apply_pull_changes_soft_delete() {
        let conn = setup_project_db();

        // Insert an existing document
        conn.execute(
            "INSERT INTO planning (_id, data, updatedAt, createdAt) VALUES (?1, ?2, ?3, ?4)",
            params!["doc_del", r#"{"name":"to_delete"}"#, "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z"],
        )
        .unwrap();

        // Pull a delete for the same doc
        let changes = vec![PullChange {
            doc_id: "doc_del".to_string(),
            data: serde_json::json!({}),
            updated_at: "2025-07-01T00:00:00Z".to_string(),
            deleted: true,
        }];

        let (applied, deleted) = apply_pull_changes(&conn, "planning", &changes).unwrap();
        assert_eq!(applied, 0);
        assert_eq!(deleted, 1);

        // Verify the row is soft-deleted
        let is_deleted: i32 = conn
            .query_row(
                "SELECT _deleted FROM planning WHERE _id = ?1",
                params!["doc_del"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(is_deleted, 1);

        // updatedAt should be updated
        let updated_at: String = conn
            .query_row(
                "SELECT updatedAt FROM planning WHERE _id = ?1",
                params!["doc_del"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(updated_at, "2025-07-01T00:00:00Z");
    }

    #[test]
    fn test_apply_pull_changes_delete_nonexistent_is_noop() {
        let conn = setup_project_db();

        // Try to soft-delete a doc that doesn't exist
        let changes = vec![PullChange {
            doc_id: "doc_ghost".to_string(),
            data: serde_json::json!({}),
            updated_at: "2025-07-01T00:00:00Z".to_string(),
            deleted: true,
        }];

        let (applied, deleted) = apply_pull_changes(&conn, "planning", &changes).unwrap();
        assert_eq!(applied, 0);
        assert_eq!(deleted, 0);
    }

    #[test]
    fn test_apply_pull_changes_mixed_batch() {
        let conn = setup_project_db();

        // Pre-insert a doc that will be deleted
        conn.execute(
            "INSERT INTO planning (_id, data, updatedAt, createdAt) VALUES (?1, ?2, ?3, ?4)",
            params!["doc_b", r#"{"v":1}"#, "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z"],
        )
        .unwrap();

        let changes = vec![
            PullChange {
                doc_id: "doc_a".to_string(),
                data: serde_json::json!({"new": true}),
                updated_at: "2025-07-01T00:00:00Z".to_string(),
                deleted: false,
            },
            PullChange {
                doc_id: "doc_b".to_string(),
                data: serde_json::json!({}),
                updated_at: "2025-07-01T00:00:00Z".to_string(),
                deleted: true,
            },
            PullChange {
                doc_id: "doc_c".to_string(),
                data: serde_json::json!({"name": "c"}),
                updated_at: "2025-07-01T00:00:00Z".to_string(),
                deleted: false,
            },
        ];

        let (applied, deleted) = apply_pull_changes(&conn, "planning", &changes).unwrap();
        assert_eq!(applied, 2);
        assert_eq!(deleted, 1);
    }

    #[test]
    fn test_apply_pull_changes_does_not_write_change_log() {
        let conn = setup_project_db();

        let changes = vec![PullChange {
            doc_id: "doc_pull".to_string(),
            data: serde_json::json!({"from": "server"}),
            updated_at: "2025-07-01T00:00:00Z".to_string(),
            deleted: false,
        }];

        apply_pull_changes(&conn, "planning", &changes).unwrap();

        // Verify _change_log is empty — pulled changes should NOT be logged
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM _change_log", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    // -- Checkpoint get/update tests --

    #[test]
    fn test_get_checkpoint_none() {
        let conn = setup_global_db();
        let result = get_checkpoint(&conn, "planning", "project", "p_001").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_update_and_get_checkpoint() {
        let conn = setup_global_db();

        update_checkpoint(&conn, "planning", "project", "p_001", "2025-07-01T00:00:00Z").unwrap();

        let result = get_checkpoint(&conn, "planning", "project", "p_001").unwrap();
        assert_eq!(result, Some("2025-07-01T00:00:00Z".to_string()));
    }

    #[test]
    fn test_update_checkpoint_overwrites() {
        let conn = setup_global_db();

        update_checkpoint(&conn, "planning", "project", "p_001", "2025-01-01T00:00:00Z").unwrap();
        update_checkpoint(&conn, "planning", "project", "p_001", "2025-07-01T00:00:00Z").unwrap();

        let result = get_checkpoint(&conn, "planning", "project", "p_001").unwrap();
        assert_eq!(result, Some("2025-07-01T00:00:00Z".to_string()));
    }

    #[test]
    fn test_checkpoint_isolation_by_table() {
        let conn = setup_global_db();

        update_checkpoint(&conn, "planning", "project", "p_001", "2025-01-01T00:00:00Z").unwrap();
        update_checkpoint(&conn, "sample", "project", "p_001", "2025-06-01T00:00:00Z").unwrap();

        let planning = get_checkpoint(&conn, "planning", "project", "p_001").unwrap();
        let sample = get_checkpoint(&conn, "sample", "project", "p_001").unwrap();

        assert_eq!(planning, Some("2025-01-01T00:00:00Z".to_string()));
        assert_eq!(sample, Some("2025-06-01T00:00:00Z".to_string()));
    }

    #[test]
    fn test_checkpoint_isolation_by_mode() {
        let conn = setup_global_db();

        update_checkpoint(&conn, "planning", "user", "uid_1", "2025-01-01T00:00:00Z").unwrap();
        update_checkpoint(&conn, "planning", "company", "cid_1", "2025-06-01T00:00:00Z").unwrap();

        let user = get_checkpoint(&conn, "planning", "user", "uid_1").unwrap();
        let company = get_checkpoint(&conn, "planning", "company", "cid_1").unwrap();

        assert_eq!(user, Some("2025-01-01T00:00:00Z".to_string()));
        assert_eq!(company, Some("2025-06-01T00:00:00Z".to_string()));
    }

    #[test]
    fn test_checkpoint_isolation_by_filter_key() {
        let conn = setup_global_db();

        update_checkpoint(&conn, "planning", "project", "p_001", "2025-01-01T00:00:00Z").unwrap();
        update_checkpoint(&conn, "planning", "project", "p_002", "2025-06-01T00:00:00Z").unwrap();

        let p1 = get_checkpoint(&conn, "planning", "project", "p_001").unwrap();
        let p2 = get_checkpoint(&conn, "planning", "project", "p_002").unwrap();

        assert_eq!(p1, Some("2025-01-01T00:00:00Z".to_string()));
        assert_eq!(p2, Some("2025-06-01T00:00:00Z".to_string()));
    }
}
