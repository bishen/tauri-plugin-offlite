use reqwest::Client;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::changelog::{self, ChangeRecord};

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

/// A single change entry in the push request payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PushChange {
    /// `"upsert"` or `"delete"`
    pub op: String,
    pub doc_id: String,
    /// Full document JSON for upsert; `None` for delete.
    pub data: Option<Value>,
    /// ISO 8601 timestamp of the change.
    pub updated_at: String,
}

/// Request body sent to `POST /offlite/sync/{table}/push`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushRequest {
    pub changes: Vec<PushChange>,
}

/// Information about a single conflict returned by the server.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConflictInfo {
    pub doc_id: String,
    pub server_data: Value,
    pub server_updated_at: String,
}

/// Response body from the push endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushResponse {
    pub accepted: Vec<String>,
    pub conflicts: Vec<ConflictInfo>,
}

/// Summary returned after pushing all pending changes for a table.
#[derive(Debug, Clone, Serialize)]
pub struct PushSummary {
    pub pushed: usize,
    pub accepted: usize,
    pub conflicts: usize,
    pub errors: Vec<String>,
}

// ---------------------------------------------------------------------------
// Conversion: ChangeRecord → PushChange
// ---------------------------------------------------------------------------

/// Convert a `ChangeRecord` (from `_change_log`) into a `PushChange` suitable
/// for the push request payload.
///
/// Mapping:
/// - `INSERT` / `UPDATE` → `op: "upsert"`, with `data` parsed from the
///   record's JSON string.
/// - `DELETE` → `op: "delete"`, `data` is `None`.
pub fn change_record_to_push(record: &ChangeRecord) -> Result<PushChange, String> {
    let (op, data) = match record.operation.as_str() {
        "INSERT" | "UPDATE" => {
            let json_str = record
                .data
                .as_deref()
                .ok_or_else(|| {
                    format!(
                        "ChangeRecord {} ({}) has no data for {}",
                        record.id, record.doc_id, record.operation
                    )
                })?;
            let val: Value = serde_json::from_str(json_str).map_err(|e| {
                format!(
                    "Failed to parse data JSON for record {}: {}",
                    record.id, e
                )
            })?;
            ("upsert".to_string(), Some(val))
        }
        "DELETE" => ("delete".to_string(), None),
        other => {
            return Err(format!(
                "Unknown operation '{}' in change record {}",
                other, record.id
            ))
        }
    };

    Ok(PushChange {
        op,
        doc_id: record.doc_id.clone(),
        data,
        updated_at: record.timestamp.clone(),
    })
}

// ---------------------------------------------------------------------------
// Network: push changes to server
// ---------------------------------------------------------------------------

/// Serialize `changes` as MessagePack and POST them to the server push
/// endpoint.
///
/// Endpoint: `POST {base_url}/offlite/sync/{table}/push`
/// Content-Type: `application/sjs` (MessagePack)
/// Authorization: `Bearer {token}`
pub async fn push_changes(
    client: &Client,
    base_url: &str,
    token: &str,
    table_name: &str,
    changes: Vec<PushChange>,
) -> Result<PushResponse, String> {
    if changes.is_empty() {
        return Ok(PushResponse {
            accepted: vec![],
            conflicts: vec![],
        });
    }

    let request = PushRequest { changes };
    let body = rmp_serde::to_vec(&request)
        .map_err(|e| format!("Failed to encode push request as MessagePack: {}", e))?;

    let url = format!(
        "{}/offlite/sync/{}/push",
        base_url.trim_end_matches('/'),
        table_name
    );

    let resp = client
        .post(&url)
        .header("Content-Type", "application/sjs")
        .header("Authorization", format!("Bearer {}", token))
        .body(body)
        .send()
        .await
        .map_err(|e| format!("Push request failed: {}", e))?;

    if !resp.status().is_success() {
        return Err(format!(
            "Push endpoint returned status {}",
            resp.status()
        ));
    }

    let resp_bytes = resp
        .bytes()
        .await
        .map_err(|e| format!("Failed to read push response body: {}", e))?;

    let push_resp: PushResponse = rmp_serde::from_slice(&resp_bytes)
        .map_err(|e| format!("Failed to decode push response MessagePack: {}", e))?;

    Ok(push_resp)
}

// ---------------------------------------------------------------------------
// Orchestration: read pending changes, push, update changelog
// ---------------------------------------------------------------------------

/// Push all pending (unsynced) changes for `table_name`.
///
/// 1. Read pending changes from `_change_log`.
/// 2. Convert to `PushChange` list.
/// 3. POST to server.
/// 4. Mark accepted changes as `synced = 1`.
/// 5. Record `sync_error` for conflicts.
///
/// Supports two strategies controlled by the caller:
/// - **write-through** (online-stable): call this immediately after each write.
/// - **batch push** (online-unstable): call this periodically to flush the
///   queue.
pub async fn push_table_changes(
    client: &Client,
    base_url: &str,
    token: &str,
    conn: &Connection,
    table_name: &str,
) -> Result<PushSummary, String> {
    // 1. Get pending changes
    let pending = changelog::get_pending_changes(conn, table_name)?;
    if pending.is_empty() {
        return Ok(PushSummary {
            pushed: 0,
            accepted: 0,
            conflicts: 0,
            errors: vec![],
        });
    }

    // 2. Convert to PushChange, collecting per-record errors
    let mut push_changes_list: Vec<PushChange> = Vec::with_capacity(pending.len());
    let mut record_map: std::collections::HashMap<String, i64> =
        std::collections::HashMap::new();
    let mut errors: Vec<String> = Vec::new();

    for record in &pending {
        match change_record_to_push(record) {
            Ok(pc) => {
                // Map doc_id → change_log id for later marking
                record_map.insert(record.doc_id.clone(), record.id);
                push_changes_list.push(pc);
            }
            Err(e) => {
                changelog::mark_sync_error(conn, record.id, &e)?;
                errors.push(e);
            }
        }
    }

    let pushed = push_changes_list.len();

    if push_changes_list.is_empty() {
        return Ok(PushSummary {
            pushed: 0,
            accepted: 0,
            conflicts: errors.len(),
            errors,
        });
    }

    // 3. Push to server
    let resp = push_changes(client, base_url, token, table_name, push_changes_list).await?;

    // 4. Mark accepted changes as synced
    let accepted_ids: Vec<i64> = resp
        .accepted
        .iter()
        .filter_map(|doc_id| record_map.get(doc_id).copied())
        .collect();

    if !accepted_ids.is_empty() {
        changelog::mark_synced(conn, &accepted_ids)?;
    }

    // 5. Record sync_error for conflicts
    for conflict in &resp.conflicts {
        if let Some(&change_id) = record_map.get(&conflict.doc_id) {
            let err_msg = format!(
                "Conflict: server has newer version (server_updated_at={})",
                conflict.server_updated_at
            );
            changelog::mark_sync_error(conn, change_id, &err_msg)?;
            errors.push(err_msg);
        }
    }

    Ok(PushSummary {
        pushed,
        accepted: resp.accepted.len(),
        conflicts: resp.conflicts.len(),
        errors,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::ChangeRecord;

    /// Helper to build a ChangeRecord for testing.
    fn make_record(
        id: i64,
        doc_id: &str,
        operation: &str,
        data: Option<&str>,
        timestamp: &str,
    ) -> ChangeRecord {
        ChangeRecord {
            id,
            table_name: "planning".to_string(),
            doc_id: doc_id.to_string(),
            operation: operation.to_string(),
            data: data.map(|s| s.to_string()),
            timestamp: timestamp.to_string(),
            synced: 0,
            sync_error: None,
        }
    }

    // -- change_record_to_push conversion tests --

    #[test]
    fn test_insert_converts_to_upsert() {
        let rec = make_record(1, "doc_1", "INSERT", Some(r#"{"name":"a"}"#), "2025-01-01T00:00:00Z");
        let pc = change_record_to_push(&rec).unwrap();

        assert_eq!(pc.op, "upsert");
        assert_eq!(pc.doc_id, "doc_1");
        assert_eq!(pc.data, Some(serde_json::json!({"name": "a"})));
        assert_eq!(pc.updated_at, "2025-01-01T00:00:00Z");
    }

    #[test]
    fn test_update_converts_to_upsert() {
        let rec = make_record(2, "doc_2", "UPDATE", Some(r#"{"v":2}"#), "2025-06-01T12:00:00Z");
        let pc = change_record_to_push(&rec).unwrap();

        assert_eq!(pc.op, "upsert");
        assert_eq!(pc.doc_id, "doc_2");
        assert_eq!(pc.data, Some(serde_json::json!({"v": 2})));
        assert_eq!(pc.updated_at, "2025-06-01T12:00:00Z");
    }

    #[test]
    fn test_delete_converts_to_delete() {
        let rec = make_record(3, "doc_3", "DELETE", None, "2025-07-01T00:00:00Z");
        let pc = change_record_to_push(&rec).unwrap();

        assert_eq!(pc.op, "delete");
        assert_eq!(pc.doc_id, "doc_3");
        assert!(pc.data.is_none());
        assert_eq!(pc.updated_at, "2025-07-01T00:00:00Z");
    }

    #[test]
    fn test_insert_without_data_is_error() {
        let rec = make_record(4, "doc_4", "INSERT", None, "2025-01-01T00:00:00Z");
        let result = change_record_to_push(&rec);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("no data"));
    }

    #[test]
    fn test_update_without_data_is_error() {
        let rec = make_record(5, "doc_5", "UPDATE", None, "2025-01-01T00:00:00Z");
        let result = change_record_to_push(&rec);
        assert!(result.is_err());
    }

    #[test]
    fn test_unknown_operation_is_error() {
        let rec = make_record(6, "doc_6", "MERGE", Some("{}"), "2025-01-01T00:00:00Z");
        let result = change_record_to_push(&rec);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown operation"));
    }

    #[test]
    fn test_invalid_json_data_is_error() {
        let rec = make_record(7, "doc_7", "INSERT", Some("not json"), "2025-01-01T00:00:00Z");
        let result = change_record_to_push(&rec);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("parse data JSON"));
    }

    #[test]
    fn test_complex_data_preserved() {
        let data = r#"{"properties":{"地类":"有林地","树种":"杉木"},"geometry":{"type":"Polygon","coordinates":[[[1,2],[3,4],[5,6],[1,2]]]}}"#;
        let rec = make_record(8, "doc_8", "INSERT", Some(data), "2025-01-01T00:00:00Z");
        let pc = change_record_to_push(&rec).unwrap();

        let d = pc.data.unwrap();
        assert_eq!(d["properties"]["地类"], "有林地");
        assert_eq!(d["geometry"]["type"], "Polygon");
    }

    #[test]
    fn test_delete_with_data_still_produces_delete() {
        // DELETE records may sometimes carry data; op should still be "delete"
        let rec = make_record(9, "doc_9", "DELETE", Some(r#"{"old":"data"}"#), "2025-01-01T00:00:00Z");
        let pc = change_record_to_push(&rec).unwrap();
        assert_eq!(pc.op, "delete");
        assert!(pc.data.is_none());
    }

    // -- PushRequest MessagePack serialization round-trip --

    #[test]
    fn test_push_request_msgpack_roundtrip() {
        let req = PushRequest {
            changes: vec![
                PushChange {
                    op: "upsert".to_string(),
                    doc_id: "d1".to_string(),
                    data: Some(serde_json::json!({"x": 1})),
                    updated_at: "2025-01-01T00:00:00Z".to_string(),
                },
                PushChange {
                    op: "delete".to_string(),
                    doc_id: "d2".to_string(),
                    data: None,
                    updated_at: "2025-01-02T00:00:00Z".to_string(),
                },
            ],
        };

        let bytes = rmp_serde::to_vec(&req).unwrap();
        let decoded: PushRequest = rmp_serde::from_slice(&bytes).unwrap();

        assert_eq!(decoded.changes.len(), 2);
        assert_eq!(decoded.changes[0].op, "upsert");
        assert_eq!(decoded.changes[0].doc_id, "d1");
        assert_eq!(decoded.changes[1].op, "delete");
        assert_eq!(decoded.changes[1].doc_id, "d2");
    }

    // -- PushResponse MessagePack serialization round-trip --

    #[test]
    fn test_push_response_msgpack_roundtrip() {
        let resp = PushResponse {
            accepted: vec!["d1".to_string(), "d3".to_string()],
            conflicts: vec![ConflictInfo {
                doc_id: "d2".to_string(),
                server_data: serde_json::json!({"v": 99}),
                server_updated_at: "2025-07-01T00:00:00Z".to_string(),
            }],
        };

        let bytes = rmp_serde::to_vec(&resp).unwrap();
        let decoded: PushResponse = rmp_serde::from_slice(&bytes).unwrap();

        assert_eq!(decoded.accepted, vec!["d1", "d3"]);
        assert_eq!(decoded.conflicts.len(), 1);
        assert_eq!(decoded.conflicts[0].doc_id, "d2");
        assert_eq!(decoded.conflicts[0].server_data, serde_json::json!({"v": 99}));
    }

    // -- push_changes with empty list --

    #[tokio::test]
    async fn test_push_changes_empty_returns_empty_response() {
        let client = Client::new();
        let resp = push_changes(&client, "http://localhost", "tok", "tbl", vec![]).await.unwrap();
        assert!(resp.accepted.is_empty());
        assert!(resp.conflicts.is_empty());
    }

    // -- Batch conversion of multiple records --

    #[test]
    fn test_batch_conversion_mixed_operations() {
        let records = vec![
            make_record(1, "a", "INSERT", Some(r#"{"n":1}"#), "2025-01-01T00:00:00Z"),
            make_record(2, "b", "UPDATE", Some(r#"{"n":2}"#), "2025-01-02T00:00:00Z"),
            make_record(3, "c", "DELETE", None, "2025-01-03T00:00:00Z"),
        ];

        let push_changes: Vec<PushChange> = records
            .iter()
            .map(|r| change_record_to_push(r).unwrap())
            .collect();

        assert_eq!(push_changes.len(), 3);
        assert_eq!(push_changes[0].op, "upsert");
        assert_eq!(push_changes[1].op, "upsert");
        assert_eq!(push_changes[2].op, "delete");
    }
}
