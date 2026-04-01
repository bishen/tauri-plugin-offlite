//! # Tauri Plugin Offlite
//!
//! A Tauri plugin for offline-first SQLite storage with built-in sync engine.
//!
//! ## Features
//! - Per-project SQLite databases with WAL mode
//! - Schema-driven table creation
//! - Change log for offline writes
//! - Sync engine: SSE real-time + polling fallback + offline queue
//! - LWW conflict resolution
//!
//! ## Usage
//! ```rust
//! fn main() {
//!     tauri::Builder::default()
//!         .plugin(tauri_plugin_offlite::init())
//!         .run(tauri::generate_context!())
//!         .unwrap();
//! }
//! ```

use tauri::{
    plugin::{self, TauriPlugin},
    Runtime,
};

/// Initialize the offlite plugin.
pub fn init<R: Runtime>() -> TauriPlugin<R> {
    plugin::Builder::new("offlite")
        .invoke_handler(tauri::generate_handler![
            // Database lifecycle
            db_open,
            db_close,
            db_execute,
            db_query,
            db_batch,
            db_delete,
            // Sync engine
            sync_start,
            sync_stop,
            sync_status,
        ])
        .build()
}

// ---------------------------------------------------------------------------
// Placeholder commands – real implementations will be added in later tasks
// ---------------------------------------------------------------------------

#[tauri::command]
fn db_open(project_id: String) -> Result<(), String> {
    log::info!("db_open: {}", project_id);
    Ok(())
}

#[tauri::command]
fn db_close(project_id: String) -> Result<(), String> {
    log::info!("db_close: {}", project_id);
    Ok(())
}

#[tauri::command]
fn db_execute(
    project_id: String,
    sql: String,
    params: Vec<serde_json::Value>,
) -> Result<u64, String> {
    log::info!("db_execute: {} sql={}", project_id, sql);
    let _ = params;
    Ok(0)
}

#[tauri::command]
fn db_query(
    project_id: String,
    sql: String,
    params: Vec<serde_json::Value>,
) -> Result<Vec<serde_json::Value>, String> {
    log::info!("db_query: {} sql={}", project_id, sql);
    let _ = params;
    Ok(vec![])
}

#[tauri::command]
fn db_batch(
    project_id: String,
    statements: Vec<serde_json::Value>,
) -> Result<Vec<u64>, String> {
    log::info!("db_batch: {} count={}", project_id, statements.len());
    Ok(vec![])
}

#[tauri::command]
fn db_delete(project_id: String) -> Result<(), String> {
    log::info!("db_delete: {}", project_id);
    Ok(())
}

#[tauri::command]
fn sync_start(
    project_id: String,
    config: serde_json::Value,
) -> Result<(), String> {
    log::info!("sync_start: {} config={}", project_id, config);
    Ok(())
}

#[tauri::command]
fn sync_stop(project_id: String) -> Result<(), String> {
    log::info!("sync_stop: {}", project_id);
    Ok(())
}

#[tauri::command]
fn sync_status(project_id: String) -> Result<serde_json::Value, String> {
    log::info!("sync_status: {}", project_id);
    Ok(serde_json::json!({
        "active": false,
        "paused": false,
        "error": null,
        "docs_read": 0,
        "docs_written": 0,
        "mode": "offline",
        "sse_connected": false
    }))
}
