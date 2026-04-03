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
//! ```rust,ignore
//! fn main() {
//!     tauri::Builder::default()
//!         .plugin(tauri_plugin_offlite::init())
//!         .run(tauri::generate_context!())
//!         .unwrap();
//! }
//! ```

use std::path::PathBuf;
use tauri::{
    plugin::{self, TauriPlugin},
    AppHandle, Manager, Runtime,
};

pub mod changelog;
pub mod database;
pub mod schema;
pub mod sync;

use database::DatabaseManager;
use schema::TableSchema;
use sync::engine::{SyncEngineConfig, SyncEngineManager};

/// Initialize the offlite plugin.
///
/// During setup the plugin will:
/// 1. Resolve the app data directory
/// 2. Create a `DatabaseManager` and ensure the `projects/` subdirectory exists
/// 3. Open `global.db` with WAL mode and create global tables
/// 4. Store the `DatabaseManager` as Tauri managed state
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
            // Schema
            db_create_tables,
            // Sync engine
            sync_start,
            sync_stop,
            sync_status,
        ])
        .setup(|app, _api| {
            // Resolve the data directory (platform-specific)
            let data_dir = resolve_data_dir(app)?;

            log::info!(
                "offlite: initializing with data_dir = {}",
                data_dir.display()
            );

            // Create the DatabaseManager
            let db_manager = DatabaseManager::new(&data_dir).map_err(
                |e| -> Box<dyn std::error::Error> {
                    format!("Failed to create DatabaseManager: {}", e).into()
                },
            )?;

            // Open the global database
            db_manager.open_global_db().map_err(
                |e| -> Box<dyn std::error::Error> {
                    format!("Failed to open global database: {}", e).into()
                },
            )?;

            log::info!("offlite: global.db opened successfully");

            // Store as managed state so commands can access it
            app.manage(db_manager);

            // Create and manage the SyncEngineManager
            app.manage(SyncEngineManager::new());

            Ok(())
        })
        .build()
}

// ---------------------------------------------------------------------------
// Platform-specific data directory resolution
// ---------------------------------------------------------------------------

/// Resolve the data directory based on the current platform.
///
/// - Desktop (Windows/macOS/Linux): uses `{exe_dir}/data/`
///   Falls back to `app_data_dir` if the exe directory is not writable.
/// - Mobile (Android/iOS): uses `app.path().app_data_dir()`
fn resolve_data_dir<R: Runtime>(app: &AppHandle<R>) -> Result<PathBuf, Box<dyn std::error::Error>> {
    #[cfg(not(any(target_os = "android", target_os = "ios")))]
    {
        if let Ok(exe_path) = std::env::current_exe() {
            if let Some(exe_dir) = exe_path.parent() {
                let data_dir = exe_dir.join("data");
                // Try to create the directory to test write permission
                if std::fs::create_dir_all(&data_dir).is_ok() {
                    // Test write by creating a temp file
                    let test_file = data_dir.join(".write_test");
                    if std::fs::write(&test_file, b"test").is_ok() {
                        let _ = std::fs::remove_file(&test_file);
                        log::info!(
                            "offlite: using exe-relative data dir: {}",
                            data_dir.display()
                        );
                        return Ok(data_dir);
                    }
                }
                log::warn!(
                    "offlite: exe dir not writable ({}), falling back to app_data_dir",
                    data_dir.display()
                );
            }
        }
    }

    // Mobile or fallback: use app_data_dir
    let app_data_dir = app
        .path()
        .app_data_dir()
        .map_err(|e| format!("Failed to resolve app data dir: {}", e))?;
    log::info!("offlite: using app_data_dir: {}", app_data_dir.display());
    Ok(app_data_dir)
}

// ---------------------------------------------------------------------------
// Tauri commands – use DatabaseManager from managed state
// ---------------------------------------------------------------------------

#[tauri::command]
fn db_open(
    state: tauri::State<'_, DatabaseManager>,
    project_id: String,
) -> Result<(), String> {
    log::info!("db_open: {}", project_id);
    if project_id == "global" {
        // Global DB is already opened during plugin setup
        return Ok(());
    }
    state.open_project_db(&project_id)
}

#[tauri::command]
fn db_close(
    state: tauri::State<'_, DatabaseManager>,
    project_id: String,
) -> Result<(), String> {
    log::info!("db_close: {}", project_id);
    state.close_project_db(&project_id)
}

#[tauri::command]
fn db_execute(
    state: tauri::State<'_, DatabaseManager>,
    project_id: String,
    sql: String,
    params: Vec<serde_json::Value>,
) -> Result<u64, String> {
    log::info!("db_execute: {} sql={}", project_id, sql);
    state.with_connection(&project_id, |conn| {
        let param_refs: Vec<Box<dyn rusqlite::types::ToSql>> = params
            .iter()
            .map(|v| json_value_to_sql(v))
            .collect();
        let param_slice: Vec<&dyn rusqlite::types::ToSql> =
            param_refs.iter().map(|b| b.as_ref()).collect();
        let affected = conn
            .execute(&sql, param_slice.as_slice())
            .map_err(|e| format!("db_execute error: {}", e))?;
        Ok(affected as u64)
    })
}

#[tauri::command]
fn db_query(
    state: tauri::State<'_, DatabaseManager>,
    project_id: String,
    sql: String,
    params: Vec<serde_json::Value>,
) -> Result<Vec<serde_json::Value>, String> {
    log::info!("db_query: {} sql={}", project_id, sql);
    state.with_connection(&project_id, |conn| {
        let param_refs: Vec<Box<dyn rusqlite::types::ToSql>> = params
            .iter()
            .map(|v| json_value_to_sql(v))
            .collect();
        let param_slice: Vec<&dyn rusqlite::types::ToSql> =
            param_refs.iter().map(|b| b.as_ref()).collect();

        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| format!("db_query prepare error: {}", e))?;

        let column_names: Vec<String> = stmt
            .column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();

        let rows = stmt
            .query_map(param_slice.as_slice(), |row| {
                let mut map = serde_json::Map::new();
                for (i, name) in column_names.iter().enumerate() {
                    let val: rusqlite::types::Value = row.get(i)?;
                    map.insert(name.clone(), sqlite_value_to_json(val));
                }
                Ok(serde_json::Value::Object(map))
            })
            .map_err(|e| format!("db_query error: {}", e))?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row.map_err(|e| format!("db_query row error: {}", e))?);
        }
        Ok(results)
    })
}

#[tauri::command]
fn db_batch(
    state: tauri::State<'_, DatabaseManager>,
    project_id: String,
    statements: Vec<serde_json::Value>,
) -> Result<Vec<u64>, String> {
    log::info!("db_batch: {} count={}", project_id, statements.len());
    state.with_connection(&project_id, |conn| {
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| format!("db_batch transaction error: {}", e))?;

        let mut results = Vec::new();
        for stmt_val in &statements {
            let sql = stmt_val
                .get("sql")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "Each statement must have a 'sql' field".to_string())?;
            let params = stmt_val
                .get("params")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();

            let param_refs: Vec<Box<dyn rusqlite::types::ToSql>> = params
                .iter()
                .map(|v| json_value_to_sql(v))
                .collect();
            let param_slice: Vec<&dyn rusqlite::types::ToSql> =
                param_refs.iter().map(|b| b.as_ref()).collect();

            let affected = tx
                .execute(sql, param_slice.as_slice())
                .map_err(|e| format!("db_batch execute error: {}", e))?;
            results.push(affected as u64);
        }

        tx.commit()
            .map_err(|e| format!("db_batch commit error: {}", e))?;
        Ok(results)
    })
}

#[tauri::command]
fn db_delete(
    state: tauri::State<'_, DatabaseManager>,
    project_id: String,
) -> Result<(), String> {
    log::info!("db_delete: {}", project_id);
    state.delete_project_db(&project_id)
}

#[tauri::command]
fn db_create_tables(
    state: tauri::State<'_, DatabaseManager>,
    project_id: String,
    schemas: Vec<TableSchema>,
) -> Result<(), String> {
    log::info!("db_create_tables: {} tables={}", project_id, schemas.len());
    state.with_connection(&project_id, |conn| {
        schema::create_tables(conn, &schemas)
    })
}

#[tauri::command]
fn sync_start<R: Runtime>(
    sync_manager: tauri::State<'_, SyncEngineManager>,
    app_handle: tauri::AppHandle<R>,
    project_id: String,
    config: serde_json::Value,
) -> Result<serde_json::Value, String> {
    log::info!("sync_start: {} config={}", project_id, config);
    let engine_config: SyncEngineConfig =
        serde_json::from_value(config).map_err(|e| format!("Invalid sync config: {}", e))?;
    let state = sync_manager.start(&project_id, engine_config, &app_handle)?;
    serde_json::to_value(&state).map_err(|e| format!("Failed to serialize sync state: {}", e))
}

#[tauri::command]
fn sync_stop<R: Runtime>(
    sync_manager: tauri::State<'_, SyncEngineManager>,
    app_handle: tauri::AppHandle<R>,
    project_id: String,
) -> Result<(), String> {
    log::info!("sync_stop: {}", project_id);
    sync_manager.stop(&project_id, &app_handle)
}

#[tauri::command]
fn sync_status(
    sync_manager: tauri::State<'_, SyncEngineManager>,
    project_id: String,
) -> Result<serde_json::Value, String> {
    log::info!("sync_status: {}", project_id);
    let state = sync_manager.status(&project_id)?;
    serde_json::to_value(&state).map_err(|e| format!("Failed to serialize sync state: {}", e))
}

// ---------------------------------------------------------------------------
// Helpers: JSON ↔ SQLite value conversion
// ---------------------------------------------------------------------------

fn json_value_to_sql(val: &serde_json::Value) -> Box<dyn rusqlite::types::ToSql> {
    match val {
        serde_json::Value::Null => Box::new(rusqlite::types::Null),
        serde_json::Value::Bool(b) => Box::new(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Box::new(i)
            } else if let Some(f) = n.as_f64() {
                Box::new(f)
            } else {
                Box::new(rusqlite::types::Null)
            }
        }
        serde_json::Value::String(s) => Box::new(s.clone()),
        // Arrays and objects are stored as JSON text
        _ => Box::new(val.to_string()),
    }
}

fn sqlite_value_to_json(val: rusqlite::types::Value) -> serde_json::Value {
    match val {
        rusqlite::types::Value::Null => serde_json::Value::Null,
        rusqlite::types::Value::Integer(i) => serde_json::json!(i),
        rusqlite::types::Value::Real(f) => serde_json::json!(f),
        rusqlite::types::Value::Text(s) => serde_json::Value::String(s),
        rusqlite::types::Value::Blob(b) => {
            serde_json::Value::String(base64_encode(&b))
        }
    }
}

fn base64_encode(data: &[u8]) -> String {
    use std::fmt::Write;
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        let _ = write!(result, "{}", CHARS[((triple >> 18) & 0x3F) as usize] as char);
        let _ = write!(result, "{}", CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            let _ = write!(result, "{}", CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            let _ = write!(result, "{}", CHARS[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}
