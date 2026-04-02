use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;

use super::sse::{SseConfig, SseHandle};

// ---------------------------------------------------------------------------
// Sync mode & state
// ---------------------------------------------------------------------------

/// Three-level degradation mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SyncMode {
    /// SSE connected + write-through push.
    Realtime,
    /// SSE failed 3 times → poll every N seconds + batch push.
    Polling,
    /// Network unreachable → queue changes locally.
    Offline,
}

impl std::fmt::Display for SyncMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncMode::Realtime => write!(f, "realtime"),
            SyncMode::Polling => write!(f, "polling"),
            SyncMode::Offline => write!(f, "offline"),
        }
    }
}

/// Observable sync state emitted to the front-end via Tauri events.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SyncState {
    pub active: bool,
    pub paused: bool,
    pub error: Option<String>,
    pub docs_read: u64,
    pub docs_written: u64,
    pub mode: SyncMode,
    pub sse_connected: bool,
}

impl Default for SyncState {
    fn default() -> Self {
        Self {
            active: false,
            paused: false,
            error: None,
            docs_read: 0,
            docs_written: 0,
            mode: SyncMode::Offline,
            sse_connected: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Per-table sync configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSyncConfig {
    pub name: String,
    #[serde(default)]
    pub json_indexes: Option<Vec<String>>,
}

/// Configuration passed from the front-end to `sync_start`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncEngineConfig {
    pub base_url: String,
    pub token: String,
    /// `"user"` | `"company"` | `"project"`
    pub sync_mode: String,
    pub tables: Vec<TableSyncConfig>,
    /// Whether to enable realtime SSE sync (default `true`).
    #[serde(default = "default_true")]
    pub realtime: bool,
    /// Polling fallback interval in seconds (default 30).
    #[serde(default = "default_poll_interval")]
    pub poll_interval: u64,
    /// SSE heartbeat interval in seconds (default 30).
    #[serde(default = "default_sse_heartbeat")]
    pub sse_heartbeat: u64,
}

fn default_true() -> bool {
    true
}
fn default_poll_interval() -> u64 {
    30
}
fn default_sse_heartbeat() -> u64 {
    30
}

// ---------------------------------------------------------------------------
// Exponential backoff helper
// ---------------------------------------------------------------------------

/// Compute the next backoff delay using exponential backoff with a cap.
///
/// `attempt` is 0-based. Formula: min(initial * factor^attempt, max_delay).
pub fn exponential_backoff(attempt: u32, initial_secs: u64, max_secs: u64, factor: u64) -> u64 {
    let delay = initial_secs.saturating_mul(factor.saturating_pow(attempt));
    delay.min(max_secs)
}

// ---------------------------------------------------------------------------
// Per-project sync engine
// ---------------------------------------------------------------------------

/// Manages the sync lifecycle for a single project.
pub struct SyncEngine {
    pub config: SyncEngineConfig,
    pub state: SyncState,
    /// Number of consecutive SSE connection failures.
    pub sse_fail_count: u32,
    /// Current retry attempt (for exponential backoff).
    pub retry_attempt: u32,
    /// Handle to the running SSE listener (if any).
    pub sse_handle: Option<SseHandle>,
    /// Shutdown signal for background tasks.
    pub shutdown: Option<tokio::sync::watch::Sender<bool>>,
}

impl SyncEngine {
    /// Create a new engine with the given config, initially in offline mode.
    pub fn new(config: SyncEngineConfig) -> Self {
        Self {
            config,
            state: SyncState {
                active: true,
                ..Default::default()
            },
            sse_fail_count: 0,
            retry_attempt: 0,
            sse_handle: None,
            shutdown: None,
        }
    }

    /// Determine the target mode based on SSE failure count and config.
    pub fn target_mode(&self) -> SyncMode {
        if !self.config.realtime {
            return SyncMode::Polling;
        }
        if self.sse_fail_count >= 3 {
            SyncMode::Polling
        } else {
            SyncMode::Realtime
        }
    }

    /// Record an SSE failure and potentially degrade the mode.
    /// Returns the new mode after degradation logic.
    pub fn record_sse_failure(&mut self) -> SyncMode {
        self.sse_fail_count += 1;
        self.state.sse_connected = false;

        if self.sse_fail_count >= 3 {
            self.state.mode = SyncMode::Polling;
        }
        self.state.mode
    }

    /// Record a successful SSE connection.
    pub fn record_sse_success(&mut self) {
        self.sse_fail_count = 0;
        self.retry_attempt = 0;
        self.state.sse_connected = true;
        self.state.mode = SyncMode::Realtime;
        self.state.error = None;
    }

    /// Switch to offline mode (network unreachable).
    pub fn go_offline(&mut self) {
        self.state.mode = SyncMode::Offline;
        self.state.sse_connected = false;
    }

    /// Get the next retry delay in seconds using exponential backoff.
    /// Initial: 1s, max: 60s, factor: 2.
    pub fn next_retry_delay(&mut self) -> u64 {
        let delay = exponential_backoff(self.retry_attempt, 1, 60, 2);
        self.retry_attempt = self.retry_attempt.saturating_add(1);
        delay
    }

    /// Reset retry counter (e.g. after a successful connection).
    pub fn reset_retry(&mut self) {
        self.retry_attempt = 0;
    }

    /// Build an `SseConfig` from the engine's configuration.
    pub fn build_sse_config(&self, filter_key: &str) -> SseConfig {
        SseConfig {
            base_url: self.config.base_url.clone(),
            token: self.config.token.clone(),
            mode: self.config.sync_mode.clone(),
            filter_key: filter_key.to_string(),
            heartbeat_timeout_secs: self.config.sse_heartbeat.saturating_mul(3),
        }
    }

    /// Stop the engine: shut down SSE handle and background tasks.
    pub fn stop(&mut self) {
        // Signal shutdown to background tasks
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(true);
        }
        // Stop SSE listener
        if let Some(ref handle) = self.sse_handle {
            super::sse::stop_sse_listener(handle);
        }
        self.sse_handle = None;
        self.state.active = false;
        self.state.sse_connected = false;
    }

    /// Return a snapshot of the current state.
    pub fn status(&self) -> SyncState {
        self.state.clone()
    }
}

// ---------------------------------------------------------------------------
// Manager: multi-project sync engine registry
// ---------------------------------------------------------------------------

/// Manages `SyncEngine` instances for multiple projects.
/// Stored as Tauri managed state.
pub struct SyncEngineManager {
    engines: Mutex<HashMap<String, SyncEngine>>,
}

impl SyncEngineManager {
    pub fn new() -> Self {
        Self {
            engines: Mutex::new(HashMap::new()),
        }
    }

    /// Start sync for a project. Creates a new `SyncEngine` and stores it.
    ///
    /// The actual SSE/polling background loop is not spawned here — that will
    /// be wired during integration. This method sets up the engine state and
    /// emits the initial `sync-state-changed` event.
    pub fn start<R: tauri::Runtime>(
        &self,
        project_id: &str,
        config: SyncEngineConfig,
        app_handle: &tauri::AppHandle<R>,
    ) -> Result<SyncState, String> {
        let mut engines = self
            .engines
            .lock()
            .map_err(|e| format!("Lock poisoned: {}", e))?;

        // Stop existing engine for this project if any
        if let Some(mut existing) = engines.remove(project_id) {
            existing.stop();
        }

        let engine = SyncEngine::new(config);
        let initial_state = engine.status();

        // Emit initial state event
        emit_sync_state(app_handle, project_id, &initial_state);

        engines.insert(project_id.to_string(), engine);
        Ok(initial_state)
    }

    /// Stop sync for a project.
    pub fn stop<R: tauri::Runtime>(
        &self,
        project_id: &str,
        app_handle: &tauri::AppHandle<R>,
    ) -> Result<(), String> {
        let mut engines = self
            .engines
            .lock()
            .map_err(|e| format!("Lock poisoned: {}", e))?;

        if let Some(mut engine) = engines.remove(project_id) {
            engine.stop();
            emit_sync_state(app_handle, project_id, &engine.state);
        }
        Ok(())
    }

    /// Get the current sync status for a project.
    pub fn status(&self, project_id: &str) -> Result<SyncState, String> {
        let engines = self
            .engines
            .lock()
            .map_err(|e| format!("Lock poisoned: {}", e))?;

        match engines.get(project_id) {
            Some(engine) => Ok(engine.status()),
            None => Ok(SyncState::default()),
        }
    }

    /// Access the engine for a project within a closure (for advanced operations).
    pub fn with_engine<F, T>(&self, project_id: &str, f: F) -> Result<T, String>
    where
        F: FnOnce(&mut SyncEngine) -> Result<T, String>,
    {
        let mut engines = self
            .engines
            .lock()
            .map_err(|e| format!("Lock poisoned: {}", e))?;

        match engines.get_mut(project_id) {
            Some(engine) => f(engine),
            None => Err(format!("No sync engine for project '{}'", project_id)),
        }
    }
}

// ---------------------------------------------------------------------------
// Tauri event emission
// ---------------------------------------------------------------------------

/// Emit a `sync-state-changed` event to the front-end.
pub fn emit_sync_state<R: tauri::Runtime>(
    app_handle: &tauri::AppHandle<R>,
    project_id: &str,
    state: &SyncState,
) {
    use tauri::Emitter;
    #[derive(Clone, Serialize)]
    struct SyncStatePayload {
        project_id: String,
        #[serde(flatten)]
        state: SyncState,
    }

    let payload = SyncStatePayload {
        project_id: project_id.to_string(),
        state: state.clone(),
    };

    if let Err(e) = app_handle.emit("sync-state-changed", payload) {
        log::warn!("Failed to emit sync-state-changed event: {}", e);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- SyncMode serialization --

    #[test]
    fn test_sync_mode_serialize() {
        assert_eq!(
            serde_json::to_string(&SyncMode::Realtime).unwrap(),
            r#""realtime""#
        );
        assert_eq!(
            serde_json::to_string(&SyncMode::Polling).unwrap(),
            r#""polling""#
        );
        assert_eq!(
            serde_json::to_string(&SyncMode::Offline).unwrap(),
            r#""offline""#
        );
    }

    #[test]
    fn test_sync_mode_deserialize() {
        assert_eq!(
            serde_json::from_str::<SyncMode>(r#""realtime""#).unwrap(),
            SyncMode::Realtime
        );
        assert_eq!(
            serde_json::from_str::<SyncMode>(r#""polling""#).unwrap(),
            SyncMode::Polling
        );
        assert_eq!(
            serde_json::from_str::<SyncMode>(r#""offline""#).unwrap(),
            SyncMode::Offline
        );
    }

    #[test]
    fn test_sync_mode_display() {
        assert_eq!(SyncMode::Realtime.to_string(), "realtime");
        assert_eq!(SyncMode::Polling.to_string(), "polling");
        assert_eq!(SyncMode::Offline.to_string(), "offline");
    }

    // -- SyncState serialization --

    #[test]
    fn test_sync_state_default() {
        let state = SyncState::default();
        assert!(!state.active);
        assert!(!state.paused);
        assert!(state.error.is_none());
        assert_eq!(state.docs_read, 0);
        assert_eq!(state.docs_written, 0);
        assert_eq!(state.mode, SyncMode::Offline);
        assert!(!state.sse_connected);
    }

    #[test]
    fn test_sync_state_json_roundtrip() {
        let state = SyncState {
            active: true,
            paused: false,
            error: Some("network timeout".to_string()),
            docs_read: 42,
            docs_written: 7,
            mode: SyncMode::Polling,
            sse_connected: false,
        };

        let json = serde_json::to_string(&state).unwrap();
        let decoded: SyncState = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, state);
    }

    #[test]
    fn test_sync_state_json_structure() {
        let state = SyncState {
            active: true,
            paused: false,
            error: None,
            docs_read: 10,
            docs_written: 5,
            mode: SyncMode::Realtime,
            sse_connected: true,
        };

        let val: serde_json::Value = serde_json::to_value(&state).unwrap();
        assert_eq!(val["active"], true);
        assert_eq!(val["paused"], false);
        assert_eq!(val["error"], serde_json::Value::Null);
        assert_eq!(val["docs_read"], 10);
        assert_eq!(val["docs_written"], 5);
        assert_eq!(val["mode"], "realtime");
        assert_eq!(val["sse_connected"], true);
    }

    // -- SyncEngineConfig deserialization --

    #[test]
    fn test_config_deserialize_full() {
        let json = r#"{
            "base_url": "https://api.example.com",
            "token": "jwt_token_here",
            "sync_mode": "project",
            "tables": [
                { "name": "planning" },
                { "name": "sample", "json_indexes": ["properties.地类"] }
            ],
            "realtime": true,
            "poll_interval": 60,
            "sse_heartbeat": 45
        }"#;

        let config: SyncEngineConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.base_url, "https://api.example.com");
        assert_eq!(config.token, "jwt_token_here");
        assert_eq!(config.sync_mode, "project");
        assert_eq!(config.tables.len(), 2);
        assert_eq!(config.tables[0].name, "planning");
        assert_eq!(config.tables[1].name, "sample");
        assert!(config.realtime);
        assert_eq!(config.poll_interval, 60);
        assert_eq!(config.sse_heartbeat, 45);
    }

    #[test]
    fn test_config_deserialize_defaults() {
        let json = r#"{
            "base_url": "https://api.example.com",
            "token": "tok",
            "sync_mode": "user",
            "tables": []
        }"#;

        let config: SyncEngineConfig = serde_json::from_str(json).unwrap();
        assert!(config.realtime);
        assert_eq!(config.poll_interval, 30);
        assert_eq!(config.sse_heartbeat, 30);
    }

    // -- SyncMode transitions --

    #[test]
    fn test_mode_transition_sse_failures() {
        let config = make_test_config();
        let mut engine = SyncEngine::new(config);

        // Initially active, offline mode (not yet connected)
        assert_eq!(engine.state.mode, SyncMode::Offline);
        assert_eq!(engine.sse_fail_count, 0);

        // Simulate SSE success → realtime
        engine.record_sse_success();
        assert_eq!(engine.state.mode, SyncMode::Realtime);
        assert!(engine.state.sse_connected);
        assert_eq!(engine.sse_fail_count, 0);

        // Simulate 1st SSE failure → still realtime target
        let mode = engine.record_sse_failure();
        assert_eq!(engine.sse_fail_count, 1);
        assert!(!engine.state.sse_connected);
        // Mode stays as-is after 1 failure (not yet degraded to polling)
        assert_eq!(mode, SyncMode::Realtime);

        // 2nd failure
        let mode = engine.record_sse_failure();
        assert_eq!(engine.sse_fail_count, 2);
        assert_eq!(mode, SyncMode::Realtime);

        // 3rd failure → degrade to polling
        let mode = engine.record_sse_failure();
        assert_eq!(engine.sse_fail_count, 3);
        assert_eq!(mode, SyncMode::Polling);
        assert_eq!(engine.state.mode, SyncMode::Polling);
    }

    #[test]
    fn test_mode_transition_offline_and_recovery() {
        let config = make_test_config();
        let mut engine = SyncEngine::new(config);

        engine.record_sse_success();
        assert_eq!(engine.state.mode, SyncMode::Realtime);

        // Network goes down
        engine.go_offline();
        assert_eq!(engine.state.mode, SyncMode::Offline);
        assert!(!engine.state.sse_connected);

        // Network recovers, SSE reconnects
        engine.record_sse_success();
        assert_eq!(engine.state.mode, SyncMode::Realtime);
        assert!(engine.state.sse_connected);
        assert_eq!(engine.sse_fail_count, 0);
    }

    #[test]
    fn test_target_mode_realtime_disabled() {
        let mut config = make_test_config();
        config.realtime = false;
        let engine = SyncEngine::new(config);

        // When realtime is disabled, target mode is always polling
        assert_eq!(engine.target_mode(), SyncMode::Polling);
    }

    #[test]
    fn test_target_mode_with_failures() {
        let config = make_test_config();
        let mut engine = SyncEngine::new(config);

        assert_eq!(engine.target_mode(), SyncMode::Realtime);

        engine.sse_fail_count = 2;
        assert_eq!(engine.target_mode(), SyncMode::Realtime);

        engine.sse_fail_count = 3;
        assert_eq!(engine.target_mode(), SyncMode::Polling);
    }

    // -- Exponential backoff --

    #[test]
    fn test_exponential_backoff_values() {
        // initial=1s, max=60s, factor=2
        assert_eq!(exponential_backoff(0, 1, 60, 2), 1);  // 1 * 2^0 = 1
        assert_eq!(exponential_backoff(1, 1, 60, 2), 2);  // 1 * 2^1 = 2
        assert_eq!(exponential_backoff(2, 1, 60, 2), 4);  // 1 * 2^2 = 4
        assert_eq!(exponential_backoff(3, 1, 60, 2), 8);  // 1 * 2^3 = 8
        assert_eq!(exponential_backoff(4, 1, 60, 2), 16); // 1 * 2^4 = 16
        assert_eq!(exponential_backoff(5, 1, 60, 2), 32); // 1 * 2^5 = 32
        assert_eq!(exponential_backoff(6, 1, 60, 2), 60); // 1 * 2^6 = 64 → capped at 60
        assert_eq!(exponential_backoff(10, 1, 60, 2), 60); // capped
    }

    #[test]
    fn test_next_retry_delay_increments() {
        let config = make_test_config();
        let mut engine = SyncEngine::new(config);

        assert_eq!(engine.next_retry_delay(), 1);
        assert_eq!(engine.next_retry_delay(), 2);
        assert_eq!(engine.next_retry_delay(), 4);
        assert_eq!(engine.next_retry_delay(), 8);
        assert_eq!(engine.next_retry_delay(), 16);
        assert_eq!(engine.next_retry_delay(), 32);
        assert_eq!(engine.next_retry_delay(), 60); // capped
        assert_eq!(engine.next_retry_delay(), 60); // stays capped
    }

    #[test]
    fn test_reset_retry() {
        let config = make_test_config();
        let mut engine = SyncEngine::new(config);

        engine.next_retry_delay();
        engine.next_retry_delay();
        engine.next_retry_delay();
        assert_eq!(engine.retry_attempt, 3);

        engine.reset_retry();
        assert_eq!(engine.retry_attempt, 0);
        assert_eq!(engine.next_retry_delay(), 1);
    }

    // -- Engine lifecycle --

    #[test]
    fn test_engine_new_initial_state() {
        let config = make_test_config();
        let engine = SyncEngine::new(config);

        assert!(engine.state.active);
        assert!(!engine.state.paused);
        assert!(engine.state.error.is_none());
        assert_eq!(engine.state.docs_read, 0);
        assert_eq!(engine.state.docs_written, 0);
        assert_eq!(engine.state.mode, SyncMode::Offline);
        assert!(!engine.state.sse_connected);
        assert_eq!(engine.sse_fail_count, 0);
        assert_eq!(engine.retry_attempt, 0);
    }

    #[test]
    fn test_engine_stop() {
        let config = make_test_config();
        let mut engine = SyncEngine::new(config);
        engine.record_sse_success();
        assert!(engine.state.active);
        assert!(engine.state.sse_connected);

        engine.stop();
        assert!(!engine.state.active);
        assert!(!engine.state.sse_connected);
    }

    #[test]
    fn test_build_sse_config() {
        let config = make_test_config();
        let engine = SyncEngine::new(config);

        let sse_config = engine.build_sse_config("p_001");
        assert_eq!(sse_config.base_url, "https://api.example.com");
        assert_eq!(sse_config.token, "test_token");
        assert_eq!(sse_config.mode, "project");
        assert_eq!(sse_config.filter_key, "p_001");
        // heartbeat_timeout = sse_heartbeat * 3 = 30 * 3 = 90
        assert_eq!(sse_config.heartbeat_timeout_secs, 90);
    }

    // -- SyncEngineManager --

    #[test]
    fn test_manager_status_no_engine() {
        let manager = SyncEngineManager::new();
        let status = manager.status("nonexistent").unwrap();
        assert_eq!(status, SyncState::default());
    }

    #[test]
    fn test_manager_with_engine_not_found() {
        let manager = SyncEngineManager::new();
        let result = manager.with_engine("nope", |_| Ok(()));
        assert!(result.is_err());
    }

    // -- Helper --

    fn make_test_config() -> SyncEngineConfig {
        SyncEngineConfig {
            base_url: "https://api.example.com".to_string(),
            token: "test_token".to_string(),
            sync_mode: "project".to_string(),
            tables: vec![TableSyncConfig {
                name: "planning".to_string(),
                json_indexes: None,
            }],
            realtime: true,
            poll_interval: 30,
            sse_heartbeat: 30,
        }
    }
}
