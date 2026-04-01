const COMMANDS: &[&str] = &[
    "db_open",
    "db_close",
    "db_execute",
    "db_query",
    "db_batch",
    "db_delete",
    "sync_start",
    "sync_stop",
    "sync_status",
];

fn main() {
    tauri_plugin::Builder::new(COMMANDS).build();
}
