# tauri-plugin-offlite

离线优先的 SQLite 存储 + 实时同步引擎，Tauri 2 插件。

## 功能

- 每项目独立 SQLite 数据库（WAL 模式，并发读写）
- Schema 驱动建表（固定元数据列 + JSON data 列）
- `_status` 列变更追踪（synced/created/updated/deleted，无需 changelog 表）
- 统一数据模型支持（entityType 声明，标准实体自动路由到 sync schema）
- 混合实时同步：WebSocket 通知 + write-through 推送 + 轮询兜底
- 三级降级：实时（WebSocket）→ 轮询 → 离线，自动切换
- LWW（Last-Write-Wins）冲突解决
- MessagePack 编码通信（Content-Type: application/sjs）
- 指数退避重试（1s → 60s）

## 安装

```toml
# Cargo.toml
[dependencies]
tauri-plugin-offlite = { path = "../tauri-plugin-offlite" }
```

```rust
// src-tauri/src/lib.rs
fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_offlite::init())
        .run(tauri::generate_context!())
        .unwrap();
}
```

## 权限配置

```json
// src-tauri/capabilities/default.json
{
  "permissions": ["redb-cache:default", "offlite:default"]
}
```

## 前端 JS 绑定

```bash
yarn add tauri-plugin-offlite-api
```

```javascript
import {
  dbOpen, dbClose, dbDelete,
  dbQuery, dbExecute, dbBatch,
  dbCreateTables,
  syncStart, syncStop, syncStatus
} from 'tauri-plugin-offlite-api'
```

## Tauri 命令

### 数据库生命周期

| 命令 | 说明 |
|------|------|
| `db_open(projectId)` | 打开项目数据库（"global" 为全局库） |
| `db_close(projectId)` | 关闭项目数据库 |
| `db_delete(projectId)` | 删除项目数据库文件 |

### CRUD 操作

| 命令 | 说明 |
|------|------|
| `db_execute(projectId, sql, params)` | 执行写 SQL，返回受影响行数 |
| `db_query(projectId, sql, params)` | 执行读 SQL，返回 JSON 行数组 |
| `db_batch(projectId, statements)` | 事务内批量执行 SQL |
| `db_create_tables(projectId, schemas)` | Schema 驱动建表 |

### 同步引擎

| 命令 | 说明 |
|------|------|
| `sync_start(projectId, config)` | 启动同步（SSE + push/pull） |
| `sync_stop(projectId)` | 停止同步 |
| `sync_status(projectId)` | 获取同步状态 |

### 同步配置

```javascript
{
  base_url: "https://api.example.com",
  token: "jwt_token",
  sync_mode: "project",       // "user" | "company" | "project"
  app_name: "survey",         // 应用名前缀（服务端表名: survey_{table}）
  tables: [{ name: "planning" }],
  realtime: true,              // 启用 SSE 实时同步
  poll_interval: 30,           // 轮询间隔（秒）
  sse_heartbeat: 30            // SSE 心跳间隔（秒）
}
```

### 同步状态

```javascript
{
  active: true,
  paused: false,
  error: null,
  docs_read: 42,
  docs_written: 7,
  mode: "realtime",            // "realtime" | "polling" | "offline"
  sse_connected: true
}
```

通过 Tauri 事件 `sync-state-changed` 实时推送状态变更。

## 数据库结构

### 业务表（Schema 驱动）

```sql
_id         TEXT PRIMARY KEY,
uid         INTEGER,
companyId   INTEGER,
p_id        TEXT,
createdAt   TEXT NOT NULL,
updatedAt   TEXT NOT NULL,
_deleted    INTEGER DEFAULT 0,
_version    INTEGER DEFAULT 1,
_status     TEXT DEFAULT 'synced',  -- synced/created/updated/deleted
data        TEXT NOT NULL           -- 业务数据 JSON
```

> 注意：`_status` 列替代了原来的 `_change_log` 表，每次写操作从 2 次 IPC 减少到 1 次。

### 统一数据模型（服务端）

声明了 `entityType` 的标准实体，服务端会将数据写入 `sync` schema 的标准实体表（独立 PostgreSQL 列）。
详见 [UNIFIED_DATA_MODEL.md](./UNIFIED_DATA_MODEL.md)。

### 全局数据库（global.db）

- `project_meta` — 项目元数据
- `_migration_history` — 版本迁移记录
- `_sync_checkpoint` — 同步断点

## 文件布局

```
{app_data_dir}/
├── global.db
├── projects/
│   ├── {project_id}.db
│   └── ...
└── cache.redb
```

## 同步引擎

同步逻辑在 `guest-js/src/sync.js` 中实现（JS SDK 层），不在 Rust 侧。
与 WatermelonDB、PowerSync 等主流方案一致：Rust 只管存储，JS 管同步。

```javascript
import { createSyncEngine } from 'tauri-plugin-offlite-api'

const engine = createSyncEngine({
  baseUrl: 'https://api.example.com',
  token: 'jwt_token',
  appName: 'survey',
})

engine.start('project_001', ['planning', 'sample'])

// 或传入 entityType 配置（统一数据模型）
engine.start('project_001', [
  { name: 'planning_feature', entityType: 'sub_compartment' },
  { name: 'samples', entityType: 'sample_plot' },
  { name: 'dbh_actual', entityType: 'survey' },
  'photos',  // 不传 entityType，走 offlite 旧路径
])

engine.pushChanges('planning_feature')  // write-through 即时推送
engine.stop()
```

## 开发

```bash
cargo test          # 运行 96 个单元测试
cargo check         # 编译检查
```

## 许可证

MIT
