# tauri-plugin-offlite

离线优先的 SQLite 存储 + 实时同步引擎，Tauri 2 插件。

## 功能

- 每项目独立 SQLite 数据库（WAL 模式，并发读写）
- Schema 驱动建表（固定元数据列 + JSON data 列）
- `_status` 列变更追踪（synced/created/updated/deleted）
- Rust 侧保留 `_change_log` 表（向后兼容），JS 同步引擎仅依赖 `_status` 列
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

### 底层 API（直接调用 Tauri 命令）

```javascript
import {
  dbOpen, dbClose, dbDelete,
  dbQuery, dbExecute, dbBatch,
  dbCreateTables,
  syncStart, syncStop, syncStatus
} from 'tauri-plugin-offlite-api'
```

### 高层 API（推荐，开箱即用）

```javascript
import {
  createDB, defineSchema, createSyncManager,
  createSyncEngine, createChildSync, generateId,
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
| `sync_start(projectId, config)` | 启动同步（WebSocket + push/pull） |
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
  realtime: true,              // 启用 WebSocket 实时同步
  poll_interval: 30,           // 轮询间隔（秒）
}
```

### 同步状态

Rust 侧（`sync_status` 命令返回）：

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

JS SDK 侧（`engine.getState()` 返回）：

```javascript
{
  active: true,
  mode: "realtime",            // "realtime" | "polling" | "offline"
  sse_connected: true,
  syncing: false,
  error: null,
  docs_pushed: 7,
  docs_pulled: 42,
  last_synced_at: "2025-01-01T00:00:00.000Z"
}
```

通过 Tauri 事件 `sync-state-changed` 实时推送状态变更。

## 数据库结构

### 主键生成（_id）

`_id` 由客户端本地生成，不依赖服务端。离线时也能正常创建记录。

**格式**：`{base36_timestamp}_{random4}`，共 12 字符
**示例**：`kz7f8g0_a3x1`

- 前 7 位：`Date.now()` 的 Base36 编码（毫秒精度，天然可排序）
- 下划线分隔
- 后 4 位：随机 Base36 字符（防碰撞，同一毫秒内碰撞概率 1/1,679,616）

```javascript
import { generateId } from 'tauri-plugin-offlite-api/idgen'
const id = generateId()  // 'kz7f8g0_a3x1'
```

如果业务需要自定义 ID（如 `report_${projectId}_${timestamp}`），可以在 `db.add()` 时传入 `_id` 字段覆盖自动生成。

### 业务表（Schema 驱动）

```sql
_id         TEXT PRIMARY KEY,
uid         INTEGER,
company_id  INTEGER,
project_id  TEXT,
created_at  TEXT NOT NULL,
updated_at  TEXT NOT NULL,
_deleted    INTEGER DEFAULT 0,
_version    INTEGER DEFAULT 1,
_status     TEXT DEFAULT 'synced',  -- synced/created/updated/deleted
data        TEXT NOT NULL           -- 业务数据 JSON（全 snake_case 键名）
```

> **变更追踪机制**：JS 同步引擎仅依赖 `_status` 列追踪变更（每次写操作 1 次 IPC）。
> Rust 侧仍保留 `_change_log` 表的创建（向后兼容），但 JS SDK 不再写入该表。

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
  syncMode: 'project',
  onTokenRefresh: async () => { /* 返回新 token */ },
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
await engine.synchronize()              // 手动触发完整 pull-then-push
await engine.hasUnsyncedChanges()       // 检查未同步变更
engine.stop()
```

### 实时通道

同步引擎使用 **WebSocket** 作为实时通道（非 SSE）：
- 连接建立后通过 JSON 消息完成认证（`{ type: 'auth', token }`）
- 数据通知使用 MessagePack 二进制帧
- 支持 200ms 防抖合并同一表的多个通知
- 断线后指数退避重连，3 次失败降级为轮询

## 开发

```bash
cargo test          # 运行 96 个单元测试
cargo check         # 编译检查
```

## JS SDK 模块一览

| 模块 | 导入路径 | 说明 |
|------|---------|------|
| `createDB` | `tauri-plugin-offlite-api/db` | 通用 CRUD 封装（add/get/update/remove/query/bulk） |
| `defineSchema` | `tauri-plugin-offlite-api/schema` | Schema 验证 + 模型定义 |
| `createSyncManager` | `tauri-plugin-offlite-api/syncManager` | 多表同步生命周期管理 |
| `createSyncEngine` | `tauri-plugin-offlite-api/sync` | 单引擎 pull/push/WebSocket |
| `createChildSync` | `tauri-plugin-offlite-api/childSync` | 父子表关联同步 |
| `generateId` | `tauri-plugin-offlite-api/idgen` | 12 字符短 ID 生成 |

## 新 App 快速接入

```javascript
import { createDB, defineSchema, createSyncManager } from 'tauri-plugin-offlite-api'

// 1. 定义 Schema
const projectSchema = defineSchema('project', {
  name: { type: 'string', required: true },
  area: { type: 'number', default: 0 },
  status: { type: 'number', default: 0 },
}, { syncMode: 'user', entityType: 'project' })

// 2. 创建同步管理器
const manager = createSyncManager({
  baseUrl: 'https://api.example.com',
  appName: 'my-app',
  getToken: () => localStorage.getItem('token'),
  onTokenRefresh: async () => { /* 刷新并返回新 token */ },
})
manager.register(projectSchema)

// 3. 创建 DB 实例
const projectDB = createDB('project', {
  ...projectSchema,
  uid: currentUser.id,
  company_id: currentUser.companyId,
  getProjectId: () => 'global',
  getSyncEngine: () => manager.getEngine('project'),
})

// 4. CRUD 操作
await projectDB.add({ name: '新项目', area: 1000 })
const { data } = await projectDB.query({ status: 0 })
await projectDB.update(id, { status: 1 })

// 5. 启动同步
await manager.startGlobal({ uid: 1, company_id: 100 })
await manager.startProject('project_001')
```

## 许可证

MIT
