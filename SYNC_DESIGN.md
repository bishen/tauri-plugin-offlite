# Offlite 同步引擎设计

## 主流方案对比分析

| 特性 | WatermelonDB | RxDB | PowerSync | Supabase Realtime | **Offlite** |
|------|-------------|------|-----------|-------------------|-------------|
| 同步触发 | 手动 `synchronize()` | 自动（RxJS） | 自动（后台流） | 自动（WebSocket） | **手动 + 自动混合** |
| 推送方式 | 批量 push（整库变更集） | 逐文档 push + checkpoint | 写入即推（CRUD proxy） | 直接写服务端 | **write-through + 批量兜底** |
| 拉取方式 | 批量 pull（since timestamp） | pull handler + stream | SSE/WebSocket 流 | WebSocket CDC | **WebSocket 通知触发 pull + pull-then-push 兜底** |
| 变更追踪 | `_status` 列 | RxDB 内部 checkpoint | 服务端 oplog | PostgreSQL WAL | **`_status` 列（学习 WatermelonDB）+ Rust 侧保留 `_change_log` 表** |
| 冲突解决 | 服务端决定 | 自定义 handler | 服务端 LWW | 服务端 LWW | **服务端 LWW + 自动回写本地** |
| 离线支持 | ✅ 本地写 → 上线 push | ✅ 本地写 → 上线 push | ✅ 本地写 → 上线 push | ❌ 无离线 | **✅ 本地优先，_status 保留** |
| 数据完整性 | pull-then-push 顺序 | checkpoint 保证 | oplog 序列号 | WAL 序列号 | **pull-then-push + 不覆盖未推送** |
| 存储引擎 | SQLite（React Native） | IndexedDB / SQLite | SQLite | PostgreSQL | **SQLite WAL（Tauri Rust 插件）** |
| 通信协议 | JSON | JSON / GraphQL | HTTP + SSE | WebSocket | **MessagePack（体积小 30-50%）** |
| 平台 | React Native | 浏览器 / Node | React Native / Flutter / Web | Web | **Tauri（桌面 + Android）** |
| 服务端要求 | 自建 REST API | 自建 / CouchDB / Supabase | PowerSync Cloud / 自建 | Supabase Cloud | **自建 Fastify + PostgreSQL** |
| 多应用隔离 | ❌ 单应用 | ❌ 单应用 | ✅ Bucket 机制 | ❌ 单项目 | **✅ appName 前缀动态建表** |
| 每次写 IPC 开销 | 1 次（_status 列） | 1 次 | 0 次（proxy） | 0 次（直连） | **1 次（_status 列，JS 不写 changelog）** |

## 核心洞察

### WatermelonDB 的精髓：pull-then-push
- 先拉后推，避免推送时覆盖服务端新数据
- 整库级别的 `synchronize()` 调用，不是逐表
- 变更追踪用 `_status` 列（synced/created/updated/deleted），不用单独的 changelog 表
- 推送后服务端返回确认，客户端标记为 synced

### RxDB 的精髓：checkpoint + stream 双通道
- Pull 用 checkpoint（类似游标），不是 timestamp
- Stream 用 EventSource/WebSocket 接收实时变更
- 两个通道互补：stream 保证实时性，checkpoint pull 保证完整性
- 冲突解决在 push handler 里，支持自定义策略

### PowerSync 的精髓：oplog + bucket
- 服务端维护 oplog（操作日志），每条有递增序列号
- 客户端记录 last_op_id，拉取时只要 > last_op_id 的操作
- 比 timestamp 更可靠（不受时钟偏移影响）
- Bucket 机制实现数据分区（类似我们的 syncMode）

### Supabase 的精髓：PostgreSQL WAL CDC
- 利用 PostgreSQL 原生 WAL（Write-Ahead Log）捕获变更
- 零侵入：不需要在业务表加额外列
- 但不支持离线，不适合我们的场景

## Offlite 最佳方案

综合以上分析，Offlite 采用以下设计：

### 1. 变更追踪：内置 `_status` 列（学习 WatermelonDB）

**JS 同步引擎不再使用 `_change_log` 表**，改为在每个业务表中依赖 `_status` 列：

```sql
_status TEXT DEFAULT 'synced'  -- 'synced' | 'created' | 'updated' | 'deleted'
```

> **注意**：Rust 侧 `schema.rs` 仍会创建 `_change_log` 表（向后兼容），`changelog.rs` 模块保留完整的
> CRUD 函数。但 JS SDK 的同步引擎（`sync.js`）仅通过 `_status` 列追踪变更，不写入 `_change_log`。

优势：
- JS 侧每次写操作只需 1 次 IPC（不需要额外写 changelog）
- 查询未同步变更直接 `WHERE _status != 'synced'`，无需 JOIN
- 与 WatermelonDB 一致，经过大规模验证

### 2. 同步流程：pull-then-push（学习 WatermelonDB）

```
synchronize() {
  1. PULL: 从服务端拉取 since last_pulled_at 的所有变更
     → 应用到本地（INSERT/UPDATE/DELETE）
     → 冲突时服务端版本优先（LWW）
  
  2. PUSH: 收集本地所有 _status != 'synced' 的记录
     → 批量推送到服务端
     → 服务端确认后标记 _status = 'synced'
  
  3. 更新 last_pulled_at
}
```

### 3. 实时通道：WebSocket 通知（替代 SSE）

在 pull-then-push 基础上，增加 WebSocket 实时通知：

```
启动同步:
  1. 执行一次完整的 synchronize()（pull + push）
  2. 建立 WebSocket 连接，通过 JSON 消息认证（{ type: 'auth', token }）
  3. WebSocket 收到二进制通知（MessagePack）→ 防抖 200ms → pull 对应表
  4. 本地写操作 → 立即 push（write-through）
  5. WebSocket 断开 → 指数退避重连，3 次失败降级为定时 synchronize()
```

> **为什么选择 WebSocket 而非 SSE**：
> - Android WebView 对 EventSource 支持不稳定
> - WebSocket 支持双向通信（可用于编辑锁等场景）
> - 认证信息不暴露在 URL 中（通过连接后发送 auth 消息）

### 4. Checkpoint：时间戳（当前）+ 序列号（预留）

当前实现仅使用时间戳：

```javascript
// 当前实现（_sync_checkpoint 表）
checkpoint = {
  last_sync_at: '2025-01-01T00:00:00Z',  // 服务端返回的最后一条变更时间戳
}
```

未来如果服务端支持 oplog，可扩展为混合模式：

```javascript
// 未来扩展（预留）
checkpoint = {
  last_pulled_at: '2025-01-01T00:00:00Z',  // 时间戳（兼容性好）
  last_op_id: 12345,                         // 序列号（精确性好）
}
```

拉取时优先用 `last_op_id`（如果服务端支持），回退到 `last_pulled_at`。

### 5. 数据完整性保证

- **本地数据不丢失**：所有写操作先写本地 SQLite，再异步推送
- **推送失败不丢失**：`_status` 保持 created/updated/deleted，下次 sync 重试
- **拉取幂等**：INSERT OR REPLACE，重复拉取不会产生重复数据
- **顺序保证**：先 pull 后 push，避免覆盖服务端新数据
- **离线累积**：离线期间所有写操作标记 _status，上线后批量推送

## 新的表结构

```sql
-- 业务表（_status 列追踪变更，_change_log 表由 Rust 侧保留但 JS SDK 不使用）
CREATE TABLE {table_name} (
    _id         TEXT PRIMARY KEY,
    uid         INTEGER,
    company_id  INTEGER,
    project_id  TEXT,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    _deleted    INTEGER DEFAULT 0,
    _version    INTEGER DEFAULT 1,
    _status     TEXT DEFAULT 'synced',  -- synced/created/updated/deleted
    data        TEXT NOT NULL
);
CREATE INDEX idx_{table}_status ON {table}(_status);
```

## 新的 JS SDK API

```javascript
import { createSyncEngine } from 'tauri-plugin-offlite-api/sync'

const engine = createSyncEngine({
  baseUrl: 'https://api.example.com',
  token: 'jwt_token',
  appName: 'survey',
  syncMode: 'project',
  onTokenRefresh: async () => { /* 返回新 token */ },
})

// 启动同步（执行初始 sync + 建立 WebSocket + 定时 sync 兜底）
engine.start('project_001', ['planning', 'sample', 'dbh_actual'])

// 手动触发一次完整同步
await engine.synchronize()

// 检查是否有未同步的变更
const hasChanges = await engine.hasUnsyncedChanges()

// 写入后即时推送（write-through）
engine.pushChanges('planning')

// 更新 token（401 自动刷新后也会内部调用）
engine.updateToken('new_jwt_token')

// 重置 checkpoint（强制下次全量拉取）
await engine.resetCheckpoints()

// 监听状态变更
const unlisten = engine.onStateChange((state) => {
  console.log(state.mode, state.docs_pushed, state.docs_pulled)
})

// 停止同步
engine.stop()
```

## 性能优化

1. **批量推送**：收集所有未同步记录一次性推送，不是逐条（每批最多 50 条）
2. **`_status` 列替代 changelog 写入**：JS 侧每次写操作只需 1 次 IPC
3. **WebSocket 增量通知**：实时变更通过 WebSocket 推送，收到通知后按需 pull
4. **分表推送**：每个表独立推送，大表不阻塞小表
5. **压缩传输**：MessagePack 编码，比 JSON 小 30-50%
6. **防抖合并**：200ms 窗口内同一表的多个 WebSocket 通知合并为一次 pull
7. **push 兜底定时器**：realtime 模式下每 60s 兜底推送未同步记录


## 应用层协同模式：小班区划多端实时协同

基于 Offlite 同步引擎，应用层实现了小班区划（planning_feature）的多端多人实时协同编辑。

### 增量保存模式

每次 mapbox-gl-draw 的绘制事件（`draw.create`/`draw.update`/`draw.delete`）触发增量保存：

```
draw 事件 → scheduleIncrementalSave() (500ms 防抖)
  → addFeature() / saveFeature() / deleteFeature() (单条记录)
  → db.add/update/remove → _status 标记 → pushChanges() (write-through)
```

替代了原来的全量 `replaceAllFeatures()`，手动"保存"按钮仍保留全量替换作为兜底。

### feature_id → docId 映射

三层 ID 映射关系：

| ID 类型 | 说明 | 生命周期 |
|---------|------|---------|
| draw 内部 ID | mapbox-gl-draw 分配 | 每次加载不同 |
| feature_id (`properties._id`) | 小班业务编号 | 跨设备一致 |
| _id (docId) | 数据库记录主键 | 跨设备一致 |

映射表 `featureIdToDocId: Map<feature_id, _id>` 在进入编辑模式时构建，`addFeature` 后更新，`deleteFeature` 后移除，退出编辑模式时清空。

### 编辑模式远程变更保护

当远程变更到达时，通过 `draw.getSelectedIds()` 检测正在编辑的小班：
- **未选中的小班**：立即合并远程变更到 draw 图层
- **正在编辑的小班**：跳过远程更新，由 LWW 在下次 pull-then-push 同步时自动解决冲突

### 增量/全量刷新阈值

远程变更到达后的刷新策略：
- `docIds.length ≤ 10`：增量合并（按 feature_id 匹配替换/新增/删除）
- `docIds.length > 10`：全量重载（裁剪导入等批量场景）

### 拓扑修复检测

draw.js 的 `ensureSharedVertices()` 维护共边拓扑时可能修改相邻小班的 geometry。通过 geometry 快照对比（`lastSavedGeometries`）检测间接变更并一并保存，确保所有受影响的小班都被推送到服务端。
