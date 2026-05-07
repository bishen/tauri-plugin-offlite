# Offlite 字段命名规范

## 核心原则

**全栈统一 snake_case**：从客户端 SQLite 到服务端 PostgreSQL，所有字段名使用 snake_case。

消除 camelCase ↔ snake_case 转换层，减少 bug 来源。

## 规范定义

### 1. 系统元数据列（`_` 前缀）

插件内部使用的控制列，业务代码不应直接修改：

| 列名 | 类型 | 说明 |
|------|------|------|
| `_id` | TEXT PK | 记录唯一标识 |
| `_deleted` | INTEGER | 软删除标记（0/1） |
| `_version` | INTEGER | 乐观锁版本号 |
| `_status` | TEXT | 同步状态（synced/created/updated/deleted） |

### 2. 固定元数据列

每张业务表自动包含的列：

| 列名 | 类型 | 说明 |
|------|------|------|
| `uid` | INTEGER | 创建者用户 ID |
| `company_id` | INTEGER | 所属企业 ID |
| `project_id` | TEXT | 所属项目 ID |
| `created_at` | TEXT | 创建时间（ISO 8601） |
| `updated_at` | TEXT | 更新时间（ISO 8601） |

### 3. 业务字段（data JSON 键名）

所有业务字段统一使用 snake_case：

```javascript
// ✅ 正确
{
  manager_id: 302,
  chief_engineer_id: 15,
  tech_leader_id: 8,
  invst_ids: [1, 2, 3],
  project_type: 'forest',
  dbh_type: 0,
  dbh_value: 2,
  height_type: 0,
  survey_area_threshold: 1,
  sync_enabled: true,
  feature_id: 42,
  class_id: 'abc123',
}

// ❌ 错误（不再使用 camelCase）
{
  managerId: 302,
  chiefEngineerId: 15,
  techLeaderId: 8,
  invstIds: [1, 2, 3],
  projectType: 'forest',
  dbhType: 0,
}
```

### 4. 命名细则

| 类别 | 规则 | 示例 |
|------|------|------|
| 普通字段 | 小写 + 下划线分隔 | `tree_height`, `slope_angle` |
| 外键 | `{entity}_id` | `project_id`, `class_id`, `sample_id`, `plant_id`, `tree_group_id` |
| 布尔字段 | `is_` 或 `has_` 前缀 | `is_mother`, `is_deleted`, `has_boundary` |
| 数组字段 | 复数名词或 `_ids` 后缀 | `invst_ids`, `surveys`, `factors`, `watermark` |
| 枚举字段 | 名词（值为字符串） | `origin`, `quality`, `status` |
| 计算字段 | 描述性名词 | `biomass_above`, `carbon_storage`, `avg_dbh` |
| 缩写 | 保持全小写 | `dbh`(胸径), `ew`(东西), `ns`(南北) |

### 5. 缩写词表

项目中允许的标准缩写（不展开）：

| 缩写 | 全称 | 说明 |
|------|------|------|
| `id` | identifier | 标识符 |
| `uid` | user id | 用户 ID |
| `dbh` | diameter at breast height | 胸径 |
| `avg` | average | 平均值 |
| `seq` | sequence | 序号 |
| `lng` | longitude | 经度 |
| `lat` | latitude | 纬度 |
| `ew` | east-west | 东西 |
| `ns` | north-south | 南北 |
| `geom` | geometry | 几何 |
| `ext` | extension | 扩展 |
| `desc` | description | 描述 |
| `invst` | investigator | 调查员 |
| `calc` | calculation | 计算 |

**不允许自创缩写**。新字段如果没有对应的标准缩写，使用完整单词。

## 建表 SQL（新规范）

```sql
CREATE TABLE IF NOT EXISTS {table_name} (
    _id         TEXT PRIMARY KEY,
    uid         INTEGER,
    company_id  INTEGER,
    project_id  TEXT,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    _deleted    INTEGER DEFAULT 0,
    _version    INTEGER DEFAULT 1,
    _status     TEXT DEFAULT 'synced',
    data        TEXT NOT NULL
);
```

**变更点**（相对旧规范）：
- `companyId` → `company_id`
- `createdAt` → `created_at`
- `updatedAt` → `updated_at`

## 服务端对齐

服务端 PostgreSQL 已经是 snake_case，无需改动。

**消除的转换层**：
- ~~`camelToSnake()`~~ — push 时不再需要转换
- ~~`snakeToCamel()`~~ — pull 时不再需要转换
- ~~`normalizePulledKeys()`~~ — 客户端不再需要转换
- ~~`convertKeys()`~~ — 不再需要

**push 流程简化**：
```
客户端 data (snake_case) → 直接发送 → 服务端 splitColumns → 写入 PostgreSQL
```

**pull 流程简化**：
```
PostgreSQL (snake_case) → 直接返回 → 客户端直接存入 SQLite data 列
```

## 迁移策略

> 开发阶段无需数据迁移，直接清库重建即可。

### Phase 1：插件层改动（tauri-plugin-offlite）✅

1. `schema.rs` 固定列改为 snake_case（`company_id`, `created_at`, `updated_at`）
2. `sync.js` 移除 `normalizePulledKeys()` 函数
3. `sync.js` 中 `applyPulledChanges` 直接使用服务端返回的 snake_case 数据

### Phase 2：服务端改动（msg-api）✅

1. `schemaRegistry.ts` 标记 `camelToSnake`/`snakeToCamel`/`convertKeys` 为 deprecated
2. `sync.ts` push 路径不再调用 `convertKeys(change.data, camelToSnake)`
3. `sync.ts` pull 路径不做任何键名转换
4. `offlite.ts` 建表 SQL 已是 snake_case

### Phase 3：客户端改动（survey）✅

1. 所有 `composables/schema/*.js` 字段名改为 snake_case
2. 业务代码中引用字段名的地方批量替换
3. `db.js` 中 `add`/`update` 方法使用 snake_case 元数据列名

## Vue 组件中的使用

```javascript
// ✅ 新规范
const project = await useProjectDB.get(id)
console.log(project.data.manager_id)
console.log(project.data.chief_engineer_id)
console.log(project.data.survey_area_threshold)

// 模板中
<span>{{ project.data.project_type }}</span>
```

## 与 JavaScript 惯例的权衡

JavaScript 社区惯例是 camelCase，但本项目选择 snake_case 的理由：

1. **数据字段不是 JS 变量**——它们是数据库列名的映射，跨语言传输
2. **消除转换层**——每次转换都是 bug 来源（漏转、双重转换、特殊字段）
3. **SQL 查询一致性**——`WHERE json_extract(data, '$.manager_id')` 比 `'$.managerId'` 更自然
4. **服务端已定型**——PostgreSQL 列名不可能改为 camelCase
5. **先例**：Supabase、Prisma 等现代工具也在 JS 中暴露 snake_case 字段

**JS 变量名仍然使用 camelCase**：
```javascript
// 变量名 camelCase，数据字段 snake_case
const projectData = await useProjectDB.get(id)
const managerId = projectData.data.manager_id  // 取出后赋给 camelCase 变量
```
