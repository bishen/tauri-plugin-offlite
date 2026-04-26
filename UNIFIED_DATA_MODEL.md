# 统一 App 数据模型接入指南

## 概述

算金山平台的统一数据模型（Unified Data Model）为所有 App 产品定义了标准化的数据结构。通过声明 `entityType`，App 的同步数据会自动路由到服务端的标准实体表（独立 PostgreSQL 列），便于中台统一查询和聚合。

**核心原则：**
- 标准字段使用独立 PostgreSQL 列（可索引、可聚合）
- App 差异化字段存入 `ext` JSONB 列
- 客户端本地 SQLite 存储不变（仍用 `data` TEXT 列）
- 字段名转换由服务端自动处理（客户端 camelCase ↔ 服务端 snake_case）

## 标准实体类型

| entityType | 服务端表 | 说明 | syncMode |
|------------|---------|------|----------|
| `project` | sync.projects | 项目 | user |
| `sub_compartment` | sync.sub_compartments | 小班（逐条） | project |
| `planning_meta` | sync.planning_meta | 小班区划元数据 | project |
| `sample_plot` | sync.sample_plots | 样地 | project |
| `survey` | sync.surveys | 调查数据（9种类型） | project |
| `output` | sync.outputs | 成果产品（报告等） | project |
| `other`（默认） | offlite.{app}_{collection} | 非标准实体 | 任意 |

## 如何声明 entityType

在 `defineSyncModel` 的第三个参数（options）中添加 `entityType`：

```javascript
import { defineSyncModel } from '../db'

// 标准实体 — 数据写入 sync schema 标准表
export const useProjectDB = defineSyncModel('project', {
  name: { type: 'string', required: true },
  area: { type: 'number', default: 0 },
  // ...
}, { syncMode: 'user', entityType: 'project' })

// 非标准实体 — 不声明 entityType，走 offlite 旧路径
export const usePhotoDB = defineSyncModel('photos', {
  p_id: { type: 'string', required: true },
  url: { type: 'string' },
  // ...
})
```

**未声明 `entityType` 时默认为 `'other'`**，数据继续写入 offlite schema 的动态表。

## 各实体标准列定义

### Project（项目）

| 客户端字段 (camelCase) | 服务端列 (snake_case) | 类型 | required | 默认值 | 说明 |
|---|---|---|---|---|---|
| name | name | TEXT | ✅ | - | 项目名称 |
| area | area | DOUBLE PRECISION | | 0 | 面积（平米） |
| status | status | INTEGER | ✅ | 0 | 项目状态 |
| projectType | project_type | TEXT | | 'forest' | 项目类型 |
| unit | unit | TEXT | | 'hm' | 面积单位 |
| managerId | manager_id | INTEGER | ✅ | 0 | 项目负责人 uid |
| chiefEngineerId | chief_engineer_id | INTEGER | | 0 | 总工程师 uid |
| techLeaderId | tech_leader_id | INTEGER | | 0 | 技术负责人 uid |
| invstIds | invst_ids | INTEGER[] | | [] | 调查人员 uid 数组 |
| start | start_date | TIMESTAMPTZ | | null | 开始时间 |
| end | end_date | TIMESTAMPTZ | | null | 结束时间 |
| center[0] | center_lng | DOUBLE PRECISION | | null | 中心经度 |
| center[1] | center_lat | DOUBLE PRECISION | | null | 中心纬度 |
| location.province | province | TEXT | | null | 省 |
| location.city | city | TEXT | | null | 市 |
| location.county | county | TEXT | | null | 县 |
| boundary | boundary | GEOMETRY | | null | 项目边界（PostGIS） |
| coordSys | coord_sys | INTEGER | | 4490 | 坐标系 EPSG |
| surveys | surveys | TEXT[] | | ['乔木'] | 调查内容 |
| dbhType | dbh_type | INTEGER | | 0 | 胸径类型 |
| dbhValue | dbh_value | INTEGER | | 2 | 径阶步长 |
| heightType | height_type | INTEGER | | 0 | 树高类型 |
| heightValue | height_value | INTEGER | | 1 | 高阶值 |
| heightExcel | height_excel | TEXT | | '' | 高阶公式 |
| rootType | root_type | INTEGER | | 0 | 地径类型 |
| rootValue | root_value | INTEGER | | 2 | 径阶值 |
| watermark | watermark | TEXT[] | | ['项目名称','坐标','日期','调查员'] | 水印配置 |
| forestEdge | forest_edge | DOUBLE PRECISION | | 0 | 林缘距离(m) |
| surveyAreaThreshold | survey_area_threshold | DOUBLE PRECISION | | 1 | 全小班调查阈值(亩) |
| syncEnabled | sync_enabled | BOOLEAN | | true | 同步开关 |
| factors | → ext.factors | JSONB (ext) | | [] | 调查因子（存入 ext） |

### SubCompartment（小班）

| 客户端字段 | 服务端列 | 类型 | required | 默认值 | 说明 |
|---|---|---|---|---|---|
| p_id | p_id | TEXT | ✅ | - | 所属项目 ID |
| linban | linban | TEXT | | null | 林班号 |
| feature_id | feature_id | INTEGER | ✅ | - | 小班唯一标识 |
| sort | sort | INTEGER | | 0 | 小班序号 |
| group | group | INTEGER | | 0 | 地块分组 |
| area | area | DOUBLE PRECISION | | 0 | 面积(m²) |
| geometry | geom | GEOMETRY | | null | 小班几何（PostGIS） |
| properties | properties | JSONB | | null | 因子属性 |

### SamplePlot（样地）

| 客户端字段 | 服务端列 | 类型 | required | 默认值 | 说明 |
|---|---|---|---|---|---|
| p_id | p_id | TEXT | ✅ | - | 所属项目 ID |
| class_id | class_id | TEXT | ✅ | - | 所属小班 ID |
| seq | seq | INTEGER | | 1 | 样地编号 |
| type | plot_type | TEXT | | 'rect' | 样地类型 |
| width | width | DOUBLE PRECISION | | 20 | 宽度(m) |
| height | height | DOUBLE PRECISION | | 20 | 高度(m) |
| radius | radius | DOUBLE PRECISION | | 12 | 半径(m) |
| rotation | rotation | DOUBLE PRECISION | | 0 | 旋转角度 |
| area | area | DOUBLE PRECISION | | 0 | 面积(m²) |
| center[0] | center_lng | DOUBLE PRECISION | | null | 中心经度 |
| center[1] | center_lat | DOUBLE PRECISION | | null | 中心纬度 |
| elevation | elevation | DOUBLE PRECISION | | 0 | 海拔(m) |
| features | geom | GEOMETRY | | null | 样地边界（PostGIS） |
| *(碳汇概况、统计字段等 30+ 列详见设计文档)* |

### Survey（调查数据）

所有 9 种调查类型共享一张 `sync.surveys` 表，通过 `survey_type` 和 `collection_name` 区分。

**通用 required 列：**
| 客户端字段 | 服务端列 | required |
|---|---|---|
| p_id | p_id | ✅ |
| surveyType | survey_type | ✅ |
| *(collection_name 由服务端自动填入)* | collection_name | ✅ |

**同名字段映射规则（避免冲突）：**
| 客户端字段 | 调查类型 | 服务端列 |
|---|---|---|
| height | dbh_actual | tree_height |
| height | shrub_survey | shrub_height |
| height | bamboo_survey | bamboo_height |
| dbh | bamboo_survey | bamboo_dbh |
| count | herb_survey | herb_count |
| count | bamboo_survey | bamboo_count |
| name | dbh_actual/dbh_step | tree_name |
| volume | dbh_actual/dbh_step | tree_volume |
| status | dbh_actual/dbh_step | tree_status |
| species | fallen_wood_survey | fallen_species |
| carbonStorage | litter_survey | litter_carbon |
| color | soil_survey | soil_color |
| layers | soil_survey | soil_layers |

### Output（成果产品）

| 客户端字段 | 服务端列 | 类型 | required | 说明 |
|---|---|---|---|---|
| p_id | p_id | TEXT | ✅ | 所属项目 ID |
| outputType | output_type | TEXT | ✅ | 产品类型 |
| name | name | TEXT | ✅ | 名称 |
| fileRef | file_ref | TEXT | | 文件引用 |
| sections | → ext.sections | JSONB (ext) | | 章节数组（存入 ext） |
| docxStyle | → ext.docxStyle | JSONB (ext) | | 排版配置（存入 ext） |

## ext 扩展字段使用说明

`ext` JSONB 列用于存储标准列之外的 App 差异化字段。

**规则：**
1. ext 中的键名不能与同实体的标准列名冲突
2. ext 默认为空对象 `{}`
3. 客户端 push 时，不在标准列集合中的字段自动归入 ext
4. 客户端 pull 时，ext 中的字段自动合并到扁平数据中返回

**示例（survey App 的 project）：**
```javascript
// 客户端 push 的数据
{
  name: '项目A',        // → 标准列 name
  area: 1000,           // → 标准列 area
  managerId: 302,       // → 标准列 manager_id
  factors: [{...}],     // → ext.factors（不在标准列中）
  customField: 'xxx',   // → ext.customField（不在标准列中）
}

// 服务端存储
// sync.projects 表：name='项目A', area=1000, manager_id=302
// ext = {"factors": [{...}], "customField": "xxx"}
```

## 新 App 接入指南

### 1. 确定实体类型

分析你的 App 有哪些数据实体，对应到标准实体类型：

| 你的数据 | 对应 entityType | 说明 |
|---------|----------------|------|
| 项目/工程 | `project` | 顶层业务对象 |
| 小班/地块 | `sub_compartment` | 空间管理单元 |
| 样地/采样点 | `sample_plot` | 调查采样单元 |
| 调查记录 | `survey` | 具体调查数据 |
| 报告/成果 | `output` | 输出产品 |
| 其他 | 不声明（默认 other） | 走 offlite 旧路径 |

### 2. 定义 schema 并声明 entityType

```javascript
// 你的 App 的项目模型
export const useProjectDB = defineSyncModel('project', {
  // 标准字段（会写入独立列）
  name: { type: 'string', required: true },
  area: { type: 'number', default: 0 },
  status: { type: 'number', default: 0 },
  managerId: { type: 'number', required: true },
  // ...其他标准字段

  // 你的 App 特有字段（会自动存入 ext）
  myCustomField: { type: 'string', default: '' },
  myConfig: { type: 'object', default: {} },
}, { syncMode: 'user', entityType: 'project' })
```

### 3. 标准字段命名

客户端使用 camelCase，服务端自动转为 snake_case。确保你的标准字段名与本文档中的定义一致。

### 4. 非标准实体

不需要声明 entityType 的数据（如照片、字典、配置等），直接用 `defineSyncModel` 不传 entityType 即可：

```javascript
export const usePhotoDB = defineSyncModel('photos', {
  p_id: { type: 'string', required: true },
  url: { type: 'string' },
})
```

## 字段名转换规则

| 方向 | 转换 | 示例 |
|------|------|------|
| push（客户端→服务端） | camelCase → snake_case | `managerId` → `manager_id` |
| pull（服务端→客户端） | snake_case → camelCase | `manager_id` → `managerId` |

**特殊映射（surveys 表）：** 部分同名字段有特殊映射，详见上方"同名字段映射规则"表。

## 向后兼容

- 不传 `entityType` 的旧版 App 请求正常工作，数据写入 offlite schema
- 新增标准列时，旧版数据缺失的列自动用默认值补全
- 标准列不会做不兼容变更（不删列、不改类型），只会新增
