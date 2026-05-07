/**
 * Schema 验证 + 模型定义
 *
 * 纯 JS 实现，框架无关。提供：
 * 1. defineSchema() — 定义数据模型（表名、字段规则、同步选项）
 * 2. validateField() — 单字段验证
 * 3. validateDoc() — 整文档验证
 * 4. splitDoc() — 元数据/业务数据拆分
 * 5. parseRow() — SQLite 行 → 扁平文档
 *
 * @example
 * import { defineSchema } from 'tauri-plugin-offlite-api/schema'
 *
 * const projectSchema = defineSchema('project', {
 *   name: { type: 'string', required: true },
 *   area: { type: 'number', default: 0 },
 * }, { syncMode: 'user', entityType: 'project' })
 */

// ============ 常量 ============

/**
 * 元数据字段集合（存储为 SQLite 独立列，不放入 data JSON）
 */
export const META_FIELDS = new Set([
  '_id', 'uid', 'company_id', 'project_id',
  'created_at', 'updated_at', '_deleted', '_version', '_status'
])

// ============ Schema 验证 ============

/**
 * 验证单个字段值
 * @param {string} field - 字段名
 * @param {*} value - 字段值
 * @param {Object} rules - 验证规则
 * @returns {{ valid: boolean, error?: string, value: * }}
 */
export function validateField(field, value, rules) {
  // 处理默认值
  if (value === undefined || value === null) {
    if (rules.default !== undefined) {
      value = typeof rules.default === 'function' ? rules.default() : rules.default
    }
  }

  // 必填检查
  if (rules.required && (value === undefined || value === null || value === '')) {
    return { valid: false, error: `字段 "${field}" 是必填的` }
  }

  // 如果值为空且非必填，跳过后续验证
  if (value === undefined || value === null) {
    return { valid: true, value }
  }

  // 类型检查
  if (rules.type) {
    const type = rules.type.toLowerCase()
    let typeValid = false

    switch (type) {
      case 'string':
        typeValid = typeof value === 'string'
        break
      case 'number':
        typeValid = typeof value === 'number' && !isNaN(value)
        break
      case 'boolean':
        typeValid = typeof value === 'boolean'
        break
      case 'array':
        typeValid = Array.isArray(value)
        break
      case 'object':
        typeValid = typeof value === 'object' && !Array.isArray(value)
        break
      case 'date':
        typeValid = value instanceof Date || !isNaN(Date.parse(value))
        break
      default:
        typeValid = true
    }

    if (!typeValid) {
      return { valid: false, error: `字段 "${field}" 应为 ${type} 类型` }
    }
  }

  // 枚举检查
  if (rules.enum && !rules.enum.includes(value)) {
    return { valid: false, error: `字段 "${field}" 的值必须是 [${rules.enum.join(', ')}] 之一` }
  }

  // 正则检查
  if (rules.pattern && typeof value === 'string' && !rules.pattern.test(value)) {
    return { valid: false, error: `字段 "${field}" 格式不正确` }
  }

  // 最小值/长度
  if (rules.min !== undefined) {
    if (typeof value === 'number' && value < rules.min) {
      return { valid: false, error: `字段 "${field}" 不能小于 ${rules.min}` }
    }
    if (typeof value === 'string' && value.length < rules.min) {
      return { valid: false, error: `字段 "${field}" 长度不能小于 ${rules.min}` }
    }
  }

  // 最大值/长度
  if (rules.max !== undefined) {
    if (typeof value === 'number' && value > rules.max) {
      return { valid: false, error: `字段 "${field}" 不能大于 ${rules.max}` }
    }
    if (typeof value === 'string' && value.length > rules.max) {
      return { valid: false, error: `字段 "${field}" 长度不能大于 ${rules.max}` }
    }
  }

  // 自定义验证
  if (rules.validate && typeof rules.validate === 'function') {
    const customResult = rules.validate(value)
    if (customResult !== true) {
      return { valid: false, error: customResult || `字段 "${field}" 验证失败` }
    }
  }

  return { valid: true, value }
}

/**
 * 验证整个文档
 * @param {Object} doc - 文档数据
 * @param {Object} schema - Schema 定义
 * @param {boolean} strict - 严格模式（只保留 schema 定义的字段）
 * @returns {{ valid: boolean, errors: string[], data: Object }}
 */
export function validateDoc(doc, schema, strict = false) {
  if (!schema) return { valid: true, errors: [], data: doc }

  const errors = []
  const validatedData = {}

  // 验证 schema 中定义的字段
  for (const [field, rules] of Object.entries(schema)) {
    const result = validateField(field, doc[field], rules)
    if (!result.valid) {
      errors.push(result.error)
    } else if (result.value !== undefined) {
      validatedData[field] = result.value
    }
  }

  // 非严格模式下保留 schema 中未定义的字段
  if (!strict) {
    for (const [field, value] of Object.entries(doc)) {
      if (!(field in schema) && !field.startsWith('_')) {
        validatedData[field] = value
      }
    }
  }

  return {
    valid: errors.length === 0,
    errors,
    data: validatedData
  }
}

// ============ 文档拆分/合并 ============

/**
 * 将文档拆分为元数据和业务数据
 * @param {Object} doc - 完整文档
 * @returns {{ meta: Object, data: Object }}
 */
export function splitDoc(doc) {
  const meta = {}
  const data = {}
  for (const [key, value] of Object.entries(doc)) {
    if (META_FIELDS.has(key)) {
      meta[key] = value
    } else {
      data[key] = value
    }
  }
  return { meta, data }
}

/**
 * 从 SQLite 行中解析出完整文档（元数据列 + data JSON 合并）
 * @param {Object} row - SQLite 查询返回的行
 * @returns {Object|null} 合并后的扁平文档
 */
export function parseRow(row) {
  if (!row) return null
  const { data: dataJson, ...meta } = row
  let businessFields = {}
  if (dataJson) {
    try {
      businessFields = typeof dataJson === 'string' ? JSON.parse(dataJson) : dataJson
    } catch {
      businessFields = {}
    }
  }
  return { ...meta, ...businessFields }
}

// ============ 模型定义 ============

/**
 * 从 schema 中提取 localOnly 字段名列表
 * @param {Object} schema - 数据模型
 * @returns {string[]}
 */
function getLocalOnlyFields(schema) {
  if (!schema) return []
  return Object.entries(schema)
    .filter(([_, rules]) => rules.localOnly === true)
    .map(([field]) => field)
}

/**
 * 定义数据模型（纯描述，不创建 DB 实例）
 *
 * @param {string} dbName - 表名
 * @param {Object} schema - 字段定义
 * @param {Object} options - { syncMode, entityType, strict }
 * @returns {Object} 模型描述对象
 */
export function defineSchema(dbName, schema, options = {}) {
  const {
    syncMode = 'project',
    entityType = undefined,
    strict = false,
  } = options

  const localOnlyFields = getLocalOnlyFields(schema)

  return {
    dbName,
    schema,
    syncMode,
    entityType,
    strict,
    localOnlyFields,
    validate: (doc) => validateDoc(doc, schema, strict),
  }
}
