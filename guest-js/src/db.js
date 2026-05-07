/**
 * 通用 CRUD 封装（纯 JS，框架无关）
 *
 * 封装 Tauri invoke 调用，提供文档级 CRUD 操作。
 * 不依赖 Vue/React 等框架，不包含响应式状态。
 *
 * @example
 * import { createDB } from 'tauri-plugin-offlite-api/db'
 *
 * const db = createDB('planning_feature', {
 *   schema: { project_id: { type: 'string', required: true } },
 *   syncMode: 'project',
 *   entityType: 'sub_compartment',
 *   uid: 1,
 *   company_id: 100,
 *   getProjectId: () => currentProjectId || 'global',
 *   getSyncEngine: () => syncManager.getEngine('planning_feature'),
 * })
 *
 * await db.add({ project_id: 'p1', feature_id: 1 })
 * const { data } = await db.get(id)
 */

import { invoke } from '@tauri-apps/api/core'
import { generateId } from './idgen.js'
import { validateDoc, splitDoc, parseRow, META_FIELDS } from './schema.js'

// ============ 常量 ============

/** 批量操作每批最大条数 */
const BATCH_SIZE = 50

// ============ 核心函数 ============

/**
 * 创建数据库操作实例
 *
 * @param {string} dbName - 表名
 * @param {Object} options
 * @param {Object} options.schema - 字段定义（可选）
 * @param {boolean} options.strict - 严格模式
 * @param {string} options.syncMode - 同步模式 'user'|'company'|'project'|'local'
 * @param {string} options.entityType - 标准实体类型
 * @param {number|null} options.uid - 当前用户 ID
 * @param {number|null} options.company_id - 当前企业 ID
 * @param {Function} options.getProjectId - 返回当前应使用的 projectId（用于 invoke）
 * @param {Function} options.getSyncEngine - 返回当前表对应的 syncEngine 实例（用于 write-through）
 * @returns {Object} CRUD 操作对象
 */
export function createDB(dbName, options = {}) {
  const {
    schema = null,
    strict = false,
    syncMode = 'project',
    uid = null,
    company_id = null,
    getProjectId = () => 'global',
    getSyncEngine = () => null,
  } = options

  // 表是否已初始化
  let tableReady = false

  /**
   * 确保 SQLite 表已创建（首次操作时自动建表）
   */
  const ensureTable = async () => {
    if (tableReady) return
    try {
      const projectId = getProjectId()
      await invoke('plugin:offlite|db_create_tables', {
        projectId,
        schemas: [{ name: dbName, json_indexes: [] }]
      })
      tableReady = true
    } catch (e) {
      if (String(e).includes('already exists') || String(e).includes('table')) {
        tableReady = true
      } else {
        console.error(`[DB] 建表失败 ${dbName}:`, e)
      }
    }
  }

  /**
   * 验证并处理文档数据
   */
  const validateAndProcess = (doc) => {
    const result = validateDoc(doc, schema, strict)
    if (!result.valid) {
      throw new Error(result.errors.join('; '))
    }
    return result.data
  }

  // ---- add ----
  const add = async (doc) => {
    try {
      await ensureTable()
      const validatedDoc = validateAndProcess(doc)
      const now = new Date().toISOString()
      const projectId = getProjectId()
      const docId = doc._id || generateId()

      const { meta, data } = splitDoc(validatedDoc)

      const fullMeta = {
        _id: docId,
        uid: uid,
        company_id: company_id,
        project_id: meta.project_id || '',
        created_at: now,
        updated_at: now,
        _deleted: 0,
        _version: 1,
      }

      const dataJson = JSON.stringify(data)

      await invoke('plugin:offlite|db_execute', {
        projectId,
        sql: `INSERT INTO ${dbName} (_id, uid, company_id, project_id, created_at, updated_at, _deleted, _version, _status, data)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'created', ?)`,
        params: [
          fullMeta._id, fullMeta.uid, fullMeta.company_id, fullMeta.project_id,
          fullMeta.created_at, fullMeta.updated_at, fullMeta._deleted, fullMeta._version,
          dataJson
        ]
      })

      // Write-through
      const engine = getSyncEngine()
      if (engine) engine.pushChanges(dbName).catch(() => {})

      return { success: true, data: { _id: fullMeta._id, ...fullMeta, ...data } }
    } catch (error) {
      console.error('[DB] add 失败:', error)
      return { success: false, error: error.message || error }
    }
  }

  // ---- get ----
  const get = async (id) => {
    try {
      await ensureTable()
      const projectId = getProjectId()
      const rows = await invoke('plugin:offlite|db_query', {
        projectId,
        sql: `SELECT * FROM ${dbName} WHERE _id = ? AND _deleted = 0`,
        params: [id]
      })

      if (!rows || rows.length === 0) {
        return { success: false, error: { status: 404, message: '文档不存在' } }
      }

      return { success: true, data: parseRow(rows[0]) }
    } catch (error) {
      console.error('[DB] get 失败:', error)
      return { success: false, error }
    }
  }

  // ---- getAll ----
  const getAll = async () => {
    try {
      await ensureTable()
      const projectId = getProjectId()
      const rows = await invoke('plugin:offlite|db_query', {
        projectId,
        sql: `SELECT * FROM ${dbName} WHERE _deleted = 0`,
        params: []
      })

      const docs = (rows || []).map(parseRow)
      return { success: true, data: docs, total: docs.length }
    } catch (error) {
      console.error('[DB] getAll 失败:', error)
      return { success: false, error }
    }
  }

  // ---- update ----
  const update = async (id, changes, _retryCount = 0) => {
    try {
      await ensureTable()
      const { data: existing, success } = await get(id)
      if (!success) {
        if (_retryCount < 1) {
          await new Promise(r => setTimeout(r, 1000))
          return update(id, changes, _retryCount + 1)
        }
        throw new Error('文档不存在或无权访问')
      }

      // 合并数据（排除内部元数据字段）
      const { _id, uid: docUid, company_id: docCompanyId, project_id: docProjectId,
        created_at, updated_at: _, _deleted, _version: oldVersion, _status, ...existingBiz } = existing
      const mergedData = { project_id: docProjectId, ...existingBiz, ...changes }

      const validatedData = validateAndProcess(mergedData)

      const now = new Date().toISOString()
      const newVersion = (oldVersion || 1) + 1
      const { data: bizData } = splitDoc(validatedData)
      const dataJson = JSON.stringify(bizData)
      const projectId = getProjectId()

      await invoke('plugin:offlite|db_execute', {
        projectId,
        sql: `UPDATE ${dbName} SET data = ?, updated_at = ?, _version = ?, _status = 'updated' WHERE _id = ?`,
        params: [dataJson, now, newVersion, id]
      })

      // Write-through
      const engine = getSyncEngine()
      if (engine) engine.pushChanges(dbName).catch(() => {})

      return {
        success: true,
        data: { _id: id, uid: docUid, company_id: docCompanyId, project_id: docProjectId,
          created_at, updated_at: now, _version: newVersion, ...validatedData }
      }
    } catch (error) {
      console.error('[DB] update 失败:', error)
      return { success: false, error: error.message || error }
    }
  }

  // ---- remove ----
  const remove = async (id) => {
    try {
      await ensureTable()
      const projectId = getProjectId()
      const now = new Date().toISOString()

      await invoke('plugin:offlite|db_execute', {
        projectId,
        sql: `UPDATE ${dbName} SET _deleted = 1, updated_at = ?, _status = 'deleted' WHERE _id = ?`,
        params: [now, id]
      })

      // Write-through
      const engine = getSyncEngine()
      if (engine) engine.pushChanges(dbName).catch(() => {})

      return { success: true, data: { ok: true, id } }
    } catch (error) {
      console.error('[DB] remove 失败:', error)
      return { success: false, error: error.message || error }
    }
  }

  // ---- addBulk ----
  const addBulk = async (docs) => {
    try {
      await ensureTable()
      const projectId = getProjectId()
      const results = []

      for (let i = 0; i < docs.length; i += BATCH_SIZE) {
        const batch = docs.slice(i, i + BATCH_SIZE)
        const statements = []

        for (const doc of batch) {
          const validatedDoc = validateAndProcess(doc)
          const now = new Date().toISOString()
          const docId = doc._id || generateId()

          const { data: bizData } = splitDoc(validatedDoc)
          const dataJson = JSON.stringify(bizData)

          statements.push({
            sql: `INSERT INTO ${dbName} (_id, uid, company_id, project_id, created_at, updated_at, _deleted, _version, _status, data)
                  VALUES (?, ?, ?, ?, ?, ?, 0, 1, 'created', ?)`,
            params: [
              docId, uid, company_id, validatedDoc.project_id || '',
              doc.created_at || now, now, dataJson
            ]
          })

          results.push({ id: docId, ok: true })
        }

        await invoke('plugin:offlite|db_batch', { projectId, statements })
      }

      // Write-through（批量完成后仅触发一次）
      const engine = getSyncEngine()
      if (engine) engine.pushChanges(dbName).catch(() => {})

      return { success: true, data: results }
    } catch (error) {
      console.error('[DB] addBulk 失败:', error)
      return { success: false, error: error.message || error }
    }
  }

  // ---- removeBulk ----
  const removeBulk = async (ids) => {
    try {
      await ensureTable()
      const projectId = getProjectId()
      const now = new Date().toISOString()
      const results = []

      for (let i = 0; i < ids.length; i += BATCH_SIZE) {
        const batch = ids.slice(i, i + BATCH_SIZE)
        const statements = []

        for (const id of batch) {
          statements.push({
            sql: `UPDATE ${dbName} SET _deleted = 1, updated_at = ?, _status = 'deleted' WHERE _id = ?`,
            params: [now, id]
          })
          results.push({ id, ok: true })
        }

        await invoke('plugin:offlite|db_batch', { projectId, statements })
      }

      // Write-through
      const engine = getSyncEngine()
      if (engine) engine.pushChanges(dbName).catch(() => {})

      return { success: true, data: results }
    } catch (error) {
      console.error('[DB] removeBulk 失败:', error)
      return { success: false, error: error.message || error }
    }
  }

  // ---- query ----
  const query = async (selector = {}) => {
    try {
      await ensureTable()
      const projectId = getProjectId()
      const whereClauses = ['_deleted = 0']
      const params = []

      for (const [field, value] of Object.entries(selector)) {
        if (META_FIELDS.has(field)) {
          whereClauses.push(`${field} = ?`)
          params.push(value)
        } else {
          whereClauses.push(`json_extract(data, '$.${field}') = ?`)
          params.push(typeof value === 'object' ? JSON.stringify(value) : value)
        }
      }

      const sql = `SELECT * FROM ${dbName} WHERE ${whereClauses.join(' AND ')}`
      const rows = await invoke('plugin:offlite|db_query', { projectId, sql, params })

      const docs = (rows || []).map(parseRow)
      return { success: true, data: docs, total: docs.length }
    } catch (error) {
      console.error('[DB] query 失败:', error)
      return { success: false, error }
    }
  }

  return {
    dbName,
    syncMode,
    add,
    get,
    getAll,
    update,
    remove,
    addBulk,
    removeBulk,
    query,
    queryRemote: query, // 兼容旧 API
    validate: (doc) => validateDoc(doc, schema, strict),
    getSchema: () => schema,
    ensureTable,
  }
}
