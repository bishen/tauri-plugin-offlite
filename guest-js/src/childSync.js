/**
 * 通用父子表关联同步
 *
 * 实现"一条父记录 + N 条子记录"的模式，不依赖具体的表名或字段结构。
 * 任何 Tauri app 都可以用这个来管理父子表关系。
 *
 * 典型用例:
 * - planning（父）+ planning_feature（子）：区划小班
 * - ownership（父）+ ownership_feature（子）：林权小班
 * - 任何需要"一对多 + 增量同步"的场景
 *
 * @example
 * const childSync = createChildSync({
 *   childDB: usePlanningFeatureDB,
 *   parentDB: usePlanningDB,
 *   filterField: 'project_id',
 *   countField: 'featureCount',
 *   checksumField: 'checksum',
 *   childIdField: 'feature_id',
 *   batchSize: 500,
 * })
 * const records = await childSync.loadChildren('project__123')
 */

import { generateId } from './idgen.js'

const DEFAULT_BATCH_SIZE = 500

/**
 * 计算 SHA-256 哈希（浏览器环境）
 * @param {string} text
 * @returns {Promise<string>} hex 字符串
 */
async function sha256(text) {
  const encoder = new TextEncoder()
  const data = encoder.encode(text)
  const hashBuffer = await crypto.subtle.digest('SHA-256', data)
  const hashArray = Array.from(new Uint8Array(hashBuffer))
  return hashArray.map(b => b.toString(16).padStart(2, '0')).join('')
}

/**
 * 创建父子表关联同步实例
 *
 * @param {Object} config
 * @param {Object} config.childDB - 子表的 DB 实例（useDB/defineSyncModel 返回的对象）
 * @param {Object} config.parentDB - 父表的 DB 实例
 * @param {string} config.filterField - 子表中关联父表的字段名（如 'project_id'）
 * @param {string} config.countField - 父表中存储子记录数量的字段名（如 'featureCount'）
 * @param {string} config.checksumField - 父表中存储校验和的字段名（如 'checksum'）
 * @param {string} config.childIdField - 子记录的业务 ID 字段名（如 'feature_id'）
 * @param {number} [config.batchSize=500] - 批量操作每批条数
 * @returns {Object} 父子表操作接口
 */
export function createChildSync(config) {
  const {
    childDB,
    parentDB,
    filterField,
    countField,
    checksumField,
    childIdField,
    batchSize = DEFAULT_BATCH_SIZE,
  } = config

  /**
   * 查询所有非删除子记录
   * @param {string} filterValue - 过滤值（如 projectId）
   * @returns {Promise<Array>} 子记录数组
   */
  async function loadChildren(filterValue) {
    const { success, data } = await childDB.query({ [filterField]: filterValue })
    if (!success) return []
    return data || []
  }

  /**
   * 保存单条子记录（更新）
   * @param {string} docId - 子记录的 _id
   * @param {Object} updates - 要更新的字段
   * @returns {Promise<Object>} { success, data, error }
   */
  async function saveChild(docId, updates) {
    return childDB.update(docId, { ...updates, updatedAt: new Date() })
  }

  /**
   * 新增子记录
   * @param {string} filterValue - 过滤值（如 projectId）
   * @param {Object} data - 子记录数据（不含 _id，自动生成短 ID）
   * @returns {Promise<Object>} { success, data, error }
   */
  async function addChild(filterValue, data) {
    return childDB.add({
      [filterField]: filterValue,
      ...data,
      updatedAt: new Date(),
    })
  }

  /**
   * 删除子记录（soft delete）
   * @param {string} docId - 子记录的 _id
   * @returns {Promise<Object>} { success, data, error }
   */
  async function deleteChild(docId) {
    return childDB.remove(docId)
  }

  /**
   * 批量替换所有子记录
   * 1. soft delete 所有旧记录
   * 2. 分批 addBulk 新记录
   * 3. 更新父表元数据
   *
   * @param {string} filterValue - 过滤值
   * @param {Array} newRecords - 新子记录数组（每条需包含业务字段，不含 _id）
   * @param {string} [parentDocId] - 父记录的 _id（用于更新元数据）
   * @returns {Promise<{ added: number, deleted: number }>}
   */
  async function replaceAll(filterValue, newRecords, parentDocId) {
    // 1. soft delete 所有旧记录
    const existing = await loadChildren(filterValue)
    let deleted = 0
    if (existing.length > 0) {
      const ids = existing.map(r => r._id)
      await childDB.removeBulk(ids)
      deleted = ids.length
    }

    // 2. 分批 addBulk 新记录
    let added = 0
    for (let i = 0; i < newRecords.length; i += batchSize) {
      const batch = newRecords.slice(i, i + batchSize).map(r => ({
        [filterField]: filterValue,
        ...r,
        updatedAt: new Date(),
      }))
      await childDB.addBulk(batch)
      added += batch.length
    }

    // 3. 更新父表元数据
    if (parentDocId) {
      await updateParentMeta(filterValue, parentDocId)
    }

    return { added, deleted }
  }

  /**
   * 清空所有子记录（soft delete all）
   * @param {string} filterValue - 过滤值
   * @param {string} [parentDocId] - 父记录的 _id
   * @returns {Promise<number>} 删除的记录数
   */
  async function clearAll(filterValue, parentDocId) {
    const existing = await loadChildren(filterValue)
    if (existing.length > 0) {
      await childDB.removeBulk(existing.map(r => r._id))
    }

    if (parentDocId) {
      await parentDB.update(parentDocId, {
        [countField]: 0,
        [checksumField]: '',
        updatedAt: new Date(),
      })
    }

    return existing.length
  }

  /**
   * 更新父表元数据（count + checksum）
   * @param {string} filterValue - 过滤值
   * @param {string} parentDocId - 父记录的 _id
   */
  async function updateParentMeta(filterValue, parentDocId) {
    const children = await loadChildren(filterValue)
    const count = children.length
    const ids = children.map(r => r[childIdField]).filter(id => id != null)
    const checksum = await calcChecksum(ids)

    await parentDB.update(parentDocId, {
      [countField]: count,
      [checksumField]: checksum,
      updatedAt: new Date(),
    })
  }

  /**
   * 计算 checksum（子记录业务 ID 排序后 SHA-256）
   * @param {Array<number|string>} childIds - 子记录业务 ID 数组
   * @returns {Promise<string>} 'sha256:...' 或空字符串
   */
  async function calcChecksum(childIds) {
    if (!childIds || childIds.length === 0) return ''
    const sorted = [...childIds].sort((a, b) => {
      const na = Number(a), nb = Number(b)
      if (!isNaN(na) && !isNaN(nb)) return na - nb
      return String(a).localeCompare(String(b))
    })
    const hash = await sha256(sorted.join(','))
    return `sha256:${hash}`
  }

  /**
   * 完整性校验
   * 对比本地子记录数 vs 父表 count，缺失时通过 idsEndpoint 补拉
   *
   * @param {string} filterValue - 过滤值
   * @param {string} parentDocId - 父记录的 _id
   * @param {string} idsEndpoint - 服务端 ID 列表端点 URL（如 '/sync/planning_feature/ids'）
   * @param {Function} [fetchFn] - 可选的 fetch 函数（用于注入 auth headers）
   * @returns {Promise<{ ok: boolean, missing: number }>}
   */
  async function checkIntegrity(filterValue, parentDocId, idsEndpoint, fetchFn) {
    try {
      // 读取父表元数据
      const { success: pSuccess, data: parentData } = await parentDB.get(parentDocId)
      if (!pSuccess || !parentData) return { ok: true, missing: 0 }

      const expectedCount = parentData[countField] || 0
      if (expectedCount === 0) return { ok: true, missing: 0 }

      // 本地子记录数
      const localChildren = await loadChildren(filterValue)
      const localCount = localChildren.length

      if (localCount >= expectedCount) return { ok: true, missing: 0 }

      // 缺失，尝试通过 idsEndpoint 获取服务端 ID 集合
      if (!idsEndpoint || !fetchFn) {
        return { ok: false, missing: expectedCount - localCount }
      }

      try {
        const resp = await fetchFn(`${idsEndpoint}?${filterField}=${encodeURIComponent(filterValue)}`)
        if (!resp.ok) return { ok: false, missing: expectedCount - localCount }

        const { ids: serverIds } = await resp.json()
        const localIdSet = new Set(localChildren.map(r => r[childIdField]))
        const missingIds = serverIds.filter(id => !localIdSet.has(id))

        return { ok: missingIds.length === 0, missing: missingIds.length, missingIds }
      } catch {
        return { ok: false, missing: expectedCount - localCount }
      }
    } catch {
      return { ok: true, missing: 0 }
    }
  }

  /**
   * 分批工具函数
   * @param {Array} items - 要分批的数组
   * @param {number} [size] - 每批大小
   * @returns {Array<Array>} 分批后的二维数组
   */
  function splitBatches(items, size = batchSize) {
    const batches = []
    for (let i = 0; i < items.length; i += size) {
      batches.push(items.slice(i, i + size))
    }
    return batches
  }

  return {
    loadChildren,
    saveChild,
    addChild,
    deleteChild,
    replaceAll,
    clearAll,
    updateParentMeta,
    checkIntegrity,
    calcChecksum,
    splitBatches,
  }
}

export default createChildSync
