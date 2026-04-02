/**
 * Offlite 同步引擎（JS SDK）
 *
 * 设计参考：WatermelonDB（pull-then-push）+ RxDB（checkpoint + stream）+ PowerSync（oplog）
 *
 * 核心原则：
 * 1. 本地优先：所有写操作先写 SQLite，再异步推送
 * 2. pull-then-push：先拉后推，避免覆盖服务端新数据
 * 3. _status 列追踪变更：synced/created/updated/deleted，无需 changelog 表
 * 4. SSE 实时 + 定时 sync 兜底：三级降级
 * 5. 数据不丢失：推送失败保留 _status，下次重试
 */

import { invoke } from '@tauri-apps/api/core'
import { encode, decode } from '@msgpack/msgpack'

// ============ 常量 ============

const MAX_SSE_FAILURES = 3
const DEFAULT_SYNC_INTERVAL = 30000
const BACKOFF_INITIAL = 1000
const BACKOFF_MAX = 60000
const BACKOFF_FACTOR = 2
const PUSH_BATCH_SIZE = 50
const PULL_PAGE_SIZE = 100

// ============ 同步引擎 ============

/**
 * 创建同步引擎实例
 */
export function createSyncEngine(config) {
  const {
    baseUrl,
    appName = 'default',
    syncMode = 'project',
    syncInterval = DEFAULT_SYNC_INTERVAL,
  } = config

  let token = config.token || ''
  let projectId = null
  let tables = []
  let sseSource = null
  let syncTimer = null
  let sseFailCount = 0
  let retryAttempt = 0
  let stopped = true
  let syncing = false
  const listeners = new Set()

  const state = {
    active: false,
    mode: 'offline',
    sse_connected: false,
    syncing: false,
    error: null,
    docs_pushed: 0,
    docs_pulled: 0,
    last_synced_at: null,
  }

  function emit() {
    const s = { ...state }
    for (const fn of listeners) { try { fn(s) } catch (_) {} }
  }

  function setState(patch) {
    Object.assign(state, patch)
    emit()
  }

  function updateToken(t) { token = t }

  // ============ 本地数据操作 ============

  /** 获取某表所有未同步的记录 */
  async function getUnsyncedDocs(table) {
    return await invoke('plugin:offlite|db_query', {
      projectId,
      sql: `SELECT _id, uid, companyId, p_id, createdAt, updatedAt, _deleted, _version, _status, data
            FROM ${table} WHERE _status != 'synced' LIMIT ${PUSH_BATCH_SIZE}`,
      params: [],
    }) || []
  }

  /** 标记记录为已同步 */
  async function markSynced(table, ids) {
    if (!ids.length) return
    const ph = ids.map(() => '?').join(',')
    await invoke('plugin:offlite|db_execute', {
      projectId,
      sql: `UPDATE ${table} SET _status = 'synced' WHERE _id IN (${ph})`,
      params: ids,
    })
  }

  /** 应用服务端拉取的变更到本地 */
  async function applyPulledChanges(table, changes) {
    for (const c of changes) {
      if (c.deleted) {
        // 软删除（不覆盖本地未推送的修改）
        await invoke('plugin:offlite|db_execute', {
          projectId,
          sql: `UPDATE ${table} SET _deleted = 1, updatedAt = ?, _status = 'synced'
                WHERE _id = ? AND _status = 'synced'`,
          params: [c.updatedAt, c.doc_id],
        })
      } else {
        const dataJson = typeof c.data === 'string' ? c.data : JSON.stringify(c.data || {})
        // INSERT OR REPLACE，但不覆盖本地未推送的修改
        // 先检查本地是否有未推送的版本
        const local = await invoke('plugin:offlite|db_query', {
          projectId,
          sql: `SELECT _status FROM ${table} WHERE _id = ?`,
          params: [c.doc_id],
        })
        const localStatus = local?.[0]?._status

        if (localStatus && localStatus !== 'synced') {
          // 本地有未推送的修改，跳过（push 时由服务端 LWW 决定）
          continue
        }

        await invoke('plugin:offlite|db_execute', {
          projectId,
          sql: `INSERT OR REPLACE INTO ${table}
                (_id, uid, companyId, p_id, createdAt, updatedAt, _deleted, _version, _status, data)
                VALUES (?, ?, ?, ?, ?, ?, 0, 1, 'synced', ?)`,
          params: [
            c.doc_id,
            c.uid ?? null, c.company_id ?? null, c.p_id ?? null,
            c.createdAt || c.updatedAt, c.updatedAt,
            dataJson,
          ],
        })
        state.docs_pulled++
      }
    }
  }

  /** 读取 checkpoint */
  async function getCheckpoint(table) {
    try {
      const rows = await invoke('plugin:offlite|db_query', {
        projectId: 'global',
        sql: `SELECT last_sync_at FROM _sync_checkpoint
              WHERE table_name = ? AND sync_mode = ? AND filter_key = ?`,
        params: [table, syncMode, projectId],
      })
      return rows?.[0]?.last_sync_at || ''
    } catch (_) { return '' }
  }

  /** 更新 checkpoint */
  async function setCheckpoint(table, serverTime) {
    await invoke('plugin:offlite|db_execute', {
      projectId: 'global',
      sql: `INSERT OR REPLACE INTO _sync_checkpoint (table_name, sync_mode, filter_key, last_sync_at)
            VALUES (?, ?, ?, ?)`,
      params: [table, syncMode, projectId, serverTime],
    })
  }

  // ============ 网络操作 ============

  function headers() {
    return {
      'Content-Type': 'application/sjs',
      'Accept': 'application/sjs',
      'Authorization': `Bearer ${token}`,
      'X-App-Name': appName,
    }
  }

  /** PULL：从服务端拉取变更 */
  async function pullTable(table) {
    const since = await getCheckpoint(table)
    const url = `${baseUrl}/offlite/sync/${table}/pull?since=${encodeURIComponent(since)}&mode=${syncMode}&filter_key=${encodeURIComponent(projectId)}&app=${appName}`

    const resp = await fetch(url, { headers: headers() })
    if (!resp.ok) throw new Error(`Pull ${table}: ${resp.status}`)

    const bytes = new Uint8Array(await resp.arrayBuffer())
    const result = decode(bytes)

    if (result.changes?.length) {
      await applyPulledChanges(table, result.changes)
    }

    if (result.server_time) {
      await setCheckpoint(table, result.server_time)
    }

    return { pulled: result.changes?.length || 0, hasMore: result.has_more }
  }

  /** PUSH：推送本地变更到服务端 */
  async function pushTable(table) {
    const docs = await getUnsyncedDocs(table)
    if (!docs.length) return { pushed: 0 }

    const changes = docs.map(doc => {
      const status = doc._status
      if (status === 'deleted' || doc._deleted === 1) {
        return { op: 'delete', doc_id: doc._id, updatedAt: doc.updatedAt }
      }
      let data = {}
      try { data = typeof doc.data === 'string' ? JSON.parse(doc.data) : (doc.data || {}) } catch (_) {}
      // 合并元数据到 data 中供服务端存储
      data.p_id = doc.p_id
      return { op: 'upsert', doc_id: doc._id, data, updatedAt: doc.updatedAt }
    })

    const body = encode({ changes, syncMode })
    const url = `${baseUrl}/offlite/sync/${table}/push`
    const resp = await fetch(url, { method: 'POST', headers: headers(), body })
    if (!resp.ok) throw new Error(`Push ${table}: ${resp.status}`)

    const bytes = new Uint8Array(await resp.arrayBuffer())
    const result = decode(bytes)

    // 标记已接受的为 synced
    const accepted = new Set(result.accepted || [])
    const syncedIds = docs.filter(d => accepted.has(d._id)).map(d => d._id)
    if (syncedIds.length) {
      await markSynced(table, syncedIds)
      state.docs_pushed += syncedIds.length
    }

    // 冲突处理：服务端版本更新，用服务端数据覆盖本地
    for (const conflict of (result.conflicts || [])) {
      if (conflict.server_data) {
        const dataJson = JSON.stringify(conflict.server_data)
        await invoke('plugin:offlite|db_execute', {
          projectId,
          sql: `UPDATE ${table} SET data = ?, updatedAt = ?, _status = 'synced' WHERE _id = ?`,
          params: [dataJson, conflict.server_updated_at, conflict.doc_id],
        })
      }
    }

    return { pushed: syncedIds.length, conflicts: (result.conflicts || []).length }
  }

  // ============ 核心同步流程（WatermelonDB 风格） ============

  /**
   * 执行一次完整同步：pull-then-push
   * 先拉后推，保证不覆盖服务端新数据
   */
  async function synchronize() {
    if (syncing || stopped) return
    syncing = true
    setState({ syncing: true })

    try {
      // 1. PULL：拉取所有表的服务端变更
      for (const table of tables) {
        let hasMore = true
        while (hasMore) {
          const result = await pullTable(table)
          hasMore = result.hasMore
        }
      }

      // 2. PUSH：推送所有表的本地变更
      for (const table of tables) {
        let hasMore = true
        while (hasMore) {
          const docs = await getUnsyncedDocs(table)
          if (!docs.length) break
          await pushTable(table)
          hasMore = docs.length >= PUSH_BATCH_SIZE
        }
      }

      setState({
        syncing: false,
        error: null,
        last_synced_at: new Date().toISOString(),
      })
    } catch (err) {
      console.error('[Sync] synchronize error:', err.message)
      setState({ syncing: false, error: err.message })
    } finally {
      syncing = false
    }
  }

  /** 检查是否有未同步的变更 */
  async function hasUnsyncedChanges() {
    for (const table of tables) {
      const rows = await invoke('plugin:offlite|db_query', {
        projectId,
        sql: `SELECT COUNT(*) as cnt FROM ${table} WHERE _status != 'synced'`,
        params: [],
      })
      if (rows?.[0]?.cnt > 0) return true
    }
    return false
  }

  // ============ SSE 实时流（RxDB 风格） ============

  function connectSSE() {
    if (stopped || sseSource) return

    const url = `${baseUrl}/offlite/sync/sse?token=${encodeURIComponent(token)}&mode=${syncMode}&filter_key=${encodeURIComponent(projectId)}&app=${appName}`

    try {
      sseSource = new EventSource(url)

      sseSource.onopen = () => {
        sseFailCount = 0
        retryAttempt = 0
        setState({ mode: 'realtime', sse_connected: true, error: null })
        // SSE 连接成功后停止定时同步（SSE 负责实时拉取）
        stopSyncTimer()
      }

      sseSource.addEventListener('change', async (event) => {
        try {
          const bytes = Uint8Array.from(atob(event.data), c => c.charCodeAt(0))
          const { table, changes } = decode(bytes)
          if (table && changes?.length) {
            await applyPulledChanges(table, changes)
            emit()
          }
        } catch (err) {
          console.error('[Sync] SSE change error:', err)
        }
      })

      sseSource.addEventListener('heartbeat', () => {})

      sseSource.onerror = () => {
        sseFailCount++
        closeSse()

        if (sseFailCount >= MAX_SSE_FAILURES) {
          setState({ mode: 'polling', sse_connected: false })
          startSyncTimer()
        } else {
          const delay = Math.min(BACKOFF_INITIAL * Math.pow(BACKOFF_FACTOR, retryAttempt), BACKOFF_MAX)
          retryAttempt++
          setTimeout(() => connectSSE(), delay)
        }
      }
    } catch (_) {
      setState({ mode: 'polling', sse_connected: false })
      startSyncTimer()
    }
  }

  function closeSse() {
    if (sseSource) { sseSource.close(); sseSource = null }
    state.sse_connected = false
  }

  // ============ 定时同步兜底 ============

  function startSyncTimer() {
    stopSyncTimer()
    if (stopped) return
    syncTimer = setInterval(() => synchronize(), syncInterval)
  }

  function stopSyncTimer() {
    if (syncTimer) { clearInterval(syncTimer); syncTimer = null }
  }

  // ============ Write-through 即时推送 ============

  /**
   * 写入后立即推送单表变更
   * 由 db.js 的 add/update/remove 调用
   */
  async function pushChanges(tableName) {
    if (stopped || state.mode === 'offline') return
    try {
      await pushTable(tableName)
      emit()
    } catch (err) {
      console.error(`[Sync] pushChanges ${tableName}:`, err.message)
      // 推送失败不影响本地操作，_status 保持未同步，下次 sync 重试
    }
  }

  // ============ 生命周期 ============

  async function start(pid, tableNames) {
    projectId = pid
    tables = tableNames || []
    stopped = false
    sseFailCount = 0
    retryAttempt = 0

    setState({ active: true, mode: 'offline', error: null })

    // 1. 执行一次完整的 pull-then-push 同步
    await synchronize()

    // 2. 尝试 SSE 实时连接
    connectSSE()

    // 3. 如果 SSE 失败，startSyncTimer 会在 onerror 里启动
    emit()
  }

  function stop() {
    stopped = true
    closeSse()
    stopSyncTimer()
    setState({ active: false, mode: 'offline', sse_connected: false, syncing: false })
  }

  return {
    start,
    stop,
    synchronize,
    pushChanges,
    hasUnsyncedChanges,
    updateToken,
    getState: () => ({ ...state }),
    onStateChange: (fn) => { listeners.add(fn); return () => listeners.delete(fn) },
  }
}
