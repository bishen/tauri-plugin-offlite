/**
 * Offlite 同步引擎（JS SDK）
 *
 * 设计参考：WatermelonDB（pull-then-push）+ RxDB（checkpoint + stream）+ PowerSync（oplog）
 *
 * 核心原则：
 * 1. 本地优先：所有写操作先写 SQLite，再异步推送
 * 2. pull-then-push：先拉后推，避免覆盖服务端新数据
 * 3. _status 列追踪变更：synced/created/updated/deleted，无需 changelog 表
 * 4. SSE 实时 + 定时 sync 兜底：三级降级（realtime → polling → offline）
 * 5. 数据不丢失：推送失败保留 _status，下次重试
 * 6. Token 刷新：401 自动刷新 + 重试
 * 7. Android 兼容：EventSource 检测 + 降级策略
 */

import { invoke } from '@tauri-apps/api/core'
import { listen } from '@tauri-apps/api/event'
import { encode, decode } from '@msgpack/msgpack'

// ============ 常量 ============

const MAX_SSE_FAILURES = 3
const DEFAULT_SYNC_INTERVAL = 30000
const ANDROID_SYNC_INTERVAL = 15000
const BACKOFF_INITIAL = 1000
const BACKOFF_MAX = 60000
const BACKOFF_FACTOR = 2
const PUSH_BATCH_SIZE = 50
const PULL_PAGE_SIZE = 100

// ============ 平台检测 ============

/** 检测是否为 Android WebView 环境 */
function isAndroid() {
  return typeof navigator !== 'undefined' &&
    /Android/i.test(navigator.userAgent)
}

/** 检测 EventSource 是否可用 */
function hasEventSource() {
  return typeof EventSource !== 'undefined'
}

// ============ 同步引擎 ============

/**
 * 创建同步引擎实例
 *
 * @param {Object} config
 * @param {string} config.baseUrl - 服务端 API 地址
 * @param {string} config.token - JWT Token
 * @param {string} config.appName - 应用名前缀（默认 'default'）
 * @param {string} config.syncMode - 同步模式 'user'|'company'|'project'
 * @param {number} config.syncInterval - 轮询间隔毫秒（默认 30000）
 * @param {Function} config.onTokenRefresh - Token 刷新回调，返回 Promise<string>
 * @returns {SyncEngine}
 */
export function createSyncEngine(config) {
  const {
    baseUrl,
    appName = 'default',
    syncMode = 'project',
    syncInterval = DEFAULT_SYNC_INTERVAL,
    onTokenRefresh = null,
  } = config

  let token = config.token || ''
  let projectId = null
  let dbId = null  // SQLite 数据库标识（可能和 projectId/filter_key 不同）
  let tables = []
  let tableEntityTypes = {}  // table → entityType 映射
  let sseSource = null
  let syncTimer = null
  let sseFailCount = 0
  let retryAttempt = 0
  let sseRetryTimer = null
  let stopped = true
  let syncing = false
  let checkpointTableReady = false
  const listeners = new Set()

  // 事件监听器清理函数
  let onlineHandler = null
  let offlineHandler = null
  let unlistenResumed = null

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

  // ============ Task 3.3: fetchWithAuth - Token 刷新与 401 重试 ============

  /**
   * 带认证的 fetch 封装
   * 拦截 401 响应，调用 onTokenRefresh 获取新 token 后重试一次
   * Token 刷新失败时降级为 offline
   */
  async function fetchWithAuth(url, options = {}) {
    const resp = await fetch(url, options)

    if (resp.status === 401 && onTokenRefresh) {
      try {
        const newToken = await onTokenRefresh()
        if (newToken) {
          token = newToken
          // 用新 token 重试请求
          const retryHeaders = { ...options.headers, Authorization: `Bearer ${token}` }
          return await fetch(url, { ...options, headers: retryHeaders })
        }
      } catch (err) {
        console.error('[Sync] Token refresh failed:', err.message)
        // Token 刷新失败 → 降级为 offline
        setState({ mode: 'offline', error: 'Token refresh failed' })
        closeSse()
        stopSyncTimer()
      }
    }

    return resp
  }

  // ============ 本地数据操作 ============

  /** 获取某表所有未同步的记录（限 PUSH_BATCH_SIZE 条） */
  async function getUnsyncedDocs(table) {
    return await invoke('plugin:offlite|db_query', {
      projectId: dbId,
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
      projectId: dbId,
      sql: `UPDATE ${table} SET _status = 'synced' WHERE _id IN (${ph})`,
      params: ids,
    })
  }

  /** 应用服务端拉取的变更到本地 */
  async function applyPulledChanges(table, changes) {
    // 确保表存在（SSE 推送可能在表建好之前到达）
    try {
      await invoke('plugin:offlite|db_create_tables', {
        projectId: dbId,
        schemas: [{ name: table, json_indexes: [] }]
      })
    } catch (_) { /* 表已存在则忽略 */ }

    for (const c of changes) {
      if (c.deleted) {
        // 软删除（不覆盖本地未推送的修改）
        await invoke('plugin:offlite|db_execute', {
          projectId: dbId,
          sql: `UPDATE ${table} SET _deleted = 1, updatedAt = ?, _status = 'synced'
                WHERE _id = ? AND _status = 'synced'`,
          params: [c.updatedAt, c.doc_id],
        })
      } else {
        const dataJson = typeof c.data === 'string' ? c.data : JSON.stringify(c.data || {})
        // 先检查本地是否有未推送的版本
        const local = await invoke('plugin:offlite|db_query', {
          projectId: dbId,
          sql: `SELECT _status FROM ${table} WHERE _id = ?`,
          params: [c.doc_id],
        })
        const localStatus = local?.[0]?._status

        if (localStatus && localStatus !== 'synced') {
          // 本地有未推送的修改，跳过（push 时由服务端 LWW 决定）
          continue
        }

        await invoke('plugin:offlite|db_execute', {
          projectId: dbId,
          sql: `INSERT OR REPLACE INTO ${table}
                (_id, uid, companyId, p_id, createdAt, updatedAt, _deleted, _version, _status, data)
                VALUES (?, ?, ?, ?, ?, ?, 0, 1, 'synced', ?)`,
          params: [
            c.doc_id,
            c.uid ?? null, c.company_id ?? null,
            c.p_id ?? (c.data?.p_id) ?? null,
            c.createdAt || c.updatedAt, c.updatedAt,
            dataJson,
          ],
        })
        state.docs_pulled++
      }
    }
  }

  // ============ Task 3.1: Checkpoint 表自动创建 ============

  /**
   * 确保 _sync_checkpoint 表存在（首次调用时自动创建）
   */
  async function ensureCheckpointTable() {
    if (checkpointTableReady) return
    try {
      await invoke('plugin:offlite|db_execute', {
        projectId: 'global',
        sql: `CREATE TABLE IF NOT EXISTS _sync_checkpoint (
          table_name  TEXT NOT NULL,
          sync_mode   TEXT NOT NULL,
          filter_key  TEXT NOT NULL,
          last_sync_at TEXT NOT NULL,
          PRIMARY KEY (table_name, sync_mode, filter_key)
        )`,
        params: [],
      })
      checkpointTableReady = true
    } catch (err) {
      // 表可能已存在，标记为就绪
      if (String(err).includes('already exists') || String(err).includes('table')) {
        checkpointTableReady = true
      } else {
        console.error('[Sync] Failed to create checkpoint table:', err)
      }
    }
  }

  /** 读取 checkpoint */
  async function getCheckpoint(table) {
    try {
      await ensureCheckpointTable()
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
    await ensureCheckpointTable()
    await invoke('plugin:offlite|db_execute', {
      projectId: 'global',
      sql: `INSERT OR REPLACE INTO _sync_checkpoint (table_name, sync_mode, filter_key, last_sync_at)
            VALUES (?, ?, ?, ?)`,
      params: [table, syncMode, projectId, serverTime],
    })
  }

  // ============ 网络操作 ============

  function buildHeaders() {
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
    const entityType = tableEntityTypes[table] || ''
    const url = `${baseUrl}/offlite/${table}/pull?since=${encodeURIComponent(since)}&mode=${syncMode}&filter_key=${encodeURIComponent(projectId)}&app=${appName}${entityType ? `&entity_type=${entityType}` : ''}`

    const resp = await fetchWithAuth(url, { headers: buildHeaders() })
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
      data.p_id = doc.p_id
      return { op: 'upsert', doc_id: doc._id, data, updatedAt: doc.updatedAt }
    })

    const entityType = tableEntityTypes[table] || undefined
    const encoded = encode({ changes, syncMode, entityType })
    const body = new Blob([encoded], { type: 'application/sjs' })
    const pushUrl = `${baseUrl}/offlite/${table}/push`
    const resp = await fetchWithAuth(pushUrl, { method: 'POST', headers: buildHeaders(), body })
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
          projectId: dbId,
          sql: `UPDATE ${table} SET data = ?, updatedAt = ?, _status = 'synced' WHERE _id = ?`,
          params: [dataJson, conflict.server_updated_at, conflict.doc_id],
        })
      }
    }

    return { pushed: syncedIds.length, conflicts: (result.conflicts || []).length }
  }

  // ============ Task 3.5: synchronize() 互斥执行与错误恢复 ============

  /**
   * 执行一次完整同步：pull-then-push
   * - syncing 标志防止并发执行
   * - 每张表独立 try/catch，单表失败不影响其他表
   * - 错误记录到 state.error
   */
  async function synchronize() {
    if (syncing || stopped) return
    syncing = true
    setState({ syncing: true })

    const errors = []

    try {
      // 1. PULL：拉取所有表的服务端变更
      for (const table of tables) {
        try {
          let hasMore = true
          while (hasMore) {
            const result = await pullTable(table)
            hasMore = result.hasMore
          }
        } catch (err) {
          const msg = err?.message || String(err)
          console.error(`[Sync] Pull ${table} error:`, msg)
          errors.push(`Pull ${table}: ${msg}`)
        }
      }

      // 2. PUSH：推送所有表的本地变更
      for (const table of tables) {
        try {
          let hasMore = true
          while (hasMore) {
            const docs = await getUnsyncedDocs(table)
            if (!docs.length) break
            await pushTable(table)
            hasMore = docs.length >= PUSH_BATCH_SIZE
          }
        } catch (err) {
          const msg = err?.message || String(err)
          console.error(`[Sync] Push ${table} error:`, msg)
          errors.push(`Push ${table}: ${msg}`)
        }
      }

      setState({
        syncing: false,
        error: errors.length ? errors.join('; ') : null,
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
        projectId: dbId,
        sql: `SELECT COUNT(*) as cnt FROM ${table} WHERE _status != 'synced'`,
        params: [],
      })
      if (rows?.[0]?.cnt > 0) return true
    }
    return false
  }

  // ============ Task 3.4: SSE 实时流 + Android 兼容性 ============

  /** Android 环境下 SSE 最大失败次数（降级更快） */
  const androidMaxSseFailures = 1

  /** 获取当前环境的 SSE 最大失败次数 */
  function getMaxSseFailures() {
    return isAndroid() ? androidMaxSseFailures : MAX_SSE_FAILURES
  }

  /** 获取当前环境的轮询间隔 */
  function getSyncInterval() {
    return isAndroid() ? ANDROID_SYNC_INTERVAL : syncInterval
  }

  function connectSSE() {
    if (stopped || sseSource) return

    // Task 3.4: 检测 EventSource 是否可用
    if (!hasEventSource()) {
      console.warn('[Sync] EventSource not available, falling back to polling')
      setState({ mode: 'polling', sse_connected: false })
      startSyncTimer()
      return
    }

    const sseUrl = `${baseUrl}/offlite/sse?token=${encodeURIComponent(token)}&mode=${syncMode}&filter_key=${encodeURIComponent(projectId)}&app=${appName}`

    try {
      sseSource = new EventSource(sseUrl)

      sseSource.onopen = () => {
        sseFailCount = 0
        retryAttempt = 0
        setState({ mode: 'realtime', sse_connected: true, error: null })
        // SSE 连接成功后停止定时同步
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
          console.warn('[Sync] SSE change skipped:', err?.message || err)
        }
      })

      sseSource.addEventListener('heartbeat', () => {
        // heartbeat 确认连接存活，无需额外操作
      })

      sseSource.onerror = () => {
        sseFailCount++
        closeSse()

        const maxFailures = getMaxSseFailures()

        if (sseFailCount >= maxFailures) {
          // 超过阈值，降级为轮询
          setState({ mode: 'polling', sse_connected: false })
          startSyncTimer()
        } else {
          // 指数退避重连
          const delay = Math.min(BACKOFF_INITIAL * Math.pow(BACKOFF_FACTOR, retryAttempt), BACKOFF_MAX)
          retryAttempt++
          sseRetryTimer = setTimeout(() => connectSSE(), delay)
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
    const interval = getSyncInterval()
    syncTimer = setInterval(() => synchronize(), interval)
  }

  function stopSyncTimer() {
    if (syncTimer) { clearInterval(syncTimer); syncTimer = null }
  }

  // ============ Task 3.6: pushChanges() offline 模式跳过 ============

  /**
   * 写入后立即推送单表变更（Write-Through）
   * - offline 模式下直接返回，不发起网络请求
   * - 由 db.js 的 add/update/remove/addBulk 调用
   */
  async function pushChanges(tableName) {
    if (stopped || state.mode === 'offline') return
    if (!tables.includes(tableName)) {
      console.warn(`[Sync] pushChanges 跳过: ${tableName} 不在同步表列表中`, tables)
      return
    }
    try {
      console.log(`[Sync] pushChanges: ${tableName}`)
      await pushTable(tableName)
      emit()
    } catch (err) {
      console.error(`[Sync] pushChanges ${tableName}:`, err.message)
      // 推送失败不影响本地操作，_status 保持未同步，下次 sync 重试
    }
  }

  // ============ Task 3.2: 网络状态检测与 online/offline 事件监听 ============

  /** 设置网络状态监听器 */
  function setupNetworkListeners() {
    if (typeof window === 'undefined' || typeof navigator === 'undefined') return

    onlineHandler = () => {
      console.info('[Sync] Network online, resuming sync')
      // 网络恢复 → 立即同步 + 尝试 SSE
      synchronize().then(() => {
        if (!stopped && !sseSource) {
          connectSSE()
        }
      }).catch(() => {})
    }

    offlineHandler = () => {
      console.info('[Sync] Network offline')
      // 网络断开 → offline 模式，停止定时器和 SSE
      closeSse()
      stopSyncTimer()
      clearSseRetryTimer()
      setState({ mode: 'offline', sse_connected: false })
    }

    window.addEventListener('online', onlineHandler)
    window.addEventListener('offline', offlineHandler)
  }

  /** 移除网络状态监听器 */
  function removeNetworkListeners() {
    if (typeof window === 'undefined') return
    if (onlineHandler) {
      window.removeEventListener('online', onlineHandler)
      onlineHandler = null
    }
    if (offlineHandler) {
      window.removeEventListener('offline', offlineHandler)
      offlineHandler = null
    }
  }

  /** 清除 SSE 重连定时器 */
  function clearSseRetryTimer() {
    if (sseRetryTimer) { clearTimeout(sseRetryTimer); sseRetryTimer = null }
  }

  // ============ Task 3.4: Android resumed 事件监听 ============

  /** 设置 Tauri resumed 事件监听（Android 从后台恢复） */
  async function setupResumedListener() {
    try {
      unlistenResumed = await listen('resumed', () => {
        console.info('[Sync] App resumed from background')
        if (!stopped) {
          synchronize().catch(() => {})
        }
      })
    } catch (_) {
      // listen 可能在非 Tauri 环境下失败，忽略
    }
  }

  /** 移除 resumed 事件监听 */
  function removeResumedListener() {
    if (unlistenResumed) {
      unlistenResumed()
      unlistenResumed = null
    }
  }

  // ============ 生命周期 ============

  /**
   * 启动同步
   * @param {string} pid - 项目 ID
   * @param {string[]|Array<{name: string, entityType?: string}>} tableNames - 表名数组或表配置数组
   * @param {Object} options
   */
  async function start(pid, tableNames, options = {}) {
    projectId = pid
    dbId = options.dbId || pid  // 默认和 projectId 相同

    // 支持两种格式：纯字符串数组 或 带 entityType 的配置数组
    tableEntityTypes = {}
    if (Array.isArray(tableNames) && tableNames.length > 0 && typeof tableNames[0] === 'object') {
      tables = tableNames.map(t => t.name)
      for (const t of tableNames) {
        if (t.entityType) tableEntityTypes[t.name] = t.entityType
      }
    } else {
      tables = tableNames || []
    }

    stopped = false
    sseFailCount = 0
    retryAttempt = 0
    checkpointTableReady = false

    setState({ active: true, mode: 'offline', error: null })

    // Task 3.2: 设置网络状态监听
    setupNetworkListeners()

    // Task 3.4: 设置 Android resumed 监听
    setupResumedListener()

    // Task 3.2: 检查初始网络状态
    const isOnline = typeof navigator !== 'undefined' ? navigator.onLine : true

    if (!isOnline) {
      // 离线状态，不执行同步
      setState({ mode: 'offline' })
      emit()
      return
    }

    // 1. 执行一次完整的 pull-then-push 同步
    await synchronize()

    // 2. 尝试 SSE 实时连接（connectSSE 内部会检测 EventSource 可用性）
    if (!stopped) {
      connectSSE()
    }

    // 3. 如果 SSE 未连接且不在 realtime 模式，确保有轮询兜底
    if (!stopped && state.mode !== 'realtime' && !syncTimer) {
      startSyncTimer()
    }

    emit()
  }

  function stop() {
    stopped = true
    closeSse()
    stopSyncTimer()
    clearSseRetryTimer()

    // Task 3.2: 移除网络监听器
    removeNetworkListeners()

    // Task 3.4: 移除 resumed 监听器
    removeResumedListener()

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
