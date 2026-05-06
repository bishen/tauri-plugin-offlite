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

  /**
   * 标准化 pull 数据的 key：camelCase → snake_case
   * 服务端标准实体 pull 路径会做 snakeToCamel 转换，客户端需要转回来
   */
  function normalizePulledKeys(data) {
    if (!data || typeof data !== 'object') return data
    const result = {}
    for (const [key, value] of Object.entries(data)) {
      // camelCase → snake_case
      const snakeKey = key.replace(/[A-Z]/g, letter => `_${letter.toLowerCase()}`)
      result[snakeKey] = value
    }
    return result
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

    const conflicts = [] // 收集"删除 vs 编辑"冲突

    for (const c of changes) {
      if (c.deleted) {
        // 服务端删除：无论本地状态如何，都执行软删除
        // （如果本地有未推送的修改，服务端的删除优先 — 数据已被其他用户删除）
        await invoke('plugin:offlite|db_execute', {
          projectId: dbId,
          sql: `UPDATE ${table} SET _deleted = 1, updatedAt = ?, _status = 'synced'
                WHERE _id = ?`,
          params: [c.updatedAt, c.doc_id],
        })
      } else {
        let serverData = typeof c.data === 'string' ? JSON.parse(c.data) : (c.data || {})
        // 标准化 key：服务端 pull 可能返回 camelCase，统一转为 snake_case 存储
        serverData = normalizePulledKeys(serverData)
        // 先检查本地是否有未推送的版本
        const local = await invoke('plugin:offlite|db_query', {
          projectId: dbId,
          sql: `SELECT _status, data FROM ${table} WHERE _id = ?`,
          params: [c.doc_id],
        })
        const localStatus = local?.[0]?._status

        if (localStatus && localStatus !== 'synced') {
          // 本地有未推送的修改 → 用服务端数据覆盖本地 data（服务端更新优先）
          // _status 保持不变（本地修改仍需 push，push 时由服务端 LWW 决定最终版本）
          try {
            const dataJson = JSON.stringify(serverData)
            await invoke('plugin:offlite|db_execute', {
              projectId: dbId,
              sql: `UPDATE ${table} SET data = ?, updatedAt = ? WHERE _id = ?`,
              params: [dataJson, c.updatedAt, c.doc_id],
            })
            state.docs_pulled++
          } catch (mergeErr) {
            console.warn('[Sync] 覆盖本地数据失败，跳过:', c.doc_id, mergeErr?.message)
          }
          continue
        }

        const dataJson = JSON.stringify(serverData)
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

    // 通知 UI 层存在"删除 vs 编辑"冲突，由用户决定
    if (conflicts.length > 0) {
      try {
        const { emit: tauriEmit } = await import('@tauri-apps/api/event')
        await tauriEmit('sync-conflict', { table, conflicts })
      } catch (_) {}
    }

    // 通知 App 层有新数据到达（用于 UI 实时刷新）
    if (changes.length > 0) {
      try {
        const { emit: tauriEmit } = await import('@tauri-apps/api/event')
        const upsertedDocIds = changes.filter(c => !c.deleted).map(c => c.doc_id)
        const deletedDocIds = changes.filter(c => c.deleted).map(c => c.doc_id)
        await tauriEmit('sync-data-changed', {
          table,
          docIds: upsertedDocIds,
          deletedDocIds,
          action: 'pull',
        })
      } catch (_) {
        // 非 Tauri 环境忽略
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
      // 只在有实际变更时才更新 checkpoint
      // 防止 server_time 跳过了还未返回的并发写入记录
      if (result.server_time) {
        await setCheckpoint(table, result.server_time)
      }
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

  // ============ WebSocket 实时通知 ============

  let wsConnection = null
  let wsRetryTimer = null
  let wsFailCount = 0

  // 防抖：200ms 窗口内合并同一表的多个通知
  const pendingPulls = new Map() // table → timeout
  const DEBOUNCE_MS = 200

  function connectWebSocket() {
    if (stopped || wsConnection) return

    const wsUrl = baseUrl.replace(/^http/, 'ws') + '/offlite/ws?app=' + appName
    let wsAuthenticated = false

    try {
      wsConnection = new WebSocket(wsUrl)

      wsConnection.onopen = () => {
        // 连接建立后发送 auth 消息（不在 URL 中暴露 token）
        wsConnection.send(JSON.stringify({ type: 'auth', token }))
      }

      wsConnection.onmessage = async (event) => {
        try {
          // 支持文本消息（auth 响应）和二进制消息（数据通知）
          if (typeof event.data === 'string') {
            const msg = JSON.parse(event.data)
            if (msg.type === 'auth_ok') {
              wsAuthenticated = true
              wsFailCount = 0
              retryAttempt = 0
              setState({ mode: 'realtime', sse_connected: true, error: null })
              stopSyncTimer()
              // WS 认证成功后兜底推送一次所有未同步记录
              // （防止 pushChanges 并发丢失、或上次会话异常退出留下的未推送数据）
              for (const tbl of tables) {
                pushChanges(tbl).catch(() => {})
              }
              // 启动低频 push 兜底定时器（realtime 模式下不启动全量 sync 定时器）
              startPushBackupTimer()
              return
            }
            if (msg.type === 'auth_fail') {
              console.error('[Sync] WS auth failed:', msg.error)
              wsConnection.close(4001, 'Auth failed')
              return
            }
            // 编辑锁等 JSON 消息
            if (msg.type === 'edit_lock') {
              try {
                const { emit: tauriEmit } = await import('@tauri-apps/api/event')
                await tauriEmit('edit-lock-changed', msg)
              } catch (_) {}
              return
            }
            return
          }

          // 二进制消息（MessagePack 编码的数据通知）
          if (!wsAuthenticated) return // 未认证前忽略数据消息

          let bytes
          if (event.data instanceof ArrayBuffer) {
            bytes = new Uint8Array(event.data)
          } else if (event.data instanceof Blob) {
            bytes = new Uint8Array(await event.data.arrayBuffer())
          } else {
            console.warn('[Sync] WS unknown data type:', typeof event.data, event.data)
            return
          }
          const msg = decode(bytes)
          console.log('[Sync] WS 收到通知:', msg.table, msg)

          const { table: notifyTable, syncMode: notifySyncMode, filterKey: notifyFilterKey } = msg

          if (!notifyTable) return
          // 只处理当前正在同步的表
          if (!tables.includes(notifyTable)) return

          // 防抖：合并 200ms 内同一表的多个通知
          if (pendingPulls.has(notifyTable)) {
            clearTimeout(pendingPulls.get(notifyTable))
          }
          pendingPulls.set(notifyTable, setTimeout(async () => {
            pendingPulls.delete(notifyTable)
            try {
              let hasMore = true
              while (hasMore) {
                const result = await pullTable(notifyTable)
                hasMore = result.hasMore
              }
              emit()
            } catch (err) {
              console.warn('[Sync] WS pull failed for ' + notifyTable + ':', err?.message || err)
            }
          }, DEBOUNCE_MS))
        } catch (err) {
          console.warn('[Sync] WS message skipped:', err?.message || err)
        }
      }

      wsConnection.onclose = () => {
        wsConnection = null
        state.sse_connected = false
        wsFailCount++

        if (stopped) return

        if (wsFailCount >= MAX_SSE_FAILURES) {
          setState({ mode: 'polling', sse_connected: false })
          startSyncTimer()
        } else {
          // 指数退避重连
          const delay = Math.min(BACKOFF_INITIAL * Math.pow(BACKOFF_FACTOR, retryAttempt), BACKOFF_MAX)
          retryAttempt++
          wsRetryTimer = setTimeout(() => connectWebSocket(), delay)
        }
      }

      wsConnection.onerror = () => {
        // onerror 后会触发 onclose，在 onclose 中处理重连
      }
    } catch (_) {
      setState({ mode: 'polling', sse_connected: false })
      startSyncTimer()
    }
  }

  function closeWebSocket() {
    if (wsConnection) {
      wsConnection.close()
      wsConnection = null
    }
    if (wsRetryTimer) {
      clearTimeout(wsRetryTimer)
      wsRetryTimer = null
    }
    // 清除所有待处理的防抖 pull
    for (const timer of pendingPulls.values()) {
      clearTimeout(timer)
    }
    pendingPulls.clear()
    stopPushBackupTimer()
    state.sse_connected = false
  }

  // ============ 定时同步兜底 ============

  function startSyncTimer() {
    stopSyncTimer()
    if (stopped) return
    const interval = isAndroid() ? ANDROID_SYNC_INTERVAL : syncInterval
    syncTimer = setInterval(() => synchronize(), interval)
  }

  function stopSyncTimer() {
    if (syncTimer) { clearInterval(syncTimer); syncTimer = null }
  }

  // ============ Realtime 模式下的 push 兜底定时器 ============
  // WS 连接成功后会停掉全量 sync 定时器（stopSyncTimer），
  // 但仍需一个低频定时器兜底推送未同步记录（防止 pushChanges 并发丢失）
  let pushBackupTimer = null
  const PUSH_BACKUP_INTERVAL = 60000

  function startPushBackupTimer() {
    stopPushBackupTimer()
    if (stopped) return
    pushBackupTimer = setInterval(() => {
      if (stopped || state.mode === 'offline') return
      for (const tbl of tables) {
        pushChanges(tbl).catch(() => {})
      }
    }, PUSH_BACKUP_INTERVAL)
  }

  function stopPushBackupTimer() {
    if (pushBackupTimer) { clearInterval(pushBackupTimer); pushBackupTimer = null }
  }

  // ============ Task 3.6: pushChanges() offline 模式跳过 ============

  /**
   * 写入后立即推送单表变更（Write-Through）
   * - offline 模式下直接返回，不发起网络请求
   * - 由 db.js 的 add/update/remove/addBulk 调用
   * - 循环 push 直到所有未同步记录清空（批量导入场景）
   * - 串行执行：如果已有 push 在进行中，设置 pending 标志，当前循环结束后继续 push
   */
  const pushingTables = new Set()
  const pendingPushTables = new Set()  // 有并发调用时设置，当前循环结束后再跑一次
  async function pushChanges(tableName) {
    if (stopped || state.mode === 'offline') return
    if (!tables.includes(tableName)) {
      console.warn(`[Sync] pushChanges 跳过: ${tableName} 不在同步表列表中`, tables)
      return
    }
    // 已有 push 在进行 → 标记 pending，当前循环结束后会再跑一次
    // （防止并发 add 时，第一次循环的 getUnsyncedDocs 漏掉后入库的记录）
    if (pushingTables.has(tableName)) {
      pendingPushTables.add(tableName)
      return
    }
    pushingTables.add(tableName)
    try {
      // 外层循环：处理 pending 标志
      do {
        pendingPushTables.delete(tableName)
        // 内层循环：push 所有未同步记录
        let hasMore = true
        while (hasMore) {
          const docs = await getUnsyncedDocs(tableName)
          if (!docs.length) break
          await pushTable(tableName)
          hasMore = docs.length >= PUSH_BATCH_SIZE
        }
      } while (pendingPushTables.has(tableName))
      emit()
    } catch (err) {
      console.error(`[Sync] pushChanges ${tableName}:`, err.message)
    } finally {
      pushingTables.delete(tableName)
      pendingPushTables.delete(tableName)
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
        if (!stopped && !wsConnection) {
          connectWebSocket()
        }
      }).catch(() => {})
    }

    offlineHandler = () => {
      console.info('[Sync] Network offline')
      // 网络断开 → offline 模式，停止定时器和 SSE
      closeWebSocket()
      stopSyncTimer()
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

  /** (removed — WebSocket handles reconnect internally) */

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

    // 支持两种格式：纯字符串数组、带 entityType 的配置数组、或混合数组
    tableEntityTypes = {}
    tables = []
    if (Array.isArray(tableNames)) {
      for (const t of tableNames) {
        if (typeof t === 'object' && t.name) {
          tables.push(t.name)
          if (t.entityType) tableEntityTypes[t.name] = t.entityType
        } else if (typeof t === 'string') {
          tables.push(t)
        }
      }
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

    // 2. 建立 WebSocket 实时连接
    if (!stopped) {
      connectWebSocket()
    }

    // 3. 如果 WebSocket 未连接且不在 realtime 模式，确保有轮询兜底
    if (!stopped && state.mode !== 'realtime' && !syncTimer) {
      startSyncTimer()
    }

    emit()
  }

  function stop() {
    stopped = true
    closeWebSocket()
    stopSyncTimer()

    // 移除网络监听器
    removeNetworkListeners()

    // 移除 resumed 监听器
    removeResumedListener()

    setState({ active: false, mode: 'offline', sse_connected: false, syncing: false })
  }

  /** 通过 WebSocket 发送自定义消息（用于编辑锁等），离线时静默失败 */
  function sendMessage(data) {
    if (wsConnection && wsConnection.readyState === WebSocket.OPEN) {
      // 字符串直接发送（文本帧），二进制数据原样发送
      wsConnection.send(data)
    }
    // 离线时 WebSocket 不可用，静默失败
  }

  /** 重置所有表的 checkpoint（强制下次 pull 全量拉取） */
  async function resetCheckpoints() {
    await ensureCheckpointTable()
    for (const table of tables) {
      await invoke('plugin:offlite|db_execute', {
        projectId: 'global',
        sql: `DELETE FROM _sync_checkpoint WHERE table_name = ? AND sync_mode = ? AND filter_key = ?`,
        params: [table, syncMode, projectId],
      })
    }
    console.info('[Sync] 已重置所有 checkpoint，下次 pull 将全量拉取')
  }

  return {
    start,
    stop,
    synchronize,
    pushChanges,
    hasUnsyncedChanges,
    updateToken,
    resetCheckpoints,
    getState: () => ({ ...state }),
    onStateChange: (fn) => { listeners.add(fn); return () => listeners.delete(fn) },
    sendMessage,
  }
}
