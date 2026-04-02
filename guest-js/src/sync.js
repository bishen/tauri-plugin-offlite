/**
 * tauri-plugin-offlite 同步引擎（JS SDK）
 *
 * 实现 write-through 推送 + SSE 实时拉取 + 轮询兜底的三级降级同步。
 * 运行在前端 JS 层，通过 Tauri IPC 操作本地 SQLite，通过 fetch 与服务端通信。
 *
 * @example
 * import { createSyncEngine } from 'tauri-plugin-offlite-api/sync'
 * const engine = createSyncEngine({
 *   baseUrl: 'https://api.example.com',
 *   token: 'jwt_token',
 *   appName: 'survey',
 * })
 * engine.start('project_001', ['planning', 'sample'])
 */

import { invoke } from '@tauri-apps/api/core'
import { encode, decode } from '@msgpack/msgpack'

// ============ 常量 ============

const MAX_SSE_FAILURES = 3
const DEFAULT_POLL_INTERVAL = 30000 // 30s
const BACKOFF_INITIAL = 1000
const BACKOFF_MAX = 60000
const BACKOFF_FACTOR = 2
const PUSH_BATCH_SIZE = 50

// ============ 同步引擎 ============

/**
 * 创建同步引擎实例
 * @param {Object} config
 * @param {string} config.baseUrl - 服务端地址
 * @param {string} config.token - JWT Bearer Token
 * @param {string} config.appName - 应用名前缀（如 'survey'）
 * @param {string} [config.syncMode='project'] - 同步模式
 * @param {number} [config.pollInterval=30000] - 轮询间隔（ms）
 * @returns {Object}
 */
export function createSyncEngine(config) {
  const {
    baseUrl,
    appName = 'default',
    syncMode = 'project',
    pollInterval = DEFAULT_POLL_INTERVAL,
  } = config

  let token = config.token || ''
  let projectId = null
  let tables = []
  let sseSource = null
  let pollTimer = null
  let sseFailCount = 0
  let retryAttempt = 0
  let stopped = true
  const listeners = new Set()

  // 同步状态
  const state = {
    active: false,
    paused: false,
    error: null,
    mode: 'offline', // 'realtime' | 'polling' | 'offline'
    sse_connected: false,
    docs_read: 0,
    docs_written: 0,
  }

  function emitState() {
    const snapshot = { ...state }
    for (const fn of listeners) {
      try { fn(snapshot) } catch (_) {}
    }
  }

  function setState(patch) {
    Object.assign(state, patch)
    emitState()
  }

  // ---- Token 管理 ----

  function updateToken(newToken) {
    token = newToken
  }

  // ---- 变更日志操作（通过 Tauri IPC） ----

  async function getPendingChanges(tableName) {
    const rows = await invoke('plugin:offlite|db_query', {
      projectId,
      sql: `SELECT id, table_name, doc_id, operation, data, timestamp
            FROM _change_log
            WHERE synced = 0 AND table_name = ?
            ORDER BY timestamp ASC
            LIMIT ${PUSH_BATCH_SIZE}`,
      params: [tableName],
    })
    return rows || []
  }

  async function markSynced(changeIds) {
    if (!changeIds.length) return
    const placeholders = changeIds.map(() => '?').join(',')
    await invoke('plugin:offlite|db_execute', {
      projectId,
      sql: `UPDATE _change_log SET synced = 1 WHERE id IN (${placeholders})`,
      params: changeIds,
    })
  }

  async function markSyncError(changeId, error) {
    await invoke('plugin:offlite|db_execute', {
      projectId,
      sql: `UPDATE _change_log SET sync_error = ? WHERE id = ?`,
      params: [error, changeId],
    })
  }

  // ---- 推送（Push） ----

  async function pushTable(tableName) {
    const pending = await getPendingChanges(tableName)
    if (!pending.length) return { pushed: 0, accepted: 0, conflicts: 0 }

    // 转换为推送格式
    const changes = pending.map(row => {
      const op = row.operation === 'DELETE' ? 'delete' : 'upsert'
      let data = null
      if (op === 'upsert' && row.data) {
        try { data = typeof row.data === 'string' ? JSON.parse(row.data) : row.data } catch (_) { data = {} }
      }
      return { op, doc_id: row.doc_id, data, updatedAt: row.timestamp }
    })

    // 编码为 MessagePack
    const body = encode({ changes, syncMode })
    const url = `${baseUrl}/offlite/sync/${tableName}/push`

    try {
      const resp = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/sjs',
          'Authorization': `Bearer ${token}`,
          'X-App-Name': appName,
        },
        body,
      })

      if (!resp.ok) {
        throw new Error(`Push failed: ${resp.status}`)
      }

      const respBytes = new Uint8Array(await resp.arrayBuffer())
      const result = decode(respBytes)

      // 标记已同步
      const acceptedDocIds = new Set(result.accepted || [])
      const acceptedChangeIds = pending
        .filter(r => acceptedDocIds.has(r.doc_id))
        .map(r => r.id)
      await markSynced(acceptedChangeIds)

      // 记录冲突
      for (const conflict of (result.conflicts || [])) {
        const changeRow = pending.find(r => r.doc_id === conflict.doc_id)
        if (changeRow) {
          await markSyncError(changeRow.id, `Conflict: server version newer (${conflict.server_updated_at})`)
        }
      }

      state.docs_written += acceptedChangeIds.length
      return { pushed: changes.length, accepted: acceptedChangeIds.length, conflicts: (result.conflicts || []).length }
    } catch (err) {
      console.error(`[Sync] Push ${tableName} failed:`, err.message)
      return { pushed: 0, accepted: 0, conflicts: 0, error: err.message }
    }
  }

  /** Write-through：写入后立即推送单表 */
  async function pushChanges(tableName) {
    if (stopped || state.mode === 'offline') return
    try {
      await pushTable(tableName)
      emitState()
    } catch (err) {
      console.error(`[Sync] pushChanges ${tableName} error:`, err)
    }
  }

  /** 批量推送所有表的待同步变更 */
  async function pushAll() {
    for (const table of tables) {
      let hasMore = true
      while (hasMore) {
        const result = await pushTable(table)
        hasMore = result.pushed >= PUSH_BATCH_SIZE
      }
    }
  }

  // ---- 拉取（Pull） ----

  async function pullTable(tableName) {
    // 读取 checkpoint
    let since = ''
    try {
      const rows = await invoke('plugin:offlite|db_query', {
        projectId: 'global',
        sql: `SELECT last_sync_at FROM _sync_checkpoint
              WHERE table_name = ? AND sync_mode = ? AND filter_key = ?`,
        params: [tableName, syncMode, projectId],
      })
      if (rows?.length) since = rows[0].last_sync_at || ''
    } catch (_) {}

    const url = `${baseUrl}/offlite/sync/${tableName}/pull?since=${encodeURIComponent(since)}&mode=${syncMode}&filter_key=${encodeURIComponent(projectId)}&app=${appName}`

    try {
      const resp = await fetch(url, {
        headers: {
          'Accept': 'application/sjs',
          'Authorization': `Bearer ${token}`,
          'X-App-Name': appName,
        },
      })

      if (!resp.ok) throw new Error(`Pull failed: ${resp.status}`)

      const respBytes = new Uint8Array(await resp.arrayBuffer())
      const result = decode(respBytes)

      // 应用变更到本地
      for (const change of (result.changes || [])) {
        if (change.deleted) {
          await invoke('plugin:offlite|db_execute', {
            projectId,
            sql: `UPDATE ${tableName} SET _deleted = 1, updatedAt = ? WHERE _id = ?`,
            params: [change.updatedAt, change.doc_id],
          })
        } else {
          const dataJson = typeof change.data === 'string' ? change.data : JSON.stringify(change.data || {})
          await invoke('plugin:offlite|db_execute', {
            projectId,
            sql: `INSERT OR REPLACE INTO ${tableName} (_id, data, updatedAt, _deleted) VALUES (?, ?, ?, 0)`,
            params: [change.doc_id, dataJson, change.updatedAt],
          })
        }
        state.docs_read++
      }

      // 更新 checkpoint
      if (result.server_time) {
        await invoke('plugin:offlite|db_execute', {
          projectId: 'global',
          sql: `INSERT OR REPLACE INTO _sync_checkpoint (table_name, sync_mode, filter_key, last_sync_at)
                VALUES (?, ?, ?, ?)`,
          params: [tableName, syncMode, projectId, result.server_time],
        })
      }

      return { pulled: (result.changes || []).length, hasMore: result.has_more }
    } catch (err) {
      console.error(`[Sync] Pull ${tableName} failed:`, err.message)
      return { pulled: 0, hasMore: false, error: err.message }
    }
  }

  async function pullAll() {
    for (const table of tables) {
      let hasMore = true
      while (hasMore) {
        const result = await pullTable(table)
        hasMore = result.hasMore
      }
    }
  }

  // ---- SSE 实时拉取 ----

  function connectSSE() {
    if (stopped || sseSource) return

    const sseUrl = `${baseUrl}/offlite/sync/sse?token=${encodeURIComponent(token)}&mode=${syncMode}&filter_key=${encodeURIComponent(projectId)}&app=${appName}`

    try {
      sseSource = new EventSource(sseUrl)

      sseSource.addEventListener('open', () => {
        sseFailCount = 0
        retryAttempt = 0
        setState({ mode: 'realtime', sse_connected: true, error: null })
        console.log('[Sync] SSE connected')
      })

      sseSource.addEventListener('change', async (event) => {
        try {
          // Base64 → MessagePack → Object
          const bytes = Uint8Array.from(atob(event.data), c => c.charCodeAt(0))
          const payload = decode(bytes)
          const { table, changes } = payload

          for (const change of (changes || [])) {
            if (change.deleted) {
              await invoke('plugin:offlite|db_execute', {
                projectId,
                sql: `UPDATE ${table} SET _deleted = 1, updatedAt = ? WHERE _id = ?`,
                params: [change.updatedAt, change.doc_id],
              })
            } else {
              const dataJson = typeof change.data === 'string' ? change.data : JSON.stringify(change.data || {})
              await invoke('plugin:offlite|db_execute', {
                projectId,
                sql: `INSERT OR REPLACE INTO ${table} (_id, data, updatedAt, _deleted) VALUES (?, ?, ?, 0)`,
                params: [change.doc_id, dataJson, change.updatedAt],
              })
            }
            state.docs_read++
          }
          emitState()
        } catch (err) {
          console.error('[Sync] SSE change processing error:', err)
        }
      })

      sseSource.addEventListener('heartbeat', () => {
        // 心跳保活，无需处理
      })

      sseSource.addEventListener('error', () => {
        sseFailCount++
        closeSse()

        if (sseFailCount >= MAX_SSE_FAILURES) {
          console.warn(`[Sync] SSE failed ${sseFailCount} times, degrading to polling`)
          setState({ mode: 'polling', sse_connected: false })
          startPolling()
        } else {
          // 指数退避重连
          const delay = Math.min(BACKOFF_INITIAL * Math.pow(BACKOFF_FACTOR, retryAttempt), BACKOFF_MAX)
          retryAttempt++
          console.log(`[Sync] SSE reconnecting in ${delay}ms (attempt ${retryAttempt})`)
          setTimeout(() => connectSSE(), delay)
        }
      })
    } catch (err) {
      console.error('[Sync] SSE connection error:', err)
      setState({ mode: 'polling', sse_connected: false })
      startPolling()
    }
  }

  function closeSse() {
    if (sseSource) {
      sseSource.close()
      sseSource = null
    }
    state.sse_connected = false
  }

  // ---- 轮询兜底 ----

  function startPolling() {
    stopPolling()
    if (stopped) return

    pollTimer = setInterval(async () => {
      try {
        await pushAll()
        await pullAll()
        emitState()
      } catch (err) {
        console.error('[Sync] Poll cycle error:', err)
      }
    }, pollInterval)
  }

  function stopPolling() {
    if (pollTimer) {
      clearInterval(pollTimer)
      pollTimer = null
    }
  }

  // ---- 生命周期 ----

  /**
   * 启动同步
   * @param {string} pid - 项目 ID
   * @param {string[]} tableNames - 需要同步的表名列表
   */
  async function start(pid, tableNames) {
    projectId = pid
    tables = tableNames || []
    stopped = false
    sseFailCount = 0
    retryAttempt = 0

    setState({ active: true, mode: 'offline', error: null })

    // 先批量推送离线期间的变更
    try {
      await pushAll()
    } catch (err) {
      console.error('[Sync] Initial push failed:', err)
    }

    // 先拉取一次
    try {
      await pullAll()
    } catch (err) {
      console.error('[Sync] Initial pull failed:', err)
    }

    // 尝试 SSE 实时连接
    connectSSE()

    emitState()
  }

  function stop() {
    stopped = true
    closeSse()
    stopPolling()
    setState({ active: false, mode: 'offline', sse_connected: false })
  }

  function getState() {
    return { ...state }
  }

  function onStateChange(fn) {
    listeners.add(fn)
    return () => listeners.delete(fn)
  }

  return {
    start,
    stop,
    getState,
    onStateChange,
    pushChanges,
    updateToken,
  }
}
