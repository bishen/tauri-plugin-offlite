/**
 * 多表同步生命周期管理器（纯 JS，框架无关）
 *
 * 管理多个 syncEngine 实例，按 syncMode 自动分组：
 * - user 模式：按 uid 过滤，全局库
 * - company 模式：按 company_id 过滤，全局库
 * - project 模式：按 projectId 过滤，项目库
 *
 * @example
 * import { createSyncManager } from 'tauri-plugin-offlite-api/syncManager'
 *
 * const manager = createSyncManager({
 *   baseUrl: 'https://api.example.com',
 *   appName: 'survey',
 *   getToken: () => localStorage.getItem('token'),
 *   onTokenRefresh: async () => { ... },
 * })
 *
 * manager.register({ dbName: 'project', syncMode: 'user', entityType: 'project' })
 * manager.register({ dbName: 'planning_feature', syncMode: 'project', entityType: 'sub_compartment' })
 *
 * await manager.startGlobal({ uid: 1, company_id: 100 })
 * await manager.startProject('project_001')
 * manager.stopProject()
 * manager.stopAll()
 */

import { invoke } from '@tauri-apps/api/core'
import { createSyncEngine } from './sync.js'

// ============ 核心函数 ============

/**
 * 创建同步管理器
 *
 * @param {Object} config
 * @param {string} config.baseUrl - 服务端 API 地址
 * @param {string} config.appName - 应用名前缀
 * @param {Function} config.getToken - 获取当前 JWT token
 * @param {Function} config.onTokenRefresh - token 刷新回调，返回 Promise<string>
 * @returns {Object} 同步管理器
 */
export function createSyncManager(config) {
  const { baseUrl, appName, getToken, onTokenRefresh } = config

  // 注册表（按 syncMode 分组）
  const userRegistry = new Map()     // dbName → { entityType }
  const companyRegistry = new Map()  // dbName → { entityType }
  const projectRegistry = new Map()  // dbName → { entityType }

  // 引擎实例
  let userEngine = null
  let companyEngine = null
  let projectEngine = null

  // 当前活跃项目
  let activeProjectId = null

  // 状态变更回调
  const stateListeners = new Map() // dbName → Set<callback>

  /**
   * 构建引擎配置
   */
  function buildEngineConfig(syncMode) {
    return {
      baseUrl,
      appName,
      token: getToken() || '',
      syncMode,
      onTokenRefresh: onTokenRefresh || null,
    }
  }

  /**
   * 注册模型到管理器
   * 如果对应的同步引擎已启动，会动态加入到运行中的引擎
   * @param {Object} model - defineSchema() 返回的模型描述对象
   */
  function register(model) {
    const { dbName, syncMode, entityType } = model
    const entry = { entityType }
    const tableConfig = entityType ? { name: dbName, entityType } : dbName

    switch (syncMode) {
      case 'user':
        if (userRegistry.has(dbName)) return
        userRegistry.set(dbName, entry)
        // 如果 user 引擎已运行，动态加入
        if (userEngine) {
          ensureAllTables('global', [dbName]).then(() => userEngine.addTable(tableConfig))
        }
        break
      case 'company':
        if (companyRegistry.has(dbName)) return
        companyRegistry.set(dbName, entry)
        if (companyEngine) {
          ensureAllTables('global', [dbName]).then(() => companyEngine.addTable(tableConfig))
        }
        break
      case 'project':
        if (projectRegistry.has(dbName)) return
        projectRegistry.set(dbName, entry)
        if (projectEngine && activeProjectId) {
          ensureAllTables(activeProjectId, [dbName]).then(() => projectEngine.addTable(tableConfig))
        }
        break
      // 'local' 模式不注册同步
    }
  }

  /**
   * 确保所有同步表在 SQLite 中已创建
   */
  async function ensureAllTables(dbId, tableNames) {
    for (const name of tableNames) {
      try {
        await invoke('plugin:offlite|db_create_tables', {
          projectId: dbId,
          schemas: [{ name, json_indexes: [] }]
        })
      } catch (e) {
        if (!String(e).includes('already exists')) {
          console.warn(`[SyncManager] 建表 ${name} 失败:`, e)
        }
      }
    }
  }

  /**
   * 检查本地表是否为空，如果为空则清除 checkpoint 强制全量 pull
   */
  async function resetEmptyCheckpoints(dbId, tableNames, syncMode, filterKey) {
    for (const table of tableNames) {
      try {
        const rows = await invoke('plugin:offlite|db_query', {
          projectId: dbId,
          sql: `SELECT COUNT(*) as cnt FROM ${table} WHERE _deleted = 0`,
          params: [],
        })
        if (rows?.[0]?.cnt === 0 || rows?.[0]?.cnt === '0') {
          await invoke('plugin:offlite|db_execute', {
            projectId: 'global',
            sql: `DELETE FROM _sync_checkpoint WHERE table_name = ? AND sync_mode = ? AND filter_key = ?`,
            params: [table, syncMode, filterKey],
          })
        }
      } catch (_) {
        // 表可能不存在，忽略
      }
    }
  }

  /**
   * 构建表配置数组（带 entityType）
   */
  function buildTableConfigs(registry) {
    return [...registry.entries()].map(([name, { entityType }]) =>
      entityType ? { name, entityType } : name
    )
  }

  /**
   * 启动全局同步（user + company 模式）
   * @param {Object} user - { uid, company_id }
   */
  async function startGlobal(user) {
    const { uid, company_id } = user
    if (!uid) return

    // user 模式
    const userTableNames = [...userRegistry.keys()]
    if (userTableNames.length && !userEngine) {
      await ensureAllTables('global', userTableNames)
      await resetEmptyCheckpoints('global', userTableNames, 'user', String(uid))
      userEngine = createSyncEngine(buildEngineConfig('user'))
      userEngine.onStateChange((state) => notifyStateChange(userRegistry, state))
      userEngine.start(String(uid), buildTableConfigs(userRegistry), { dbId: 'global' })
    }

    // company 模式
    const companyTableNames = [...companyRegistry.keys()]
    if (companyTableNames.length && !companyEngine && company_id) {
      await ensureAllTables('global', companyTableNames)
      await resetEmptyCheckpoints('global', companyTableNames, 'company', String(company_id))
      companyEngine = createSyncEngine(buildEngineConfig('company'))
      companyEngine.onStateChange((state) => notifyStateChange(companyRegistry, state))
      companyEngine.start(String(company_id), buildTableConfigs(companyRegistry), { dbId: 'global' })
    }
  }

  /**
   * 启动项目级同步
   * @param {string} projectId - 项目 _id
   */
  async function startProject(projectId) {
    if (!projectId) return
    if (activeProjectId === projectId && projectEngine) return

    // 停止旧项目
    if (activeProjectId) stopProject()

    activeProjectId = projectId

    // 注意：即使 projectRegistry 为空也要创建引擎
    // 因为业务代码的 schema 文件可能在 startProject 之后才通过 Nuxt auto-import 加载
    // 新表被 register() 后，通过 addTable() 动态加入现有引擎
    const tableNames = [...projectRegistry.keys()]
    if (tableNames.length) {
      await ensureAllTables(projectId, tableNames)
    }

    projectEngine = createSyncEngine(buildEngineConfig('project'))
    projectEngine.onStateChange((state) => notifyStateChange(projectRegistry, state))
    projectEngine.start(projectId, buildTableConfigs(projectRegistry))
  }

  /**
   * 停止项目级同步
   */
  function stopProject() {
    if (projectEngine) {
      projectEngine.stop()
      projectEngine = null
    }
    activeProjectId = null
  }

  /**
   * 停止所有同步
   */
  function stopAll() {
    stopProject()
    if (userEngine) { userEngine.stop(); userEngine = null }
    if (companyEngine) { companyEngine.stop(); companyEngine = null }
  }

  /**
   * 获取指定表对应的 syncEngine
   * @param {string} dbName - 表名
   * @returns {Object|null} syncEngine 实例
   */
  function getEngine(dbName) {
    if (projectRegistry.has(dbName) && projectEngine) return projectEngine
    if (userRegistry.has(dbName) && userEngine) return userEngine
    if (companyRegistry.has(dbName) && companyEngine) return companyEngine
    return null
  }

  /**
   * 监听指定表的同步状态变更
   * @param {string} dbName - 表名
   * @param {Function} callback - 状态变更回调
   * @returns {Function} 取消监听函数
   */
  function onStateChange(dbName, callback) {
    if (!stateListeners.has(dbName)) {
      stateListeners.set(dbName, new Set())
    }
    stateListeners.get(dbName).add(callback)
    return () => stateListeners.get(dbName)?.delete(callback)
  }

  /**
   * 通知状态变更
   */
  function notifyStateChange(registry, state) {
    for (const dbName of registry.keys()) {
      const listeners = stateListeners.get(dbName)
      if (listeners) {
        for (const cb of listeners) {
          try { cb(state) } catch (_) {}
        }
      }
    }
  }

  return {
    register,
    startGlobal,
    startProject,
    stopProject,
    stopAll,
    getEngine,
    onStateChange,
    get activeProjectId() { return activeProjectId },
  }
}
