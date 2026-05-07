/**
 * tauri-plugin-offlite JavaScript bindings
 *
 * Provides wrapper functions that call Tauri invoke for the offlite plugin
 * commands: db_open, db_close, db_execute, db_query, db_batch, db_delete,
 * sync_start, sync_stop, sync_status.
 */

import { invoke } from '@tauri-apps/api/core'

// ==================== Database Lifecycle ====================

/**
 * Open a project database (or the global database with project_id = "global").
 * @param {string} projectId
 */
export async function dbOpen(projectId) {
  return invoke('plugin:offlite|db_open', { projectId })
}

/**
 * Close a project database.
 * @param {string} projectId
 */
export async function dbClose(projectId) {
  return invoke('plugin:offlite|db_close', { projectId })
}

/**
 * Execute a write SQL statement (INSERT / UPDATE / DELETE).
 * @param {string} projectId
 * @param {string} sql
 * @param {any[]} params
 * @returns {Promise<number>} affected rows
 */
export async function dbExecute(projectId, sql, params = []) {
  return invoke('plugin:offlite|db_execute', { projectId, sql, params })
}

/**
 * Execute a read SQL statement (SELECT).
 * @param {string} projectId
 * @param {string} sql
 * @param {any[]} params
 * @returns {Promise<any[]>} rows
 */
export async function dbQuery(projectId, sql, params = []) {
  return invoke('plugin:offlite|db_query', { projectId, sql, params })
}

/**
 * Execute a batch of SQL statements inside a transaction.
 * @param {string} projectId
 * @param {Array<{sql: string, params: any[]}>} statements
 * @returns {Promise<number[]>} affected rows per statement
 */
export async function dbBatch(projectId, statements) {
  return invoke('plugin:offlite|db_batch', { projectId, statements })
}

/**
 * Delete a project database file.
 * @param {string} projectId
 */
export async function dbDelete(projectId) {
  return invoke('plugin:offlite|db_delete', { projectId })
}

// ==================== Schema ====================

/**
 * Create business tables from schema definitions.
 * Each schema creates a table with fixed metadata columns (including _status),
 * standard indexes, optional json_extract indexes, and the legacy _change_log
 * table (retained for backward compatibility, not used by JS sync engine).
 *
 * @param {string} projectId
 * @param {Array<{name: string, json_indexes?: Array<{name: string, json_path: string}>}>} schemas
 */
export async function dbCreateTables(projectId, schemas) {
  return invoke('plugin:offlite|db_create_tables', { projectId, schemas })
}

// ==================== Sync Engine ====================

/**
 * Start sync for a project.
 * @param {string} projectId
 * @param {object} config - { baseUrl, token, syncMode, tables, realtime, pollInterval, sseHeartbeat }
 */
export async function syncStart(projectId, config) {
  return invoke('plugin:offlite|sync_start', { projectId, config })
}

/**
 * Stop sync for a project.
 * @param {string} projectId
 */
export async function syncStop(projectId) {
  return invoke('plugin:offlite|sync_stop', { projectId })
}

/**
 * Get sync status for a project.
 * @param {string} projectId
 * @returns {Promise<{active: boolean, paused: boolean, error: string|null, docs_read: number, docs_written: number, mode: string, sse_connected: boolean}>}
 */
export async function syncStatus(projectId) {
  return invoke('plugin:offlite|sync_status', { projectId })
}


// ==================== Sync Engine (JS SDK) ====================

export { createSyncEngine } from './sync.js'
export { createChildSync } from './childSync.js'
export { generateId } from './idgen.js'
export { createDB } from './db.js'
export { defineSchema, validateDoc, validateField, splitDoc, parseRow, META_FIELDS } from './schema.js'
export { createSyncManager } from './syncManager.js'
