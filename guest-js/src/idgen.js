/**
 * 短 ID 生成器
 *
 * 生成 12 字符的唯一 ID，格式: {base36_timestamp}_{random4}
 * 示例: kz7f8g0_a3x1
 *
 * - 前 7 位: Date.now() 的 Base36 编码（精确到毫秒，可排序）
 * - 下划线分隔
 * - 后 4 位: 随机 Base36 字符（防碰撞）
 *
 * 碰撞概率: 同一毫秒内 1/1,679,616（36^4），实际使用中可忽略
 * 相比旧格式 `{type}_{p_id}_{timestamp}`（27+ 字符），节省 15+ 字节/条
 * 1 万条记录节省约 150KB
 */

const CHARS = '0123456789abcdefghijklmnopqrstuvwxyz'

/**
 * 生成 4 位随机 Base36 字符串
 */
function randomSuffix() {
  let s = ''
  for (let i = 0; i < 4; i++) {
    s += CHARS[Math.floor(Math.random() * 36)]
  }
  return s
}

/**
 * 生成 12 字符短 ID
 * @returns {string} 格式: {base36_timestamp}_{random4}
 */
export function generateId() {
  const ts = Date.now().toString(36).padStart(7, '0')
  return `${ts}_${randomSuffix()}`
}

export default generateId
