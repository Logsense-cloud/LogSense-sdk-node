'use strict'

/**
 * LogSense Node SDK — structured log ingestion with automatic AI-powered error analysis.
 *
 * Zero dependencies. Requires Node 18+ (built-in `fetch` and `AbortSignal.timeout`).
 * Mirrors the LogSense Go SDK: bounded buffer, single in-flight sender, retries,
 * surfaced errors, and stable messages so repeated errors group correctly.
 */

const DEFAULT_ENDPOINT = 'https://api.logsense.cloud/ai-service'
const DEFAULT_BATCH_SIZE = 50
const DEFAULT_FLUSH_INTERVAL_MS = 2000
const DEFAULT_TIMEOUT_MS = 5000
const DEFAULT_MAX_QUEUE = 10000
const DEFAULT_MAX_RETRIES = 3
const SOURCE = 'sdk-node'

// Per-event caps keep any single event well under the server's 64KB limit.
const MAX_MESSAGE_BYTES = 16 * 1024
const MAX_STACK_BYTES = 16 * 1024

function truncate(s, max) {
  if (typeof s !== 'string' || s.length <= max) return s
  return s.slice(0, max) + '…(truncated)'
}

function backoffMs(attempt) {
  const d = 200 * 2 ** (attempt - 1)
  return Math.min(d, 5000)
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

class LogSenseClient {
  /**
   * @param {string} apiKey  Your project API key (ls_live_...).
   * @param {object} [options]
   */
  constructor(apiKey, options = {}) {
    this.apiKey = apiKey
    this.endpoint = String(options.endpoint || DEFAULT_ENDPOINT).replace(/\/+$/, '')
    this.service = options.service || 'unknown'
    this.environment = options.environment || 'production'
    this.batchSize = options.batchSize || DEFAULT_BATCH_SIZE
    this.flushIntervalMs = options.flushIntervalMs || DEFAULT_FLUSH_INTERVAL_MS
    this.timeoutMs = options.timeoutMs || DEFAULT_TIMEOUT_MS
    this.maxQueue = options.maxQueue || DEFAULT_MAX_QUEUE
    this.maxRetries = options.maxRetries != null ? options.maxRetries : DEFAULT_MAX_RETRIES
    // Called when a batch can't be delivered — surface this, don't lose logs silently.
    this.onError = typeof options.onError === 'function' ? options.onError : null

    this._queue = []
    this._dropped = 0
    this._sending = false
    this._timer = setInterval(() => { this._drain() }, this.flushIntervalMs)
    // Never keep the process alive just for the flush ticker.
    if (this._timer && typeof this._timer.unref === 'function') this._timer.unref()
  }

  /**
   * Capture an error with its stack trace. Never throws.
   *
   * The error message is used verbatim as the event message so repeated
   * occurrences group together; the stack goes into a structured field.
   * @param {unknown} err
   * @param {Record<string, unknown>} [extra] Extra structured fields.
   */
  capture(err, extra) {
    if (!err) return
    const e = err instanceof Error ? err : new Error(String(err))
    const structured = Object.assign(
      { error: e.message, stack: truncate(e.stack || '', MAX_STACK_BYTES) },
      extra || {}
    )
    this._enqueue('error', e.message, structured)
  }

  /**
   * Send a structured log line. Never throws.
   * @param {string} level  error | warn | info | debug
   * @param {string} message
   * @param {Record<string, unknown>} [fields]
   */
  log(level, message, fields) {
    this._enqueue(level, message, fields)
  }

  /** Number of events dropped because the queue was full. */
  get dropped() {
    return this._dropped
  }

  _enqueue(level, message, structured) {
    if (this._queue.length >= this.maxQueue) {
      // Bounded buffer: drop-newest and count it rather than grow without bound.
      this._dropped++
      return
    }
    const event = {
      source: SOURCE,
      service: this.service,
      environment: this.environment,
      level,
      message: truncate(message, MAX_MESSAGE_BYTES),
      timestamp: new Date().toISOString(),
    }
    if (structured && Object.keys(structured).length > 0) {
      event.structured = structured
    }
    this._queue.push(event)
    if (this._queue.length >= this.batchSize) {
      this._drain()
    }
  }

  /**
   * Drain the queue through a single in-flight sender — never overlapping
   * requests. Returns when the queue is empty (or a batch fails permanently).
   */
  async _drain() {
    if (this._sending) return
    this._sending = true
    try {
      while (this._queue.length > 0) {
        const batch = this._queue.splice(0, this.batchSize)
        await this._sendWithRetry(batch)
      }
    } finally {
      this._sending = false
    }
  }

  /** Public flush: send everything currently buffered. Never throws. */
  async flush() {
    await this._drain()
  }

  async _sendWithRetry(batch) {
    let body
    try {
      body = JSON.stringify({ logs: batch })
    } catch (err) {
      this._report(new Error(`logsense: failed to serialize ${batch.length} events: ${err.message}`))
      return
    }

    let lastErr
    for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
      if (attempt > 0) await sleep(backoffMs(attempt))
      lastErr = await this._send(body)
      if (!lastErr) return
      if (!lastErr.retryable) break
    }
    this._report(
      new Error(`logsense: dropped ${batch.length} events after ${this.maxRetries + 1} attempt(s): ${lastErr.message}`)
    )
  }

  /** @returns {null | { message: string, retryable: boolean }} */
  async _send(body) {
    let res
    try {
      res = await fetch(`${this.endpoint}/v1/logs/batch`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-API-Key': this.apiKey },
        body,
        signal: AbortSignal.timeout(this.timeoutMs),
      })
    } catch (err) {
      // Network / timeout — worth retrying.
      return { message: err.message || 'network error', retryable: true }
    }
    // Drain the body so the socket returns to the pool.
    if (res && typeof res.arrayBuffer === 'function') {
      await res.arrayBuffer().catch(() => {})
    }
    if (res.ok) return null
    // 429 and 5xx are transient; 4xx (bad key, bad request) are not.
    const retryable = res.status === 429 || res.status >= 500
    return { message: `server returned status ${res.status}`, retryable }
  }

  _report(err) {
    if (this.onError) {
      try {
        this.onError(err)
      } catch (_) {
        // A throwing error handler must not take down the app.
      }
    }
  }

  /** Flush and stop the background ticker. Call before process exit. */
  async shutdown() {
    if (this._timer) {
      clearInterval(this._timer)
      this._timer = null
    }
    await this.flush()
  }
}

// ─── Package-level convenience API ──────────────────────────────────────────

/** @type {LogSenseClient | null} */
let _default = null

/** Initialise the package-level client. Call once at startup. */
function init(apiKey, options) {
  if (!_default) _default = new LogSenseClient(apiKey, options)
  return _default
}

function capture(err, extra) { if (_default) _default.capture(err, extra) }
function log(level, message, fields) { if (_default) _default.log(level, message, fields) }
function dropped() { return _default ? _default.dropped : 0 }
async function flush() { if (_default) await _default.flush() }
async function shutdown() {
  if (_default) {
    await _default.shutdown()
    _default = null
  }
}

module.exports = { LogSenseClient, init, capture, log, flush, dropped, shutdown }
