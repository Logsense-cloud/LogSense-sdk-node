'use strict'

/**
 * LogSense Node SDK — structured log + distributed-trace ingestion with automatic
 * AI-powered error analysis.
 *
 * Zero external dependencies (uses only built-in `node:crypto` and
 * `node:async_hooks`). Requires Node 18+ (built-in `fetch` and
 * `AbortSignal.timeout`). Mirrors the LogSense Go SDK: bounded buffer, single
 * in-flight sender, retries, surfaced errors, and stable messages so repeated
 * errors group correctly. Spans share the same batching/retry machinery and post
 * to the OTLP-compatible `/v1/traces/batch` endpoint.
 */

const { randomBytes } = require('node:crypto')
const { AsyncLocalStorage } = require('node:async_hooks')

const DEFAULT_ENDPOINT = 'https://api.logsense.cloud/ai-service'
const DEFAULT_BATCH_SIZE = 50
const DEFAULT_FLUSH_INTERVAL_MS = 2000
const DEFAULT_TIMEOUT_MS = 5000
const DEFAULT_MAX_QUEUE = 10000
const DEFAULT_MAX_RETRIES = 3
const SOURCE = 'sdk-node'

// Ingest paths, appended to the configured endpoint.
const LOGS_BATCH_PATH = '/v1/logs/batch'
const TRACES_BATCH_PATH = '/v1/traces/batch'

// W3C trace-context ID sizes: a 16-byte trace ID and 8-byte span ID, each sent
// as lowercase hex.
const TRACE_ID_BYTES = 16
const SPAN_ID_BYTES = 8

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

/** n cryptographically-random bytes as lowercase hex (a W3C-style trace/span ID). */
function newHexId(n) {
  return randomBytes(n).toString('hex')
}

/**
 * A Span is an in-progress unit of work. Create one with `client.startSpan()` (or
 * the ergonomic `client.startActiveSpan()`), then call `end()` — typically in a
 * `finally` — to record its duration and enqueue it for delivery. `end()` is
 * idempotent. Mirrors the Go SDK's core.Span.
 */
class Span {
  constructor(client, event) {
    this._client = client
    this._event = event
    this._startHr = process.hrtime.bigint()
    this._ended = false
  }

  /** The span's trace ID (W3C hex) — useful for correlating logs. */
  get traceID() {
    return this._event.traceID
  }

  /** The span's own ID (W3C hex). */
  get spanID() {
    return this._event.spanID
  }

  /** Merge attributes into the span (last write wins). No-op on empty. */
  setAttributes(attrs) {
    if (!attrs || typeof attrs !== 'object') return this
    this._event.attributes = Object.assign(this._event.attributes || {}, attrs)
    return this
  }

  /** Set the span's status code (e.g. "OK", "ERROR") and optional message. */
  setStatus(code, message) {
    if (code) this._event.statusCode = code
    if (message) this._event.statusMessage = message
    return this
  }

  /** Mark the span failed: status ERROR + err's message, and an `error` attribute. */
  setError(err) {
    if (!err) return this
    const msg = err instanceof Error ? err.message : String(err)
    this.setStatus('ERROR', msg)
    this.setAttributes({ error: msg })
    return this
  }

  /**
   * Record the span's end time + duration and enqueue it for delivery.
   * Idempotent — only the first call takes effect.
   */
  end() {
    if (this._ended) return
    this._ended = true
    const durationMs = Number(process.hrtime.bigint() - this._startHr) / 1e6
    this._event.endTime = new Date().toISOString()
    this._event.durationMs = durationMs
    this._client._enqueueSpan(this._event)
  }
}

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
    this._spanQueue = []
    this._dropped = 0
    this._sending = false
    // Carries the active span across async boundaries so nested spans link to
    // their parent and logs emitted inside a span inherit its trace ID.
    this._als = new AsyncLocalStorage()
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

  /** The span currently active on this async context, or null. */
  activeSpan() {
    return this._als.getStore() || null
  }

  /**
   * Begin a span named `name`. If a parent is available (an explicit
   * `options.parent`, otherwise the span active on this async context) the new
   * span inherits its trace ID and becomes its child; otherwise a new trace is
   * started. You must call `span.end()` yourself — prefer `startActiveSpan` when
   * you can, which ends it for you and propagates it to nested async work.
   *
   * @param {string} name
   * @param {{ kind?: string, attributes?: Record<string, unknown>, parent?: Span }} [options]
   * @returns {Span}
   */
  startSpan(name, options = {}) {
    const parent = options.parent || this._als.getStore() || null
    const traceID = parent ? parent.traceID : newHexId(TRACE_ID_BYTES)
    const parentSpanID = parent ? parent.spanID : ''
    const event = {
      source: SOURCE,
      traceID,
      spanID: newHexId(SPAN_ID_BYTES),
      parentSpanID,
      name: String(name || ''),
      service: this.service,
      environment: this.environment,
      startTime: new Date().toISOString(),
    }
    if (options.kind) event.kind = options.kind
    const span = new Span(this, event)
    if (options.attributes) span.setAttributes(options.attributes)
    return span
  }

  /**
   * Start a span, run `fn(span)` with that span active on the async context, and
   * end the span automatically when `fn` returns or its promise settles. A thrown
   * (or rejected) error is recorded on the span (status ERROR) and re-thrown.
   * Nested `startSpan`/`startActiveSpan` calls and any logs inside `fn` link to
   * this span. Returns whatever `fn` returns (awaitable if `fn` is async).
   *
   * @template T
   * @param {string} name
   * @param {{ kind?: string, attributes?: Record<string, unknown>, parent?: Span }|((span: Span) => T)} [options]
   * @param {(span: Span) => T} [fn]
   * @returns {T}
   */
  startActiveSpan(name, options, fn) {
    if (typeof options === 'function') {
      fn = options
      options = {}
    }
    const span = this.startSpan(name, options)
    return this._als.run(span, () => {
      let result
      try {
        result = fn(span)
      } catch (err) {
        span.setError(err)
        span.end()
        throw err
      }
      if (result && typeof result.then === 'function') {
        return result.then(
          (v) => {
            span.end()
            return v
          },
          (err) => {
            span.setError(err)
            span.end()
            throw err
          }
        )
      }
      span.end()
      return result
    })
  }

  /** Number of events (logs + spans) dropped because a queue was full. */
  get dropped() {
    return this._dropped
  }

  _enqueueSpan(event) {
    if (this._spanQueue.length >= this.maxQueue) {
      // Bounded buffer: drop-newest and count it rather than grow without bound.
      this._dropped++
      return
    }
    this._spanQueue.push(event)
    if (this._spanQueue.length >= this.batchSize) {
      this._drain()
    }
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
    // Correlate with the active span, if any, so logs emitted inside a span line
    // up with the trace on the server.
    const active = this._als.getStore()
    if (active && active.traceID) event.traceID = active.traceID
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
      // Alternate logs and spans so neither queue starves the other; the single
      // in-flight guard keeps requests from overlapping (mirrors the Go sender).
      while (this._queue.length > 0 || this._spanQueue.length > 0) {
        if (this._queue.length > 0) {
          const batch = this._queue.splice(0, this.batchSize)
          await this._sendWithRetry(LOGS_BATCH_PATH, 'logs', batch)
        }
        if (this._spanQueue.length > 0) {
          const batch = this._spanQueue.splice(0, this.batchSize)
          await this._sendWithRetry(TRACES_BATCH_PATH, 'spans', batch)
        }
      }
    } finally {
      this._sending = false
    }
  }

  /** Public flush: send everything currently buffered (logs + spans). Never throws. */
  async flush() {
    await this._drain()
  }

  async _sendWithRetry(path, key, batch) {
    let body
    try {
      body = JSON.stringify({ [key]: batch })
    } catch (err) {
      this._report(new Error(`logsense: failed to serialize ${batch.length} ${key}: ${err.message}`))
      return
    }

    let lastErr
    for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
      if (attempt > 0) await sleep(backoffMs(attempt))
      lastErr = await this._send(path, body)
      if (!lastErr) return
      if (!lastErr.retryable) break
    }
    this._report(
      new Error(`logsense: dropped ${batch.length} ${key} after ${this.maxRetries + 1} attempt(s): ${lastErr.message}`)
    )
  }

  /** @returns {null | { message: string, retryable: boolean }} */
  async _send(path, body) {
    let res
    try {
      res = await fetch(`${this.endpoint}${path}`, {
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

/** Start a span on the package-level client. Returns null if not initialised. */
function startSpan(name, options) {
  return _default ? _default.startSpan(name, options) : null
}

/**
 * Start an active span on the package-level client, running `fn(span)` with it
 * active and ending it automatically. If the client isn't initialised, `fn` is
 * still run (with a null span) so instrumentation never breaks the app.
 */
function startActiveSpan(name, options, fn) {
  if (typeof options === 'function') {
    fn = options
    options = {}
  }
  if (_default) return _default.startActiveSpan(name, options, fn)
  return fn(null)
}

/** The span active on the current async context (package-level client), or null. */
function activeSpan() {
  return _default ? _default.activeSpan() : null
}

module.exports = {
  LogSenseClient,
  Span,
  init,
  capture,
  log,
  startSpan,
  startActiveSpan,
  activeSpan,
  flush,
  dropped,
  shutdown,
}
