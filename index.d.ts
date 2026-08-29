export interface LogSenseOptions {
  /** Service name attached to every log. Default: "unknown". */
  service?: string
  /** Environment tag (prod / staging / dev). Default: "production". */
  environment?: string
  /** Override the LogSense API base URL. Default: LogSense cloud. */
  endpoint?: string
  /** Flush when this many events accumulate. Default: 50. */
  batchSize?: number
  /** Flush interval in milliseconds. Default: 2000. */
  flushIntervalMs?: number
  /** Per-request timeout in milliseconds. Default: 5000. */
  timeoutMs?: number
  /** Max buffered events; excess is dropped (drop-newest) and counted. Default: 10000. */
  maxQueue?: number
  /** Retry attempts for transient failures (network, 429, 5xx). Default: 3. */
  maxRetries?: number
  /**
   * Called when a batch cannot be delivered (serialize failure, network error,
   * or non-2xx after retries). Use it to surface delivery problems instead of
   * losing logs silently. Must not throw.
   */
  onError?: (err: Error) => void
}

export type LogLevel = 'error' | 'warn' | 'info' | 'debug'
export type Fields = Record<string, unknown>

/** OpenTelemetry-style span kind. */
export type SpanKind = 'server' | 'client' | 'producer' | 'consumer' | 'internal'

export interface SpanOptions {
  /** Span kind (server | client | producer | consumer | internal). */
  kind?: SpanKind
  /** Initial attributes attached to the span. */
  attributes?: Fields
  /**
   * Explicit parent span. When omitted, the span active on the current async
   * context (if any) is used as the parent; otherwise a new trace is started.
   */
  parent?: Span
}

/**
 * An in-progress unit of work. Create with `startSpan` / `startActiveSpan`, then
 * call `end()` to record its duration and enqueue it for delivery. `end()` is
 * idempotent.
 */
export class Span {
  /** The span's trace ID (W3C hex) — useful for correlating logs. */
  readonly traceID: string
  /** The span's own ID (W3C hex). */
  readonly spanID: string
  /** Merge attributes into the span (last write wins). */
  setAttributes(attrs: Fields): this
  /** Set the span's status code (e.g. "OK", "ERROR") and optional message. */
  setStatus(code: string, message?: string): this
  /** Mark the span failed: status ERROR + the error's message. */
  setError(err: unknown): this
  /** Record end time + duration and enqueue the span. Idempotent. */
  end(): void
}

export class LogSenseClient {
  constructor(apiKey: string, options?: LogSenseOptions)
  /** Capture an error with its stack trace. Never throws. */
  capture(err: unknown, extra?: Fields): void
  /** Send a structured log line. Never throws. */
  log(level: LogLevel | string, message: string, fields?: Fields): void
  /**
   * Begin a span. Inherits the active/explicit parent's trace, else starts a new
   * trace. You must call `span.end()` yourself.
   */
  startSpan(name: string, options?: SpanOptions): Span
  /**
   * Start a span, run `fn(span)` with it active on the async context, and end it
   * automatically when `fn` returns or its promise settles. A thrown/rejected
   * error is recorded on the span and re-thrown. Returns whatever `fn` returns.
   */
  startActiveSpan<T>(name: string, fn: (span: Span) => T): T
  startActiveSpan<T>(name: string, options: SpanOptions, fn: (span: Span) => T): T
  /** The span active on the current async context, or null. */
  activeSpan(): Span | null
  /** Send all buffered events (logs + spans) through the single in-flight sender. */
  flush(): Promise<void>
  /** Flush and stop the background ticker. Call before process exit. */
  shutdown(): Promise<void>
  /** Number of events (logs + spans) dropped because a queue was full. */
  readonly dropped: number
}

/** Initialise the package-level client. Call once at startup. */
export function init(apiKey: string, options?: LogSenseOptions): LogSenseClient
export function capture(err: unknown, extra?: Fields): void
export function log(level: LogLevel | string, message: string, fields?: Fields): void
/** Start a span on the package-level client. Returns null if not initialised. */
export function startSpan(name: string, options?: SpanOptions): Span | null
/**
 * Start an active span on the package-level client. If not initialised, `fn` is
 * still run with a null span so instrumentation never breaks the app.
 */
export function startActiveSpan<T>(name: string, fn: (span: Span | null) => T): T
export function startActiveSpan<T>(name: string, options: SpanOptions, fn: (span: Span | null) => T): T
/** The span active on the current async context (package-level client), or null. */
export function activeSpan(): Span | null
export function flush(): Promise<void>
export function shutdown(): Promise<void>
/** Package-level client's dropped-event count (logs + spans). */
export function dropped(): number
