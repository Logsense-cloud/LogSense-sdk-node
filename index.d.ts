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

export class LogSenseClient {
  constructor(apiKey: string, options?: LogSenseOptions)
  /** Capture an error with its stack trace. Never throws. */
  capture(err: unknown, extra?: Fields): void
  /** Send a structured log line. Never throws. */
  log(level: LogLevel | string, message: string, fields?: Fields): void
  /** Send all buffered events through the single in-flight sender. */
  flush(): Promise<void>
  /** Flush and stop the background ticker. Call before process exit. */
  shutdown(): Promise<void>
  /** Number of events dropped because the queue was full. */
  readonly dropped: number
}

/** Initialise the package-level client. Call once at startup. */
export function init(apiKey: string, options?: LogSenseOptions): LogSenseClient
export function capture(err: unknown, extra?: Fields): void
export function log(level: LogLevel | string, message: string, fields?: Fields): void
export function flush(): Promise<void>
export function shutdown(): Promise<void>
/** Package-level client's dropped-event count. */
export function dropped(): number
