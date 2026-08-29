# LogSense Node SDK

[![npm](https://img.shields.io/badge/npm-%40logsense%2Fnode-red)](https://www.npmjs.com/package/@logsense/node)
[![Node](https://img.shields.io/badge/node-%3E%3D18-blue)](https://nodejs.org)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

The official Node SDK for [LogSense](https://aryangoyal.space) — structured log ingestion and distributed tracing with automatic AI-powered error analysis.

No external dependencies (uses only built-in `node:crypto` and `node:async_hooks`). Requires **Node 18+** (uses the built-in `fetch` and `AbortSignal.timeout`).

---

## Installation

```bash
npm install @logsense/node
```

---

## Quick start

```js
const logsense = require('@logsense/node')

logsense.init('ls_live_your_api_key', {
  service: 'my-service',
  environment: 'production',
})

// Capture an error with its full stack trace
try {
  await doSomething()
} catch (err) {
  logsense.capture(err)
}

// Send a structured log line
logsense.log('info', 'user signed up', { user_id: '123', plan: 'free' })

// Flush buffered events before the process exits
process.on('SIGTERM', () => logsense.shutdown())
```

Logs are buffered and flushed every 2 seconds, or when the batch reaches 50 events. `shutdown()` flushes anything remaining before your process exits.

ESM works too:

```js
import logsense from '@logsense/node'
// or: import { LogSenseClient, init, capture, log } from '@logsense/node'
```

---

## API

### `init(apiKey, options?)`

Initialise the package-level client. Call once at startup.

For multiple isolated clients in one process:

```js
const { LogSenseClient } = require('@logsense/node')
const client = new LogSenseClient('ls_live_...', { service: 'worker' })
```

### Options

| Option | Description | Default |
|---|---|---|
| `service` | Service name attached to every log | `"unknown"` |
| `environment` | Environment tag (`prod`, `staging`, `dev`) | `"production"` |
| `endpoint` | Override the LogSense API base URL | LogSense cloud |
| `batchSize` | Flush when this many events accumulate | `50` |
| `flushIntervalMs` | Flush interval in ms | `2000` |
| `timeoutMs` | Per-request timeout in ms | `5000` |
| `maxQueue` | Cap buffered events; excess is dropped (drop-newest) and counted via `dropped` | `10000` |
| `maxRetries` | Retry attempts for transient failures (network, 429, 5xx) | `3` |
| `onError` | Callback to surface delivery failures instead of losing logs silently | none |

### `capture(err, extra?)`

Enqueues an `error`-level event with the error message, its stack trace, and any extra structured fields.

```js
logsense.capture(err, { user_id: '123', route: '/checkout' })
```

### `log(level, message, fields?)`

Enqueues a log line. Levels: `error`, `warn`, `info`, `debug`.

```js
logsense.log('warn', 'slow database query', { query_ms: 342, collection: 'users' })
```

### `flush()` / `shutdown()`

```js
await logsense.flush()     // send buffered events (logs + spans) now
await logsense.shutdown()  // flush, then stop the background ticker
```

---

## Distributed tracing

Spans share the same batching/retry machinery as logs and post to the
OTLP-compatible `/v1/traces/batch` endpoint. Trace/span IDs are W3C-style hex
(16-byte trace, 8-byte span).

### `startActiveSpan(name, options?, fn)`

The ergonomic API: starts a span, runs `fn(span)` with it active, and **ends it
automatically** when `fn` returns or its promise settles. Nested spans link up as
children, and any `log()` emitted inside inherits the span's trace ID — so logs
and traces line up in the dashboard. A thrown/rejected error is recorded on the
span (status `ERROR`) and re-thrown.

```js
await logsense.startActiveSpan('GET /checkout', { kind: 'server' }, async (span) => {
  span.setAttributes({ 'user.id': userId })
  logsense.log('info', 'handling checkout') // correlated with this trace

  await logsense.startActiveSpan('db.query', { kind: 'client' }, async (child) => {
    child.setAttributes({ table: 'orders' })
    return db.query('SELECT …') // child links to the parent span
  })
})
```

### `startSpan(name, options?)`

The manual API when you can't wrap your work in a callback — you must call
`span.end()` yourself (typically in `finally`). Pass `{ parent }` to link
explicitly; otherwise it inherits the span active on the current async context.

```js
const span = logsense.startSpan('cache.get', { kind: 'client', attributes: { key } })
try {
  return await cache.get(key)
} catch (err) {
  span.setError(err) // status ERROR + error message
  throw err
} finally {
  span.end()
}
```

`options`: `kind` (`server` | `client` | `producer` | `consumer` | `internal`),
`attributes` (object), `parent` (a `Span`). A `Span` exposes `traceID` / `spanID`
and `setAttributes()`, `setStatus(code, message?)`, `setError(err)`, `end()`.
`activeSpan()` returns the span active on the current async context, or `null`.

---

## Behaviour

| Property | Detail |
|---|---|
| Non-blocking | Calls return immediately — no latency impact on your app |
| Batched delivery | A single in-flight sender ships events every 2 s or when 50 accumulate — never overlapping requests |
| Bounded memory | The buffer is capped; if the endpoint is slow or down, events are dropped (and counted via `dropped`) rather than growing without limit |
| Retries + visibility | Transient failures (network, 429, 5xx) are retried with backoff; anything that still fails is reported via `onError`, never swallowed |
| Never throws | The SDK never interrupts your application, even on delivery failure |
| Stable grouping | `capture` uses the bare error message (stack goes in a structured field) so repeated errors group into one incident |
| Source tagging | Events are tagged `source: "sdk-node"` so you can tell SDK traffic apart in the dashboard |
| Correlated traces | Spans reuse the same sender and post to `/v1/traces/batch`; logs emitted inside a span inherit its trace ID, and context propagates across async boundaries via `AsyncLocalStorage` |

---

## Example: Express

Capture every error handled by Express — see [`examples/express.js`](examples/express.js):

```js
app.use((err, req, res, next) => {
  logsense.capture(err, { path: req.path, method: req.method })
  res.status(500).json({ error: 'internal error' })
})
```

---

## Verify

A smoke test spins up a local server and asserts the exact wire format:

```bash
npm test
```

---

## License

MIT
