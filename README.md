# LogSense Node SDK

[![npm](https://img.shields.io/badge/npm-%40logsense%2Fnode-red)](https://www.npmjs.com/package/@logsense/node)
[![Node](https://img.shields.io/badge/node-%3E%3D18-blue)](https://nodejs.org)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

The official Node SDK for [LogSense](https://logsense.cloud) — structured log ingestion with automatic AI-powered error analysis.

No dependencies. Requires **Node 18+** (uses the built-in `fetch` and `AbortSignal.timeout`).

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
await logsense.flush()     // send buffered events now
await logsense.shutdown()  // flush, then stop the background ticker
```

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
