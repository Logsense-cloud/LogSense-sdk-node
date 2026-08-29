// Smoke test: proves the Node SDK speaks the exact LogSense ingest wire format
// and honours the reliability contract (stable message, retries, surfaced
// errors, bounded queue). Run with: node test/smoke.mjs
//
// Uses only the Node standard library.

import http from 'node:http'
import assert from 'node:assert/strict'
import { LogSenseClient } from '../index.js'

// Spin up a controllable ingest server. `status` can be changed per test; a
// one-shot `failNext` count makes the first N requests fail with 500.
function startServer() {
  const received = []
  let failNext = 0
  let status = 202
  const server = http.createServer((req, res) => {
    let body = ''
    req.on('data', (c) => { body += c })
    req.on('end', () => {
      if (failNext > 0) {
        failNext--
        res.writeHead(500)
        res.end()
        return
      }
      received.push({
        url: req.url,
        method: req.method,
        apiKey: req.headers['x-api-key'],
        contentType: req.headers['content-type'],
        body: JSON.parse(body || '{}'),
      })
      res.writeHead(status, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify({ accepted: 1 }))
    })
  })
  return {
    server,
    received,
    setStatus: (s) => { status = s },
    setFailNext: (n) => { failNext = n },
    listen: () => new Promise((r) => server.listen(0, r)),
    url: () => `http://127.0.0.1:${server.address().port}/ai-service`,
    close: () => server.close(),
  }
}

let passed = 0
const ok = (msg) => { passed++; console.log(`  ✓ ${msg}`) }

// ── Test 1: wire format + stable message + stack in structured ──────────────
{
  const s = startServer()
  await s.listen()
  const client = new LogSenseClient('ls_live_test_key', {
    service: 'smoke-svc', environment: 'test', endpoint: s.url(), flushIntervalMs: 50,
  })
  client.log('info', 'user signed up', { user_id: '123', plan: 'free' })
  client.capture(new Error('redis GET auth:token -> nil'), { path: '/login' })
  await client.shutdown()

  const all = s.received.flatMap((r) => {
    assert.equal(r.method, 'POST')
    assert.equal(r.url, '/ai-service/v1/logs/batch', 'posts to /v1/logs/batch')
    assert.equal(r.apiKey, 'ls_live_test_key', 'sends X-API-Key')
    assert.equal(r.contentType, 'application/json')
    assert.ok(Array.isArray(r.body.logs), 'body is { logs: [...] }')
    return r.body.logs
  })
  assert.equal(all.length, 2, 'delivered both events')

  const info = all.find((e) => e.level === 'info')
  assert.equal(info.source, 'sdk-node', 'tagged source=sdk-node')
  assert.deepEqual(info.structured, { user_id: '123', plan: 'free' }, 'structured fields preserved')
  assert.ok(!Number.isNaN(Date.parse(info.timestamp)), 'ISO timestamp present')

  const err = all.find((e) => e.level === 'error')
  // #7: message must be the bare error text (stable) so occurrences group.
  assert.equal(err.message, 'redis GET auth:token -> nil', 'message is stable (no stack embedded)')
  assert.ok(err.structured.stack && err.structured.stack.includes('at '), 'stack lives in structured')
  assert.equal(err.structured.error, 'redis GET auth:token -> nil', 'error text in structured.error')
  assert.equal(err.structured.path, '/login', 'extra fields merged')
  s.close()
  ok('wire format · stable message · stack in structured')
}

// ── Test 2: retries transient 5xx, then succeeds, without surfacing an error ─
{
  const s = startServer()
  await s.listen()
  s.setFailNext(2) // first two requests 500, third succeeds
  let errCount = 0
  const client = new LogSenseClient('k', { endpoint: s.url(), onError: () => { errCount++ } })
  client.log('info', 'hi')
  await client.flush()
  await client.shutdown()
  assert.equal(s.received.length, 1, 'event delivered after retries')
  assert.equal(errCount, 0, 'onError not called on eventual success')
  s.close()
  ok('retries transient 5xx then succeeds')
}

// ── Test 3: 4xx does not retry and surfaces the error (no silent loss) ───────
{
  const s = startServer()
  await s.listen()
  s.setStatus(401) // bad API key
  let reqCount = 0
  s.server.on('request', () => { reqCount++ })
  let gotErr = null
  const client = new LogSenseClient('bad', { endpoint: s.url(), onError: (e) => { gotErr = e } })
  client.log('info', 'hi')
  await client.flush()
  await client.shutdown()
  assert.equal(reqCount, 1, '4xx must not retry')
  assert.ok(gotErr instanceof Error, 'onError fires on 401 — no silent failure')
  assert.match(gotErr.message, /401/, 'error mentions the status')
  s.close()
  ok('4xx surfaces error, no retry')
}

// ── Test 4: bounded queue drops and counts when the endpoint stalls ─────────
{
  const s = startServer()
  await s.listen()
  // Make the endpoint hang so the single sender stays busy on its first batch.
  let release
  const gate = new Promise((r) => { release = r })
  s.server.removeAllListeners('request')
  s.server.on('request', async (req, res) => { await gate; res.writeHead(202); res.end() })

  const client = new LogSenseClient('k', { endpoint: s.url(), maxQueue: 5, batchSize: 1 })
  for (let i = 0; i < 100; i++) client.log('info', `n${i}`)
  assert.ok(client.dropped > 0, `expected drops when queue full, got ${client.dropped}`)
  release()
  await client.shutdown()
  s.close()
  ok(`bounded queue drops + counts (dropped=${client.dropped})`)
}

// ── Test 5: span wire format · parent/child · attrs · error · log correlation ─
{
  const s = startServer()
  await s.listen()
  const client = new LogSenseClient('ls_live_test_key', {
    service: 'trace-svc', environment: 'test', endpoint: s.url(), flushIntervalMs: 50,
  })

  // Active span: nested child links up, and a log inside inherits the trace ID.
  const parentTrace = await client.startActiveSpan('GET /checkout', { kind: 'server' }, async (span) => {
    client.log('info', 'handling checkout')
    await client.startActiveSpan('db.query', { kind: 'client', attributes: { table: 'orders' } }, async () => {})
    return span.traceID
  })

  // A thrown error is recorded on the span and re-thrown.
  await assert.rejects(
    () => client.startActiveSpan('will-fail', async () => { throw new Error('boom') }),
    /boom/,
    'startActiveSpan re-throws',
  )

  await client.shutdown()

  const traceReqs = s.received.filter((r) => r.url.endsWith('/v1/traces/batch'))
  assert.ok(traceReqs.length > 0, 'posts spans to /v1/traces/batch')
  for (const r of traceReqs) {
    assert.equal(r.method, 'POST')
    assert.equal(r.apiKey, 'ls_live_test_key', 'sends X-API-Key')
    assert.ok(Array.isArray(r.body.spans), 'body is { spans: [...] }')
  }
  const spans = traceReqs.flatMap((r) => r.body.spans)

  const server = spans.find((sp) => sp.name === 'GET /checkout')
  const child = spans.find((sp) => sp.name === 'db.query')
  const failed = spans.find((sp) => sp.name === 'will-fail')
  assert.ok(server && child && failed, 'all three spans delivered')

  assert.equal(server.source, 'sdk-node', 'span tagged source=sdk-node')
  assert.equal(server.service, 'trace-svc', 'service stamped')
  assert.equal(server.kind, 'server', 'kind preserved')
  assert.match(server.traceID, /^[0-9a-f]{32}$/, 'trace ID is 16-byte hex')
  assert.match(server.spanID, /^[0-9a-f]{16}$/, 'span ID is 8-byte hex')
  assert.ok(!server.parentSpanID, 'root span has no parent')
  assert.ok(typeof server.durationMs === 'number' && server.durationMs >= 0, 'durationMs recorded')
  assert.ok(
    !Number.isNaN(Date.parse(server.startTime)) && !Number.isNaN(Date.parse(server.endTime)),
    'start/end times set',
  )

  assert.equal(child.traceID, server.traceID, 'child shares the parent trace')
  assert.equal(child.parentSpanID, server.spanID, 'child links to parent span')
  assert.deepEqual(child.attributes, { table: 'orders' }, 'span attributes preserved')

  assert.equal(failed.statusCode, 'ERROR', 'thrown error marks span ERROR')
  assert.equal(failed.statusMessage, 'boom', 'error message recorded')
  assert.equal(failed.attributes.error, 'boom', 'error attribute recorded')

  const logs = s.received.filter((r) => r.url.endsWith('/v1/logs/batch')).flatMap((r) => r.body.logs)
  const correlated = logs.find((l) => l.message === 'handling checkout')
  assert.ok(correlated, 'the in-span log was delivered')
  assert.equal(correlated.traceID, parentTrace, 'log inside a span inherits its trace ID')

  s.close()
  ok('span wire format · parent/child · attrs · error · log correlation')
}

console.log(`\nPASS  ${passed}/5  Node SDK wire format + reliability contract`)
