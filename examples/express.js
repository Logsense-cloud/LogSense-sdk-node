// Example: capture every Express error with LogSense.
//
//   npm install express @logsense/node
//   LOGSENSE_API_KEY=ls_live_... node examples/express.js

const express = require('express')
const logsense = require('@logsense/node') // within this repo: require('../index.js')

logsense.init(process.env.LOGSENSE_API_KEY || 'ls_live_your_api_key', {
  service: 'my-api',
  environment: process.env.NODE_ENV || 'production',
})

const app = express()

app.get('/', (req, res) => res.send('ok'))

// A route that throws — LogSense will capture it below.
app.get('/boom', () => {
  throw new Error('kaboom from /boom')
})

// A structured log line, no error required.
app.get('/signup', (req, res) => {
  logsense.log('info', 'user signed up', { plan: 'free', ip: req.ip })
  res.json({ ok: true })
})

// Error-handling middleware: capture, then respond.
app.use((err, req, res, _next) => {
  logsense.capture(err, { path: req.path, method: req.method, status: 500 })
  res.status(500).json({ error: 'internal error' })
})

const server = app.listen(3000, () => console.log('listening on http://localhost:3000'))

// Flush buffered events before the process exits.
async function stop() {
  await logsense.shutdown()
  server.close(() => process.exit(0))
}
process.on('SIGTERM', stop)
process.on('SIGINT', stop)
