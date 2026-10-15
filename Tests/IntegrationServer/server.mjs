// Real AdonisJS routing, body parser, Transmit provider and Transmit controllers.
// Test-only control routes are bound exclusively to loopback.
import 'reflect-metadata'
import { createServer } from 'node:http'
import { Application } from '@adonisjs/core/app'
import { setApp } from '@adonisjs/core/services/app'
import { defineConfig as httpConfig } from '@adonisjs/core/http'
import { defineConfig as bodyConfig } from '@adonisjs/core/bodyparser'

const app = new Application(new URL('./', import.meta.url), { environment: 'web' })
setApp(app)
app.rcContents({ providers: [
  () => import('@adonisjs/core/providers/app_provider'),
  () => import('@adonisjs/transmit/transmit_provider'),
] })
app.useConfig({
  app: { appKey: 'integration-only-key-do-not-use-in-production', http: httpConfig({}) },
  logger: { default: 'app', loggers: { app: { enabled: false, name: 'integration', level: 'silent' } } },
  bodyparser: bodyConfig({}),
  transmit: { pingInterval: '100ms', transport: null },
})
await app.init()
await app.boot()
const server = await app.container.make('server')
const router = await app.container.make('router')
const transmit = await app.container.make('transmit')
const history = []
const sockets = new Set()
const eventSockets = new Set()
let requiredToken = 'first'
let rejectNext = false
let invalidContent = false
let endNext = false
server.use([() => import('@adonisjs/core/bodyparser_middleware')])
router.use([() => Promise.resolve({ default: class {
  async handle(ctx, next) {
    if (ctx.request.url().startsWith('/__transmit/')) {
      history.push({ path: ctx.request.url(), authorization: ctx.request.header('authorization') ?? null,
        query: ctx.request.qs(), body: ctx.request.body(), sessionHeader: ctx.request.header('x-transmit-session') ?? null })
      if (ctx.request.header('authorization') !== `Bearer ${requiredToken}`) return ctx.response.unauthorized()
      if (rejectNext) { rejectNext = false; return ctx.response.serviceUnavailable() }
      if (invalidContent) { invalidContent = false; return ctx.response.send('not SSE') }
      if (ctx.request.url().endsWith('/events')) {
        const socket = ctx.request.request.socket
        eventSockets.add(socket)
        socket.once('close', () => eventSockets.delete(socket))
        ctx.response.response.once('finish', () => eventSockets.delete(socket))
      }
      if (endNext && ctx.request.url().endsWith('/events')) {
        endNext = false
        ctx.response.header('content-type', 'text/event-stream')
        return ctx.response.send(': goodbye\n\n')
      }
    }
    return next()
  }
} })])
transmit.registerRoutes()
transmit.authorize('denied', () => false)
router.get('/health', () => ({ ok: true }))
router.get('/history', () => history)
router.post('/broadcast', ({ request }) => {
  transmit.broadcast(request.input('channel'), request.input('payload'))
  return { ok: true }
})
router.post('/control', ({ request }) => {
  requiredToken = request.input('token', requiredToken)
  rejectNext = request.input('rejectNext', false)
  invalidContent = request.input('invalidContent', false)
  endNext = request.input('endNext', false)
  if (request.input('disconnect', false)) {
    for (const socket of eventSockets) socket.destroy()
  }
  return { ok: true }
})
await app.start(async () => {
  await server.boot()
  const http = createServer(server.handle.bind(server))
  server.setNodeServer(http)
  http.on('connection', socket => { sockets.add(socket); socket.on('close', () => sockets.delete(socket)) })
  http.listen(Number(process.env.PORT ?? 0), '127.0.0.1', () => {
    console.log(`http://127.0.0.1:${http.address().port}`)
  })
  process.on('SIGTERM', async () => {
    for (const socket of sockets) socket.destroy()
    http.close()
    await app.terminate()
    process.exit(0)
  })
})
