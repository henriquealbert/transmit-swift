import 'reflect-metadata'
import { randomUUID } from 'node:crypto'
import { createServer } from 'node:http'
import { Application } from '@adonisjs/core/app'
import { setApp } from '@adonisjs/core/services/app'
import { defineConfig as httpConfig } from '@adonisjs/core/http'
import { defineConfig as bodyConfig } from '@adonisjs/core/bodyparser'

// An actual Adonis application, configured inline to keep the example small.
const app = new Application(new URL('./', import.meta.url), { environment: 'web' })
setApp(app)
app.rcContents({ providers: [
  () => import('@adonisjs/core/providers/app_provider'),
  () => import('@adonisjs/transmit/transmit_provider'),
] })
app.useConfig({
  app: { appKey: 'local-example-only-not-a-production-secret', http: httpConfig({}) },
  logger: { default: 'app', loggers: { app: { enabled: false, name: 'example', level: 'silent' } } },
  bodyparser: bodyConfig({}),
  transmit: { pingInterval: '1s', transport: null },
})
await app.init()
await app.boot()
const server = await app.container.make('server')
const router = await app.container.make('router')
const transmit = await app.container.make('transmit')
const channel = 'demo/messages'
const serverInstance = randomUUID().slice(0, 8)
let sequence = 0
server.use([() => import('@adonisjs/core/bodyparser_middleware')])
transmit.registerRoutes()
transmit.on('subscribe', ({ uid, channel }) => console.log(`SUBSCRIBED uid=${uid} channel=${channel} server=${serverInstance}`))

function publish(text, source) {
  const message = { id: randomUUID(), text, source, serverInstance, sequence: ++sequence, sentAt: new Date().toISOString() }
  transmit.broadcast(channel, message)
  console.log(`PUBLISHED server=${serverInstance} sequence=${sequence} source=${source} text=${text}`)
  return message
}
router.get('/health', () => ({ ok: true, channel, serverInstance }))
router.post('/broadcast', ({ request, response }) => {
  const text = request.input('text')
  if (typeof text !== 'string' || !text.trim() || text.length > 200) {
    return response.badRequest({ error: 'Provide a nonempty text string of at most 200 characters.' })
  }
  return publish(text.trim(), 'manual')
})

await app.start(async () => {
  await server.boot()
  const http = createServer(server.handle.bind(server))
  const sockets = new Set()
  server.setNodeServer(http)
  http.on('connection', socket => {
    sockets.add(socket)
    socket.on('close', () => sockets.delete(socket))
  })
  const port = Number(process.env.PORT ?? 3333)
  await new Promise((resolve, reject) => {
    http.once('error', reject)
    http.listen(port, '127.0.0.1', resolve)
  })
  console.log(`READY http://127.0.0.1:${port} channel=${channel} server=${serverInstance}`)
  const timer = setInterval(() => publish('Hello from the Adonis timer', 'timer'), 5000)
  async function shutdown() {
    clearInterval(timer)
    for (const socket of sockets) socket.destroy()
    http.close()
    await app.terminate()
    process.exit(0)
  }
  process.once('SIGINT', shutdown)
  process.once('SIGTERM', shutdown)
})
