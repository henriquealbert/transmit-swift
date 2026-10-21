# Transmit for Swift

A native, zero-dependency Swift client for [AdonisJS Transmit](https://docs.adonisjs.com/guides/digging-deeper/server-sent-events), compatible with the `@adonisjs/transmit-client` 1.x wire protocol. Requires Swift 5.9+, iOS 15+ or macOS 12+.

It uses `URLSession.bytes`, Swift concurrency and typed `AsyncThrowingStream` consumers. Each SSE connection attempt uses a fresh UUID. Channels subscribe only after the SSE response opens, and active channels are restored after a dropped connection.

For a runnable AdonisJS backend and SwiftUI iOS app, see the [end-to-end example](Examples/README.md), including Simulator and reconnection instructions.

Tested against AdonisJS **7.6.0** with `@adonisjs/transmit` **3.0.2**, and AdonisJS **6.21.0** with `@adonisjs/transmit` **2.0.2**. Both combinations run in CI; the Swift client uses the same subscription endpoints, event envelopes, authentication and reconnect behavior for both.

## Install

Add the package in Xcode, or add it to `Package.swift`:

```swift
.package(url: "https://github.com/henriquealbert/transmit-swift.git", from: "0.1.2")
```

Add `.product(name: "Transmit", package: "transmit-swift")` to your target's dependencies.

## Receive messages

```swift
import Foundation
import Transmit

struct Notification: Codable, Sendable {
    let title: String
    let unreadCount: Int
}

let client = try TransmitClient(baseURL: URL(string: "https://api.example.com")!)
let messages = await client.messages(on: "users/42/notifications", as: Notification.self)
try await client.awaitSubscribed("users/42/notifications")
let listener = Task {
    do {
        for try await message in messages {
            print(message.title, message.unreadCount)
        }
    } catch {
        print("Subscription ended:", error)
    }
}

// When this consumer is no longer needed:
listener.cancel()

// When the owner of the client goes away (for example, logout):
await client.close()
```

Each call to `messages` creates an independent consumer. Consumers on the same channel share one server subscription. Cancelling the consuming task removes that consumer; the last consumer's termination sends `POST /__transmit/unsubscribe`. `await client.unsubscribe(from: "users/42/notifications")` ends all consumers on that channel. Merely breaking a loop does not reliably terminate an async stream; cancel its task or explicitly unsubscribe.

`try await client.awaitSubscribed(channel)` confirms that the subscribe POST succeeded on the current SSE connection. Waiting does not create a subscription implicitly: if there is no consumer yet, it remains pending until a consumer is registered with `messages` and its subscribe POST succeeds. A confirmed active channel returns immediately, and multiple callers can wait independently. Cancelling one wait throws `CancellationError` without removing the consumer or cancelling other waits.

Unsubscribing or ending the last consumer fails pending waits with `TransmitError.notSubscribed(channel)`, including a wait started before a consumer. Subscription failures are retained so even a fast failed POST is reported to a later waiter; registering a new consumer or explicitly unsubscribing clears that failure. Closing fails all pending waits with `closed`, including channels without a consumer, and terminal SSE authentication failure propagates its HTTP error to all waits. Confirmation is invalidated on disconnect or authentication refresh, so await it again before starting new work on the replacement stream. It confirms the connection at that moment; a subsequent network failure is still possible.

Payloads may be objects, arrays, scalars or `null` (use an optional `Decodable` type). Malformed envelopes and ping comments are ignored. A payload decoding error or failed subscription finishes the affected stream with an error. Network failures opening or reading SSE reconnect automatically. Transmit does not replay messages lost during a disconnect. Applications should also tolerate duplicate events after reconnection/resubscription, especially when their backend implements replay or redelivery. Include a stable `seq` field in each payload and deduplicate it per channel; the chat example below does this. A server restart must not reuse sequence values within that same deduplication scope.

Streams are unbounded by default. To bound a slow consumer's memory, pass `bufferingPolicy: .bufferingNewest(100)`; this intentionally drops older buffered messages. Retain the client for the lifetime of its consumers and call `close()` explicitly for deterministic cleanup. Releasing the last client reference also schedules connection cancellation and finishes its streams; the background loop does not retain the public client. Cleanup from `deinit` is asynchronous, so await `close()` when completion matters. The SwiftUI example also calls `close()` from `onDisappear`.

## Authentication and JWT refresh

```swift
actor Credentials {
    private var token: String
    init(token: String) { self.token = token }
    func currentToken() async throws -> String? {
        // Refresh an expired JWT here using your app's authentication service.
        token
    }
    func update(_ newToken: String) { token = newToken }
}

let credentials = Credentials(token: "your-jwt")
let client = try TransmitClient(
    baseURL: URL(string: "https://api.example.com")!,
    tokenProvider: { try await credentials.currentToken() }
)

// After your app obtains a new token:
await credentials.update("refreshed-jwt")
await client.refreshAuthentication()
```

The async provider is invoked before every stream, subscribe and unsubscribe request. A returned token becomes the `Authorization: Bearer …` header. Return `nil` for anonymous requests; for a fixed token, use `tokenProvider: { "your-jwt" }`.

If the backend requires query authentication, pass `tokenQueryParameter: "token"`. This adds the token to both GET and POST URLs, in addition to the header. Enable it only when needed because URLs can appear in proxy logs. Header authentication works without it.

`refreshAuthentication()` schedules immediate reopening of a connected stream with the provider's latest credentials and restores active channels. It does not wait for the new connection. The provider is called again for each retry. After three consecutive HTTP 401/403 responses from the SSE GET, retries stop, `state` becomes `.authenticationFailed`, `lastError` contains the HTTP error, and active message streams finish with that error. A successful open or a different opening error resets the consecutive-authentication-failure count. After repairing credentials, call `refreshAuthentication()` and create new message consumers, or create a new client. A failed subscribe request is reported to its consumers so the app can decide when to retry the channel. Configure authentication and native-client CSRF rules on the Adonis server for all three Transmit routes.

## Connection behavior

- `await client.connect()` starts connecting without a subscription; it returns before the stream opens. Otherwise, the first consumer opens `GET {baseURL}/__transmit/events?uid=…` with `Accept: text/event-stream`.
- Subscriptions use JSON `{ "uid": "…", "channel": "…" }` and the official subscribe/unsubscribe endpoints.
- `client.uid` is a synchronous, thread-safe getter for the current connection attempt's UUID. It changes whenever reconnect or authentication refresh opens a replacement stream; each stream's subscribe and unsubscribe POSTs use its UID. Do not use it as a persistent client or user identity. Earlier versions reused a single UID across streams, allowing a delayed old-stream close handler to erase the replacement's server mapping. Transmit 3 then rejects new subscriptions with HTTP 400; Transmit 2 can acknowledge an unregistered subscription instead.
- Retries wait 1, 2, 4, 8, 16, then 30 seconds, capped at 30. Opening the SSE stream resets the delay to one second, even before any event arrives. Explicit authentication refresh while connected reopens immediately without a backoff delay.
- `await client.state` reports idle, connecting, connected, reconnecting, authenticationFailed or closed. `await client.lastError` exposes the latest SSE opening failure and clears when a stream opens successfully. Connected means the HTTP stream has opened; use `awaitSubscribed(channel)` to confirm a specific channel.
- `close()` is permanent and idempotent. It ends consumers, cancels the reader and retry delay, and attempts unsubscription. Cleanup POST failures are best effort when offline. Create a new client to start again.
- A base URL can include a path prefix, but cannot include credentials, a query or a fragment. Use HTTPS in production. An injected `URLSession` can supply app-specific timeout and networking configuration.

The parser follows Transmit's one-JSON-envelope-per-`data:`-line protocol. It is not a general multiline SSE/EventSource implementation. The package targets Apple platforms; Foundation's streaming URLSession API is used directly.

## URLSession and SSE read timeout

Pass your own session to configure headers, cookies, caching and timeouts for both SSE and the subscription POSTs. A read/idle timeout around **90 seconds** is a useful starting point; set the server's ping interval comfortably below it:

```swift
let baseURL = URL(string: "https://api.example.com")!
let configuration = URLSessionConfiguration.default
configuration.timeoutIntervalForRequest = 90
let session = URLSession(configuration: configuration)
let client = try TransmitClient(
    baseURL: baseURL,
    session: session
)

// Once your app has finished using this client and its dedicated session:
await client.close()
session.invalidateAndCancel()
```

Apple defines [`timeoutIntervalForRequest`](https://developer.apple.com/documentation/foundation/urlsessionconfiguration/timeoutintervalforrequest) as the wait for additional data; receiving data resets that timer. `timeoutIntervalForResource` separately limits the whole request's duration, so setting it to 90 seconds would cap the entire SSE connection. Tune that total-duration limit separately if needed. The library leaves an injected session's configuration and lifetime under your control; it does not invalidate a shared session.

## Subscribe, publish and receive a chat message

Use channels such as `chats/general` for chat and `users/42/notifications` for notifications. Transmit carries server-to-client events; publishing from an app uses your own HTTP endpoint. Register the consumer and call `awaitSubscribed(channel)` before that POST so the server subscription is confirmed before publishing. Events arriving during the POST are already buffered by the registered Swift stream.

Here is a small Adonis example for `start/routes.ts`, with Transmit configured and body parsing enabled:

```typescript
import { randomUUID } from 'node:crypto'
import router from '@adonisjs/core/services/router'
import transmit from '@adonisjs/transmit/services/main'

const channel = 'chats/general'
const serverInstance = randomUUID()
let seq = 0
transmit.registerRoutes() // Register once in your application.

router.post('/broadcast', ({ request, response }) => {
  const text = request.input('text')
  if (typeof text !== 'string' || !text.trim() || text.length > 200) {
    return response.badRequest({ error: 'Provide a nonempty text string of at most 200 characters.' })
  }
  const message = { id: randomUUID(), text: text.trim(), seq: ++seq, serverInstance }
  transmit.broadcast(channel, message)
  return message
})
```

The Swift example performs one message round trip, then unsubscribes and closes its client. A continuous chat screen can keep consuming until its task is cancelled. Apply your application's authentication to `/broadcast` and channel authorization to private channels; the Transmit token provider applies only to its own three endpoints.

```swift
import Foundation
import Transmit

struct ChatMessage: Decodable, Sendable {
    let id: String
    let text: String
    let seq: Int
    let serverInstance: String
}

func sendAndReceive(_ text: String, baseURL: URL) async throws {
    let configuration = URLSessionConfiguration.default
    configuration.timeoutIntervalForRequest = 90
    let session = URLSession(configuration: configuration)
    defer { session.invalidateAndCancel() }
    let client = try TransmitClient(baseURL: baseURL, session: session)
    let channel = "chats/general"
    let messages = await client.messages(on: channel, as: ChatMessage.self)

    do {
        try await client.awaitSubscribed(channel)
        var request = URLRequest(url: baseURL.appendingPathComponent("broadcast"))
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONEncoder().encode(["text": text])
        let (data, response) = try await session.data(for: request)
        guard let response = response as? HTTPURLResponse else {
            throw TransmitError.invalidResponse
        }
        guard (200..<300).contains(response.statusCode) else {
            throw TransmitError.httpStatus(response.statusCode)
        }
        let published = try JSONDecoder().decode(ChatMessage.self, from: data)
        var seenSequences: Set<String> = []
        var received = false
        for try await message in messages {
            let key = "\(message.serverInstance):\(message.seq)"
            guard seenSequences.insert(key).inserted else { continue }
            print(message.text)
            if message.id == published.id {
                received = true
                break
            }
        }
        try Task.checkCancellation()
        guard received else { throw TransmitError.closed }
        await client.unsubscribe(from: channel)
        await client.close()
    } catch {
        await client.unsubscribe(from: channel)
        await client.close()
        throw error
    }
}

// try await sendAndReceive("Hello from Swift", baseURL: URL(string: "http://127.0.0.1:3333")!)
```

The `seq` value is scoped to a channel and server instance, so a server restart cannot make new messages look like duplicates. This example does not persist or replay messages missed during an outage. Resubscription restores future delivery; applications that require replay must store messages on the backend and deduplicate redeliveries using stable sequence values. `awaitSubscribed` confirms the current connection only; a later disconnect can still interrupt delivery. A screen can cancel its consuming task when it leaves, and explicitly unsubscribe and close for deterministic cleanup.

See the [runnable chat example](Examples/README.md) for a backend with periodic broadcasts and a SwiftUI app that publishes a greeting after subscription confirmation.

## Tests and coverage

Unit tests need only the Swift toolchain:

```sh
swift build
swift test
```

The integration test launches its own loopback-only server on a dynamic port and shuts it down afterward. By default it runs real AdonisJS 7.6.0 routing, body parsing and `@adonisjs/transmit` 3.0.2 controllers, with actual socket disconnects, channel authorization, token rotation and broadcasts. Transmit 3.0.2 declares an AdonisJS core peer dependency of `^7.0.0-next`. Install Node.js 24 and run:

```sh
npm ci --prefix Tests/IntegrationServer --no-audit --no-fund
TRANSMIT_INTEGRATION=1 swift test --enable-code-coverage
python3 scripts/check-coverage.py
```

The original AdonisJS 6.21.0 / Transmit 2.0.2 dependency manifests and lockfile are retained in `Tests/IntegrationServer/adonis6`. To run that combination without changing the v7 manifests, use a temporary fixture directory:

```sh
fixture=$(mktemp -d)
cp Tests/IntegrationServer/adonis6/package{,-lock}.json "$fixture/"
cp Tests/IntegrationServer/server.mjs "$fixture/"
(cd "$fixture" && npm ci --no-audit --no-fund)
TRANSMIT_INTEGRATION=1 TRANSMIT_INTEGRATION_SERVER="$fixture/server.mjs" swift test --enable-code-coverage
python3 scripts/check-coverage.py
rm -rf "$fixture"
```

CI runs the build, unit tests, real-server integration and an iOS 15 build against both server combinations using locked dependencies and the same server script. The integration test holds an old stream's close notification until its replacement has subscribed, releases the old cleanup, then verifies a new channel can subscribe and receive a real broadcast. A separate Swift 6 language-mode matrix builds and runs the same tests with complete concurrency checking and compiler warnings treated as errors; all four jobs enforce the coverage gate. The shared fixture supplies AdonisJS 7's required encryption configuration when that API is available. The coverage gate requires exactly **100% of library lines, functions and regions** in every file under `Sources`, with no source exclusions. Tests and the JavaScript fixture are outside that library metric. Integration is explicitly skipped unless `TRANSMIT_INTEGRATION=1` is set; the coverage command requires it to exercise the live transport.

## License

MIT. An independent community client; not an official AdonisJS package.
