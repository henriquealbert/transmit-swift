import Foundation

/// A native AdonisJS Transmit client. Call `close()` when its owner goes away.
public actor TransmitClient {
    public typealias TokenProvider = @Sendable () async throws -> String?

    public enum State: Sendable {
        case idle, connecting, connected, reconnecting, closed
    }

    /// Stable for this instance, including reconnects and authentication refreshes.
    public nonisolated let uid = UUID().uuidString
    public private(set) var state: State = .idle

    private let baseURL: URL
    private let tokenProvider: TokenProvider
    private let tokenQueryParameter: String?
    private let transport: Transport
    private let sleep: (UInt64) async throws -> Void
    private let commands: AsyncStream<Command>
    private let commandSink: AsyncStream<Command>.Continuation
    private var worker: Task<Void, Never>?
    private var channels: [String: [UUID: Sink]] = [:]

    private enum Command {
        case reconcile(String), line(UUID, String), ended(UUID), reconnect
    }

    private struct Sink {
        let receive: (Data) -> Void
        let finish: (Error?) -> Void
    }

    /// The provider is called before every HTTP request. Return a fresh JWT when necessary.
    /// Query authentication is opt-in because URLs can be logged by proxies.
    public init(
        baseURL: URL,
        tokenQueryParameter: String? = nil,
        session: URLSession = .shared,
        tokenProvider: @escaping TokenProvider = { nil }
    ) throws {
        try self.init(baseURL: baseURL, tokenQueryParameter: tokenQueryParameter,
                      tokenProvider: tokenProvider, transport: .live(session: session),
                      sleep: { try await Task.sleep(nanoseconds: $0) })
    }

    init(baseURL: URL, tokenQueryParameter: String? = nil,
         tokenProvider: @escaping TokenProvider = { nil }, transport: Transport,
         sleep: @escaping (UInt64) async throws -> Void) throws {
        guard ["http", "https"].contains(baseURL.scheme), baseURL.host != nil,
              baseURL.query == nil, baseURL.fragment == nil,
              baseURL.user == nil, baseURL.password == nil else {
            throw TransmitError.invalidBaseURL
        }
        self.baseURL = baseURL
        self.tokenQueryParameter = tokenQueryParameter
        self.tokenProvider = tokenProvider
        self.transport = transport
        self.sleep = sleep
        var continuation: AsyncStream<Command>.Continuation!
        self.commands = AsyncStream { continuation = $0 }
        self.commandSink = continuation
    }

    /// Each call creates an independent consumer; consumers on the same channel share one subscription.
    /// Cancellation removes that consumer. The last consumer's end unsubscribes the channel.
    public func messages<Payload: Decodable & Sendable>(
        on channel: String, as type: Payload.Type = Payload.self,
        bufferingPolicy: AsyncThrowingStream<Payload, Error>.Continuation.BufferingPolicy = .unbounded
    ) -> AsyncThrowingStream<Payload, Error> {
        let id = UUID()
        return AsyncThrowingStream(bufferingPolicy: bufferingPolicy) { continuation in
            guard state != .closed else {
                continuation.finish(throwing: TransmitError.closed)
                return
            }
            channels[channel, default: [:]][id] = Sink(receive: { data in
                do {
                    continuation.yield(try JSONDecoder().decode(type, from: data))
                } catch {
                    continuation.finish(throwing: error)
                }
            }, finish: { error in continuation.finish(throwing: error) })
            continuation.onTermination = { _ in
                Task { await self.remove(channel: channel, id: id) }
            }
            commandSink.yield(.reconcile(channel))
            if worker == nil {
                worker = Task { await self.run() }
            }
        }
    }

    /// Reopens the SSE connection using the provider's current token and restores active channels.
    public func refreshAuthentication() {
        commandSink.yield(.reconnect)
    }

    /// Ends every consumer of a channel and sends an unsubscribe once the stream is open.
    public func unsubscribe(from channel: String) {
        finish(channel: channel, error: nil)
        commandSink.yield(.reconcile(channel))
    }

    /// Permanently closes this client. Unsubscriptions are best effort if the network is unavailable.
    public func close() async {
        guard state != .closed else { return }
        state = .closed
        let active = Array(channels.keys)
        for channel in active { finish(channel: channel, error: nil) }
        worker?.cancel()
        commandSink.finish()
        await worker?.value
        for channel in active { try? await post("unsubscribe", channel: channel) }
        worker = nil
    }

    private func remove(channel: String, id: UUID) {
        channels[channel]?[id] = nil
        if channels[channel]?.isEmpty == true {
            channels[channel] = nil
            commandSink.yield(.reconcile(channel))
        }
    }

    private func finish(channel: String, error: Error?) {
        let sinks = channels.removeValue(forKey: channel)
        for sink in sinks?.values ?? [:].values { sink.finish(error) }
    }

    private func request(_ action: String, channel: String? = nil) async throws -> URLRequest {
        let token = try await tokenProvider()
        var components = URLComponents(url: baseURL.appendingPathComponent("__transmit/\(action)"),
                                       resolvingAgainstBaseURL: false)!
        var query: [URLQueryItem] = []
        if channel == nil { query.append(URLQueryItem(name: "uid", value: uid)) }
        if let key = tokenQueryParameter, let token = token {
            query.append(URLQueryItem(name: key, value: token))
        }
        components.queryItems = query.isEmpty ? nil : query
        var request = URLRequest(url: components.url!)
        request.setValue("text/event-stream", forHTTPHeaderField: "Accept")
        if let token = token { request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization") }
        if let channel = channel {
            request.httpMethod = "POST"
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
            request.httpBody = try JSONEncoder().encode(["uid": uid, "channel": channel])
        }
        return request
    }

    private func post(_ action: String, channel: String) async throws {
        try await transport.send(request(action, channel: channel))
    }

    private func reconcile(_ channel: String, subscribed: inout Set<String>) async {
        if channels[channel] != nil && !subscribed.contains(channel) {
            do {
                try await post("subscribe", channel: channel)
                subscribed.insert(channel)
            } catch {
                finish(channel: channel, error: error)
            }
        } else if channels[channel] == nil && subscribed.remove(channel) != nil {
            try? await post("unsubscribe", channel: channel)
        }
    }

    private func run() async {
        var delay: UInt64 = 1_000_000_000
        var iterator = commands.makeAsyncIterator()
        while !Task.isCancelled {
            state = state == .idle ? .connecting : .reconnecting
            do {
                let connection = try await transport.open(request("events"))
                let generation = UUID()
                let reader = Task {
                    do {
                        for try await line in connection.lines { commandSink.yield(.line(generation, line)) }
                    } catch { /* EOF and network errors both reconnect. */ }
                    commandSink.yield(.ended(generation))
                }
                defer { reader.cancel(); connection.cancel() }
                try Task.checkCancellation()
                state = .connected
                var subscribed: Set<String> = []
                for channel in Array(channels.keys) { await reconcile(channel, subscribed: &subscribed) }
                connectionLoop: while let command = await iterator.next() {
                    switch command {
                    case .reconcile(let channel):
                        await reconcile(channel, subscribed: &subscribed)
                    case .line(let source, let line):
                        if source == generation, let event = SSEParser.parse(line) {
                            delay = 1_000_000_000
                            for sink in channels[event.channel]?.values ?? [:].values { sink.receive(event.payload) }
                        }
                    case .reconnect:
                        break connectionLoop
                    case .ended(let source):
                        if source == generation { break connectionLoop }
                    }
                }
            } catch { /* Failed opens (including expired tokens) retry with a fresh provider call. */ }
            if Task.isCancelled { break }
            state = .reconnecting
            do { try await sleep(delay) } catch { break }
            delay = min(delay * 2, 30_000_000_000)
        }
    }
}
