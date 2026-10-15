import Foundation

/// A native AdonisJS Transmit client. Explicitly close for deterministic cleanup.
/// Releasing the last client reference also schedules cancellation of its connection.
public actor TransmitClient {
    public typealias TokenProvider = @Sendable () async throws -> String?

    public enum State: Sendable, Equatable {
        case idle, connecting, connected, reconnecting, authenticationFailed, closed
    }

    /// Stable for this instance, including reconnects and authentication refreshes.
    public nonisolated let uid: String
    let connection: TransmitConnection

    public var state: State { get async { await connection.state } }
    /// The latest SSE opening failure; cleared when a stream opens successfully.
    public var lastError: (any Error)? { get async { await connection.lastError } }

    /// The provider runs before every HTTP request. Inject a URLSession to configure
    /// SSE idle/read timeouts (90 seconds is a useful starting point) and networking.
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
         sleep: @escaping @Sendable (UInt64) async throws -> Void) throws {
        uid = UUID().uuidString
        connection = try TransmitConnection(uid: uid, baseURL: baseURL,
            tokenQueryParameter: tokenQueryParameter, tokenProvider: tokenProvider,
            transport: transport, sleep: sleep)
    }

    deinit {
        // The loop owns the connection, never this public lifetime owner.
        let connection = connection
        Task { await connection.close() }
    }

    /// Starts connecting without subscribing to a channel; inspect `state` for readiness.
    public func connect() async { await connection.start() }

    /// Each call creates an independent consumer; a channel shares one server subscription.
    public func messages<Payload: Decodable & Sendable>(
        on channel: String, as type: Payload.Type = Payload.self,
        bufferingPolicy: AsyncThrowingStream<Payload, Error>.Continuation.BufferingPolicy = .unbounded
    ) async -> AsyncThrowingStream<Payload, Error> {
        let id = UUID()
        var continuation: AsyncThrowingStream<Payload, Error>.Continuation!
        let stream = AsyncThrowingStream<Payload, Error>(bufferingPolicy: bufferingPolicy) { continuation = $0 }
        let sink = continuation!
        sink.onTermination = { [weak connection] _ in
            Task { await connection?.remove(channel: channel, id: id) }
        }
        await connection.add(channel: channel, id: id, sink: TransmitConnection.Sink(receive: { data in
            do {
                sink.yield(try JSONDecoder().decode(type, from: data))
            } catch {
                sink.finish(throwing: error)
            }
        }, finish: { error in sink.finish(throwing: error) }))
        return stream
    }

    /// Reopens with fresh credentials. After authenticationFailed, register new consumers.
    public func refreshAuthentication() async { await connection.refreshAuthentication() }

    /// Ends every consumer of a channel and sends an unsubscribe once the stream is open.
    public func unsubscribe(from channel: String) async { await connection.unsubscribe(from: channel) }

    /// Permanently closes the client. Unsubscription is best effort when offline.
    public func close() async { await connection.close() }
}

/// Separate ownership lets TransmitClient.deinit cancel a loop that is awaiting network I/O.
actor TransmitConnection {
    private(set) var state: TransmitClient.State = .idle
    private(set) var lastError: (any Error)?
    private let uid: String
    private let baseURL: URL
    private let tokenProvider: TransmitClient.TokenProvider
    private let tokenQueryParameter: String?
    private let transport: Transport
    private let sleep: @Sendable (UInt64) async throws -> Void
    private let commands: AsyncStream<Command>
    private let commandSink: AsyncStream<Command>.Continuation
    private var worker: Task<Void, Never>?
    private var channels: [String: [UUID: Sink]] = [:]

    private enum Command: Sendable {
        case reconcile(String), line(UUID, String), ended(UUID), reconnect
    }

    struct Sink: Sendable {
        let receive: @Sendable (Data) -> Void
        let finish: @Sendable (Error?) -> Void
    }

    init(uid: String, baseURL: URL, tokenQueryParameter: String?,
         tokenProvider: @escaping TransmitClient.TokenProvider, transport: Transport,
         sleep: @escaping @Sendable (UInt64) async throws -> Void) throws {
        guard ["http", "https"].contains(baseURL.scheme), baseURL.host != nil,
              baseURL.query == nil, baseURL.fragment == nil,
              baseURL.user == nil, baseURL.password == nil else {
            throw TransmitError.invalidBaseURL
        }
        self.uid = uid
        self.baseURL = baseURL
        self.tokenQueryParameter = tokenQueryParameter
        self.tokenProvider = tokenProvider
        self.transport = transport
        self.sleep = sleep
        var continuation: AsyncStream<Command>.Continuation!
        self.commands = AsyncStream { continuation = $0 }
        self.commandSink = continuation
    }

    func start() {
        guard worker == nil, state != .closed, state != .authenticationFailed else { return }
        worker = Task { await self.run() }
    }

    func add(channel: String, id: UUID, sink: Sink) {
        guard state != .closed else {
            sink.finish(TransmitError.closed)
            return
        }
        guard state != .authenticationFailed else {
            sink.finish(lastError)
            return
        }
        channels[channel, default: [:]][id] = sink
        commandSink.yield(.reconcile(channel))
        start()
    }

    func refreshAuthentication() {
        if state == .authenticationFailed {
            state = .idle
            lastError = nil
            start()
        } else {
            commandSink.yield(.reconnect)
        }
    }

    func unsubscribe(from channel: String) {
        finish(channel: channel, error: nil)
        commandSink.yield(.reconcile(channel))
    }

    func close() async {
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

    func remove(channel: String, id: UUID) {
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
        var authenticationFailures = 0
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
                authenticationFailures = 0
                lastError = nil
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
            } catch {
                if Task.isCancelled { break }
                lastError = error
                switch error as? TransmitError {
                case .httpStatus(401), .httpStatus(403): authenticationFailures += 1
                default: authenticationFailures = 0
                }
                if authenticationFailures == 3 {
                    state = .authenticationFailed
                    for channel in Array(channels.keys) { finish(channel: channel, error: error) }
                    break
                }
            }
            if Task.isCancelled { break }
            state = .reconnecting
            do { try await sleep(delay) } catch { break }
            delay = min(delay * 2, 30_000_000_000)
        }
        worker = nil
    }
}
