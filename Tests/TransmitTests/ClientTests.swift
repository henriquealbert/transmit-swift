import XCTest
@testable import Transmit

func eventually(_ condition: @escaping () async throws -> Bool, file: StaticString = #filePath, line: UInt = #line) async throws {
    for _ in 0..<500 {
        if try await condition() { return }
        try await Task.sleep(nanoseconds: 10_000_000)
    }
    XCTFail("Timed out waiting for condition", file: file, line: line)
    throw URLError(.timedOut)
}

final class WeakReference<Value: AnyObject> {
    private(set) weak var value: Value?
    init(_ value: Value?) { self.value = value }
}

actor FakeTransport {
    var requests: [URLRequest] = []
    var streams: [AsyncThrowingStream<String, Error>.Continuation] = []
    var delays: [UInt64] = []
    var failures = 0
    var openErrors: [TransmitError] = []
    var sleepCancelled = false
    var denied = Set<String>()
    var openingPaused = false
    var openGate: CheckedContinuation<Void, Never>?
    var cancellations = 0

    nonisolated var transport: Transport {
        Transport(open: { try await self.open($0) }, send: { try await self.send($0) })
    }
    func open(_ request: URLRequest) async throws -> EventConnection {
        requests.append(request)
        if openingPaused { await withCheckedContinuation { openGate = $0 } }
        if !openErrors.isEmpty { throw openErrors.removeFirst() }
        if failures > 0 { failures -= 1; throw URLError(.cannotConnectToHost) }
        var sink: AsyncThrowingStream<String, Error>.Continuation!
        let stream = AsyncThrowingStream<String, Error> { sink = $0 }
        streams.append(sink)
        let index = streams.count - 1
        return EventConnection(lines: stream, cancel: { Task { await self.cancel(index) } })
    }
    func cancel(_ index: Int) { cancellations += 1; streams[index].finish() }
    func send(_ request: URLRequest) throws {
        requests.append(request)
        if denied.contains(body(request)["channel"] ?? "") { throw TransmitError.httpStatus(403) }
    }
    func sleep(_ delay: UInt64) async throws {
        delays.append(delay)
        try await Task.sleep(nanoseconds: 5_000_000)
    }
    func configure(failures: Int = 0, denied: Set<String> = [], paused: Bool = false, errors: [TransmitError] = []) {
        self.openErrors = errors
        self.failures = failures; self.denied = denied; self.openingPaused = paused
    }
    func waitForCancellation(_ delay: UInt64) async throws {
        delays.append(delay)
        do { try await Task.sleep(nanoseconds: 10_000_000_000) }
        catch { sleepCancelled = true; throw error }
    }
    func releaseOpen() { openingPaused = false; openGate?.resume(); openGate = nil }
    func emit(_ line: String) { streams.last?.yield(line) }
    func end(error: Error? = nil) { streams.last?.finish(throwing: error) }
    func count(_ action: String, channel: String? = nil) -> Int {
        requests.filter { $0.url!.lastPathComponent == action && (channel == nil || body($0)["channel"] == channel) }.count
    }
    func body(_ request: URLRequest) -> [String: String] {
        (try? JSONDecoder().decode([String: String].self, from: request.httpBody ?? Data())) ?? [:]
    }
}

final class ClientTests: XCTestCase {
    let base = URL(string: "https://example.com/api")!
    func client(_ fake: FakeTransport, token: @escaping TransmitClient.TokenProvider = { nil }, query: String? = nil) throws -> TransmitClient {
        try TransmitClient(baseURL: base, tokenQueryParameter: query, tokenProvider: token,
                           transport: fake.transport, sleep: { try await fake.sleep($0) })
    }

    func testParser() throws {
        for line in ["", ": ping", "event: message", "id: 5", "data:", "data: garbage", "data: []", "data: {}", "data: {\"channel\":1,\"payload\":true}", "data: {\"channel\":\"c\"}"] {
            XCTAssertNil(SSEParser.parse(line), line)
        }
        for payload in ["null", "true", "42", "\"hi 🌎\"", "[1,2]", "{\"nested\":{\"x\":1}}"] {
            let event = try XCTUnwrap(SSEParser.parse("data: {\"channel\":\"a/b\",\"payload\":\(payload)}\r"))
            XCTAssertEqual(event.channel, "a/b")
            XCTAssertNoThrow(try JSONSerialization.jsonObject(with: event.payload, options: [.fragmentsAllowed]))
        }
        XCTAssertNotNil(SSEParser.parse("data:{\"channel\":\"c\",\"payload\":false}"))
    }

    func testResponseValidationAndInvalidURLs() throws {
        for url in ["file:///tmp/test", "https://example.com?q=x", "https://example.com#x", "https://a:b@example.com", "https:relative"] {
            XCTAssertThrowsError(try TransmitClient(baseURL: URL(string: url)!)) { XCTAssertEqual($0 as? TransmitError, .invalidBaseURL) }
        }
        let plain = URLResponse(url: base, mimeType: nil, expectedContentLength: 0, textEncodingName: nil)
        XCTAssertThrowsError(try Transport.validate(plain, streaming: false)) { XCTAssertEqual($0 as? TransmitError, .invalidResponse) }
        let bad = HTTPURLResponse(url: base, statusCode: 401, httpVersion: nil, headerFields: nil)!
        XCTAssertThrowsError(try Transport.validate(bad, streaming: true)) { XCTAssertEqual($0 as? TransmitError, .httpStatus(401)) }
        let wrong = HTTPURLResponse(url: base, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!
        XCTAssertThrowsError(try Transport.validate(wrong, streaming: true)) { XCTAssertEqual($0 as? TransmitError, .invalidContentType) }
        let good = HTTPURLResponse(url: base, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "text/event-stream; charset=utf-8"])!
        XCTAssertNoThrow(try Transport.validate(good, streaming: true))
        XCTAssertNoThrow(try Transport.validate(good, streaming: false))
    }

    func testWaitsForOpenAndSharesChannelAcrossConsumers() async throws {
        let fake = FakeTransport()
        await fake.configure(paused: true)
        let client = try TransmitClient(baseURL: base, transport: fake.transport, sleep: { try await fake.sleep($0) })
        let one = await client.messages(on: "news", as: Int.self)
        let two = await client.messages(on: "news", as: Int.self)
        try await eventually { await fake.openGate != nil }
        let before = await fake.count("subscribe")
        XCTAssertEqual(before, 0)
        await fake.releaseOpen()
        try await eventually { await fake.count("subscribe") == 1 }
        await fake.emit(": ping")
        await fake.emit("data: invalid")
        await fake.emit("data: {\"channel\":\"unknown\",\"payload\":1}")
        await fake.emit("data: {\"channel\":\"news\",\"payload\":12}")
        var first = one.makeAsyncIterator(), second = two.makeAsyncIterator()
        let a = try await first.next(), b = try await second.next()
        XCTAssertEqual(a, 12); XCTAssertEqual(b, 12)
        await client.unsubscribe(from: "news")
        try await eventually { await fake.count("unsubscribe") == 1 }
        let end = try await first.next()
        XCTAssertNil(end)
        await client.unsubscribe(from: "missing")
        await client.close()
        await client.close()
        let closed = await client.messages(on: "news", as: Int.self)
        do { for try await _ in closed { XCTFail() }; XCTFail() }
        catch { XCTAssertEqual(error as? TransmitError, .closed) }
        let finalState = await client.state
        XCTAssertEqual(finalState, .closed)
    }

    func testReconnectResubscribeAndRefreshWithStableUID() async throws {
        actor Token { var value = "old"; func set(_ value: String) { self.value = value } }
        let token = Token(), fake = FakeTransport()
        let client = try client(fake, token: { await token.value }, query: "access_token")
        let stream = await client.messages(on: "news", as: Int.self)
        try await eventually { await fake.count("subscribe") == 1 }
        await fake.end(error: URLError(.networkConnectionLost))
        try await eventually { await fake.count("subscribe") == 2 }
        await token.set("new +/=&")
        await client.refreshAuthentication()
        try await eventually { await fake.count("subscribe") == 3 }
        await fake.emit("data: {\"channel\":\"news\",\"payload\":42}")
        var iterator = stream.makeAsyncIterator()
        let value = try await iterator.next()
        XCTAssertEqual(value, 42)
        let requests = await fake.requests
        for request in requests {
            let components = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)!
            XCTAssertTrue(components.path.hasPrefix("/api/__transmit/"))
            XCTAssertNotNil(request.value(forHTTPHeaderField: "Authorization"))
            if request.httpMethod == "GET" {
                XCTAssertEqual(components.queryItems?.first(where: { $0.name == "uid" })?.value, client.uid)
                XCTAssertEqual(request.value(forHTTPHeaderField: "Accept"), "text/event-stream")
            } else {
                let body = await fake.body(request)
                XCTAssertEqual(body, ["uid": client.uid, "channel": "news"])
                XCTAssertEqual(request.value(forHTTPHeaderField: "Content-Type"), "application/json")
            }
        }
        XCTAssertEqual(requests.last?.value(forHTTPHeaderField: "Authorization"), "Bearer new +/=&")
        XCTAssertEqual(URLComponents(url: requests.last!.url!, resolvingAgainstBaseURL: false)?.queryItems?.first?.value, "new +/=&")
        await client.close()
    }

    func testBackoffGrowsAndCapsAndResetsAfterMessage() async throws {
        let fake = FakeTransport()
        await fake.configure(failures: 7)
        let client = try client(fake)
        let stream = await client.messages(on: "a", as: Bool.self)
        try await eventually { await fake.count("subscribe") == 1 }
        let delays = await fake.delays
        XCTAssertEqual(delays, [1, 2, 4, 8, 16, 30, 30].map { UInt64($0) * 1_000_000_000 })
        await fake.emit("data: {\"channel\":\"a\",\"payload\":true}")
        var iterator = stream.makeAsyncIterator()
        let value = try await iterator.next()
        XCTAssertEqual(value, true)
        await fake.end()
        try await eventually { await fake.count("subscribe") == 2 }
        let reset = await fake.delays.last
        XCTAssertEqual(reset, 1_000_000_000)
        await client.close()
    }

    func testOnlyActiveChannelsSubscribeAndReconnect() async throws {
        let fake = FakeTransport()
        await fake.configure(paused: true)
        let client = try client(fake)
        let removed = await client.messages(on: "removed", as: Int.self)
        let first = await client.messages(on: "first", as: Int.self)
        let second = await client.messages(on: "second", as: Int.self)
        try await eventually { await fake.openGate != nil }
        await client.unsubscribe(from: "removed")
        await fake.releaseOpen()
        try await eventually { await fake.count("subscribe") == 2 }
        await fake.end()
        try await eventually { await fake.count("subscribe") == 4 }
        let firstCount = await fake.count("subscribe", channel: "first")
        let secondCount = await fake.count("subscribe", channel: "second")
        let removedCount = await fake.count("subscribe", channel: "removed")
        XCTAssertEqual(firstCount, 2); XCTAssertEqual(secondCount, 2); XCTAssertEqual(removedCount, 0)
        let requests = await fake.requests
        for request in requests {
            XCTAssertNil(request.value(forHTTPHeaderField: "Authorization"))
            if request.httpMethod == "POST" { XCTAssertNil(request.url?.query) }
        }
        await client.close()
        _ = [removed, first, second]
    }

    func testSubscriptionAndDecodingErrorsAndConsumerCancellation() async throws {
        let fake = FakeTransport()
        await fake.configure(denied: ["private"])
        let client = try client(fake)
        let denied = await client.messages(on: "private", as: Int.self)
        do { for try await _ in denied { XCTFail() }; XCTFail() }
        catch { XCTAssertEqual(error as? TransmitError, .httpStatus(403)) }
        let invalid = await client.messages(on: "wrong", as: Int.self)
        try await eventually { await fake.count("subscribe", channel: "wrong") == 1 }
        await fake.emit("data: {\"channel\":\"wrong\",\"payload\":\"bad\"}")
        do { for try await _ in invalid { XCTFail() }; XCTFail() }
        catch { XCTAssertTrue(error is DecodingError) }
        try await eventually { await fake.count("unsubscribe", channel: "wrong") == 1 }
        let one = await client.messages(on: "cancel", as: Int.self)
        let two = await client.messages(on: "cancel", as: Int.self)
        let task1 = Task { for try await _ in one {} }
        let task2 = Task { for try await _ in two {} }
        try await eventually { await fake.count("subscribe", channel: "cancel") == 1 }
        task1.cancel(); _ = await task1.result
        let unsubscribed = await fake.count("unsubscribe", channel: "cancel")
        XCTAssertEqual(unsubscribed, 0)
        task2.cancel(); _ = await task2.result
        try await eventually { await fake.count("unsubscribe", channel: "cancel") == 1 }
        await client.close()
    }

    func testCloseDuringOpeningAndBackoffAndTokenFailure() async throws {
        let fake = FakeTransport()
        await fake.configure(paused: true)
        let client = try client(fake)
        let stream = await client.messages(on: "a", as: Int.self)
        try await eventually { await fake.openGate != nil }
        let closing = Task { await client.close() }
        try await eventually { await client.state == .closed }
        await fake.releaseOpen()
        await closing.value
        var iterator = stream.makeAsyncIterator()
        let value = try await iterator.next()
        XCTAssertNil(value)
        let count = await fake.count("subscribe")
        XCTAssertEqual(count, 0)

        let tokenFailure = try TransmitClient(baseURL: base, tokenProvider: { throw URLError(.userAuthenticationRequired) }, transport: fake.transport,
            sleep: { _ in throw CancellationError() })
        let failed = await tokenFailure.messages(on: "a", as: Int.self)
        try await eventually { await tokenFailure.state == .reconnecting }
        await tokenFailure.close()
        _ = failed
        let idle = try TransmitClient(baseURL: base)
        await idle.refreshAuthentication()
        await idle.close()
        XCTAssertNotEqual(idle.uid, client.uid)
    }

    func testConnectWithoutConsumers() async throws {
        let fake = FakeTransport()
        let client = try client(fake)
        let initialState = await client.state
        XCTAssertEqual(initialState, .idle)
        await client.connect()
        await client.connect()
        try await eventually { await client.state == .connected }
        let subscriptions = await fake.count("subscribe")
        XCTAssertEqual(subscriptions, 0)
        await client.close()
        await client.connect()
        let opens = await fake.count("events")
        XCTAssertEqual(opens, 1)
    }

    func testRepeatedUnauthorizedAndForbiddenStopAndExposeError() async throws {
        for status in [401, 403] {
            let fake = FakeTransport()
            await fake.configure(errors: Array(repeating: .httpStatus(status), count: 5))
            let client = try client(fake)
            let stream = await client.messages(on: "private", as: Int.self)
            do { for try await _ in stream { XCTFail() }; XCTFail("Authentication must fail") }
            catch { XCTAssertEqual(error as? TransmitError, .httpStatus(status)) }
            let state = await client.state, error = await client.lastError as? TransmitError
            XCTAssertEqual(state, .authenticationFailed)
            XCTAssertEqual(error, .httpStatus(status))
            await client.connect()
            let rejected = await client.messages(on: "private", as: Int.self)
            do { for try await _ in rejected { XCTFail() }; XCTFail() }
            catch { XCTAssertEqual(error as? TransmitError, .httpStatus(status)) }
            try await Task.sleep(nanoseconds: 50_000_000)
            let opens = await fake.count("events"), delays = await fake.delays
            XCTAssertEqual(opens, 3)
            XCTAssertEqual(delays, [1_000_000_000, 2_000_000_000])

            await fake.configure()
            await client.refreshAuthentication()
            let resumed = await client.messages(on: "private", as: Int.self)
            try await eventually { await fake.count("subscribe") == 1 }
            let recoveredError = await client.lastError
            XCTAssertNil(recoveredError)
            await fake.emit("data: {\"channel\":\"private\",\"payload\":42}")
            var iterator = resumed.makeAsyncIterator()
            let value = try await iterator.next()
            XCTAssertEqual(value, 42)
            await client.close()
        }
    }

    func testAuthenticationFailuresAreConsecutiveAndResetOnSuccess() async throws {
        let fake = FakeTransport()
        await fake.configure(errors: [.httpStatus(401), .httpStatus(503), .httpStatus(403), .httpStatus(401)])
        let client = try client(fake)
        let stream = await client.messages(on: "a", as: Int.self)
        try await eventually { await fake.count("subscribe") == 1 }
        let firstOpens = await fake.count("events")
        XCTAssertEqual(firstOpens, 5)
        await fake.configure(errors: [.httpStatus(403), .httpStatus(401)])
        await fake.end()
        try await eventually { await fake.count("subscribe") == 2 }
        let secondOpens = await fake.count("events"), error = await client.lastError
        XCTAssertEqual(secondOpens, 8)
        XCTAssertNil(error)
        await client.close()
        _ = stream
    }

    func testDeinitReleasesClientAndConnectionWithAnActiveConsumer() async throws {
        let fake = FakeTransport()
        var client: TransmitClient? = try client(fake)
        let owner = WeakReference(client)
        let connection = WeakReference(await client?.connection)
        let stream = await client!.messages(on: "a", as: Int.self)
        try await eventually { await fake.count("subscribe") == 1 }
        client = nil
        XCTAssertNil(owner.value, "The connection loop must not retain the public client")
        try await eventually { await fake.cancellations == 1 }
        var iterator = stream.makeAsyncIterator()
        let end = try await iterator.next()
        XCTAssertNil(end)
        try await eventually { connection.value == nil }
        let unsubscribes = await fake.count("unsubscribe")
        XCTAssertEqual(unsubscribes, 1)
    }

    func testDeinitCancelsBackoffAndPendingOpen() async throws {
        let fake = FakeTransport()
        await fake.configure(failures: 100)
        var client: TransmitClient? = try TransmitClient(baseURL: base, transport: fake.transport,
            sleep: { try await fake.waitForCancellation($0) })
        let owner = WeakReference(client)
        let connection = WeakReference(await client?.connection)
        let stream = await client!.messages(on: "a", as: Int.self)
        try await eventually { await fake.delays.count == 1 }
        client = nil
        XCTAssertNil(owner.value)
        try await eventually { await fake.sleepCancelled }
        try await eventually { connection.value == nil }

        await fake.configure(paused: true)
        var opening: TransmitClient? = try self.client(fake)
        let openingOwner = WeakReference(opening)
        let openingConnection = WeakReference(await opening?.connection)
        let openingStream = await opening!.messages(on: "b", as: Int.self)
        try await eventually { await fake.openGate != nil }
        opening = nil
        XCTAssertNil(openingOwner.value)
        try await eventually { await openingConnection.value?.state == .closed }
        await fake.releaseOpen()
        try await eventually { openingConnection.value == nil }
        let cancellations = await fake.cancellations
        XCTAssertEqual(cancellations, 1)
        _ = [stream, openingStream]
    }

}
