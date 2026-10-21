import XCTest
@testable import Transmit

final class IntegrationTests: XCTestCase {
    private struct RequestRecord: Decodable {
        let path: String
        let authorization: String?
        let query: [String: String]
        let body: [String: String]
        let sessionHeader: String?
        let accept: String?
        var uid: String? { body["uid"] ?? query["uid"] }
    }

    func testRealAdonisServer() async throws {
        guard ProcessInfo.processInfo.environment["TRANSMIT_INTEGRATION"] == "1" else {
            throw XCTSkip("Run TRANSMIT_INTEGRATION=1 swift test after npm ci --prefix Tests/IntegrationServer")
        }
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let process = Process(), output = Pipe()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        let serverPath = ProcessInfo.processInfo.environment["TRANSMIT_INTEGRATION_SERVER"]
            ?? root.appendingPathComponent("IntegrationServer/server.mjs").path
        process.arguments = ["node", serverPath]
        process.standardOutput = output
        try process.run()
        defer { process.terminate(); process.waitUntilExit() }
        let startup = String(decoding: output.fileHandleForReading.availableData, as: UTF8.self).trimmingCharacters(in: .whitespacesAndNewlines)
        let base = try XCTUnwrap(URL(string: startup), "Server did not start: \(startup)")
        let configuration = URLSessionConfiguration.ephemeral
        configuration.timeoutIntervalForRequest = 90
        configuration.httpAdditionalHeaders = ["X-Transmit-Session": "custom"]
        let session = URLSession(configuration: configuration)
        defer { session.invalidateAndCancel() }

        func call(_ path: String, _ body: [String: Any]? = nil) async throws -> Data {
            var request = URLRequest(url: base.appendingPathComponent(path))
            if let body = body {
                request.httpMethod = "POST"
                request.setValue("application/json", forHTTPHeaderField: "Content-Type")
                request.httpBody = try JSONSerialization.data(withJSONObject: body)
            }
            let (data, response) = try await session.data(for: request)
            XCTAssertEqual((response as? HTTPURLResponse)?.statusCode, 200)
            return data
        }
        func history() async throws -> [RequestRecord] {
            let data = try await call("history")
            return try JSONDecoder().decode([RequestRecord].self, from: data)
        }
        func count(_ path: String, uid: String? = nil, channel: String? = nil) async throws -> Int {
            let rows = try await history()
            return rows.filter { row in
                row.path.hasSuffix(path) && (uid == nil || row.uid == uid)
                    && (channel == nil || row.body["channel"] == channel)
            }.count
        }

        // Exercise URLSession response validation and graceful SSE EOF against real HTTP.
        let transport = Transport.live(session: session)
        do { _ = try await transport.open(URLRequest(url: base.appendingPathComponent("health"))); XCTFail() }
        catch { XCTAssertEqual(error as? TransmitError, .invalidContentType) }
        do { _ = try await transport.open(URLRequest(url: base.appendingPathComponent("__transmit/events"))); XCTFail() }
        catch { XCTAssertEqual(error as? TransmitError, .httpStatus(401)) }
        _ = try await call("control", ["endNext": true])
        var finiteRequest = URLRequest(url: base.appendingPathComponent("__transmit/events?uid=finite"))
        finiteRequest.url = URL(string: "\(base)/__transmit/events?uid=finite")!
        finiteRequest.setValue("Bearer first", forHTTPHeaderField: "Authorization")
        let finite = try await transport.open(finiteRequest)
        var lines = [String]()
        for try await line in finite.lines { lines.append(line) }
        XCTAssertTrue(lines.contains(": goodbye"))
        finite.cancel()

        actor Credentials {
            var token = "expired"
            var requests = 0
            func current() -> String { requests += 1; return requests == 1 ? "expired" : token }
            func set(_ value: String) { token = value }
        }
        struct ChatMessage: Codable, Sendable, Equatable { let text: String; let seq: Int }
        let credentials = Credentials()
        await credentials.set("first")
        let client = try TransmitClient(baseURL: base, tokenQueryParameter: "token", session: session,
                                        tokenProvider: { await credentials.current() })
        let messages = await client.messages(on: "chats/general", as: ChatMessage.self)
        let received = expectation(description: "message before and after reconnect and refresh")
        received.expectedFulfillmentCount = 3
        let consumer = Task {
            for try await payload in messages {
                XCTAssertEqual(payload, ChatMessage(text: "Olá 🌎", seq: 7))
                received.fulfill()
            }
        }
        try await client.awaitSubscribed("chats/general")
        let firstUID = client.uid
        let firstSubscription = try await count("subscribe", uid: client.uid)
        XCTAssertEqual(firstSubscription, 1)
        _ = try await call("broadcast", ["channel": "chats/general", "payload": ["text": "Olá 🌎", "seq": 7]])
        _ = try await call("control", ["disconnect": true])
        try await eventually { await client.state == .reconnecting }
        try await client.awaitSubscribed("chats/general")
        let restoredSubscription = try await count("subscribe", uid: client.uid)
        XCTAssertEqual(restoredSubscription, 1)
        let restoredUID = client.uid
        XCTAssertNotEqual(restoredUID, firstUID)
        _ = try await call("broadcast", ["channel": "chats/general", "payload": ["text": "Olá 🌎", "seq": 7]])
        await credentials.set("second")
        _ = try await call("control", ["token": "second", "holdNextClose": true])
        await client.refreshAuthentication()
        try await client.awaitSubscribed("chats/general")
        let refreshedSubscription = try await count("subscribe", uid: client.uid)
        XCTAssertEqual(refreshedSubscription, 1)
        let refreshedUID = client.uid
        XCTAssertNotEqual(refreshedUID, restoredUID)
        XCTAssertNotEqual(refreshedUID, firstUID)
        _ = try await call("broadcast", ["channel": "chats/general", "payload": ["text": "Olá 🌎", "seq": 7]])
        await fulfillment(of: [received], timeout: 5)
        // The replacement has subscribed before the old stream removes its UID mapping.
        try await eventually {
            let health = try JSONSerialization.jsonObject(with: await call("health")) as? [String: Any]
            return health?["heldCloses"] as? Int == 1
        }
        _ = try await call("control", ["releaseCloses": true])
        let afterRefresh = await client.messages(on: "after-refresh", as: String.self)
        do { try await client.awaitSubscribed("after-refresh") }
        catch {
            XCTFail("Subscription after delayed old-stream cleanup failed: \(error)")
            await client.close()
            consumer.cancel()
            return
        }
        let delivered = expectation(description: "broadcast after old stream cleanup")
        let lateConsumer = Task {
            for try await message in afterRefresh {
                XCTAssertEqual(message, "alive")
                delivered.fulfill()
            }
        }
        defer { lateConsumer.cancel() }
        _ = try await call("broadcast", ["channel": "after-refresh", "payload": "alive"])
        await fulfillment(of: [delivered], timeout: 5)
        await client.unsubscribe(from: "after-refresh")
        _ = await lateConsumer.result

        let rejected = await client.messages(on: "denied", as: Int.self)
        do { try await client.awaitSubscribed("denied"); XCTFail() }
        catch { XCTAssertEqual(error as? TransmitError, .httpStatus(400)) }
        do { for try await _ in rejected { XCTFail() }; XCTFail() }
        catch { XCTAssertEqual(error as? TransmitError, .httpStatus(400)) }
        consumer.cancel()
        _ = await consumer.result
        try await eventually { try await count("unsubscribe", uid: client.uid, channel: "chats/general") == 1 }
        let recordedHistory = try await history()
        let streamUIDs = Set([firstUID, restoredUID, refreshedUID])
        let rows = recordedHistory.filter { streamUIDs.contains($0.uid ?? "") }
        XCTAssertGreaterThan(rows.count, 6)
        for row in rows {
            if row.path.hasSuffix("/events") {
                XCTAssertEqual(row.accept, "text/event-stream")
            } else {
                XCTAssertNotEqual(row.accept, "text/event-stream")
            }
            XCTAssertEqual(row.sessionHeader, "custom")
            let token = try XCTUnwrap(row.query["token"])
            XCTAssertEqual(row.authorization, "Bearer \(token)")
        }
        XCTAssertEqual(rows.last?.authorization, "Bearer second")
        await client.close()

        // A persistently rejected real GET must stop instead of retrying forever.
        let anonymousStart = try await history().count
        let anonymous = try TransmitClient(baseURL: base, session: session)
        let anonymousMessages = await anonymous.messages(on: "chats/general", as: Int.self)
        do { try await anonymous.awaitSubscribed("chats/general"); XCTFail() }
        catch { XCTAssertEqual(error as? TransmitError, .httpStatus(401)) }
        let anonymousState = await anonymous.state
        XCTAssertEqual(anonymousState, .authenticationFailed)
        let anonymousError = await anonymous.lastError as? TransmitError
        XCTAssertEqual(anonymousError, .httpStatus(401))
        do { for try await _ in anonymousMessages { XCTFail() }; XCTFail() }
        catch { XCTAssertEqual(error as? TransmitError, .httpStatus(401)) }
        let attempts = try await history().dropFirst(anonymousStart).filter { $0.path.hasSuffix("events") }
        XCTAssertEqual(attempts.count, 3)
        XCTAssertEqual(Set(attempts.compactMap(\.uid)).count, 3)
        XCTAssertEqual(attempts.last?.uid, anonymous.uid)
        await anonymous.close()
    }
}
