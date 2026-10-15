import XCTest
@testable import Transmit

final class IntegrationTests: XCTestCase {
    func testRealAdonisServer() async throws {
        guard ProcessInfo.processInfo.environment["TRANSMIT_INTEGRATION"] == "1" else {
            throw XCTSkip("Run TRANSMIT_INTEGRATION=1 swift test after npm ci --prefix Tests/IntegrationServer")
        }
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let process = Process(), output = Pipe()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = ["node", root.appendingPathComponent("IntegrationServer/server.mjs").path]
        process.standardOutput = output
        try process.run()
        defer { process.terminate(); process.waitUntilExit() }
        let startup = String(decoding: output.fileHandleForReading.availableData, as: UTF8.self).trimmingCharacters(in: .whitespacesAndNewlines)
        let base = try XCTUnwrap(URL(string: startup), "Server did not start: \(startup)")
        let session = URLSession(configuration: .ephemeral)
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
        func history() async throws -> [[String: Any]] {
            try await JSONSerialization.jsonObject(with: call("history")) as! [[String: Any]]
        }
        func count(_ path: String, uid: String? = nil) async throws -> Int {
            let rows = try await history()
            return rows.filter { row in
                guard (row["path"] as? String)?.hasSuffix(path) == true else { return false }
                let identifier = ((row["body"] as? [String: Any])?["uid"] ?? (row["query"] as? [String: Any])?["uid"]) as? String
                return uid == nil || identifier == uid
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
        struct Payload: Codable, Sendable, Equatable { let text: String; let number: Int }
        let credentials = Credentials()
        await credentials.set("first")
        let client = try TransmitClient(baseURL: base, tokenQueryParameter: "token", session: session,
                                        tokenProvider: { await credentials.current() })
        let messages = await client.messages(on: "news", as: Payload.self)
        let received = expectation(description: "message before and after reconnect and refresh")
        received.expectedFulfillmentCount = 3
        let consumer = Task {
            for try await payload in messages {
                XCTAssertEqual(payload, Payload(text: "Olá 🌎", number: 7))
                received.fulfill()
            }
        }
        try await eventually { try await count("subscribe", uid: client.uid) == 1 }
        _ = try await call("broadcast", ["channel": "news", "payload": ["text": "Olá 🌎", "number": 7]])
        _ = try await call("control", ["disconnect": true])
        try await eventually { try await count("subscribe", uid: client.uid) == 2 }
        _ = try await call("broadcast", ["channel": "news", "payload": ["text": "Olá 🌎", "number": 7]])
        await credentials.set("second")
        _ = try await call("control", ["token": "second"])
        await client.refreshAuthentication()
        try await eventually { try await count("subscribe", uid: client.uid) == 3 }
        _ = try await call("broadcast", ["channel": "news", "payload": ["text": "Olá 🌎", "number": 7]])
        await fulfillment(of: [received], timeout: 5)

        let rejected = await client.messages(on: "denied", as: Int.self)
        do { for try await _ in rejected { XCTFail() }; XCTFail() }
        catch { XCTAssertEqual(error as? TransmitError, .httpStatus(400)) }
        consumer.cancel()
        _ = await consumer.result
        try await eventually { try await count("unsubscribe", uid: client.uid) == 1 }
        let rows = try await history().filter { row in
            let body = row["body"] as? [String: Any], query = row["query"] as? [String: Any]
            return body?["uid"] as? String == client.uid || query?["uid"] as? String == client.uid
        }
        XCTAssertGreaterThan(rows.count, 6)
        for row in rows {
            let token = (row["query"] as? [String: Any])?["token"] as? String
            XCTAssertEqual(row["authorization"] as? String, "Bearer \(token!)")
        }
        XCTAssertEqual(rows.last?["authorization"] as? String, "Bearer second")
        await client.close()

        // Default unauthenticated provider is retried, and closing interrupts its backoff.
        let anonymous = try TransmitClient(baseURL: base, session: session)
        let anonymousMessages = await anonymous.messages(on: "news", as: Int.self)
        try await eventually { try await count("events", uid: anonymous.uid) == 1 }
        try await eventually { await anonymous.state == .reconnecting }
        await anonymous.close()
        _ = anonymousMessages
    }
}
