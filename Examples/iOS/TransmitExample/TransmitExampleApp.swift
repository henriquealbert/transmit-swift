import OSLog
import SwiftUI
import Transmit

@main
struct TransmitExampleApp: App {
    var body: some Scene {
        WindowGroup { FeedView().preferredColorScheme(.light) }
    }
}

struct ChatMessage: Decodable, Identifiable, Sendable {
    let id: String
    let text: String
    let source: String
    let serverInstance: String
    let seq: Int
    let sentAt: String
}

@MainActor
final class FeedModel: ObservableObject {
    @Published private(set) var messages: [ChatMessage] = []
    @Published private(set) var status = "Connecting"
    @Published private(set) var connected = false
    @Published private(set) var totalReceived = 0
    @Published private(set) var serverInstances: Set<String> = []
    @Published private(set) var error: String?
    let endpoint = ProcessInfo.processInfo.environment["TRANSMIT_BASE_URL"] ?? "http://127.0.0.1:3333"
    private var client: TransmitClient?
    private var seenSequences: [String: Int] = [:]
    private let logger = Logger(subsystem: "io.elcubo.TransmitExample", category: "messages")

    func listen() async {
        guard client == nil else { return }
        error = nil
        guard let url = URL(string: endpoint) else {
            error = "Invalid server URL: \(endpoint)"
            return
        }
        do {
            let configuration = URLSessionConfiguration.default
            configuration.timeoutIntervalForRequest = 90
            let session = URLSession(configuration: configuration)
            defer { session.invalidateAndCancel() }
            let client = try TransmitClient(baseURL: url, session: session)
            self.client = client
            let monitor = Task {
                var previousStatus: String?
                while !Task.isCancelled {
                    let state = await client.state
                    connected = state == .connected
                    switch state {
                    case .idle, .connecting: status = "Connecting"
                    case .connected: status = "Connected"
                    case .reconnecting: status = "Reconnecting"
                    case .closed: status = "Closed"
                    case .authenticationFailed:
                        status = "Authentication failed"
                        if let failure = await client.lastError { error = String(describing: failure) }
                    }
                    if status != previousStatus {
                        logger.info("STATE \(self.status, privacy: .public) uid=\(client.uid, privacy: .public)")
                        previousStatus = status
                    }
                    do { try await Task.sleep(nanoseconds: 1_000_000_000) }
                    catch { break }
                }
            }
            do {
                let stream = await client.messages(on: "chats/general", as: ChatMessage.self)
                try await client.awaitSubscribed("chats/general")
                var request = URLRequest(url: url.appendingPathComponent("broadcast"))
                request.httpMethod = "POST"
                request.setValue("application/json", forHTTPHeaderField: "Content-Type")
                request.httpBody = try JSONEncoder().encode(["text": "Hello from the SwiftUI client"])
                let (_, response) = try await session.data(for: request)
                guard let response = response as? HTTPURLResponse else { throw TransmitError.invalidResponse }
                guard (200..<300).contains(response.statusCode) else {
                    throw TransmitError.httpStatus(response.statusCode)
                }
                logger.info("PUBLISHED after subscription confirmation uid=\(client.uid, privacy: .public)")
                for try await message in stream {
                    // This channel delivers increasing sequence values per server instance.
                    guard message.seq > seenSequences[message.serverInstance, default: 0] else { continue }
                    seenSequences[message.serverInstance] = message.seq
                    messages.insert(message, at: 0)
                    if messages.count > 100 { messages.removeLast() }
                    totalReceived += 1
                    serverInstances.insert(message.serverInstance)
                    logger.info("RECEIVED server=\(message.serverInstance, privacy: .public) seq=\(message.seq) source=\(message.source, privacy: .public) text=\(message.text, privacy: .public) uid=\(client.uid, privacy: .public)")
                }
            } catch {
                self.error = String(describing: error)
                connected = false
                status = await client.state == .authenticationFailed ? "Authentication failed" : "Stopped"
                logger.error("Subscription ended: \(error.localizedDescription, privacy: .public)")
            }
            monitor.cancel()
            await client.unsubscribe(from: "chats/general")
            await client.close()
            if self.client?.uid == client.uid { self.client = nil }
        } catch {
            self.error = error.localizedDescription
        }
    }

    func close() async {
        let current = client
        client = nil
        await current?.unsubscribe(from: "chats/general")
        await current?.close()
    }
}

struct FeedView: View {
    @StateObject private var model = FeedModel()
    private let ink = Color(red: 0.10, green: 0.17, blue: 0.24)
    private let accent = Color(red: 0.04, green: 0.43, blue: 0.40)

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 24) {
                HStack {
                    Label("TRANSMIT / SWIFT", systemImage: "dot.radiowaves.left.and.right")
                        .font(.system(size: 12, weight: .bold, design: .monospaced))
                    Spacer()
                    HStack(spacing: 6) {
                        Circle().fill(model.connected ? accent : .orange).frame(width: 7, height: 7)
                        Text(model.status).font(.caption.weight(.semibold))
                    }
                }
                Text("Live chat")
                    .font(.system(size: 38, weight: .bold, design: .rounded))
                Text("AdonisJS → your iPhone\nA live feed, delivered over SSE.")
                    .font(.subheadline).foregroundColor(.secondary)

                HStack(spacing: 0) {
                    metric("RECEIVED", value: "\(model.totalReceived)")
                    Spacer()
                    metric("SERVER SESSIONS", value: "\(model.serverInstances.count)")
                }
                .padding(20)
                .background(accent.opacity(0.09), in: RoundedRectangle(cornerRadius: 20))

                HStack {
                    Text("chats/general").font(.system(.subheadline, design: .monospaced).weight(.semibold))
                    Spacer()
                    Text("NEWEST FIRST").font(.system(size: 10, weight: .bold)).foregroundColor(.secondary)
                }

                if let error = model.error {
                    Text(error).foregroundColor(.red).font(.subheadline)
                }
                if model.messages.isEmpty {
                    Text("Waiting for the next message…\nThe server publishes every five seconds.")
                        .font(.subheadline).foregroundColor(.secondary).padding(.vertical, 24)
                }
                LazyVStack(spacing: 12) {
                    ForEach(model.messages) { message in
                        VStack(alignment: .leading, spacing: 12) {
                            HStack {
                                Label(message.source == "manual" ? "BROADCAST" : "TIMER",
                                      systemImage: message.source == "manual" ? "paperplane" : "clock")
                                    .font(.system(size: 10, weight: .bold, design: .monospaced))
                                    .foregroundColor(accent)
                                Spacer()
                                Text("#\(message.seq)").font(.caption.monospacedDigit()).foregroundColor(.secondary)
                            }
                            Text(message.text).font(.system(size: 19, weight: .semibold))
                            HStack {
                                Text("server \(message.serverInstance)")
                                Spacer()
                                Text(String(message.sentAt.dropFirst(11).prefix(8)) + " UTC")
                            }
                            .font(.system(size: 10, design: .monospaced)).foregroundColor(.secondary)
                        }
                        .frame(maxWidth: .infinity, alignment: .leading).padding(18)
                        .background(.white, in: RoundedRectangle(cornerRadius: 18))
                    }
                }
                Text(model.endpoint).font(.system(.caption, design: .monospaced)).foregroundColor(.secondary)
            }
            .padding(24)
        }
        .foregroundColor(ink)
        .background(Color(red: 0.96, green: 0.96, blue: 0.94).ignoresSafeArea())
        .task { await model.listen() }
        .onDisappear { Task { await model.close() } }
    }

    private func metric(_ title: String, value: String) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            Text(value).font(.system(size: 30, weight: .bold, design: .rounded)).foregroundColor(accent)
            Text(title).font(.system(size: 10, weight: .bold, design: .monospaced)).foregroundColor(.secondary)
        }
    }
}
