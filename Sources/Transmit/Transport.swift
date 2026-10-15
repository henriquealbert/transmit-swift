import Foundation

/// Failures reported by the HTTP transport and configuration validation.
public enum TransmitError: Error, Equatable {
    case invalidBaseURL
    case invalidResponse
    case httpStatus(Int)
    case invalidContentType
    case closed
}

struct EventConnection {
    let lines: AsyncThrowingStream<String, Error>
    let cancel: () -> Void
}

struct Transport {
    var open: (URLRequest) async throws -> EventConnection
    var send: (URLRequest) async throws -> Void

    static func validate(_ response: URLResponse, streaming: Bool) throws {
        guard let response = response as? HTTPURLResponse else {
            throw TransmitError.invalidResponse
        }
        guard (200..<300).contains(response.statusCode) else {
            throw TransmitError.httpStatus(response.statusCode)
        }
        if streaming && response.mimeType?.lowercased() != "text/event-stream" {
            throw TransmitError.invalidContentType
        }
    }

    static func live(session: URLSession) -> Self {
        Self(open: { request in
            let (bytes, response) = try await session.bytes(for: request)
            do {
                try validate(response, streaming: true)
            } catch {
                bytes.task.cancel()
                throw error
            }
            var continuation: AsyncThrowingStream<String, Error>.Continuation!
            let lines = AsyncThrowingStream<String, Error> { continuation = $0 }
            let sink = continuation!
            let reader = Task {
                do {
                    for try await line in bytes.lines {
                        sink.yield(line)
                    }
                    sink.finish()
                } catch {
                    sink.finish(throwing: error)
                }
            }
            sink.onTermination = { _ in reader.cancel(); bytes.task.cancel() }
            return EventConnection(lines: lines, cancel: { reader.cancel(); bytes.task.cancel() })
        }, send: { request in
            let (_, response) = try await session.data(for: request)
            try validate(response, streaming: false)
        })
    }
}
