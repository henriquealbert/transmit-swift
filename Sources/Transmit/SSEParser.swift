import Foundation

/// Transmit emits one complete JSON envelope per data line (not multiline SSE data).
enum SSEParser {
    static func parse(_ line: String) -> (channel: String, payload: Data)? {
        guard line.hasPrefix("data:") else { return nil }
        let data = Data(line.dropFirst(5).utf8)
        guard let object = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let channel = object["channel"] as? String,
              let payload = object["payload"],
              let encoded = try? JSONSerialization.data(withJSONObject: payload, options: [.fragmentsAllowed])
        else { return nil }
        return (channel, encoded)
    }
}
