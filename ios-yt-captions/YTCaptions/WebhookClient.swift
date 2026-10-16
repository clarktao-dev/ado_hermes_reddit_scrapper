import Foundation

enum WebhookClient {
    enum WebhookError: LocalizedError {
        case badURL
        case http(Int, String)

        var errorDescription: String? {
            switch self {
            case .badURL: return "Webhook URL 無效"
            case .http(let code, let body): return "VPS 回傳 HTTP \(code): \(body.prefix(200))"
            }
        }
    }

    static func post(
        webhookURL: String,
        token: String,
        result: TranscriptFetcher.Result,
        originalURL: String
    ) async throws {
        guard let url = URL(string: webhookURL.trimmingCharacters(in: .whitespacesAndNewlines)) else {
            throw WebhookError.badURL
        }
        let payload: [String: Any] = [
            "video_id": result.videoID,
            "url": originalURL.hasPrefix("http")
                ? originalURL
                : "https://www.youtube.com/watch?v=\(result.videoID)",
            "title": result.title,
            "language": result.language,
            "is_generated": result.isGenerated,
            "source": result.source,
            "text": result.text,
            "segments": [],
            "meta": ["channel": "", "client": "ios-yt-captions"],
            "fetched_at": Date().timeIntervalSince1970,
        ]
        let data = try JSONSerialization.data(withJSONObject: payload, options: [])
        var req = URLRequest(url: url)
        req.httpMethod = "POST"
        req.httpBody = data
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        req.setValue("ios-yt-captions/1.0", forHTTPHeaderField: "User-Agent")
        if !token.isEmpty {
            req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }
        let (respData, resp) = try await URLSession.shared.data(for: req)
        let code = (resp as? HTTPURLResponse)?.statusCode ?? 0
        let body = String(data: respData, encoding: .utf8) ?? ""
        guard (200...299).contains(code) else {
            throw WebhookError.http(code, body)
        }
    }
}
