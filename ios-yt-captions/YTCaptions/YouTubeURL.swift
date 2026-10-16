import Foundation

enum YouTubeURL {
    /// Extract 11-char video id from watch / youtu.be / shorts / embed URLs.
    static func videoID(from raw: String) -> String? {
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.range(of: #"^[A-Za-z0-9_-]{11}$"#, options: .regularExpression) != nil {
            return trimmed
        }
        guard let url = URL(string: trimmed), let host = url.host?.lowercased() else {
            return nil
        }
        if host.contains("youtu.be") {
            let id = url.path.split(separator: "/").first.map(String.init)
            return valid(id)
        }
        if host.contains("youtube.com") || host.contains("youtube-nocookie.com") {
            if let q = URLComponents(url: url, resolvingAgainstBaseURL: false)?
                .queryItems?
                .first(where: { $0.name == "v" })?
                .value {
                return valid(q)
            }
            let parts = url.path.split(separator: "/").map(String.init)
            if parts.count >= 2, ["shorts", "embed", "live", "v"].contains(parts[0]) {
                return valid(parts[1])
            }
        }
        return nil
    }

    private static func valid(_ id: String?) -> String? {
        guard let id, id.range(of: #"^[A-Za-z0-9_-]{11}$"#, options: .regularExpression) != nil else {
            return nil
        }
        return id
    }
}
