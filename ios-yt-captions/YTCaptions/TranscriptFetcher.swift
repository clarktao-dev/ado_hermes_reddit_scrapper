import Foundation

struct TranscriptPayload: Codable {
    var video_id: String
    var url: String
    var title: String
    var language: String
    var is_generated: Bool
    var source: String
    var text: String
    var segments: [[String: AnyCodable]]
    var meta: [String: String]
    var fetched_at: Double
}

/// Minimal residential-IP caption fetch for iOS (no yt-dlp).
/// Strategy: watch HTML → captionTracks → download json3/srv3 timedtext.
enum TranscriptFetcher {
    struct Result {
        var videoID: String
        var title: String
        var language: String
        var isGenerated: Bool
        var text: String
        var source: String
    }

    enum FetchError: LocalizedError {
        case badURL
        case noPlayerResponse
        case noCaptions
        case emptyText
        case http(Int)

        var errorDescription: String? {
            switch self {
            case .badURL: return "無法辨識 YouTube 網址"
            case .noPlayerResponse: return "解析影頁失敗（可能被擋或需登入）"
            case .noCaptions: return "這支影片沒有可用字幕"
            case .emptyText: return "字幕是空的"
            case .http(let c): return "HTTP \(c)"
            }
        }
    }

    static func fetch(urlString: String, languages: [String]) async throws -> Result {
        guard let videoID = YouTubeURL.videoID(from: urlString) else {
            throw FetchError.badURL
        }
        let watchURL = URL(string: "https://www.youtube.com/watch?v=\(videoID)&hl=de")!
        var req = URLRequest(url: watchURL)
        req.setValue(
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
            forHTTPHeaderField: "User-Agent"
        )
        req.setValue("https://www.youtube.com/", forHTTPHeaderField: "Accept-Language")

        let (data, resp) = try await URLSession.shared.data(for: req)
        if let http = resp as? HTTPURLResponse, !(200...299).contains(http.statusCode) {
            throw FetchError.http(http.statusCode)
        }
        let html = String(data: data, encoding: .utf8) ?? ""
        guard let player = extractPlayerResponse(from: html) else {
            throw FetchError.noPlayerResponse
        }

        let title = (((player["videoDetails"] as? [String: Any])?["title"] as? String) ?? "")
        let tracks = captionTracks(from: player)
        guard !tracks.isEmpty else { throw FetchError.noCaptions }

        guard let chosen = pickTrack(tracks, preferred: languages) else {
            throw FetchError.noCaptions
        }
        guard var base = chosen.baseURL, !base.isEmpty else { throw FetchError.noCaptions }
        // Prefer json3 for easy parsing
        if !base.contains("fmt=") {
            base += (base.contains("?") ? "&" : "?") + "fmt=json3"
        } else {
            base = base.replacingOccurrences(of: "fmt=srv3", with: "fmt=json3")
            base = base.replacingOccurrences(of: "fmt=vtt", with: "fmt=json3")
        }

        guard let capURL = URL(string: base) else { throw FetchError.badURL }
        var capReq = URLRequest(url: capURL)
        capReq.setValue(req.value(forHTTPHeaderField: "User-Agent"), forHTTPHeaderField: "User-Agent")
        let (capData, capResp) = try await URLSession.shared.data(for: capReq)
        if let http = capResp as? HTTPURLResponse, !(200...299).contains(http.statusCode) {
            throw FetchError.http(http.statusCode)
        }

        let text = parseCaptionJSON(capData)
        guard !text.isEmpty else { throw FetchError.emptyText }

        return Result(
            videoID: videoID,
            title: title,
            language: chosen.languageCode,
            isGenerated: chosen.kind == "asr",
            text: text,
            source: "ios-timedtext"
        )
    }

    // MARK: - Player JSON

    private static func extractPlayerResponse(from html: String) -> [String: Any]? {
        // ytInitialPlayerResponse = {...};
        let markers = [
            "ytInitialPlayerResponse = ",
            "var ytInitialPlayerResponse = ",
        ]
        for marker in markers {
            if let range = html.range(of: marker) {
                let start = range.upperBound
                if let json = extractJSONObject(from: html, startingAt: start) {
                    return json
                }
            }
        }
        return nil
    }

    private static func extractJSONObject(from html: String, startingAt: String.Index) -> [String: Any]? {
        var depth = 0
        var started = false
        var end = startingAt
        var i = startingAt
        while i < html.endIndex {
            let ch = html[i]
            if ch == "{" {
                depth += 1
                started = true
            } else if ch == "}" {
                depth -= 1
                if started && depth == 0 {
                    end = html.index(after: i)
                    break
                }
            }
            i = html.index(after: i)
        }
        guard started, depth == 0 else { return nil }
        let slice = String(html[startingAt..<end])
        guard let data = slice.data(using: .utf8) else { return nil }
        return (try? JSONSerialization.jsonObject(with: data)) as? [String: Any]
    }

    private struct Track {
        var languageCode: String
        var baseURL: String?
        var kind: String // "" or "asr"
        var name: String
    }

    private static func captionTracks(from player: [String: Any]) -> [Track] {
        let captions = player["captions"] as? [String: Any]
        let renderer = captions?["playerCaptionsTracklistRenderer"] as? [String: Any]
        let list = renderer?["captionTracks"] as? [[String: Any]] ?? []
        return list.compactMap { t in
            let code = (t["languageCode"] as? String) ?? ""
            let base = t["baseUrl"] as? String
            let kind = (t["kind"] as? String) ?? ""
            let name = ((t["name"] as? [String: Any])?["simpleText"] as? String) ?? code
            guard !code.isEmpty, base != nil else { return nil }
            return Track(languageCode: code, baseURL: base, kind: kind, name: name)
        }
    }

    private static func pickTrack(_ tracks: [Track], preferred: [String]) -> Track? {
        let prefs = preferred.map { $0.lowercased() }
        // Prefer non-ASR matching language, then ASR, then first.
        for pref in prefs {
            if let t = tracks.first(where: {
                $0.languageCode.lowercased().hasPrefix(pref) && $0.kind != "asr"
            }) { return t }
            if let t = tracks.first(where: {
                $0.languageCode.lowercased().hasPrefix(pref)
            }) { return t }
        }
        return tracks.first(where: { $0.kind != "asr" }) ?? tracks.first
    }

    private static func parseCaptionJSON(_ data: Data) -> String {
        // json3: { events: [ { segs: [ { utf8: "..." } ] } ] }
        guard let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let events = obj["events"] as? [[String: Any]] else {
            // Fallback: treat as XML / plain
            let raw = String(data: data, encoding: .utf8) ?? ""
            return stripXML(raw)
        }
        var parts: [String] = []
        var prev = ""
        for ev in events {
            guard let segs = ev["segs"] as? [[String: Any]] else { continue }
            let line = segs.compactMap { $0["utf8"] as? String }.joined()
                .replacingOccurrences(of: "\n", with: " ")
                .trimmingCharacters(in: .whitespacesAndNewlines)
            guard !line.isEmpty, line != prev else { continue }
            parts.append(line)
            prev = line
        }
        return parts.joined(separator: "\n")
    }

    private static func stripXML(_ raw: String) -> String {
        let noTags = raw.replacingOccurrences(of: #"<[^>]+>"#, with: " ", options: .regularExpression)
        return noTags
            .replacingOccurrences(of: #"\s+"#, with: " ", options: .regularExpression)
            .trimmingCharacters(in: .whitespacesAndNewlines)
    }
}

/// Type-erased Codable helper for optional segment arrays in webhook JSON.
struct AnyCodable: Codable {
    let value: Any
    init(_ value: Any) { self.value = value }
    init(from decoder: Decoder) throws {
        let c = try decoder.singleValueContainer()
        if let v = try? c.decode(String.self) { value = v; return }
        if let v = try? c.decode(Double.self) { value = v; return }
        if let v = try? c.decode(Bool.self) { value = v; return }
        value = ""
    }
    func encode(to encoder: Encoder) throws {
        var c = encoder.singleValueContainer()
        switch value {
        case let v as String: try c.encode(v)
        case let v as Double: try c.encode(v)
        case let v as Int: try c.encode(v)
        case let v as Bool: try c.encode(v)
        default: try c.encode(String(describing: value))
        }
    }
}
