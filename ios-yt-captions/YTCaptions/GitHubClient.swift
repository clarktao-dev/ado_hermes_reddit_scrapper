import Foundation

/// Minimal GitHub Contents API client for transcript-bridge inbox/outbox.
struct GitHubClient {
    var token: String
    var owner: String
    var repo: String
    var branch: String

    private var base: String { "https://api.github.com/repos/\(owner)/\(repo)/contents" }

    struct RemoteFile {
        var path: String
        var name: String
        var sha: String
        var json: [String: Any]
    }

    enum GHError: LocalizedError {
        case badResponse(Int, String)
        case decode
        case missingToken

        var errorDescription: String? {
            switch self {
            case .badResponse(let c, let b): return "GitHub HTTP \(c): \(b.prefix(180))"
            case .decode: return "無法解析 GitHub 回應"
            case .missingToken: return "請先在設定填 GitHub Token"
            }
        }
    }

    func listInbox() async throws -> [RemoteFile] {
        try await listJSONFiles(dir: "transcript-bridge/inbox")
    }

    func listOutboxNames() async throws -> Set<String> {
        let files = (try? await listJSONFiles(dir: "transcript-bridge/outbox")) ?? []
        return Set(files.map(\.name))
    }

    func uploadOutbox(videoID: String, payload: [String: Any]) async throws {
        let path = "transcript-bridge/outbox/\(videoID).json"
        let data = try JSONSerialization.data(withJSONObject: payload, options: [.prettyPrinted, .sortedKeys])
        let b64 = data.base64EncodedString()
        // If file exists, need sha to update
        let existingSHA = try await fileSHA(path: path)
        var body: [String: Any] = [
            "message": "transcript-bridge: outbox \(videoID) (ios)",
            "content": b64,
            "branch": branch,
        ]
        if let existingSHA { body["sha"] = existingSHA }
        _ = try await put(path: path, jsonBody: body)
    }

    func deleteInbox(name: String, sha: String) async throws {
        let path = "transcript-bridge/inbox/\(name)"
        let body: [String: Any] = [
            "message": "transcript-bridge: clear inbox \(name) (ios)",
            "sha": sha,
            "branch": branch,
        ]
        try await delete(path: path, jsonBody: body)
    }

    // MARK: - HTTP

    private func listJSONFiles(dir: String) async throws -> [RemoteFile] {
        let url = URL(string: "\(base)/\(dir)?ref=\(branch)")!
        let (data, resp) = try await request(url, method: "GET")
        let code = (resp as? HTTPURLResponse)?.statusCode ?? 0
        if code == 404 { return [] }
        guard (200...299).contains(code) else {
            throw GHError.badResponse(code, String(data: data, encoding: .utf8) ?? "")
        }
        guard let arr = try JSONSerialization.jsonObject(with: data) as? [[String: Any]] else {
            throw GHError.decode
        }
        var out: [RemoteFile] = []
        for item in arr {
            guard let name = item["name"] as? String,
                  name.hasSuffix(".json"),
                  name != ".gitkeep",
                  let path = item["path"] as? String,
                  let sha = item["sha"] as? String else { continue }
            // Need file content — list API may include download_url
            let file = try await fetchFile(path: path)
            out.append(file)
        }
        return out
    }

    private func fetchFile(path: String) async throws -> RemoteFile {
        let url = URL(string: "\(base)/\(path)?ref=\(branch)")!
        let (data, resp) = try await request(url, method: "GET")
        let code = (resp as? HTTPURLResponse)?.statusCode ?? 0
        guard (200...299).contains(code) else {
            throw GHError.badResponse(code, String(data: data, encoding: .utf8) ?? "")
        }
        guard let obj = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let name = obj["name"] as? String,
              let sha = obj["sha"] as? String,
              let contentB64 = obj["content"] as? String else {
            throw GHError.decode
        }
        let cleaned = contentB64.replacingOccurrences(of: "\n", with: "")
        guard let raw = Data(base64Encoded: cleaned),
              let json = try JSONSerialization.jsonObject(with: raw) as? [String: Any] else {
            throw GHError.decode
        }
        return RemoteFile(path: path, name: name, sha: sha, json: json)
    }

    private func fileSHA(path: String) async throws -> String? {
        let url = URL(string: "\(base)/\(path)?ref=\(branch)")!
        let (data, resp) = try await request(url, method: "GET")
        let code = (resp as? HTTPURLResponse)?.statusCode ?? 0
        if code == 404 { return nil }
        guard (200...299).contains(code),
              let obj = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let sha = obj["sha"] as? String else { return nil }
        return sha
    }

    private func put(path: String, jsonBody: [String: Any]) async throws -> [String: Any] {
        let url = URL(string: "\(base)/\(path)")!
        let body = try JSONSerialization.data(withJSONObject: jsonBody)
        let (data, resp) = try await request(url, method: "PUT", body: body)
        let code = (resp as? HTTPURLResponse)?.statusCode ?? 0
        guard (200...299).contains(code) else {
            throw GHError.badResponse(code, String(data: data, encoding: .utf8) ?? "")
        }
        return (try JSONSerialization.jsonObject(with: data) as? [String: Any]) ?? [:]
    }

    private func delete(path: String, jsonBody: [String: Any]) async throws {
        let url = URL(string: "\(base)/\(path)")!
        let body = try JSONSerialization.data(withJSONObject: jsonBody)
        let (data, resp) = try await request(url, method: "DELETE", body: body)
        let code = (resp as? HTTPURLResponse)?.statusCode ?? 0
        guard (200...299).contains(code) else {
            throw GHError.badResponse(code, String(data: data, encoding: .utf8) ?? "")
        }
    }

    private func request(_ url: URL, method: String, body: Data? = nil) async throws -> (Data, URLResponse) {
        if token.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            throw GHError.missingToken
        }
        var req = URLRequest(url: url)
        req.httpMethod = method
        req.httpBody = body
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        req.setValue("application/vnd.github+json", forHTTPHeaderField: "Accept")
        req.setValue("2022-11-28", forHTTPHeaderField: "X-GitHub-Api-Version")
        req.setValue("ios-yt-captions/2.0", forHTTPHeaderField: "User-Agent")
        if body != nil {
            req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        }
        return try await URLSession.shared.data(for: req)
    }
}
