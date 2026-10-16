import SwiftUI
import UIKit

struct ContentView: View {
    @StateObject private var settings = SettingsStore()
    @State private var urlText: String = ""
    @State private var status: String = "按「同步佇列」：從 GitHub 撈 Agent 丟的連結。"
    @State private var log: String = ""
    @State private var busy = false
    @State private var showSettings = false

    var body: some View {
        NavigationStack {
            VStack(alignment: .leading, spacing: 16) {
                Button {
                    Task { await syncQueue() }
                } label: {
                    if busy {
                        ProgressView().padding(.horizontal, 12)
                    } else {
                        Label("同步佇列（撈 GitHub → 抓字幕 → 上傳）", systemImage: "arrow.triangle.2.circlepath")
                    }
                }
                .buttonStyle(.borderedProminent)
                .disabled(busy)

                Divider()

                Text("或手動處理一支")
                    .font(.headline)

                TextField("https://youtube.com/watch?v=…", text: $urlText, axis: .vertical)
                    .textInputAutocapitalization(.never)
                    .autocorrectionDisabled()
                    .keyboardType(.URL)
                    .lineLimit(2...4)
                    .padding(10)
                    .background(Color(.secondarySystemBackground))
                    .clipShape(RoundedRectangle(cornerRadius: 10))

                HStack {
                    Button("貼上") {
                        if let c = UIPasteboard.general.string { urlText = c }
                    }
                    .buttonStyle(.bordered)

                    Button("抓取並上傳 GitHub") {
                        Task { await runOne(urlText: urlText, channel: "") }
                    }
                    .buttonStyle(.borderedProminent)
                    .disabled(busy || urlText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                }

                Text(status)
                    .font(.footnote)
                    .foregroundStyle(.secondary)

                if !log.isEmpty {
                    ScrollView {
                        Text(log)
                            .font(.system(.caption, design: .monospaced))
                            .frame(maxWidth: .infinity, alignment: .leading)
                    }
                    .padding(8)
                    .background(Color(.secondarySystemBackground))
                    .clipShape(RoundedRectangle(cornerRadius: 10))
                }

                Spacer()
            }
            .padding()
            .navigationTitle("YT Captions")
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Button("設定") { showSettings = true }
                }
            }
            .sheet(isPresented: $showSettings) {
                SettingsView(settings: settings)
            }
        }
    }

    @MainActor
    private func syncQueue() async {
        busy = true
        defer { busy = false }
        log = ""
        status = "讀取 GitHub inbox…"
        do {
            let files = try await settings.github.listInbox()
            if files.isEmpty {
                status = "inbox 是空的（Agent 還沒丟連結）"
                return
            }
            status = "找到 \(files.count) 筆，開始抓…"
            var ok = 0
            var fail = 0
            for file in files {
                let job = file.json
                let url = (job["url"] as? String)
                    ?? "https://www.youtube.com/watch?v=\(job["video_id"] as? String ?? "")"
                let channel = (job["channel"] as? String) ?? ""
                let langs = (job["languages"] as? [String]) ?? settings.languageList
                appendLog("→ \(file.name)")
                do {
                    try await processAndUpload(
                        urlString: url,
                        channel: channel,
                        languages: langs,
                        deleteInbox: file
                    )
                    ok += 1
                    appendLog("  ✓ 完成")
                } catch {
                    fail += 1
                    appendLog("  ✗ \(error.localizedDescription)")
                }
            }
            status = "同步結束：成功 \(ok)，失敗 \(fail)"
        } catch {
            status = "❌ \(error.localizedDescription)"
        }
    }

    @MainActor
    private func runOne(urlText: String, channel: String) async {
        busy = true
        defer { busy = false }
        do {
            try await processAndUpload(
                urlString: urlText,
                channel: channel,
                languages: settings.languageList,
                deleteInbox: nil
            )
            status = "✅ 已上傳到 GitHub outbox"
        } catch {
            status = "❌ \(error.localizedDescription)"
        }
    }

    private func processAndUpload(
        urlString: String,
        channel: String,
        languages: [String],
        deleteInbox: GitHubClient.RemoteFile?
    ) async throws {
        let result = try await TranscriptFetcher.fetch(urlString: urlString, languages: languages)
        var meta: [String: String] = ["client": "ios-yt-captions"]
        if !channel.isEmpty { meta["channel"] = channel }

        let payload: [String: Any] = [
            "video_id": result.videoID,
            "url": urlString.hasPrefix("http")
                ? urlString
                : "https://www.youtube.com/watch?v=\(result.videoID)",
            "title": result.title,
            "language": result.language,
            "is_generated": result.isGenerated,
            "source": result.source,
            "text": result.text,
            "segments": [],
            "meta": meta,
            "fetched_at": Date().timeIntervalSince1970,
        ]

        try await settings.github.uploadOutbox(videoID: result.videoID, payload: payload)

        // Optional legacy webhook
        let hook = settings.webhookURL.trimmingCharacters(in: .whitespacesAndNewlines)
        if !hook.isEmpty {
            try? await WebhookClient.post(
                webhookURL: hook,
                token: settings.webhookToken,
                result: result,
                originalURL: urlString
            )
        }

        if let deleteInbox {
            try await settings.github.deleteInbox(name: deleteInbox.name, sha: deleteInbox.sha)
        }
    }

    @MainActor
    private func appendLog(_ line: String) {
        log += (log.isEmpty ? "" : "\n") + line
    }
}

struct SettingsView: View {
    @ObservedObject var settings: SettingsStore
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationStack {
            Form {
                Section("GitHub 信箱（推薦）") {
                    TextField("Owner", text: $settings.ghOwner)
                        .textInputAutocapitalization(.never)
                    TextField("Repo", text: $settings.ghRepo)
                        .textInputAutocapitalization(.never)
                    TextField("Branch", text: $settings.ghBranch)
                        .textInputAutocapitalization(.never)
                    SecureField("Personal Access Token", text: $settings.ghToken)
                }
                Section {
                    Text("Token 權限：Contents Read/Write。在 GitHub → Settings → Developer settings → Fine-grained token。")
                        .font(.footnote)
                        .foregroundStyle(.secondary)
                }
                Section("語言偏好") {
                    TextField("de,en", text: $settings.languages)
                        .textInputAutocapitalization(.never)
                }
                Section("可選：直接 Webhook（Tailscale）") {
                    TextField("http://100.x:8791/hooks/transcript", text: $settings.webhookURL)
                        .textInputAutocapitalization(.never)
                        .keyboardType(.URL)
                    SecureField("Token", text: $settings.webhookToken)
                }
            }
            .navigationTitle("設定")
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("完成") { dismiss() }
                }
            }
        }
    }
}

#Preview {
    ContentView()
}
