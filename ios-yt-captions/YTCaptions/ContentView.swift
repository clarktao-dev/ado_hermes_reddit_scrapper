import SwiftUI
import UIKit

struct ContentView: View {
    @StateObject private var settings = SettingsStore()
    @State private var urlText: String = ""
    @State private var status: String = "貼上 YouTube 連結，按「抓取並傳送」。"
    @State private var preview: String = ""
    @State private var busy = false
    @State private var showSettings = false

    var body: some View {
        NavigationStack {
            VStack(alignment: .leading, spacing: 16) {
                TextField("https://youtube.com/watch?v=…", text: $urlText, axis: .vertical)
                    .textInputAutocapitalization(.never)
                    .autocorrectionDisabled()
                    .keyboardType(.URL)
                    .lineLimit(3...5)
                    .padding(12)
                    .background(Color(.secondarySystemBackground))
                    .clipShape(RoundedRectangle(cornerRadius: 12))

                HStack {
                    Button {
                        if let clip = UIPasteboard.general.string {
                            urlText = clip
                        }
                    } label: {
                        Label("貼上", systemImage: "doc.on.clipboard")
                    }
                    .buttonStyle(.bordered)

                    Spacer()

                    Button {
                        Task { await run() }
                    } label: {
                        if busy {
                            ProgressView()
                                .padding(.horizontal, 8)
                        } else {
                            Label("抓取並傳送", systemImage: "paperplane.fill")
                        }
                    }
                    .buttonStyle(.borderedProminent)
                    .disabled(busy || urlText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                }

                Text(status)
                    .font(.footnote)
                    .foregroundStyle(.secondary)

                if !preview.isEmpty {
                    Text("預覽")
                        .font(.headline)
                    ScrollView {
                        Text(preview)
                            .font(.system(.footnote, design: .monospaced))
                            .frame(maxWidth: .infinity, alignment: .leading)
                    }
                    .padding(8)
                    .background(Color(.secondarySystemBackground))
                    .clipShape(RoundedRectangle(cornerRadius: 12))
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
    private func run() async {
        busy = true
        defer { busy = false }
        status = "抓取字幕中…"
        preview = ""
        do {
            let result = try await TranscriptFetcher.fetch(
                urlString: urlText,
                languages: settings.languageList
            )
            preview = String(result.text.prefix(1200))
            status = "抓到 \(result.text.count) 字（\(result.language)）→ 傳送到 VPS…"
            try await WebhookClient.post(
                webhookURL: settings.webhookURL,
                token: settings.webhookToken,
                result: result,
                originalURL: urlText
            )
            status = "✅ 已送到 VPS（\(result.videoID)）"
        } catch {
            status = "❌ \(error.localizedDescription)"
        }
    }
}

struct SettingsView: View {
    @ObservedObject var settings: SettingsStore
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationStack {
            Form {
                Section("VPS Webhook（建議 Tailscale）") {
                    TextField("http://100.x.x.x:8791/hooks/transcript", text: $settings.webhookURL)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled()
                        .keyboardType(.URL)
                    SecureField("Token（MAC_WEBHOOK_TOKEN）", text: $settings.webhookToken)
                }
                Section("語言偏好（逗號分隔）") {
                    TextField("de,en", text: $settings.languages)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled()
                }
                Section {
                    Text("VPS 需開著 mac_transcript_receiver.py。手機與 VPS 在同一 Tailscale 網路。")
                        .font(.footnote)
                        .foregroundStyle(.secondary)
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
