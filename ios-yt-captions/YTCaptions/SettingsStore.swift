import Foundation
import SwiftUI

/// Persists webhook target + language prefs in UserDefaults.
final class SettingsStore: ObservableObject {
    @AppStorage("webhookURL") var webhookURL: String =
        "http://100.x.x.x:8791/hooks/transcript"
    @AppStorage("webhookToken") var webhookToken: String = ""
    /// Comma-separated language codes, preferred order.
    @AppStorage("languages") var languages: String = "de,en"

    var languageList: [String] {
        languages
            .split(separator: ",")
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
    }
}
