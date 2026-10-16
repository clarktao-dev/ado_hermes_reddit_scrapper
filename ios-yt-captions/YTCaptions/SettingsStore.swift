import Foundation
import SwiftUI

final class SettingsStore: ObservableObject {
    // GitHub mailbox (preferred)
    @AppStorage("ghOwner") var ghOwner: String = "clarktao-dev"
    @AppStorage("ghRepo") var ghRepo: String = "ado_hermes_reddit_scrapper"
    @AppStorage("ghBranch") var ghBranch: String = "cursor/mac-transcript-bridge-7a35"
    @AppStorage("ghToken") var ghToken: String = ""

    // Optional direct webhook (legacy Tailscale path)
    @AppStorage("webhookURL") var webhookURL: String = ""
    @AppStorage("webhookToken") var webhookToken: String = ""

    @AppStorage("languages") var languages: String = "de,en"

    var languageList: [String] {
        languages
            .split(separator: ",")
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
    }

    var github: GitHubClient {
        GitHubClient(token: ghToken, owner: ghOwner, repo: ghRepo, branch: ghBranch)
    }
}
