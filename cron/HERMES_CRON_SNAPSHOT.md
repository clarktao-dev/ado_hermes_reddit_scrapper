# Hermes Live Cron Snapshot

> Auto-generated from production Hermes cron (`/root/.hermes/cron/jobs.json` on host `Hermes-Agent-VPS`).
> Exported at **2026-10-15T21:08:22+00:00** by Hermes (Ado) at user's request.
> For machine-readable form see `hermes_live_jobs.yaml` (same directory).

## Why this exists

Cursor Cloud Agent operates inside this repo (`ado_hermes_reddit_scrapper`) and can only read
files committed to GitHub. It cannot shell into the production VPS. The existing
`cron/jobs.yaml` (Plan 4) only documents the **reaction listener + weekly recap** subsystem
and is unaware of the actual cron jobs Hermes runs every day.

This snapshot closes that gap so Cursor can plan code changes against the **real production
schedule** instead of guessing.

---

## All Hermes cron jobs (10 total, all enabled)

| id | schedule (UTC) | enabled | script / command | one-liner | domain |
|---|---|---|---|---|---|
| `9db9b4938a2f` | `0 4 * * *` | ✅ | (skill: `germany_real_estate_monitor_esmc`) | Daily Dresden 6-room + parking scrape | realestate |
| `93503a81dcc1` | `0 0,6,12,18 * * *` | ✅ | (skill: `google-workspace`) | Memory-log snapshot to spreadsheet, 6h | system |
| **`ac28419b2498`** | **`0 2 * * *`** | ✅ | `pipeline/youtube_daily.py --mode short` | **Daily YouTube round-robin** | **🎬 youtube** |
| `053a59e0721a` | `0 3 * * *` | ✅ | `pipeline/news_daily.py` (prompt-driven) | Daily German news digest | 📰 news |
| `afec3bf1fea1` | `0 4 * * 2,5` | ✅ | (prompt → `long_form_recommendation`) | Tue/Fri long-form newsletter candidates | 📰 news (recs) |
| `2049d7a025c9` | `0 1 * * 5` | ✅ | `pipeline/destatis_daily.py --push` | Weekly Fri Destatis CSV ingest | 📊 realestate data |
| `fac3cfe02519` | `0 5 * * 1` | ✅ | (prompt → cache prune) | Weekly Mon transcript cache TTL=30d | system |
| `2686cfd36e23` | `30 0 * * *` | ✅ | (prompt → `reddit_daily.py`) | Daily 4-subreddit DE Reddit digest | 🟥 reddit |
| **`b4af9754c582`** | **`30 2 * * *`** | ✅ | `pipeline/scripts/podcast_daily.py --n-channels 1` | **Daily L'Immo podcast RSS ingest** | **🎙️ podcast** |
| `f22eb5baec7f` | `30 3 * * *` | ✅ | `bash /root/scripts/backup-agent-persona.sh` | Daily agent_persona git backup | system |

> ⏰ All schedules are **UTC**. CEST labels in the `name` column are cosmetic.
> CEST = UTC+2 (summer) / CET = UTC+1 (winter). Example: `0 2 * * *` UTC = 04:00 CEST in summer.

> 🔑 All 10 jobs `source /root/.hermes/.env` (mode 600) for secrets. No raw token is
> embedded in any prompt. See `hermes_live_jobs.yaml` → `env_source_file` field per job.

---

## The two jobs you specifically asked about

### 🎬 `ac28419b2498` — Daily YouTube Podcast (4:30 AM)

| field | value |
|---|---|
| name | `Daily YouTube Podcast (4:30 AM)` |
| schedule | `0 2 * * *` UTC (= 04:00 CEST summer) |
| enabled | ✅ true (state: `scheduled`, completed: **70** runs) |
| created | 2026-08-08 |
| last run | 2026-10-15T02:05:11Z → `ok` |
| next run | 2026-10-16T02:00:00Z |
| command | `cd /root/projects/ado_hermes_reddit_scrapper && source /root/.hermes/.env && /root/.hermes/hermes-agent/venv/bin/python pipeline/youtube_daily.py --mode short` |
| script | `pipeline/youtube_daily.py` |
| args | `--mode short` |
| deliver | `discord:1535461574460968960` (#每日podcast) |

### 🎙️ `b4af9754c582` — Daily Podcast RSS (2:15 AM)

| field | value |
|---|---|
| name | `Daily Podcast RSS (2:15 AM)` |
| schedule | `30 2 * * *` UTC (= 04:30 CEST summer) |
| enabled | ✅ true (state: `scheduled`, completed: **47** runs) |
| created | 2026-08-31 (Plan 10) |
| last run | 2026-10-15T02:33:56Z → `ok` |
| next run | 2026-10-16T02:30:00Z |
| command | `cd /root/projects/ado_hermes_reddit_scrapper && PYTHONPATH=. /root/.hermes/hermes-agent/venv/bin/python pipeline/scripts/podcast_daily.py --n-channels 1` |
| script | `pipeline/scripts/podcast_daily.py` |
| args | `--n-channels 1` |
| deliver | `origin` (chat this job was created from, not Discord) |

> ⚠️ **Schedule mismatch note**: the original spec said `b4af9754c582` runs `0 2 * * *` (same
> minute as the YouTube job). Live cron runs it `30 2 * * *` so it fires **30 min after**
> YouTube finishes, letting YouTube write its vault entries first. Phase 2 plan: make
> `b4af9754c582` a thin shim that just calls `youtube_daily.py --mode podcast` instead of
> running `podcast_daily.py` separately — see *Phase 2 plan* below.

---

## Phase 2 plan (where podcast → youtube merge is supposed to happen)

Goal: collapse `b4af9754c582` into the YouTube round-robin flow. PR branch
`cursor/youtube-podcast-unified-7a35` (current HEAD `614a582`) is already partway there.

When Phase 2 lands:

| from | to |
|---|---|
| `b4af9754c582` runs `podcast_daily.py --n-channels 1` | `b4af9754c582` runs `youtube_daily.py --mode podcast` (or similar) |
| Separate Discord deliver to `origin` | Reuse the YouTube job's Discord target (`1535461574460968960`) |
| Standalone `podcast_daily.py` maintained | `pipeline/scripts/podcast_daily.py` becomes a **shim** that re-exports `pipeline/youtube_daily.run_podcast_channel` (or equivalent) so any leftover CLI callers don't break |

Cursor agents touching podcast code: **the shim contract is your compatibility boundary** —
`podcast_daily.py` should keep accepting `--n-channels N` and emit the same vault layout
even if its body is now `from pipeline.youtube_daily import run_podcast_channel; run_podcast_channel(...)`.

---

## Failure inventory (last 24 h)

| job | last_status | last_error | failure_streak |
|---|---|---|---|
| `93503a81dcc1` Memory Log | ok (after recovery) | ⚠️ see `last_failure.detail` in YAML — today's 12:00 UTC run blocked by missing `execute_code` toolset; subsequent runs (18:00 UTC) recovered | 0 |
| all others | ok | none | 0 |

Action item for `93503a81dcc1`: either (a) re-enable `execute_code`/`bash` in this cron's
toolset profile, or (b) wrap `google-workspace`'s `google_api.py` so it's callable from
`delegate_task`-only sessions. Not urgent — it self-recovers on the next 6-hour tick.

---

## How to regenerate this snapshot

```bash
# From production Hermes host (the one running cron):
hermes cron list --json > /tmp/jobs.json      # or use cronjob_manage action=list
python3 -c "
import json, yaml
with open('/tmp/jobs.json') as f: d = json.load(f)
# ... (see tools/snapshot_hermes_cron.py for the full transformer)
"
cp cron/hermes_live_jobs.yaml cron/HERMES_CRON_SNAPSHOT.md \
   /root/projects/ado_hermes_reddit_scrapper/cron/
cd /root/projects/ado_hermes_reddit_scrapper
git add cron/hermes_live_jobs.yaml cron/HERMES_CRON_SNAPSHOT.md
git commit -m "chore(cron): refresh Hermes live cron snapshot"
git push origin cursor/youtube-podcast-unified-7a35
```

---

## Files in this snapshot

| file | audience | format |
|---|---|---|
| `cron/hermes_live_jobs.yaml` | Cursor / scripts | YAML, machine-readable, full per-job payload (38 fields per job) |
| `cron/HERMES_CRON_SNAPSHOT.md` | humans (you, Cursor agent context window) | Markdown summary + 2 detailed tables |
| `cron/jobs.yaml` *(unchanged)* | existing | Plan 4 reaction-listener + weekly-recap source of truth |

**Do not edit `hermes_live_jobs.yaml` by hand** — it will be overwritten on the next export.
If a job legitimately changes, edit it in Hermes via `cronjob_manage action=update`, then
re-run the snapshot command above.