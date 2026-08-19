---
type: topic-roster
status: active
version: v3
created: 2026-08-19
updated: 2026-08-19
owner: smilefacetao
reviewers: Ado Hermes Agent
---

# 德國房地產深度內容主題清單(v3)

## 設計原則

1. **大主題,不是子題** — 每個主題是讀者會主動搜尋的概念層級,不是技術細節
2. **鎖德國** — 只放跟德國本地買家/投資者直接相關的主題,海外置產不列
3. **讀者導向** — 買家/投資者會查,不是研究機構才會看

## 9 大主題

### 1. 暖氣能源轉型
- **涵蓋**: Wärmepumpe 普及、GEG / GMG、2045 目標、買舊屋能源效率判讀
- **目標讀者**: 買家、屋主
- **首篇**: 2026-08-19_heizung-waermepumpe_v3 ✅

### 2. 區域房價分化
- **涵蓋**: 大城市 vs 小鎮、人口外流區、Energieklasse、城鄉差距
- **目標讀者**: 買家、賣家
- **切入角度建議**: NRW / Sachsen-Anhalt 等案例、Speckgürtel 與 Grundzentren 對比

### 3. 融資環境
- **涵蓋**: Bauzinsen 走勢、聯邦公債殖利率傳導鏈、Sollzins / Effektivzins、20 vs 30 年房貸鎖定
- **目標讀者**: 買家
- **切入角度建議**: 「Bundesanleihe-Rendite 領先指標」「Volltilger vs Annuität」

### 4. 持有成本結構
- **涵蓋**: Grundsteuer 改革、Nebenkosten、CO2-Bepreisung、Gas / Öl vs Wärmepumpe 長期成本
- **目標讀者**: 屋主、投資者
- **切入角度建議**: Grundsteuer 2025 新制對第一年稅單的影響

### 5. 租屋與買房邊界
- **涵蓋**: Mietpreisbremse、Wohngeld、Kapitalanlage 報酬、首購族
- **目標讀者**: 租屋者 + 買家(雙向)
- **切入角度建議**: 「在 X 城市租房還是買房?」簡單模型

### 6. 繼承與法律通路
- **涵蓋**: Erbpacht、Erbschaftsteuer、Wohnungserbe、家庭內移轉、Teilungsversteigerung
- **目標讀者**: 繼承者、家屬
- **切入角度建議**: 「父母留下的房子要賣還是用?」三條路比較

### 7. 房屋整修修繕
- **涵蓋**: Sanierung 成本、Modernisierung 補助(KfW / BAFA)、翻修順序、能源整修
- **目標讀者**: 屋主、買家
- **切入角度建議**: 「拿到舊屋鑰匙先做什麼」checklist

### 8. 舊屋翻新
- **涵蓋**: Altbau 陷阱、Denkmalschutz、能耗、鉛管 / 石綿 / PCP / Dämmung 風險
- **目標讀者**: 買家、投資者
- **切入角度建議**: Altbau 與 Neubau 的成本 / 風險對比

### 9. 租賃買賣地雷
- **涵蓋**: Kaufvertrag 風險、Verkäufer 隱瞞、Makler 費、Grundbuch 查驗、Widerrufsrecht
- **目標讀者**: 買家
- **切入角度建議**: 「看房時的 10 個紅旗」「簽約前必查 Grundbuch 5 項」

---

## 自動更新機制

### 觸發條件

**當任一條件成立,父代理自動加候選主題並通知**:

1. **新主題有實質內容** — 新聞 ≥3 篇該主題,跟現有 9 大主題都對不上
2. **現有主題有重大發展** — 像是 2025 大選後新部長、新政策、新稅改
3. **新興術語 / 法案** — 像是 Denkmalschutz 改法、Mietpreisbremse 延長、Steuerklassenwechsel 對持屋的影響

### 通知形式

推送到 Discord `#tao` channel:

```
[主題清單更新] YYYY-MM-DD

新增:
- #10 <新主題名>
  來源: <新聞清單>
  理由: <為什麼有實質內容>

待確認:
- #11 <候選主題>
  來源: <只有 1-2 篇,待更多確認>

不變:
- 其餘 9 個主題
```

### 不動的原則

- 已定的 9 大主題**不會輕易刪除**(用戶已認可)
- 只有「新增」與「合併」,不直接刪
- 「合併」(像是兩個候選主題其實是同一件事)會通知用戶確認

---

## 三個自動更新觸發點

| 觸發 | 怎麼更新 |
|---|---|
| **#1 主題挖掘 cron**(每週日 23:59 UTC)| 掃 vault + 新聞,跟當前清單比對 → 候選主題推 `#tao` |
| **#2+#3+#4 流程中**(寫新文章時)| 發現新聞報導的是清單外主題 → 加入「候選主題」,**不立刻寫**,等下次 #1 整理 |
| **新聞 cron 主動監聽**(每天 03:00 UTC Plan 7+8+10 後)| 父代理 quick scan 當日 vault → 新候選放「待確認」清單 |

---

## 變更紀錄

- **v1 (2026-08-19)** — #1 子代理初挖 8 主題(已 archive:`2026-08-19_themen-fuer-kaeufer-und-investoren.md`)
- **v2 (2026-08-19)** — 父代理整理為 6 大主題(在對話中提案,未存檔)
- **v3 (2026-08-19)** — 整合用戶新增 3 主題,**9 大主題定型**,加入自動更新機制