# Subagents — 兩個角色（Lucky Star Notify / Go）

沿用 `Lucky_Star_Casino/.claude/agents/` 的**結構與紀律**，但**只移植 2 個角色**，
且內容全部重寫。理由見下。

一個角色一個 `.md`。YAML frontmatter：`name`（呼叫名）、`description`（Claude 用這行
決定要不要自動委派，**觸發詞就放在這裡**）、`tools`（工具白名單）、`model`（省略＝繼承
主執行緒）。body 是該角色的系統提示。每個 subagent 有獨立 context window，雜訊留在裡面，
只有結論回報給主執行緒。

## 角色

| 角色 | 職責 | 關鍵限制 |
|---|---|---|
| `go-reviewer` | 審查 diff／檔案，對照 AGENTS.md §2 的 28 條地雷 | **唯讀**（白名單沒有 Edit/Write——審查者不能是作者） |
| `go-tester` | 寫並跑測試：Go 單元測試、`test/contract/` 契約測試、`test/load/` 壓測 | 只碰測試檔；發現產品 bug 只回報不修；不 commit |

## 為什麼團隊那邊 6 個，這裡只有 2 個

逐個核對過。那 6 份的 body **幾乎全是領域知識**，對本專案 100% 無效
（Maven / JPA 雙資料源 / wallet 帳務 / RTP band / PixiJS / 三條下注鐵則）。
可轉移的只有**結構紀律**：唯讀審查者、報告格式、不自己 commit、地雷知識單一真相來源。

| 團隊角色 | 處置 |
|---|---|
| `code-reviewer` | ✅ 重寫成 `go-reviewer`。「審查者不能是作者」零領域耦合，而本專案 §2 有 28 條地雷（團隊版 20 條），checklist 反而更有料 |
| `qa-tester` | ✅ 重寫成 `go-tester`。CLAUDE.md §4 已把 tests-first 升級成硬性要求，且 Phase 5 兩項未完成 DoD 都是測試性質 |
| `dev-coder` | ❌ 整份是 Java/Spring。8,507 行、7 個 `internal/` 套件的規模，主執行緒直接做比冷啟動快 |
| `devops` | ❌ Phase 4 容器化與 CI 已完成。**且直接衝突**：那份寫 "Never touch `main`: protected branch"，本專案 AGENTS.md §3 明寫「直接在 `main` 上做，不搞 fork/PR」 |
| `frontend-dev` | ❌ 本專案「前端」只有 `cmd/demo` 一頁，Phase 4 已完成 |
| `ui-ux` | ❌ 同上，沒有對應領域 |

這跟 `AGENTS.md §6` 對團隊 31 條地雷做過的判斷是同一個模式：能轉移的是紀律，不是內容。

## 觸發詞

寫在各檔的 `description` 裡，中英並列（實際下指令多半是中文）。

| 說了這些 → | 派給 |
|---|---|
| 審查、幫我 review、檢查一下這段、commit 前看一下、有沒有踩到地雷、併發安全嗎 | `go-reviewer` |
| review this diff, code review, check before commit, is this race-free | `go-reviewer` |
| 寫測試、補測試、跑測試、契約測試、跨語言等價、壓測、效能對照、測試沒過 | `go-tester` |
| write tests, run the suite, contract test, load test, benchmark, equivalence test | `go-tester` |

也可以手動指定：「用 go-reviewer 審查 `internal/hub` 最近的 diff」。

## ⚠️ 什麼時候**不要**派

本專案的冷啟動成本比團隊那邊高。`AGENTS.md` 是 **28.9 KB** + `CLAUDE.md` 5.5 KB，
subagent 每次冷啟動都得自己重讀一遍才開始幹活——**約 9~10k token 的固定成本**。
團隊那邊是 6 個 Java 服務 + 2 個前端，那個規模才養得起 6 個角色。

所以：

- **小改動、單檔修正、問問題** → 主執行緒直接做，更快也更省。
- **主執行緒已經有的 context** → 別派。subagent 是冷的，會把你已經知道的事重推一遍。
- **值得派的是**：雜訊大、要來回試錯、隔離 context 有實質好處的工作。
  Phase 5 的契約測試（同時開兩套 stack）與壓測（輸出量大）正好符合。

## 流程

```
需求 → 主執行緒實作（或拆步驟）
     → go-tester 補測試並跑 go test -race ./...
     → go-reviewer 審查 diff
     → must-fix 回到主執行緒修 → 重審
     → PASS 且全綠 → 主執行緒 commit
       （格式 type(scope): 中文描述，並在根目錄 CHANGELOG.md 記一筆）
```

## 語言慣例

**agent 檔案 body 用英文**（那是給 AI 的系統提示，省 token），subagent 也全程用英文
工作與回報。**繁體中文只出現在用戶會直接讀到的地方**：程式碼與測試的註解、測試名稱
（學習用途），以及主執行緒回報給用戶的結論。本檔用繁中，因為它是寫給你看的。

## 地雷知識的單一真相來源

兩份 agent 檔案都以「先讀 repo 根目錄 `AGENTS.md` §2」開頭。
**地雷知識只維護在那裡，不要複製進 agent 檔案**——複製就會漂移。
agent 檔案裡只放「指向哪幾條」與「怎麼排優先序」。

## 使用注意

- **新增或修改 agent 檔案後要重開 session** 才會註冊。
- 互動式管理：session 裡打 `/agents`。
- `go-reviewer` 的白名單**刻意沒有 Edit/Write**。若哪天覺得「讓它順手修一下比較快」，
  那就是把審查者變成作者——這個角色的價值正好在於它不能。
