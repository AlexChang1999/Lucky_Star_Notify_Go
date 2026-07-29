# AGENTS.md — AI 開發前必讀（Lucky Star Notify / Go）

> 任何 AI / 自動化代理在本專案開發前，**先讀完本檔**。
> 目的：快速掌握專案、遵守既有約定、避開已知地雷。
> 本檔沿用 `Lucky_Star_Casino/AGENTS.md` 的**結構**，但**內容全部重寫**——
> 那邊 31 條地雷全是 Java/Spring/團隊 repo 專屬，對本專案無效（見 §6）。

---

## 0. 專案一句話

把 `Lucky_Star_Casino` 的 `notification-service`（Java 21 / Spring Boot 3.3.5）
用 **Go** 重寫成獨立作品集專案：**消費 3 個 Kafka topic → 透過 WebSocket/STOMP 推播給線上玩家**。
module path `github.com/AlexChang1999/lucky-star-notify-go`，**Go 1.24+**。

**核心賣點是「自己實作 STOMP 1.2 伺服器子集」**——Go 生態沒有現成的 server 端實作，
這是本專案與「用 Gin 寫 CRUD」的差別所在。**不要為了省事去找第三方 STOMP 套件。**

---

## 1. 必讀文件（照順序）

| 順序 | 檔案 | 重點 |
|---|---|---|
| 1 | `計畫說明書.md` | **單一真相來源**：為什麼選這個服務、Phase 0~5 施工計畫與 DoD、驗證策略、風險 |
| 2 | 本檔 §2 已知地雷 | 不讀會浪費時間的坑 |
| 3 | `README.md` | 對外門面（Phase 5 才會寫完，早期是骨架） |
| 4 | `docs/ADR-*.md` | 已拍板的架構決策 |

**進度查詢**：看 `計畫說明書.md` §6 各 Phase 的 DoD checkbox。
**完成一項就當場勾掉**，別留到之後補——這是判斷「現在做到哪」的唯一依據。

---

## 2. ⚠️ 已知地雷

> 前 6 條是**還沒踩就已知會踩**的（從讀 Java 原始碼與 Go 生態現況推出來）。
> 之後真的踩到新雷，**當場往下加**（§5）。

1. **JWT secret 是「原始 bytes」不是 base64**：Java 版 `PlayerJwtVerifier` 用
   `Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8))`——**直接取字串的 UTF-8 位元組**。
   Go 這邊必須 `[]byte(os.Getenv("JWT_SECRET"))`，**不可以 `base64.StdEncoding.DecodeString`**
   （網路上多數 Go JWT 教學都示範 base64，照抄就驗章永遠失敗）。
   失敗訊息只會說 `signature is invalid`，**完全指不到真因**，會 debug 到懷疑人生。
   另：`jwt.Parse` 必須帶 `jwt.WithValidMethods([]string{"HS256"})`——不帶等於接受
   `alg: none` 與 `RS256` 混淆攻擊，這是 JWT 最經典的漏洞。

2. **STOMP 1.2 的 header 要轉義，但 CONNECT / CONNECTED 例外**：
   header 的 name 與 value 中 `\` → `\\`、`\r` → `\r`、`\n` → `\n`、`:` → `\c`（**body 不轉義**）。
   但規格 §3.1 明訂 **CONNECT 與 CONNECTED 兩種帧不轉義**，為了與 STOMP 1.0 相容。
   漏了轉義 → 玩家暱稱含冒號就把帧切壞；對 CONNECT 做了轉義 → 與 `@stomp/stompjs` 握手失敗。
   已實作於 `internal/stomp`，**動那段程式碼前先看 `escapesHeaders()` 的註解**。

3. **`MESSAGE` 帧漏了 `subscription` header，client 會收不到而且不報錯**：
   `@stomp/stompjs` 用 `subscription` 值去比對自己的訂閱表，對不上就**靜默丟棄**。
   同理 `message-id` 也是必備。這種 bug 沒有錯誤訊息，只有「怎麼沒反應」。

4. **Go 版與 Java 版併存時，consumer group id 必須不同**：
   Java 版是 `notification-service-group`，Go 版一律用 `notification-service-go-group`。
   用同一個 group → Kafka 把 partition 分給兩邊，**每則事件只有其中一版收到**，
   看起來會像「Go 版隨機漏訊息」的假 bug，實際是 group 搶 partition。**會浪費半天。**

5. **`gorilla/websocket` 同時只允許一個 reader 與一個 writer**：
   每條連線固定 **2 個 goroutine**（一讀一寫），所有寫入**一律經過該連線的 outbound channel**，
   **絕不從其他 goroutine 直接 `WriteMessage`**（會 race，而且 `-race` 不一定每次抓到）。
   心跳也走 write goroutine 的 `select` + ticker，不要另開 goroutine 寫。

6. **扇出（fan-out）時絕不可阻塞**：廣播迴圈用 `select { case ch <- msg: default: 驅逐 }`。
   一旦寫成阻塞的 `ch <- msg`，**一個慢速/當掉的 client 就會卡住整個廣播迴圈，拖垮所有人**。
   滿了就斷線（fail-fast 背壓），並且要有 `notify_outbound_dropped_total` 計數器讓它可觀測。

7. **效能數字一律實測，不可虛構**（沿用團隊專案的鐵則）：
   `docs/效能對照報告.md` 的每個數字都必須有對應的實測輸出。
   同機施壓（施壓端與被測服務同一台）會被施壓端自身 CPU 污染，
   **必須在報告裡標明是 co-located，且不可當作對外可引用的容量上限**。

8. **不動團隊 repo**：`H:\Lucky_Star_Casino` 對本專案是**唯讀參考**。
   需要驗證行為時可以在本機把它跑起來、可以改本機 `.env`，
   但**不提交任何 commit、不開 PR**。所有產出都留在本資料夾。

---

## 3. 約定速查

### 技術棧
- **Go 1.24+**，module `github.com/AlexChang1999/lucky-star-notify-go`
- **STOMP 協議層＝標準庫自幹**（`internal/stomp`），這是專案價值所在
- 預定依賴（各 Phase 才加，不要提前引入）：
  `gorilla/websocket`（WS）、`segmentio/kafka-go`（**純 Go，可 `CGO_ENABLED=0` 靜態編譯**）、
  `golang-jwt/jwt/v5`（JWT）、`prometheus/client_golang`（指標）、`log/slog`（標準庫，**不用第三方 logger**）
- Port：**8088**（Java 版是 8087，刻意錯開以便併存 A/B）
- 對外端點：`/ws`（WebSocket）、`/actuator/health`、`/metrics` 與 `/actuator/prometheus`（同一個 handler 註冊兩次，方便接團隊的 Prometheus）

### 目錄
`internal/` 底下是實作細節（外部 module 無法 import，比 Java 的 package-private 更硬）。
完整規劃見 `計畫說明書.md` §5.1。**空目錄 git 不會追蹤**，所以只有裝了程式碼的目錄會進 repo。

### Git / 提交
- **個人 repo，單人開發**：直接在 `main` 上做，不搞 fork/PR（團隊 repo 那套在這裡是純負擔）
- commit 格式沿用團隊約定：`type(scope): 中文描述`
  例：`feat(stomp): 實作 1.2 帧編解碼與 header 轉義`、`test(stomp): 補畸形帧的表格驅動測試`
- scope 用套件名：`stomp` / `hub` / `auth` / `consumer` / `config` / `deploy` / `docs`

### CHANGELOG / ADR
- **`CHANGELOG.md` 在根目錄，單一份**。任何影響行為的變更都在最上方新增一筆，含：
  標題 `## [type] — YYYY-MM-DD — 一句話`、`Added / Changed / Fixed / Removed`、
  **為什麼**（決策理由）與**如何驗證**（例：`go test -race ./...` 結果）
- 純錯字、格式微調可略過
- 架構級決策另寫 `docs/ADR-00X.md` 並在 CHANGELOG 引用。目前規劃：
  - `ADR-001` 為什麼用 Go 重寫推播服務
  - `ADR-002` STOMP 子集的取捨（做哪些指令、為何不做 SockJS）
  - 未來工作要寫進 ADR-001：**多副本的連線路由問題**（Java 版同樣未解，見 `計畫說明書.md` §3.3 發現 3）

---

## 4. 驗證指令（提交前自查）

```bash
# 全部（這四步是 CI 的內容，本機先綠燈再 commit）
go vet ./...
go test -race ./...
golangci-lint run          # 需先安裝，見下
go build ./...

# 只跑 STOMP 協議層，含覆蓋率
go test -race -cover ./internal/stomp/

# 看哪幾行沒被測到
go test -coverprofile=coverage.out ./internal/stomp/ && go tool cover -html=coverage.out

# 模糊測試（找畸形輸入導致的 panic；預設不跑，要手動指定時間）
go test -fuzz=FuzzReadFrame -fuzztime=30s ./internal/stomp/
```

**規則**：
- **`-race` 是本專案的硬性要求，不是選配**。這是一個併發服務，沒有競態偵測器的測試等於沒測。
  （Java 沒有等價工具，這點值得在 README 講。）
- 模糊測試**找到的失敗案例會被自動寫進 `testdata/fuzz/`**——
  有價值的案例要**手動搬進版控**當回歸案例，別讓它被 `.gitignore` 吃掉。
- `golangci-lint` 安裝：`go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest`

---

## 5. 更新本檔

踩到新雷、改變約定、加新套件時，**當場更新對應段落**（並依 §3 記一筆 CHANGELOG）。
本檔的價值來自「累積」，不是「一次寫好」。

---

## 6. 附錄：團隊 repo 的 AGENTS.md 哪些有被沿用

原檔 31 條地雷，**逐條核對後只有 3 條有可轉移的內容**，其餘全是
Java/Spring/JPA/Maven/前端遊戲/團隊 repo 基礎設施專屬，對本專案無效：

| 原條目 | 處置 |
|---|---|
| #12 壓測數字不可虛構、co-located 污染 | ✅ **完整沿用** → 本檔 §2.7 |
| #24 消費端「非冪等累加」才需去重 | 🔶 **記著但暫不適用**（本服務是 best-effort 推播、無狀態累加；做 rank-service 時才用得到） |
| #30 `mem_limit` 與 `-Xmx` 成對 | 🔶 **概念轉移**：Go 沒有堆上限，但容器裡建議設 `GOMEMLIMIT` 略低於 `mem_limit`，避免 GC 來不及被 OOMKilled（Phase 4 處理） |
| 其餘 28 條 | ❌ 不適用（mvnw / H2 測試 / JPA 雙資料源 / wallet 帳務 / 遊戲數值四同步 / OAuth / ps1 BOM / gateway 限流 …） |

**真正被沿用的是「結構與紀律」**：必讀文件清單、已知地雷累積、CHANGELOG 單一真相、
ADR 記架構決策、提交前自查指令、以及「踩到就順手更新本檔」的習慣。
