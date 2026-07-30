# CHANGELOG

本專案所有「會影響行為」的變更都記在這裡（規則見 `AGENTS.md` §3）。
最新的在最上面。

---

## [feat] — 2026-07-30 — Phase 2 完成：Hub、訂閱、扇出、心跳與背壓

**Added**
- `internal/hub`：連線註冊表與扇出中心。
  - `Hub`：`sync.RWMutex` 保護三個索引（`sessions`、`destination → sessions`、`playerID → sessions`），
    對外只有 `Serve` / `Broadcast` / `PushToPlayer` / `Close`，完全不認識 HTTP 與 Kafka
  - `Session`：一條連線 = **read + write 兩個 goroutine + 一個緩衝 64 的 outbound channel**，
    自己保管 `subscription id → destination`
  - 處理 `SUBSCRIBE` / `UNSUBSCRIBE` / `DISCONNECT`，任何 client 帧帶 `receipt` 都回 `RECEIPT`
  - 產生 `MESSAGE` 帧（`subscription` / `message-id` / `destination` / `content-type` 齊全）
- 心跳：`negotiateHeartbeat` 依規格算出雙向間隔（伺服器宣告 `10000,10000`），
  write goroutine 用 ticker 送單一 EOL，read goroutine 依協商值設讀取逾時（寬容 2 倍）
- 背壓：扇出一律非阻塞送入，佇列滿即斷線，並計入新指標 `notify_outbound_dropped_total`
- `POST /debug/push`：手動灌訊息的除錯端點，由新環境變數 `DEBUG_PUSH`（預設 `true`）控制是否註冊
- 優雅關閉升級成兩段式：先對所有已握手連線送 STOMP `ERROR` 並等它們寫完（上限 3 秒），
  剩下還在握手的才硬關
- `stomp.ErrorFrame`：ERROR 帧的建構子從 `server` 移上來，`hub` 也要用
- 測試：`internal/hub/hub_test.go`（23 個案例，含慢速客戶端、心跳、讀取逾時、
  索引清理、併發壓力）、`internal/server/push_test.go`（端到端推播與關機道別）、
  `TestNegotiateHeartbeat` 表格測試、`TestDebugPush` 設定測試

**Changed**
- `CONNECTED` 的 `heart-beat` 從固定 `0,0` 改成伺服器實際能力 `10000,10000`
- **`CONNECTED` 改由 write goroutine 送出**（`SessionConfig.Greeting`），不再由 HTTP handler 直接寫
- `internal/server` 移除 Phase 1 的 `drain()` 佔位迴圈，握手後改交給 `hub.Serve`

**為什麼**
- **RWMutex 而不是中央 hub goroutine**：gorilla 官方 chat 範例用單一 goroutine 獨佔註冊表，
  心智模型乾淨但**所有廣播序列化通過同一個 goroutine**。推播服務是典型的讀多寫少
  （廣播 RLock 可並行、只有連線建立/斷開才 Lock），所以選 RWMutex。理由要寫進 ADR-002。
- **扇出絕不阻塞**：只要有一條寫成 `ch <- msg`，一個讀不動的 client 就會卡住整個廣播迴圈，
  拖垮所有人。`select` + `default`，滿了就踢 —— fail-fast 背壓。
  `TestSlowClientIsDroppedWithoutBlockingOthers` 同時驗證「慢的被踢掉」與「快的一則不漏」。
- **`CONNECTED` 改走 outbound channel**：原本由 handler goroutine 直接寫，於是
  「client 收到 CONNECTED」**早於**「session 登記進 hub」——中間那段空窗期的推播與
  關機通知都會漏掉這條連線。改成先 `add` 再 enqueue，順序就有保證了。
  附帶好處：整條連線從頭到尾只有 write goroutine 一個 writer，不必再論證特例。
  （這是寫 `TestGracefulShutdownSendsErrorFrame` 時才發現的真 bug，見 `AGENTS.md` §2.17。）
- **關機先送 `ERROR` 再關**：直接關的話 client 收到 1006（異常關閉），會判定成網路問題
  並進入指數退避的重連迴圈；收到 `ERROR` + 正常關閉帧則可以立刻重連。
  Phase 1 做不到是因為當時沒有 write goroutine。
- **不支援的指令（`SEND` / `ACK` / `BEGIN` …）靜默忽略而不回 `ERROR`**：Spring 的
  simple broker 收到沒有 `@MessageMapping` 的 `SEND` 只記一行 log，不會斷線。
  回 `ERROR` 會讓 Go 版拒絕 Java 版接受的 client，Phase 5 的契約測試就對不上。
- **`UNSUBSCRIBE` 不存在的 id 只記 log 不斷線**：React StrictMode 的 effect cleanup 會跑兩次，
  嚴格照規格回 `ERROR` 的話，開發模式下每次都斷線。
- **`maxSubscriptions = 32`**：前端只用 3 個 destination。沒有上限的話，一個 client
  不斷訂閱新 destination 就能把 hub 的 map 撐爆——不需要任何頻寬的記憶體耗盡攻擊。
  反過來**不做 destination 白名單**：Spring 對 `/topic` `/queue` 底下任何路徑都放行。
- **`DEBUG_PUSH` 預設 `true`**：Phase 3 之前沒有 Kafka，關掉服務就沒有訊息來源。
  ⚠️ 這個端點**沒有鑑權**，啟動時會印 WARN，接上 Kafka 或對外部署時必須設 `false`。

**如何驗證**
- `go vet ./...`、`golangci-lint run`（v2.12.2，0 issues）、`go build ./...` 全綠
- `go test -race -count=1 ./...` 全綠（hub / server / stomp / auth / config）
- 手動驗收：實際跑起 binary，用真 WebSocket client 走完
  健康檢查 → 兩個玩家握手（`heart-beat=10000,10000`）→ 各自訂閱私人頻道與 `/topic/rank`
  → 私人推播 `delivered=1` 只進 alice → 排行榜廣播 `delivered=2` 兩人都收到
  → bob 退訂後 `delivered=1` → `DISCONNECT` 拿到 `RECEIPT` 後連線關閉

---

## [feat] — 2026-07-30 — Phase 1 完成：設定、JWT 驗章、WebSocket 握手與觀測端點

**Added**
- `internal/config`：環境變數 → `Config`，缺必填或值不合法**一律拒絕啟動**。
  變數：`JWT_SECRET`（必填，≥ 32 bytes）、`PORT`（預設 8088）、`ALLOWED_ORIGINS`（預設 `*`）、`LOG_LEVEL`（預設 INFO）
- `internal/auth`：HS256 驗章 → playerID（JWT `sub`），含 `StripBearer`
- `internal/metrics`：自有 Prometheus registry、Go/process collector、`notify_ws_connections` gauge
- `internal/server`：`GET /ws` 的 WebSocket 升級 + STOMP `CONNECT` 握手、
  `GET /actuator/health`、`GET /metrics` 與 `GET /actuator/prometheus`（同一個 handler）
- `cmd/notify/main.go`：組裝 + `signal.NotifyContext` 優雅關閉（5 秒上限）
- 測試：`config_test.go`、`jwt_test.go`、`handshake_test.go`（純函式表格測試）、
  `server_test.go`（`httptest` + 真的 WebSocket 的整合測試，含 16 條併發連線）

**Changed**
- `go.mod` 的 go directive **1.24 → 1.25.0**。不是主動選的：`prometheus/client_golang v1.24.1`
  的 go directive 比 1.24 高，而 Go 的模組規則要求主模組不得低於任何依賴。
  影響：最低 toolchain 要求跟著提高。
- `AGENTS.md`：團隊 repo 路徑從不存在的 `H:\Lucky_Star_Casino` 改為
  `D:\Lucky_Star_Casino\Lucky_Star_Casino\`（雙層同名資料夾）；新增地雷 §2.9~§2.14；
  補 Windows 本機環境與 golangci-lint v2 安裝路徑
- `計畫說明書.md`：勾掉 Phase 1 的 DoD、§5.1 目錄樹補上 `internal/server/`、
  記下 Phase 1 與原計畫的差異

**為什麼**
- **`JWT_SECRET` 檢查 ≥ 32 bytes**：Java 版用 `Keys.hmacShaKeyFor()`，JJWT 對短於 256 bit
  的 key 丟 `WeakKeyException`，**服務根本起不來**。golang-jwt 不做這個檢查，
  不自己補就會出現「Go 版接受了 Java 版拒絕的設定」——而 Phase 5 的契約測試正是要證明兩版行為相同。
- **設定錯誤一次全部回報**（`errors.Join`）：Java 的 `${VAR:?...}` 只炸第一個缺的值，
  修一個、重啟、再炸下一個。一次講完，部署時少繞好幾圈。
- **`jwt.WithValidMethods` 是安全要求不是選配**：不帶等於接受 `alg: none` 與演算法混淆攻擊。
  已有測試把 `alg:none` 與「同 key 的 HS512」兩種攻擊釘住。
- **驗章失敗對外一律回 `Unauthorized`**：不區分「沒帶」/「過期」/「簽章錯」。
  能分辨「這個 token 曾經有效但過期了」對攻擊者是有用的情報。差異只寫進伺服器 log。
  有測試 `TestAuthFailuresAreIndistinguishable` 比對三種失敗的 ERROR 帧**位元組完全相同**。
- **`refresh` token 也接受**：Java 版 `PlayerJwtVerifier` 沒檢查 `type` claim。
  刻意照樣不檢查以維持契約等價（見 `AGENTS.md` 地雷 §2.14）。這是原版的弱點，
  要改**兩版要一起改**——不是這個 commit 該做的事。
- **`CONNECTED` 一律回 `heart-beat:0,0`**：心跳是 Phase 2。若照 client 要求回非零值，
  client 等不到心跳就自己斷線 → 症狀是「握手成功、幾秒後莫名斷線」，比誠實回 0,0 難查太多。
- **自己設 `CheckOrigin`**：gorilla 預設「同源才放行」，前端 `:5173` → 服務 `:8088` 會被
  403 擋掉，而瀏覽器只說 `WebSocket connection failed`。用 `ALLOWED_ORIGINS` 對齊
  Java 的 `setAllowedOriginPatterns("*")`。
- **`Server.Close()` 不是多餘的**：`http.Server.Shutdown` **不處理**被 Hijack 的連線
  （WebSocket 就是），少了它程序永遠關不掉。而 Close 裡只 `conn.Close()` 不寫入，
  因為那是別的 goroutine——gorilla 一條連線只允許一個 writer。
- **握手邏輯抽成純函式** `handshake()`：協議決策與 I/O 分離，版本協商與驗章不必開真的
  WebSocket 就測得到；整合測試只負責證明「接線正確」。
- **`internal/server` 是新加的套件**（原 §5.1 沒有）：路由與握手要有地方放，
  而 `internal/hub` 是 Phase 2 的東西，不該提前塞進去。

**如何驗證**（2026-07-30 實測）
- `go vet ./...` → 綠
- `go test -race ./...` → 全綠
  （`auth` 2.0s / `config` 2.5s / `server` 7.9s / `stomp` 2.0s）
- `golangci-lint run ./...`（v2.12.2）→ **0 issues**
- `go build ./...` → 綠
- 冒煙測試（跑實際 binary）：
  - 缺 `JWT_SECRET` → exit 1，`缺少必填的環境變數：JWT_SECRET`
  - `JWT_SECRET=short` → exit 1，`目前 5 bytes，至少要 32 bytes`
  - `PORT=notaport` → exit 1，`PORT="notaport" 不是整數`
  - `GET /actuator/health` → 200 `{"status":"UP"}`；`POST` 同路徑 → **405**（Go 1.22 方法路由）
  - `GET /metrics` 與 `GET /actuator/prometheus` → 皆 200，含 `notify_ws_connections`
  - 閒置 RSS `process_resident_memory_bytes` ≈ **11.5 MB**、`go_goroutines` = 9、binary 15.8 MB
    （Windows 原生、非容器，**只是早期參考值，不可寫進效能對照報告**——那份要用容器實測）

**已知未完成**（Phase 2 處理）
- `SUBSCRIBE` / `UNSUBSCRIBE` 尚未處理，握手後收到的帧會被讀掉丟棄
- 心跳收發未實作（故宣告 `0,0`）
- 握手逾時（10 秒）沒有自動化測試——要等 10 秒才觸發，等 Phase 2 把逾時改成可注入再補
- SIGTERM 的優雅關閉只驗到程式碼路徑；Windows 不好送 SIGTERM，
  「`Ctrl+C` 後 5 秒內乾淨結束」留到 Phase 4 在容器裡驗

---

## [feat] — 2026-07-29 — 專案初始化與 STOMP 1.2 帧編解碼

**Added**
- `go.mod`：module `github.com/AlexChang1999/lucky-star-notify-go`，Go 1.24+，目前零外部依賴
- `計畫說明書.md`：Phase 0~5 施工計畫、服務選型評估、驗證策略、風險與退場
- `AGENTS.md` / `CLAUDE.md`：開發約定與 AI 協作規範
- `internal/stomp/frame.go`：`Frame` / `Command` / `Headers` 型別與 `Marshal`
- `internal/stomp/codec.go`：串流式 `Decoder`、單訊息 `Unmarshal`、資源上限 `Limits`
- `internal/stomp/errors.go`：協議錯誤哨兵與 `ProtocolError`
- `internal/stomp/frame_test.go` / `codec_test.go`：表格驅動測試 + `FuzzReadFrame`

**為什麼**
- **自己實作 STOMP 而非找套件**：Go 生態只有 client 端實作。而且這正是本專案
  與「用 Gin 寫 CRUD」的差異所在，是履歷價值的來源。
- **`Headers` 用 slice 而非 map**：規格 §2.1 明訂重複 header「以第一個為準」，
  map 會把重複的蓋掉。這同時是個安全性質——補送第二個 `Authorization`
  蓋不掉第一個。
- **`Limits` 是安全邊界不是效能參數**：STOMP 的 body 預設讀到第一個 NULL 為止，
  不設上限的話，一個不送 NULL 的 client 就能耗盡伺服器記憶體。
- **CONNECT / CONNECTED 不轉義 header**：規格 §3.1 為相容 STOMP 1.0 的例外條款。
  做錯會與 `@stomp/stompjs` 握手失敗。
- **`Unmarshal` 遇到多個帧回報錯誤而非忽略**：靜默忽略會變成「偶爾漏訊息」
  這種極難查的 bug。
- **編解碼層不驗語意**（例如不擋「CONNECT 帶 body」）：分層乾淨，且讓測試
  能構造畸形帧。語意驗證屬於上層 handler。

**如何驗證**
- `go vet ./...`
- `go test -race ./internal/stomp/`
- `go test -fuzz=FuzzReadFrame -fuzztime=30s ./internal/stomp/`
