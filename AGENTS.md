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

8. **不動團隊 repo**：團隊 repo 對本專案是**唯讀參考**。
   實際路徑是 **`D:\Lucky_Star_Casino\Lucky_Star_Casino\`**（注意是**雙層**同名資料夾，
   `backend/` 在內層）。本檔原本寫 `H:\Lucky_Star_Casino`，那個磁碟機已經不存在了。
   需要驗證行為時可以在本機把它跑起來、可以改本機 `.env`，
   但**不提交任何 commit、不開 PR**。所有產出都留在本資料夾。
   關鍵檔案索引見 `計畫說明書.md` 附錄 A。

9. **Windows 上 `go test -race` 需要 C compiler**：`-race` 依賴 cgo，錯誤訊息是
   `go: -race requires cgo; enable cgo by setting CGO_ENABLED=1`——但**設了環境變數也沒用**，
   真正缺的是 gcc。解法：`winget install BrechtSanders.WinLibs.POSIX.UCRT`，
   然後把 `...\mingw64\bin` 加進 PATH。裝完 `-race` 就正常。
   （Linux/容器上不會遇到，所以 CI 沒事，只有本機開發卡住。）

10. **`gorilla/websocket` 的 `CheckOrigin` 預設是「同源才放行」**：
    不自己設的話，前端跑 `:5173`、服務跑 `:8088` 就是跨源，upgrade 直接回 **403**，
    而瀏覽器 console 只會說 `WebSocket connection failed`，**指不到真因**。
    Java 版是 `setAllowedOriginPatterns("*")`，Go 版用 `ALLOWED_ORIGINS` 對齊（預設 `*`）。
    已處理於 `internal/server.New`。

11. **`http.Server.Shutdown` 不會關閉 WebSocket 連線**：WebSocket 升級是靠 Hijack
    拿走底層 TCP，而 `Shutdown` 完全不追蹤被 Hijack 的連線——它會「立刻回報完成」，
    但那些 goroutine 還活著，程序永遠不會結束。**必須自己收**（見 `server.Server.Close`）。
    Phase 2 起是兩段式：先 `hub.Close` 對已握手的連線送 STOMP `ERROR`（走 outbound
    channel，所以不違反第 5 條）並等它們寫完，剩下還在握手中的才 `conn.Close()` 硬關。
    **硬關那一段仍然只能 `conn.Close()`，不能 `WriteMessage`**——那是別的 goroutine。

12. **STOMP over WebSocket 必須用 TextMessage**：送 `websocket.BinaryMessage` 時
    `@stomp/stompjs` 會**靜默忽略**，沒有錯誤訊息，只有「怎麼沒反應」（同第 3 條的症狀）。
    `internal/server.writeFrame` 已寫死 TextMessage，測試 `receive()` 也斷言了訊息型別。

13. **心跳沒實作完就不要在 CONNECTED 宣告非零的 `heart-beat`**：
    若照 client 要求回 `heart-beat:10000,10000` 但伺服器不送心跳，client 等不到就會自己斷線，
    症狀是「握手明明成功，幾秒後莫名斷線」。Phase 1 一律回 `0,0`；Phase 2 實作了收發之後
    才改成宣告 `10000,10000`。**改動 `server.serverHeartbeatSend` 前先確認
    `hub.Session.writeLoop` 的 ticker 還在送**——這兩處必須成對，拆開就是上面那個 bug。
    另外，協商公式是**取 max 不是 min**（`server→client = max(cy, sx)`），
    所以 client 要求更快的間隔是沒用的；照著 min 寫會宣告出做不到的頻率。

14. **Java 版 `PlayerJwtVerifier` 沒有檢查 `type` claim** → member-service 發的
    **refresh token 也能用來開 WS 連線**。Go 版**刻意照樣不檢查**，否則 Phase 5 的
    跨語言契約測試會對不上（Go 拒絕了 Java 接受的 token）。
    這是原版的一個小弱點（refresh token 存活期長很多），不是我們的 bug——
    要改的話**兩版要一起改**。已有測試把這個行為釘住：
    `internal/auth` 的「refresh token 也會通過（與 Java 版一致）」。

15. **握手回覆不能由 HTTP handler 直接寫出去**（Phase 2 踩到的真 bug）：
    原本流程是 `writeFrame(CONNECTED)` → `hub.Serve()`，於是「client 收到 CONNECTED」
    **早於**「session 登記進 hub」。中間那段空窗期裡，廣播與關機通知都會漏掉這條連線，
    而且**沒有任何錯誤訊息**——只會偶發地「某個剛連上的人沒收到」。
    正解是把 CONNECTED 交給 `SessionConfig.Greeting`，順序變成 `h.add` → enqueue，
    write goroutine 才送出。附帶好處：整條連線從頭到尾只有一個 writer，不再有特例要論證。
    **任何「先回覆、後註冊」的寫法都要用同樣的眼光檢查一次。**

16. **`time.After` 不能放在迴圈裡當共用逾時**：它回傳的 channel 只會收到**一次**值。
    在 `for { select { case <-ch: ...; case <-time.After(...) } }` 之外先算好一個
    `deadline := time.After(t)` 然後在迴圈裡 select 它 —— 第一個逾時的項目就把訊號吃掉，
    後面每一個都退化成**無上限等待**。用 `context.WithTimeout` 的 `ctx.Done()`：
    它關閉之後永遠可讀。`hub.Close` 就是這樣寫的。

17. **測試碼一樣受「一個 reader / 一個 writer」限制**：第 5 條講的是伺服器端，
    但 client 端的 `*websocket.Conn` 同樣如此。在測試裡「開一條 goroutine 一直
    `ReadMessage`」＋「主 goroutine 呼叫會讀取的 helper（例如等 RECEIPT）」
    就是兩個 reader，`-race` 會直接抓到。要嘛只留一個 reader，
    要嘛用不需要回覆的指令（不帶 `receipt`）。

18. **`game.result` 推出去的 body 與事件本身「不同形狀」**：Java 版是另外組一個
    `LinkedHashMap`——`type` 放**第一個**、而且**不含 `playerId`**（那是路由用的，
    不是內容）。把事件原樣轉推是最直覺也最容易犯的錯，前端拿不到 `type` 就會
    走進「系統通知」分支，**而且沒有任何錯誤訊息**。
    另兩個 topic（`notification.push` / `rank.update`）反而是原事件回吐。
    三者不一致，動 `internal/consumer/events.go` 前先看那裡的註解。

19. **`json.Marshal` 預設會做 HTML 轉義，Jackson 不會**：`<` `>` `&` 會被換成
    Unicode 轉義序列。兩邊 `JSON.parse` 之後相同，但位元組不同——本專案主打
    跨語言等價，所以一律走 `marshalCompatible`（`Encoder` + `SetEscapeHTML(false)`）。
    ⚠️ `Encoder.Encode` 會在結尾**多補一個 `\n`**，一定要 `TrimSuffix` 砍掉。
    另一個已知且刻意不處理的差異：Jackson 會把字串 `"42"` **強制轉型**成 `Long`，
    Go 會直接解析失敗並丟棄。現行 producer 都送數字，所以不值得自訂
    `UnmarshalJSON`——但有測試把這個差異釘住，別以為是 bug。

20. **消費端不論成功失敗都要 commit offset**：對齊 Java 的
    `finally { ack.acknowledge(); }`。不 commit 只會讓 offset 卡在壞訊息上，
    **一則壞訊息就能讓整個 topic 停擺**，而外部看起來只是「推播突然沒了」。
    另外 `CommitMessages` 要用 `context.WithoutCancel(ctx)` 再加逾時：關機時
    root ctx 已被取消，直接傳下去會讓最後一則的 offset 提交失敗、下次重播。

21. **`.gitattributes` 的 LF 正規化會把 `*.patch` 弄壞**：`docs/*.patch` 是給
    團隊 repo（Windows checkout、CRLF）套用的，hunk 內容行帶著 CR。被正規化掉
    之後 `git apply` 只會說 `patch does not apply`，**完全指不到「行尾被改了」
    這個真因**。已用 `*.patch -text` 排除。

22. **Windows 使用者名稱含空白時，WinLibs 的 gcc 完全不能連結**（換機器踩到的）：
    winget 預設把 WinLibs 裝在 `%LOCALAPPDATA%\...`，若使用者名稱是
    `Alex Chang` 這種含空白的，gcc 會把自己的安裝前綴解析成長路徑，然後把
    `crt2.o` / `crtbegin.o` / `-L...` **不加引號**丟給 collect2，ld 就在空白處
    斷成兩截。錯誤訊息是
    `C:/Users/Alex: file not recognized: file format not recognized`——
    **看起來像目的檔壞掉，其實是路徑被切開**。連
    `int main(){return 0;}` 都連結不了，所以 `go test -race ./...` 全滅。
    兩個直覺解法**都無效**：
    - 用 8.3 短檔名（`C:\Users\ALEXCH~1\...`）放進 PATH → gcc 用
      `GetModuleFileName` 拿到的還是長路徑
    - 做一個 `C:\mingw64` junction 指過去 → 一樣會被還原成真實路徑

    唯一解是**讓工具鏈實體位於無空白的路徑**：
    ```powershell
    winget uninstall -e --id BrechtSanders.WinLibs.POSIX.UCRT
    winget install -e --id BrechtSanders.WinLibs.POSIX.UCRT --location "C:\toolchains\winlibs"
    ```
    ⚠️ 移除 junction 時**不可以** `Remove-Item -Recurse -Force`——PowerShell 5.1
    會跟著 junction 進去把**目標內容**刪掉。要用 `cmd /c rmdir <link>`，
    它只拆 reparse point。
    （Go 本身裝在 `C:\Program Files\Go` 也有空白，但沒事——Go 自己會正確引號。）

23. **⭐ consumer 比 topic 早啟動 → 它會永遠收不到訊息**（Phase 4 實測到的真缺陷）：
    kafka-go 的 `WatchPartitionChanges` **預設是 false**。加入 group 那一刻
    topic 還不存在（例如 topic 靠 producer 自動建立，而 producer 比較晚起），
    這個 member 就被分配到 **0 個 partition**，而且之後**沒有任何事件會觸發
    rebalance**，於是一直維持那個狀態。
    症狀最惡劣的地方是**每一項檢查都顯示正常**：healthcheck 過、日誌無錯誤、
    `kafka-consumer-groups --list` 也看得到 group——但 `--describe` 是空的，
    `notify_kafka_messages_total` 這個 CounterVec 因為從沒被 Inc 過而**整個
    不出現在 /metrics 裡**。
    Java 版沒這問題：Spring Kafka 靠 `metadata.max.age.ms`（預設 5 分鐘）
    定期刷新中繼資料並重新訂閱，最多五分鐘會自己好。
    已修：`internal/consumer.New` 設 `WatchPartitionChanges: true` +
    `PartitionWatchInterval: 5s`。**動那兩行前先讀這一條。**

24. **關機時間 ≈ topic 數 × 3 秒，且改不掉**：三個 topic 是**同一個 consumer
    group 的三個 member**，coordinator 一次只處理一個 `LeaveGroup`、每次跑一輪
    rebalance，所以它們是排隊離開的（即使我們這邊三條 goroutine 是並行關的）。
    實測收工時間恰好每 **3.000 秒**一個，牆鐘 6.5~8.7 秒。
    ⚠️ **試過的無效解法**：懷疑是 kafka-go 的 `HeartbeatInterval`（預設剛好
    3 秒），改成 1 秒後**間隔仍是 3.000 秒**，假設不成立，已還原。別再試一次。
    所以 `shutdownTimeout` 是 10 秒（原訂 5 秒不夠），compose 要配
    `stop_grace_period: 15s`（**Docker 預設也是 10 秒，剛好卡在邊界，必須明寫**，
    否則優雅關閉做到一半被 SIGKILL，結束碼變 137）。
    **加第四個 topic 之前，先把這兩個數字一起調大。**

25. **demo 的觀測性埠會撞到團隊 repo**：團隊那套的 `lucky-star-prometheus`
    占 9090、`lucky-star-grafana` 占 3000。同時跑會拿到
    `Bind for 0.0.0.0:9090 failed: port is already allocated`。
    本專案一律往後挪一號：**Prometheus 9091、Grafana 3001**
    （與服務用 8088、Java 版 8087 是同一個並存理由）。容器內的埠不變，
    所以 `prometheus.yml` 與 Grafana 的資料來源都不用改。

26. **Grafana 不要掛整個 `provisioning/` 目錄**：那會把映像內建的
    `alerting/` `plugins/` `notifiers/` 一起遮掉，啟動時噴
    `level=error msg="can't read alerting provisioning files"`。
    沒有實際影響，但日誌裡一行紅字會讓人以為設定壞了。
    只掛真的有提供的子目錄（`datasources/`、`dashboards/`）就乾淨了。
    另外 datasource **一定要明寫 `uid`**——dashboard JSON 是用 uid 綁資料來源的，
    不給的話 Grafana 隨機產生，每個面板都會顯示 "Datasource not found"。

27. **背壓測試不能「先灌滿再讀」——正常連線會被自己的測試踢掉**：
    寫「主 goroutine 用緊迴圈連發 `outboundBuffer + N` 則、另一條 goroutine
    負責讀」看起來很自然，但**正常連線的緩衝跟慢速連線一樣只有 64 格**。
    只要 write goroutine 加上真實 WebSocket 的來回追不上那個沒有節流的迴圈，
    正常連線就會被**同一套背壓政策**踢掉，症狀是它自己收到
    `close 1006 (abnormal closure)`——**看起來像扇出壞了，其實是測試造成的**。
    本機（核心多）會過，CI 會掛（runner 核心少，又有 `t.Parallel()` 的其他
    測試在搶 CPU）。正解是**廣播與收訊交錯**，一則一則跟上：
    正常連線的在途訊息永遠不超過 1，只有慢速連線在累積，差別因此是確定性的。
    本機要重現 CI 的條件，用 **`go test -cpu=1`**。

28. **`t.Fatalf` 只能在測試自己那條 goroutine 上呼叫**：從別的 goroutine 呼叫
    不會中止測試，只會標記失敗然後**繼續往下跑**——於是同一個問題會印出
    兩份互相矛盾的錯誤訊息（第 27 條那次就是「讀取失敗：1006」加上
    「收訊逾時，扇出很可能被卡住」，後者其實是前者的後果）。
    在 goroutine 裡要嘛只用 `t.Errorf` 後 `return`，要嘛把錯誤用 channel
    送回主 goroutine。最簡單的解法通常是**根本不要開那條 goroutine**。

---

## 3. 約定速查

### 技術棧
- **Go 1.25+**，module `github.com/AlexChang1999/lucky-star-notify-go`
  （原訂 1.24，被 `prometheus/client_golang` 的 go directive 推上去，見地雷 §2 開頭的說明與 `go.mod` 註解）
- **STOMP 協議層＝標準庫自幹**（`internal/stomp`），這是專案價值所在
- 已引入（Phase 1）：`gorilla/websocket`、`golang-jwt/jwt/v5`、`prometheus/client_golang`、
  `log/slog`（標準庫，**不用第三方 logger**）
- 已引入（Phase 3）：`segmentio/kafka-go`（**純 Go，可 `CGO_ENABLED=0` 靜態編譯**）
- Phase 4 沒有新增任何 Go 依賴（容器化與 CI 都不需要）
- **對外埠一覽**（全部刻意與團隊 repo 錯開，兩套才能同時跑做 A/B）：

  | 埠 | 用途 | 團隊 repo 的對應 |
  |---|---|---|
  | **8088** | notify-go | Java 版 8087 |
  | **8090** | demo 頁面 + dev token 端點（`cmd/demo`，**不是服務的一部分**） | — |
  | **9091** | Prometheus（`observability` profile） | 9090 |
  | **3001** | Grafana（`observability` profile） | 3000 |
- 對外端點：`/ws`（WebSocket）、`/actuator/health`、`/metrics` 與 `/actuator/prometheus`（同一個 handler 註冊兩次，方便接團隊的 Prometheus）、
  `POST /debug/push`（**只在 `DEBUG_PUSH=true` 時註冊**，關掉時是 404 不是 403）
- 環境變數：`JWT_SECRET`（**必填**，≥ 32 bytes，原始字串不是 base64）、
  `PORT`（預設 8088）、`ALLOWED_ORIGINS`（逗號分隔，預設 `*`）、`LOG_LEVEL`（預設 INFO）、
  `DEBUG_PUSH`（預設 `true`）、
  `KAFKA_BOOTSTRAP_SERVERS`（逗號分隔，預設 `localhost:9092`，
  **明確給空字串＝不啟動 consumer**，適合只想跑 WebSocket 那一半的本機開發）、
  `KAFKA_GROUP_ID`（預設 `notification-service-go-group`，見地雷 §2.4）。
  缺必填或值不合法**一律拒絕啟動**，且一次回報所有問題（`errors.Join`）。
  ⚠️ `DEBUG_PUSH` 開著的端點**沒有鑑權**，任何連得到本服務的人都能對任意玩家推任意內容。
  預設 `true` 只是因為 Phase 3 之前沒有 Kafka、關掉就沒有訊息來源；
  **接上 Kafka 或任何對外部署都必須設 `false`**。啟動時會印一行 WARN。
- 自訂指標：`notify_ws_connections`（Gauge）、`notify_outbound_dropped_total`（Counter）。
  `notify_kafka_messages_total{topic,result}`（Counter，result 分
  `ok` / `dropped` / `skipped`——**dropped 是上游 bug、skipped 是上游的正常變異，
  混成一種就看不出差別**）、`notify_push_latency_seconds`（Histogram，
  量的是「訊息時間戳 → 進 outbound channel」，含 producer 端的時鐘偏移）。

### 目錄
`internal/` 底下是實作細節（外部 module 無法 import，比 Java 的 package-private 更硬）。
完整規劃見 `計畫說明書.md` §5.1。**空目錄 git 不會追蹤**，所以只有裝了程式碼的目錄會進 repo。

### 容器與部署（Phase 4）
- **兩個執行檔、兩個映像，共用同一份 `Dockerfile`**（`ARG CMD`，預設 `notify`）：
  - `cmd/notify` → 正式服務。`docker build .` 蓋出來的就是它。
  - `cmd/demo` → **開發用**假上游：產生假事件、服務 demo 頁、**簽發 JWT**。
    ⚠️ 簽章能力只能存在於這裡。正式映像裡沒有這段程式碼，就不可能被誤開。
- base image 是 **`scratch`**，不是計畫書原訂的 distroless（那個實測 23.7 MB
  超過 20 MB 的 DoD；差額幾乎全是用不到的 tzdata）。改用 scratch 就得自己補回：
  CA 憑證（從建置階段 COPY）、**數字 UID `65532:65532`**（沒有 /etc/passwd
  就不能寫 `USER nonroot`，寫了容器起不來）。時區資料刻意不補。
- 沒有 shell 的兩個後果：`docker exec ... sh` 進不去；`HEALTHCHECK CMD curl`
  用不了——所以探針是執行檔自己的 **`/app -healthcheck`** 旗標
  （compose 用 `test: ["CMD", "/app", "-healthcheck"]`，**不能用 CMD-SHELL**）。
- 實測數字（2026-07-30，第二台機器）：映像 **18.6 MB**（`docker images`）／
  5.29 MB（pull 下載量）；服務啟動 **230 ms**；閒置記憶體 **12.9 MiB**／128 MiB 上限；
  p95 推播延遲 **4.75 ms**。
- `GOMEMLIMIT` 要與容器記憶體上限**成對設定**且略低於它（現為 100MiB／128M）——
  這是 Java 那條「`mem_limit` 與 `-Xmx` 要成對」在 Go 的對應物。

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

**跑起來看（Phase 4）**：

```bash
# 整組 demo：Kafka + notify-go + 假上游/頁面。--wait 會等到全部 healthy 才返回
docker compose -f deploy/docker-compose.yml up --build -d --wait
#   → 打開 http://localhost:8090

# 加上 Prometheus(9091) 與 Grafana(3001)。預設不啟動，因為映像比整組 demo 還大
docker compose -f deploy/docker-compose.yml --profile observability up -d

# 收工。⚠️ 開過 observability 的話，down 也要帶同一個 --profile，
# 否則那兩個容器會留著，網路刪不掉（"Resource is still in use"）
docker compose -f deploy/docker-compose.yml --profile observability down -v
```

**看服務有沒有真的在消費**：`notify_kafka_messages_total` 是 CounterVec，
**從沒被 Inc 過就整個不會出現在 /metrics 裡**——「grep 不到」等於「一則都沒收到」，
不是「指標壞了」。這是判斷地雷 §2.23 有沒有復發的最快方法。

```bash
curl -s localhost:8088/actuator/prometheus | grep notify_kafka_messages_total
```

**規則**：
- **`-race` 是本專案的硬性要求，不是選配**。這是一個併發服務，沒有競態偵測器的測試等於沒測。
  （Java 沒有等價工具，這點值得在 README 講。）
- 模糊測試**找到的失敗案例會被自動寫進 `testdata/fuzz/`**——
  有價值的案例要**手動搬進版控**當回歸案例，別讓它被 `.gitignore` 吃掉。
- `golangci-lint` 安裝（**v2 的路徑多了 `/v2`**，用舊路徑會裝到 v1.64）：
  `go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest`
  裝好在 `%USERPROFILE%\go\bin`，記得那個目錄要在 PATH 裡。

**Windows 本機環境**：
> ⚠️ 這一節**跟機器綁定**。已經在兩台上跑過，路徑不一樣，所以下面列的是
> 「怎麼在新機器上把它裝起來」而不是某一台的絕對路徑。

- 新機器一次裝好（實測 2026-07-30，第二台）：
  ```powershell
  winget install -e --id GoLang.Go
  # gcc 的安裝位置「絕對不能含空白」，見地雷 §2.22
  winget install -e --id BrechtSanders.WinLibs.POSIX.UCRT --location "C:\toolchains\winlibs"
  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
  ```
- Go 與 gcc 都**不在**預設 PATH（而且 Claude Code 起的子行程拿到的是舊 PATH
  快照，安裝後也不會自動更新），跑指令前要自己補：
  ```powershell
  $env:PATH = "C:\Program Files\Go\bin;C:\toolchains\winlibs\mingw64\bin;$env:USERPROFILE\go\bin;$env:PATH"
  ```
  在 **Git Bash** 裡不能直接沿用上面那串（反斜線路徑塞進 bash 的 `PATH` 找不到
  gcc，錯誤訊息還是那句 `-race requires cgo`，指不到真因，見地雷 §2.9）。
  要用全正斜線的形式，`<user>` 換成實際使用者資料夾名：
  ```bash
  export PATH="/c/Program Files/Go/bin:/c/toolchains/winlibs/mingw64/bin:/c/Users/<user>/go/bin:$PATH"
  ```
- gcc 是 `-race` 的前提，見地雷 §2.9；它裝在哪更關鍵，見地雷 §2.22。
- ⚠️ 用 PowerShell 的 `Get-Content` 看含中文的檔案會顯示成亂碼（編碼判斷錯誤），
  那是**顯示問題不是檔案壞了**。要確認內容請用編輯器或 `Get-Content -Encoding utf8`。
- ⚠️ 對原生 exe 做 `2>&1` 重導向，PowerShell 5.1 會把 stderr 每一行包成 ErrorRecord
  並讓 `$?` 變 `$false`，即使程式回 0。跑服務時**不要重導 stderr**。

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
