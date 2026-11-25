# CHANGELOG

本專案所有「會影響行為」的變更都記在這裡（規則見 `AGENTS.md` §3）。
最新的在最上面。

---

## [feat] — 2026-07-31 — Phase 5：跨語言契約測試與作品集 README

Phase 5 的契約測試、ADR 與 README 完成。

**Added**
- **`test/contract/`——跨語言黑箱契約測試**。Node.js + `@stomp/stompjs` +
  `kafkajs`，14 個案例，只吃 `WS_TARGET` / `KAFKA_BROKER` / `JWT_SECRET`
  三個環境變數，**測試碼裡沒有任何一行認得出對面是 Java 還是 Go**。
  刻意用第三方 client 而不是自己寫：用自家 client 測自家 server，證明的只是
  「我的兩份實作互相相容」，用前端實際在用的那一支才是「與規格相容」。
  斷言涵蓋幾個**天生沒有錯誤訊息**的差異：`game.result` 的推播不含
  `playerId` 且 `type` 在第一個、`null` 欄位是「存在且為 null」而不是消失、
  MESSAGE 帧必帶 `subscription` 與 `message-id`、JSON 不做 HTML 轉義、
  以及 `alg: none` 等五種爛憑證都要回 ERROR 並斷線。
- **CI 多一步：契約測試（Go 版）** 進 `compose 煙霧測試` job。
  Java 版那一組進不了 CI（需要團隊 repo 的原始碼），但把 Go 這一側釘住，
  等價性至少不會**無聲**失效。
- demo 的 Kafka 多開一個 `HOST` listener（宿主 **9094**，與團隊的 9092 錯開），
  契約測試才能從宿主發事件。

**Fixed**
- **`cmd/demo` 每一則事件都被 producer 端壓了整整一秒才送出**。
  `kafka-go` 的 `Writer.BatchTimeout` 預設是 1 秒、`BatchSize` 預設 100，
  而這個產生器一次只寫一則。demo 頁上的「即時推播」因此無端慢一秒，
  **日誌與指標都看不出來**——`notify_push_latency_seconds` 量的是 Kafka
  訊息時間戳之後的事，那一秒發生在它之前。設 `BatchSize: 1` 修掉（地雷 §2.29）。

**Changed**
- README 依計畫書 §12 的順序重寫成作品集門面。**沒有做 demo GIF**——
  `docker compose up` 一行指令比一張 GIF 更能證明東西真的跑得起來。
- `AGENTS.md` 新增地雷 §2.29~§2.32。

**為什麼**
「我用 Go 重寫了一個服務」這句話本身沒有說服力，因為無法查證。
Phase 5 做的是**把主張換成證據**：等價性換成一份兩邊都跑得過的測試。

**如何驗證**
- 契約測試：Go 版 **14/14**、Java 版 **14/14**，而且 Java 版是**第一次跑就過**，
  沒有為了配合它改過任何斷言
- `gofmt -l` 無輸出、`go vet`、`golangci-lint`（0 issues）、`go test -race ./...` 全綠

**踩到並記進 AGENTS.md 的四個新雷**
1. §2.29 `kafka-go` 的 `BatchTimeout` 預設 1 秒（上面那個 demo bug 的成因）
2. §2.30 Docker Desktop 上大量連線的壓測不要從宿主打：宿主埠轉發層可能成為瓶頸
3. §2.31 `docker rm -f` 殺 consumer 會留幽靈 member（§2.4 的變形），
   下一個容器可能分不到 partition
4. §2.32 `GOMEMLIMIT` 是**軟上限**：貼上去不會 OOM，但 GC 會更積極、延遲變差

---

## [chore] — 2026-07-31 — 從團隊 repo 移植 subagent，6 個只留 2 個

團隊 repo `H:\Lucky_Star_Casino\.claude\agents\` 有 6 個角色定義。
逐份讀完後只移植 2 個，**且內容全部重寫**。

**Added**
- `.claude/agents/go-reviewer.md`——唯讀審查者（白名單刻意沒有 Edit/Write）。
  審查優先序按本專案的實際風險排：① 併發正確性（§2.5/2.6/2.11/2.15/2.16）
  ② **靜默失敗類**（§2.3/2.12/2.13/2.18/2.23——本專案的招牌 bug 類型，
  沒有錯誤訊息只有「怎麼沒反應」，讀 code 找例外是抓不到的）
  ③ 安全與跨語言等價 ④ 協議正確性 ⑤ 可維護性。
  另外明列**「不准報成 bug 的刻意設計」**：JWT 不檢查 `type` claim（§2.14）、
  不複製 Jackson 的字串轉型（§2.19）、關機每 topic 3 秒（§2.24，且
  `HeartbeatInterval` 已試過無效、禁止再提）、錯開的埠號（§2.25）、
  不准建議加 interface/factory（CLAUDE.md §2）。誤報這些會浪費時間又損信任。
- `.claude/agents/go-tester.md`——只碰測試檔、發現產品 bug 只回報不修、不 commit。
  含測試端專屬地雷（§2.17 client 也是一 reader、§2.27 背壓測試要交錯、
  §2.28 `t.Fatalf` 只能在測試 goroutine），以及 Phase 5 兩項未完成 DoD 的作法。
  特別指明 `test/contract/harness.js`（336 行）**已經存在且 export 齊全**，
  要 import 它而不是另建一套。
- `.claude/agents/README.md`——角色表、觸發詞表、流程、成本警告與逐項移植決策。

**為什麼只留 2 個**
那 6 份的 body 幾乎全是領域知識（Maven / JPA 雙資料源 / wallet 帳務 / RTP band /
PixiJS / 三條下注鐵則），對本專案 100% 無效；可轉移的只有**結構紀律**。
`dev-coder` 不留是因為 8,507 行、7 個 `internal/` 套件的規模，主執行緒直接做比
冷啟動快；`devops` 不留除了 Phase 4 已完成，更因為那份寫著
「never touch `main`: protected branch」，**與 AGENTS.md §3 的單人開發約定直接衝突**，
照抄進來就是兩份打架的指令；`frontend-dev` / `ui-ux` 沒有對應領域。
這與 `AGENTS.md §6` 對團隊 31 條地雷的判斷是同一個模式：能轉移的是紀律不是內容。

**Changed**
- `AGENTS.md` §3 新增「Subagent」小節：角色表 + **冷啟動成本警告**
  （subagent 會自己重讀 AGENTS.md 28.9 KB + CLAUDE.md，每次約 9~10k token，
  所以小改動一律主執行緒做）+ 地雷知識只維護在 §2 不要複製進 agent 檔案。

**Fixed**
- `AGENTS.md` §2.8 團隊 repo 路徑**寫錯**。原文說是雙層的
  `D:\Lucky_Star_Casino\Lucky_Star_Casino\`、且「H: 磁碟機已經不存在」。
  2026-07-31 實測相反：`H:\Lucky_Star_Casino\` 存在且是完整 git repo（HEAD `c427943`），
  `D:\Lucky_Star_Casino\` 整個不存在。**這一行已經被改錯過兩次**，所以這次不只改值，
  還加上「換機器路徑就會變，開工前先 `ls` 確認，別相信本行寫死的值」。

**如何驗證**
- 7 份團隊 agent 檔案逐份讀完後才做取捨，非抽樣
- 路徑以 `ls` + `git log` 實測三個候選路徑確認（H: 單層有、D: 兩種都沒有）
- `test/contract/harness.js` 的 exports 以 grep 核對過，agent 檔案裡列的
  `mintToken` / `connect` / `connectExpectingRejection` / `createProducer` / `send` 均為實際存在
- ⚠️ **agent 註冊本身尚未驗證**——Claude Code 要**重開 session** 才會載入新的
  agent 定義，本次 session 內無法確認。重開後用 `/agents` 檢查

## [feat] — 2026-07-31 — 跨語言契約測試完成：Java 版與 Go 版各 14/14 全綠

Phase 5 最核心的 DoD 落地。任何人都能說「我用 Go 重寫了一個服務」，
這一筆是**證明重寫出來的東西行為一樣**——而且是有證據的一樣。

**Added**
- `test/contract/contract.test.js`（312 行、**14 個案例**：9 個頂層 +
  5 個迴圈產生的鑑權案例）。Node.js + `@stomp/stompjs` + `kafkajs`，
  只吃 `WS_TARGET` / `KAFKA_BROKER` / `JWT_SECRET` 三個環境變數，
  **測試碼裡沒有任何一行認得出對面是 Java 還是 Go**——只要出現一句
  「如果是 Go 就……」，這份測試的證明力就沒了。
  刻意不用 Go 寫：用 Go 的 client 測 Go 的 server 只能證明「我的兩份實作
  互相相容」，用業界標準 client 測，證明的才是**與規格相容**。
  涵蓋的重點是幾個**天生沒有錯誤訊息**的差異：`game.result` 推播不含
  `playerId` 且 `type` 在第一個、`null` 欄位是「存在且為 null」而不是消失、
  MESSAGE 帧一定帶 `subscription` 與 `message-id`、JSON 不做 HTML 轉義
  （逐位元組比對，因為 `JSON.parse` 之後兩種寫法完全相同、抓不到）、
  以及 `alg: none` 等五種爛憑證都必須回 ERROR **並斷線**。
- `test/contract/harness.js`（336 行）與 `README.md`：共用底座，
  export `mintToken` / `connect` / `connectExpectingRejection` /
  `createProducer` / `send` 與各 `DEST_*`、`TOPIC_*` 常數。
- CI（`compose 煙霧測試` job）新增 Go 版契約測試步驟，釘住等價性不會
  因為之後某次改動而**無聲失效**。刻意排在「確認真的消費到 Kafka 訊息」
  **之後**——那一步是 consumer 已完成 rebalance 的證據，少了它，契約測試
  會在冷啟動期間逾時，而失敗訊息會指向推播壞掉，完全查錯方向。
  Java 版那一組沒進 CI：那需要團隊 repo 的原始碼，本 repo 對它唯讀（§2.8）。
- Kafka 新增 `HOST` listener（容器內 9094，對外 `9094:9094`），
  因為契約測試與壓測都從**宿主**發事件。
  advertised listener 變成兩條：`PLAINTEXT://kafka:9092` 給 compose 網路內的
  notify-go 與 demo、`HOST://localhost:9094` 給宿主上的 client。
  **只有一條時，宿主的 client bootstrap 連得上、但拿到的位址是 `kafka:9092`，
  之後每個請求都逾時，錯誤訊息完全指不到這裡**；反過來只寫 localhost，
  則換成容器內的 client 去連自己的 127.0.0.1。
- `test/contract/harness.js`（336 行）與 `package.json`：契約測試的共用底座，
  export `mintToken` / `connect` / `connectExpectingRejection` / `createProducer` / `send`
  與各 `DEST_*`、`TOPIC_*` 常數。目標由 `WS_TARGET` 切換，
  Go 版走 `localhost:9094`、Java 版走團隊那套的 `localhost:9092`。

**Changed**
- `KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT` 必須明寫——有兩個 PLAINTEXT
  安全協定的 listener 時 broker 無法自己判斷該用哪個，**不寫會啟動失敗**
  （單節點其實用不到 broker 間通訊）。
- 埠選 **9094 而不是 9092**：團隊 repo 的 `lucky-star-kafka` 占著宿主 9092，
  兩套要能同時跑才做得成 A/B 對照（同 8088 vs 8087 的理由）。
  已補進 `AGENTS.md` §3 的對外埠一覽表。
- `.gitattributes` 把 `*.js` 也納入 LF 正規化（契約測試是 Node.js 寫的）。
- `.gitignore` 忽略 `/test/contract/node_modules/`，但
  **`package-lock.json` 要進版控**——契約測試的價值來自可重現，
  「`@stomp/stompjs` 換版本後行為變了」正是最不該混進來的變數。
- `計畫說明書.md` §6 勾掉 Phase 5 第一項 DoD。

**如何驗證**
- 2026-07-31 實測：同一份測試對 **Java 版（8087）與 Go 版（8088）各 14/14 全綠**。
- **Java 版是第一次跑就過**，沒有為了配合它改過任何一條斷言。這正是
  `計畫說明書.md` §7 規定「先對 Java 版跑通再寫 Go 版」那個順序的回報：
  測試本身的正確性先被驗證過，之後 Go 版若失敗就能確定是 Go 寫錯了。
- 跑法：`cd test/contract && npm ci && CONTRACT_TARGET=go npm test`
  （Java 版把 `CONTRACT_TARGET` 換成 `java`）。

## [fix] — 2026-07-30 — CI 第一次實跑抓到的三件事

CI 在 PR #2 上第一次執行。`建置映像` 過（含映像大小斷言），
另外兩個 job 掛掉——**兩個都是本機不會出現的問題**，正好是設 CI 的理由。

**Fixed**
- `TestSlowClientIsDroppedWithoutBlockingOthers` **本機過、CI 掛**。
  原本的寫法是「主 goroutine 用緊迴圈連發 74 則、另一條 goroutine 讀」，
  但**正常連線的緩衝同樣只有 64 格**。只要 write goroutine 加上真實
  WebSocket 的來回追不上那個沒有節流的緊迴圈，正常連線就會被**同一套背壓
  政策**踢掉，症狀是它自己拿到 1006——看起來像扇出壞了，其實是測試自己
  造成的。改成廣播與收訊**交錯**（一則一則跟上），在途訊息永遠不超過 1，
  差別因此是確定性的。順帶把讀取移回主 goroutine：`t.Fatalf` 的語意才正確
  （從別的 goroutine 呼叫不會真的中止測試，只會讓失敗訊息變成兩份，
  這次就是這樣），而且整條連線只有一個 reader（`AGENTS.md` §2.17）。
- 煙霧測試的 `sleep 20` 太短，改成**有上限的輪詢**（90 秒）。
  冷啟動不只是「等 demo 發第一則」：consumer 還要等 partition watcher
  （5 秒）偵測到 topic 才會 rebalance（約 3 秒）。固定 sleep 抓太短會假紅，
  抓太長則每次 CI 都白等。

**Changed**
- `cmd/demo` 啟動時**先把三個 topic 建好**（`ensureTopics`，best-effort，
  失敗只記 WARN）。原本靠 `AllowAutoTopicCreation`，走的是
  「第一次寫入失敗 → 退避 2 秒 → 再試」的路徑。
  冷啟動後第一則被消費的等待時間因此縮短，
  順便消掉啟動時那行看起來很像壞掉的 `寫入 Kafka 失敗` WARN。

**如何驗證**
- `go test -race -count=5 -run TestSlowClient... ./internal/hub/` 全過；
  再用 `-cpu=1 -count=3` 跑整個 hub 套件（模擬 CI 的低核心環境）也全過
- `docker compose down -v` 後重新 up，輪詢確認第一則訊息有被消費
- `gofmt -l` 無輸出、`go vet`、`golangci-lint`（0 issues）、
  `go test -race ./...` 全綠

---

## [feat] — 2026-07-30 — Phase 4：容器化、可獨立運行的 demo、觀測性與 CI

**Added**
- `Dockerfile`（multi-stage，`ARG CMD` 讓兩個執行檔共用一份）與 `.dockerignore`
- `cmd/demo`：compose 展示用的**假上游服務**——定期產生三種假事件灌進 Kafka、
  用 `go:embed` 提供一頁 demo HTML、簽發玩家 token。
  ⚠️ 簽章能力**只存在於這個執行檔**，正式映像完全不含這段程式碼
- `cmd/notify` 多一個 `-healthcheck` 旗標：打自己的 `/actuator/health`，
  正常回 0。給 scratch 映像的容器探針用（映像裡沒有 curl 也沒有 shell）
- `deploy/docker-compose.yml`：Kafka（單節點 KRaft）+ notify-go + demo，
  另有 `observability` profile（Prometheus 9091 / Grafana 3001，預設不啟動）
- `deploy/docker-compose.team.yml`：只起 notify-go，接團隊 repo 的 `lucky-network`
- `deploy/prometheus.yml`、`deploy/grafana/`（資料來源與 dashboard 都用
  provisioning 自動載入，不必手動匯入）、`.env.example`
- `.github/workflows/ci.yml`：gofmt / go vet / golangci-lint / `go test -race` /
  docker build ×2 / **映像大小上限斷言** / compose 端到端煙霧測試
- 測試：`cmd/demo` 的 `events_test.go`（逐位元組釘住四條推播路徑的事件形狀）、
  `token_test.go`（用**服務端真正在用的** `auth.Verifier` 驗簽，
  而不是自己再解一次——後者是同義反覆）

**Fixed**
- ⭐ **consumer 比 topic 早啟動就永遠收不到訊息**（Phase 3 遺留的真實缺陷，
  Phase 4 實測到）。kafka-go 的 `WatchPartitionChanges` 預設 false，
  加入 group 時 topic 不存在 → 分配到 0 個 partition → 之後**沒有任何事件會
  觸發 rebalance**。已設 `WatchPartitionChanges: true` + 5 秒的
  `PartitionWatchInterval`。詳見 `AGENTS.md` §2.23

**Changed**
- `shutdownTimeout` 5 秒 → **10 秒**，compose 補 `stop_grace_period: 15s`
- base image 從計畫書原訂的 `gcr.io/distroless/static-debian12` 改成 **`scratch`**
- demo 頁面在 **8090** 而不是原訂 DoD 寫的 8088
- Prometheus/Grafana 用 9091/3001 而不是 9090/3000

**為什麼**
- **base image 改 scratch**：distroless 帶了不少用不到的東西（tzdata、base-files、
  media-types、netbase），不利於 DoD 的 20 MB 上限，而本服務不做任何時區換算。
  代價是要自己補回 CA 憑證與**數字 UID**（沒有 /etc/passwd 就不能寫
  `USER nonroot`，寫了容器直接起不來）。
- **demo 頁與 token 端點放在獨立容器**：demo 需要發 JWT 才能連上，
  而把簽章能力放進推播服務，等於在一個對外開埠、沒有帳號系統的服務裡
  開一個鑑權後門。分成兩個執行檔，正式映像就不可能被誤開。
  代價是 DoD 原本寫的「打開 localhost:8088」變成 8090。
- **demo 用 CDN 的 `@stomp/stompjs` 而不是自己手寫 client**：本專案的賣點是
  伺服器端自己實作 STOMP。連 client 也自己寫的話，證明的只是「我的兩份實作
  互相相容」，意義接近於零；用業界標準 client 連得上，證明的才是**與規格相容**。
- **關機預算 5 → 10 秒**：實測慢的不是我們的程式碼。三個 topic 是同一個
  consumer group 的三個 member，coordinator 一次只處理一個 `LeaveGroup`、
  每次跑一輪 rebalance，所以是**排隊**離開的，收工恰好每 3.000 秒一個。
  **選擇等它而不是縮短預算**：不等 LeaveGroup 送完的話，Kafka 要到
  session timeout（30 秒）才知道我們走了，滾動更新會出現 30 秒的消費空窗，
  那比關機慢 5 秒糟得多。⚠️ 這個成本隨 topic 數線性成長。
  （曾懷疑是 kafka-go 的 `HeartbeatInterval`，預設剛好 3 秒；
  **實測改成 1 秒後間隔仍是 3.000 秒，假設不成立，已還原**。）
- **觀測性埠往後挪一號**：團隊 repo 的容器占著 9090/3000，同時跑會
  `port is already allocated`。與服務用 8088（Java 版 8087）是同一個並存理由。
- **Prometheus/Grafana 預設不啟動**：那兩個映像加起來比整組 demo 還大，
  而「看推播能不能動」並不需要它們。

**如何驗證**（2026-07-30，在**第二台**沒有團隊 repo 的機器上實測）
- `gofmt -l` 無輸出、`go vet ./...` 無輸出、`golangci-lint run` 0 issues、
  `go build ./...` 通過、`go test -race ./...` 全綠（新增 cmd/demo）
- `docker compose up --build -d --wait` → 全部 healthy
- **端到端**：另寫一支 STOMP over WebSocket client 走完整條路徑
  （取 token → 握手 → 訂閱 → 收訊息），三條推播路徑
  （`/user/queue/notifications`、`/topic/notifications`、`/topic/rank`）
  都收得到，且 `subscription` 與 `message-id` header 都在
- 優雅關閉：`ExitCode=0`，三個 consumer 都乾淨收工
- Grafana 的資料來源與 dashboard 自動載入，**dashboard 的 7 條 PromQL
  逐條對 Prometheus 查過都有資料**（避免指標名稱打錯卻沒人發現）
- CI 在 PR #2 上實跑，**三個 job 全綠**（第一次是紅的，抓到兩個本機不會
  出現的問題，見上面那筆 `[fix]`）
- ⚠️ 瀏覽器端沒有自動化驗證（需要無頭瀏覽器，成本遠高於價值）

---

## [feat] — 2026-07-30 — Phase 3：接上 Kafka；補 Phase 0 的前提驗證；補上兩份 ADR

**Added**
- `internal/consumer`：三個 topic 的消費迴圈，一個 topic 一條 goroutine。
  - `events.go`：三個事件的 struct **欄位與順序都對齊 Java record**，
    可空欄位用指標（Java 是包裝型別，缺值要寫成 `null` 而不是 `""` 或 `0`）；
    `payload` / `entries` 用 `json.RawMessage`（避開 `map[string]any` 的
    key 重排與 float64 精度損失）
  - `consumer.go`：`FetchMessage` + `CommitMessages`（手動提交，**不用
    `ReadMessage`**——那個會自動提交）；`StartOffset: kafka.LastOffset`
    對齊 `auto-offset-reset: latest`；取訊息失敗退避 2 秒重試
  - `Pusher` 介面定義在**使用端**，只列 consumer 真的用到的兩個方法
- 兩個新指標：`notify_kafka_messages_total{topic,result}`（result 分
  `ok` / `dropped` / `skipped`）、`notify_push_latency_seconds`
- 兩個新環境變數：`KAFKA_BOOTSTRAP_SERVERS`（預設 `localhost:9092`，
  **明確給空值＝不啟動 consumer**）、`KAFKA_GROUP_ID`
  （預設 `notification-service-go-group`）
- `docs/ADR-001`（為什麼用 Go 重寫，含「多副本連線路由」的未來工作）、
  `docs/ADR-002`（STOMP 子集取捨 + **RWMutex vs 中央 goroutine 的論證**）
- `docs/phase0-前端脫離-SockJS.md` 與可 `git apply` 的
  `docs/phase0-useWebSocket-native-ws.patch`（feature flag
  `VITE_WS_TRANSPORT=native|sockjs`，**預設維持 SockJS**）
- 測試：`events_test.go`（19 個案例，逐位元組比對三種 topic 的推播 body）、
  `consumer_test.go`（壞訊息照樣 commit、退避後繼續、關閉後 goroutine 回基線）、
  `TestKafkaSettings`、`TestDefaultKafkaGroupDiffersFromJava`

**Changed**
- `cmd/notify/main.go`：root context 現在一路傳給 Kafka consumer，
  關閉時**先等 consumer 收工**再收 WebSocket（它們是訊息來源）
- `server.Server` 多一個 `Hub()` 存取器，讓 main 能把 hub 接給 consumer
- `.gitattributes`：`*.patch` 設 `-text`

**為什麼**
- **不論成功與否都 commit offset**：對齊 Java 版的
  `finally { ack.acknowledge(); }`。壞訊息重試一萬次還是壞的，不 commit 會讓
  offset 永遠卡在那一則——**一則壞訊息就能讓整個 topic 停擺**。
- **`commit` 用 `context.WithoutCancel`**：關機時 root ctx 已取消，但剛處理完
  的那一則還是該提交，否則下次啟動會重播它。
- **`marshalCompatible` 關掉 HTML 轉義**：`json.Marshal` 預設把 `<` `>` `&`
  轉成 Unicode 轉義，Jackson 不會。兩者 `JSON.parse` 相同但位元組不同，
  而本專案主打的正是跨語言等價。（`Encoder.Encode` 會多補一個 `\n`，要砍掉。）
- **`game.result` 的推播 body 與事件不同形狀**：Java 是另組一個 7 欄物件，
  `type` 在最前面且**不含 `playerId``**。原樣轉推的話前端拿不到 `type`，
  會走進「系統通知」分支且無任何錯誤訊息。
- **group id 預設與 Java 版不同**：同一個 group 會讓兩版互搶 partition，
  症狀是「Go 版隨機漏訊息」。不拒絕啟動（Java 沒跑時沿用是合法的），只警告。

**如何驗證**
- `gofmt -l` 無輸出、`go vet ./...` 無輸出、`go build ./...` 通過
- `go test -race ./...` 全綠（auth / config / consumer / hub / server / stomp）
- Phase 0 前提：在團隊 repo 跑 `mvn -B test -Dtest=WebSocketStompIntegrationTest`
  → `Tests run: 3, Failures: 0, Errors: 0` / `BUILD SUCCESS`。
  該測試用 `StandardWebSocketClient`（**原生 WS、無 SockJS**）連 `/ws`，
  證明 Go 版不必實作 SockJS 的前提成立。
- patch 已在乾淨副本上驗過 `git apply --check` 與 round-trip 位元組相同
- ⚠️ **尚未驗**：用真的 Kafka 灌三種事件、以及真前端登入玩一局的端到端路徑
  （本機 Docker daemon 未執行）。細節與補做步驟見
  `docs/phase0-前端脫離-SockJS.md` §3 與 `計畫說明書.md` §6 的 DoD 註記。

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
