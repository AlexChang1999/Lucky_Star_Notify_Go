# ADR-002：STOMP 子集的取捨與 Hub 的併發模型

| 項目 | 內容 |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-07-30 |
| **決策者** | AlexChang1999 |
| **相關** | [ADR-001](./ADR-001-為什麼用-Go-重寫推播服務.md)、`計畫說明書.md` §4.1、§5.2 |

---

## 1. 背景

Java 版用 `@EnableWebSocketMessageBroker` 一個註解，背後是 frame 解析、訂閱
註冊表、destination 路由、user destination 解析、heartbeat、錯誤處理。

**Go 生態沒有成熟的 STOMP server library**（`go-stomp/stomp` 主要是 client 端）。
所以只有兩條路：自己實作，或改做「純 WebSocket + 自訂 JSON 協議」。

自己實作是本專案的核心價值（ADR-001 §2.1），所以問題不是「要不要做」，
而是**做多少**——以及連線註冊表要用什麼併發模型。

---

## 2. 決策 A：只實作現行服務**實際用到**的指令

| STOMP 指令 | 方向 | 做不做 | 理由 |
|---|---|---|---|
| `CONNECT` / `STOMP` | C→S | ✅ | 鑑權入口 |
| `CONNECTED` | S→C | ✅ | 握手回應 |
| `SUBSCRIBE` | C→S | ✅ | 訂閱 destination |
| `UNSUBSCRIBE` | C→S | ✅ | 前端 hook 會用 |
| `MESSAGE` | S→C | ✅ | 推播主體 |
| `ERROR` | S→C | ✅ | 鑑權失敗、伺服器關閉 |
| `DISCONNECT` | C→S | ✅ | 優雅關閉（含 `receipt`） |
| heartbeat | 雙向 | ✅ | `@stomp/stompjs` 預設開 10s/10s |
| `SEND` | C→S | ❌ | 現行服務沒有任何 `@MessageMapping`，是純推播 |
| `ACK` / `NACK` | C→S | ❌ | 現行未使用（推播為 best-effort） |
| `BEGIN` / `COMMIT` / `ABORT` | C→S | ❌ | 同上 |

**範圍護欄的意義**：side project 最常見的死法是範圍蔓延。把「不做什麼」寫下來，
是為了在手癢的時候擋住自己。

### 兩個容易漏掉、而且**不會有錯誤訊息**的細節

1. **header 轉義有例外**：STOMP 1.2 規定 header 的 name 與 value 要轉義
   （`\` `\r` `\n` `:` 四個字元，**body 不轉義**），但規格 §3.1 明訂
   **`CONNECT` 與 `CONNECTED` 兩種帧不轉義**，為了與 STOMP 1.0 相容。
   漏了轉義 → 玩家暱稱含冒號就把帧切壞；對 CONNECT 做了轉義 → 與
   `@stomp/stompjs` 握手失敗。
2. **`MESSAGE` 帧的 `subscription` 與 `message-id` 都是必備**：
   `@stomp/stompjs` 拿 `subscription` 去比對自己的訂閱表，對不上就**靜默丟棄**。
   這種 bug 沒有錯誤訊息，只有「怎麼沒反應」。

兩者都有表格驅動測試釘住。

---

## 3. 決策 B：不實作 SockJS 降級協議

SockJS 是一整套降級協議：原生 WebSocket 不可用時退回 xhr-streaming、
xhr-polling、iframe，包含 `/ws/info` 交握、`/ws/{server}/{session}/{transport}`
路徑規則、自己的心跳格式。**在 Go 重刻這一套是災難級的工作量，而且毫無展示
價值**——沒有人會因為手刻 SockJS 而錄取你。

**為什麼可以不做**：Java 版的 `WebSocketConfig` 註冊了**兩個**端點——
一個 `.withSockJS()`，一個原生 WebSocket（註解寫著「供測試用
`WebSocketStompClient` 直連」）。所以只要把前端從
`webSocketFactory: () => new SockJS(...)` 改成 `brokerURL`
（`@stomp/stompjs` v7 原生支援），前端就走原生 WebSocket，**而 Java 版仍然
接得住**。

這個前提已經驗證過，不是推測：團隊 repo 自己的
`WebSocketStompIntegrationTest` 就是用 `StandardWebSocketClient`（原生 WS、
無 SockJS）連 `ws://localhost:{port}/ws`，跑 3/3 綠，涵蓋「有效 JWT →
訂閱 `/user/queue/notifications` → 收到私人推播」與「無效／缺 JWT 被拒」。

前端的改動做成 feature flag（`VITE_WS_TRANSPORT=native|sockjs`，**預設維持
SockJS**），所以對團隊的既有 demo 是零風險。patch 留在
`docs/phase0-useWebSocket-native-ws.patch`。

---

## 4. 決策 C：user destination 不模仿 Spring 的間接層

Spring 把 `/user/queue/notifications` 內部翻譯成
`/user/{sessionId}/queue/notifications`，繞一圈是為了它自己的訊息匯流排。

**Go 版直接做**：session 在 `CONNECT` 時已綁定 `playerID`（JWT 的 `sub`）；
要推給某玩家就查 `playerID → sessions` 索引，`destination` 原樣回
`/user/queue/notifications`。

**更簡單、更快，而且對 client 完全等價**——client 看到的行為才是契約，
Spring 的內部實作不是。這是一個可以在面試講的判斷：「我沒有照抄那個間接層，
因為它是為了 Spring 的訊息匯流排設計的，我的服務不需要。」

註：`players` 是「playerID → session **集合**」而不是單一 session，因為同一個
玩家可能開兩個分頁，兩邊都該收到——Java 版的 `convertAndSendToUser` 也是這個
行為。

---

## 5. 決策 D：連線註冊表用 `sync.RWMutex`，不用「中央 hub goroutine + channel」

**這是本 ADR 的主要內容，也是整個專案技術含量最高的一個判斷。**

### 5.1 兩個候選

**候選一：中央 goroutine（gorilla 官方 chat 範例的做法）**

註冊表由單一 goroutine 獨佔，所有操作（註冊、退訂、廣播）都送 channel 給它，
**完全沒有鎖**。這很符合 Go 那句口號「不要用共享記憶體來通訊」。

- 優點：心智模型乾淨，不可能有 data race，不必推理鎖的順序。
- 缺點：**所有廣播都被序列化通過同一個 goroutine**。廣播 N 條連線就是在那
  一條 goroutine 裡跑 N 次迴圈，其他廣播只能排隊。連線數一多，這條 goroutine
  就是瓶頸，而且它是**單核**的——機器有 16 核也只用得到 1 核。

**候選二：`sync.RWMutex` 保護 map**

- 廣播時 `RLock()`：多個廣播可以**並行**讀取註冊表，跑在不同核心上。
- 只有連線建立／斷開才 `Lock()`。

### 5.2 決策與理由

**選 RWMutex。** 理由是這個服務的存取模式：

> 推播服務是典型的**讀多寫少**。連線建立／斷開的頻率遠低於廣播的頻率
> （一個玩家連上一次，然後接收成千上萬則推播）。RWMutex 正是為這個
> 模式設計的。

換句話說，「不要用共享記憶體來通訊」是**預設建議，不是物理定律**。
當存取模式明確是讀多寫少、而且臨界區很短時，RWMutex 更貼合硬體
（多核並行讀）。**面試官要看的是能不能論證，不是背口號。**

### 5.3 這個選擇帶來的義務

選了鎖，就得自己處理鎖的順序。本專案的規則只有一條，寫在 `internal/hub`
的套件註解裡：

> **永遠是 `hub.mu` → `session.mu`，不可以反過來。**

所以：
- `Broadcast` 持有 `hub.mu.RLock` 時**可以**呼叫 `session.deliver`（會拿 `session.mu`）
- `Session.handleSubscribe` **必須先放掉** `session.mu`，才能呼叫 `hub.subscribe`

反過來就是經典的 AB-BA 死鎖。這是選 RWMutex 付出的代價——而
`go test -race` 抓 race 但**不抓死鎖**，所以這條規則只能靠紀律與註解維持。

### 5.4 連帶的第二個必考點：扇出時絕不可阻塞

```go
select {
case s.outbound <- payload:
    // 正常送入
default:
    // channel 滿了 = 這個 client 消費不及（網路慢／當掉）
    metrics.OutboundDropped.Inc()
    s.closeSlow()   // 踢掉它，保護其他人
}
```

一旦寫成阻塞的 `s.outbound <- payload`，**一個慢速客戶端就會卡住整個廣播
迴圈，拖垮所有人**——而且因為廣播是持有 `hub.mu.RLock` 在跑，連新連線都
會被卡住（`Lock()` 等不到）。用 `select` + `default` 非阻塞送入、滿了就斷線，
這叫 **fail-fast 背壓**，是即時系統的標準做法。

滿了就踢人是有代價的（那個玩家會斷線重連），所以一定要有
`notify_outbound_dropped_total` 計數器讓它**可觀測**——否則「被踢掉」這件事
在外部完全看不見，只會表現成使用者那端莫名重連。

### 5.5 每條連線兩個 goroutine（gorilla 的硬性限制）

`gorilla/websocket` 同時只允許**一個 reader 與一個 writer**。所以每條連線
固定 2 個 goroutine：read goroutine 跑 `ReadMessage()` 解析帧，write goroutine
從 outbound channel 取訊息寫出、並用 `select` 同時處理 heartbeat ticker。

**所有寫入一律經過該連線的 outbound channel，絕不從其他 goroutine 直接
`WriteMessage`**（會 race，而且 `-race` 不保證每次都抓到）。心跳也走 write
goroutine 的 ticker，不另開 goroutine。

goroutine 帳（拿來對照 JVM 執行緒）：10,000 連線 = 20,000 goroutine，
初始堆疊 2 KB → 約 **40 MB**。對照 JVM 平台執行緒預設 1 MB 堆疊，20,000 條
= 20 GB。**但這個對照要誠實**：Spring 的 WebSocket 底層是 Tomcat NIO，
本來就不是 thread-per-connection，所以這不是「Go 贏 Java」，而是
「Go 讓我用阻塞式寫法拿到 NIO 的效率」（見 ADR-001 §3）。

---

## 6. 決策 E：不支援的指令**靜默忽略**，不回 `ERROR` 斷線

規格允許伺服器對未知指令回 `ERROR` 並關閉連線。但 Spring 的 simple broker
是靜默忽略，而本專案的驗證方式是**跨語言契約測試要兩邊都綠**——
如果 Go 版回 `ERROR` 而 Java 版忽略，同一份測試就對不上。

**與規格一致 vs 與被對照的實作一致，這裡選後者**，因為專案目標是行為等價。

---

## 7. 決策 F：心跳只實作到「伺服器宣告 10s/10s」

協商公式是 `server→client = max(cy, sx)`、`client→server = max(cx, sy)`，
**取 max 不是 min**。所以 client 要求更快的間隔是沒用的——照 min 寫會宣告出
自己做不到的頻率。

伺服器固定宣告 10s/10s（對齊 `@stomp/stompjs` 的預設）。要支援更短的間隔，
得把 `serverHeartbeatSend` 變成設定項。

⚠️ **心跳沒實作完就不要在 `CONNECTED` 宣告非零值**：若照 client 要求回
`heart-beat:10000,10000` 但伺服器不送心跳，client 等不到就會自己斷線，
症狀是「握手明明成功，幾秒後莫名斷線」。Phase 1 一律回 `0,0`，Phase 2 實作
收發之後才改。

---

## 8. 決策 G：`CONNECTED` 由 write goroutine 送出，不由 HTTP handler 直接寫

這是 Phase 2 踩到的**真 bug**，值得記錄，因為它是一整類錯誤的代表。

原本流程是 `writeFrame(CONNECTED)` → `hub.Serve()`，於是「client 收到
CONNECTED」**早於**「session 登記進 hub」。中間那段空窗期裡，廣播與關機通知
都會漏掉這條連線，而且**沒有任何錯誤訊息**——只會偶發地「某個剛連上的人
沒收到」。

正解是把 CONNECTED 交給 `SessionConfig.Greeting`，順序變成
`h.add` → enqueue → write goroutine 送出。附帶好處：整條連線從頭到尾只有
一個 writer，不再有特例要論證。

**教訓**：任何「先回覆、後註冊」的寫法都要用同樣的眼光檢查一次。

---

## 9. 後果

### 正面
- 協議層是自己的，行為完全可控，也完全可測（表格驅動 + fuzz）
- 併發模型的每個選擇都有可辯護的理由，這正是面試要考的
- 不做 SockJS 省下的工作量，換成了契約測試

### 負面 / 已接受的成本
- 鎖的順序規則只能靠紀律維持（`-race` 抓 race，不抓死鎖）
- 慢速客戶端會被踢掉，不是無損的（用計數器換取可觀測性）
- STOMP 只有子集：日後若前端要用 `SEND` 或交易帧，得回來補

### 退場方案
若 STOMP 實作比預期困難超過 2 倍，降級方案是改做「純 WebSocket + 自訂 JSON
協議」，前端另寫一個薄 client。展示價值會下降（少了協議層實作），但仍保有
hub 併發模型與 Kafka 整合這兩塊核心。**先想好退路，才敢全力往前。**

（實際結果：沒有用到退場方案。Phase 1～3 如期完成。）

## 10. 怎麼驗證這些決策沒寫錯

- `internal/stomp`：表格驅動測試涵蓋正常帧、含轉義字元的 header、空 body、
  畸形帧（缺 NULL、header 沒冒號）；另有 fuzz 找畸形輸入導致的 panic
- `internal/hub`：訂閱／退訂／扇出／**慢速客戶端驅逐**／同一玩家兩條連線
- `internal/server`：用 `httptest` 起真的 HTTP 服務、走真的 WebSocket 升級，
  跑完整握手與推播路徑
- 全部跑 `go test -race`（**本專案的硬性要求，不是選配**）
