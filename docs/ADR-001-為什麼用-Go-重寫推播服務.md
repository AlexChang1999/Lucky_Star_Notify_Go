# ADR-001：為什麼用 Go 重寫推播服務

| 項目 | 內容 |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-07-30 |
| **決策者** | AlexChang1999 |
| **相關** | [ADR-002](./ADR-002-STOMP-子集的取捨.md)、`計畫說明書.md` §2、§3 |

---

## 1. 背景

`Lucky_Star_Casino`（大專小組共同專案）有 7 個 Java 21 / Spring Boot 3.3.5
的後端服務。其中 `notification-service` 負責**消費 3 個 Kafka topic，透過
WebSocket/STOMP 把訊息即時推給線上玩家**。

我要轉職 Golang 後端，需要一個能證明「我會用 Go 寫併發服務」的作品。
可選的做法有三種：

1. 從零寫一個新的 Go 專案（例如用 Gin 寫 CRUD）
2. 把團隊專案的某個服務用 Go 重寫，放進團隊 repo
3. 把團隊專案的某個服務用 Go 重寫，放進**獨立 repo**，並證明兩版行為等價

## 2. 決策

**選 3：用 Go 重寫 `notification-service` 成獨立 repo，目標是對前端與 Kafka
而言與 Java 版行為等價、可互換。**

### 2.1 為什麼是 `notification-service`

7 個服務逐一評估（完整矩陣見 `計畫說明書.md` §2），淘汰理由歸納成三類：

| 服務 | Java 檔數 | 淘汰理由 |
|---|---:|---|
| wallet | 140 | 帳務核心。雙資料源、Transactional Outbox、冪等鍵、條件 UPDATE 防超扣——這些機制的價值在於**正確性已被驗證**，重寫等於把穩定的東西丟回不確定性。而且 Go 沒有 JPA 等級的交易抽象。 |
| member | 115 | OAuth2 三家 provider 的 authorization-code 握手。Go 要自己刻整套 OIDC，是純苦工，學不到 Go 的特色。 |
| game | 111 | Provably Fair RNG、老虎機權重表、補牌規則。業務邏輯極重，Go 沒有優勢。 |
| admin | 97 | 純後台 CRUD + 報表。Spring Data JPA 寫這個比 Go 快，重寫是負收益。 |
| rank | 47 | **次選**，值得做第二個（見 `計畫說明書.md` §14）。但有 PostgreSQL + 排程 + 每日快照，複雜度約是 notification 的 3 倍。 |
| gateway | 18 | 檔案最少，但它是全站唯一入口，掛掉整個系統掛掉；而且自刻的 `AdaptiveInFlightLimiter`（AIMD 動態在途上限）**本身就是團隊專案的亮點**，重寫等於把亮點砍掉重練。 |
| **notification** | **17** | **選它。** 理由如下。 |

`notification-service` 的四個決定性條件：

1. **邊界極乾淨**：輸入只有 3 個 Kafka topic，輸出只有 WebSocket。
   `application.yml` 明講「本服務無資料庫（純事件→WebSocket 橋接）」。
2. **出錯代價極低**：無帳務、無 DB、推播是 best-effort，壞訊息本來就丟棄。
   所以我可以把心力放在 Go 本身，而不是「不能寫錯」的業務邏輯。
3. **Go 的技術優勢真的用得上**：長連線 fan-out 正是 goroutine 的主場。
4. **唯一深綁 Spring 的部分（STOMP broker）正好就是值得自己寫的部分**——
   Go 生態沒有成熟的 STOMP **server** 實作，所以這件事把專案從「用 Gin 寫
   CRUD」（人人都有）升級成「實作了 STOMP 1.2 伺服器子集」（極少人有）。
   取捨細節見 ADR-002。

### 2.2 為什麼是獨立 repo

1. **那是共同專案**。把個人求職用的實驗性重寫塞進去，會讓組員承擔一個他們
   沒同意、也不會維護的技術棧。這是硬約束，不是偏好。
2. **作品集要能單獨展示**。面試官不會、也不該為了看我的 Go 程式碼去 clone
   一個 7 服務的 monorepo。
3. **反而更能證明能力**：「我重新實作了一個服務，並讓它與原系統的 Kafka
   契約、前端 STOMP 契約完全相容」比「我在自己 repo 裡加了個資料夾」強。

**代價**：無法直接沿用團隊 repo 的 `docker-compose.yml`。解法是本專案自帶
一份最小 compose（Kafka + 本服務 + 事件產生器 + 極簡前端），並額外提供
「接上團隊 repo 基礎設施」的變體（Phase 4）。

### 2.3 為什麼「行為等價」是核心目標而不是「比 Java 快」

任何人都可以說「我用 Go 重寫了一個服務」，**能證明兩個實作等價的人非常少**。
所以驗證策略（`計畫說明書.md` §7）比效能數字重要：

- 第一層：Go 單元測試，全部跑 `go test -race`
- 第二層：**跨語言黑箱契約測試**（Node + `@stomp/stompjs`），同一份測試對
  Java 版（8087）與 Go 版（8088）都要綠
- 第三層：效能對照（**尚未做**；日後要寫數字，一律自己實測，不可虛構）

而且切換只需要改團隊 repo gateway 的一個環境變數
（`NOTIFICATION_SERVICE_URL`），因為 `/ws` 已在 `jwt.whitelist` 裡。
**同一個前端、改一個環境變數、後端從 Java 換成 Go**——這是「異質語言微服務
可互換」最漂亮的展示形式。

## 3. 後果

### 正面

- 協議層實作、併發模型、跨語言等價驗證，三個履歷賣點
- 單一靜態 binary、映像 < 20 MB（CI 有大小上限斷言）
- `go test -race` 是 Java 沒有等價物的工具，值得在 README 標出來

### 負面 / 已接受的成本

- **必須自己實作 STOMP**，這是主要工作量，也是主要風險（緩解見 ADR-002 §5）
- 無法沿用團隊的 compose，要自備一份
- 兩版併存時有一個必踩的坑：**consumer group id 必須不同**
  （Java 是 `notification-service-group`，Go 用 `notification-service-go-group`）。
  用同一個 group，Kafka 會把 partition 分給兩邊，每則事件只進其中一版——
  症狀看起來是「Go 版隨機漏訊息」，真因卻完全看不出來。
  已用 `config.DefaultKafkaGroupID` 與一條測試釘住。

### 誠實的界線

不追求「比 Java 快 N 倍」的行銷數字。Spring 的 WebSocket 底層是 Tomcat NIO，
**本來就不是一連線一執行緒**，所以效能不是主要差距。正確的說法是：

> Go 讓我可以用**最直觀的阻塞式寫法**（一連線兩個 goroutine）達到 NIO 等級
> 的效率，不需要處理響應式／回呼的複雜度。

（若說成「Go 贏在不用一連線一執行緒」，懂 NIO 的面試官會當場拆穿。）

## 4. 未來工作：多副本的連線路由問題

**現行 Java 版架構上只能單副本，Go 版維持單副本是行為等價，不是簡化偷懶。**

推論如下：`enableSimpleBroker("/topic", "/queue")` 是**記憶體內 broker**，
訂閱表存在該 JVM 的記憶體裡；而 7 個 Kafka consumer 共用同一個 group id。
若開 2 個副本，Kafka 會把 partition 分給兩個實例，每則事件只會進**其中一個**；
但玩家的 WebSocket 連線只在**某一個**實例上。事件進到 A、玩家連在 B →
**這則推播就靜默消失了**（沒有例外、沒有 log）。

要水平擴展，必須先解決玩家連線的路由問題，兩條路：

1. **Redis Pub/Sub 扇出**：每個副本都訂閱同一個頻道，收到事件後各自查自己的
   連線表。簡單，但每則事件都會送到所有副本（N 倍網路流量）。
2. **Sticky routing**：用 gateway 把玩家固定到某個副本（例如按 playerID 一致性
   雜湊）。省流量，但副本增減時要處理連線遷移。

**這是一個真實存在、目前兩版都未解決的架構限制**，不是本專案的缺陷。
被問「這個怎麼水平擴展？」時，答案是上面這段，而不是「呃我沒想過」。
