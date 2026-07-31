# 高併發虛擬幣線上娛樂城 — Golang 技術棧與人才需求規格書

> 來源：104 人力銀行台中地區 9 個 Golang / 後端 / 遊戲後端 / 低延遲交易職缺之技術要求彙整
> 整理日期：2026-07-31
> 用途：作為自建「高併發加密貨幣線上娛樂城」的技術選型基準與招募 JD 藍本

---

## 0. 需求來源一覽

| # | 公司 | 職稱 | 年資門檻 | 核心語言 | 對本專案的參考價值 |
|---|------|------|---------|---------|------------------|
| 1 | 石敢當科技 | Golang 後端技術工程師 | 3 年+ | Go | 遊戲伺服器、併發、快取架構、CI/CD |
| 2 | 必創系統 | Golang 開發工程師 | 1 年+ | Go | 區塊鏈 / 加密貨幣 / P2P / 密碼學 |
| 3 | 狄泰 | 後端工程師 | 1 年+ | C#/.NET | 架構觀念：WebSocket、索引調優、JWT/OTP |
| 4 | 捷特威科技 | 遊戲後端工程師 | 1 年+ | Go | 遊戲伺服器 + 會員系統 + 遊戲日誌 |
| 5 | 弈樂科技 | Web 後端工程師 | 1 年+ | Go | **最貼近本專案**：高併發、Kafka、分散式鎖、資安 |
| 6 | 台中商銀 | 中介平台程式設計師 | 3 年+ | Java | 金流級中介層：APIM、Gateway、MQ、監控 |
| 7 | 天鏡科技 | Software Engineer | — | C++/Go | 低延遲、Go runtime 底層、可觀測性 |
| 8 | 錦鉅城 | Golang 後端助理工程師 | 1 年+ | Go | Gin、分層架構、NATS、Elasticsearch |
| 9 | 川圖 | Backend Engineer（遠端） | 1–3 年 | Go | GKE、PostgreSQL、gRPC |

---

## 1. 語言核心能力（Must Have）

### 1.1 Go 語言本體
- 精通 Go 語法與工程慣例，能寫出模組化、可測試、高可讀性的程式碼。
- **併發模型深度掌握**：Goroutine、Channel、`select`、`context` 取消與超時傳遞。
- **Runtime 機制理解**（進階，決定能不能撐住尖峰）：
  - Goroutine 調度器（GMP 模型）行為與 goroutine 洩漏偵測
  - Channel 底層結構、緩衝與阻塞語義
  - Memory Allocator / GC 行為、逃逸分析、`sync.Pool` 物件複用
- **同步原語**：`sync.Mutex` / `RWMutex` / `WaitGroup` / `atomic`、鎖粒度設計、避免 Lock Contention。
- 傳統併發概念對照能力：task / async / await / thread / lock，能與其他語言背景工程師溝通。
- 效能剖析：`pprof`（CPU / Heap / Goroutine / Block / Mutex）、`trace`、`benchmark`、`-race` 競態偵測。

### 1.2 Web 框架與 API
- **HTTP 框架**：Gin（群組共同提及最多）；可評估 Echo / Fiber。
- **RESTful API** 設計規範、版本化、統一錯誤碼、完整 API 文件（OpenAPI / Swagger）。
- **gRPC + Protocol Buffers**：服務間內部通訊主力。
- **WebSocket**：遊戲即時推播、賠率/牌局/餘額變動的長連線核心（.NET 的 SignalR 為對應概念）。
- 高效能序列化：Protobuf / MessagePack，避免全鏈路 JSON。
- 第三方服務串接：複雜外部 API 整合、重試、冪等、超時與熔斷。

### 1.3 架構與設計模式
- **分層架構（Layered Architecture）**：Handler → Service → Repository。
- **資料模型分層**：DTO / BO / PO 嚴格分離，禁止 DB model 直接外洩到 API。
- **Interface-first 開發**：先定義介面再實作，方便 mock 與替換。
- **Dependency Injection（DI）**：降低耦合、提升可測試性（wire / fx 或手動注入）。
- **Strategy Pattern**：用於遊戲玩法、賠率規則、支付通道等高變異模組——本專案的關鍵設計。
- 微服務架構切分、服務邊界與擴充方向定義。
- Clean Code + Code Review 文化、型別安全、技術債管理。

---

## 2. 資料層（Data Layer）

### 2.1 關聯式資料庫
- 主選：**MySQL**（搭配 **GORM**）或 **PostgreSQL**（金流帳務精度較佳，建議錢包/帳本用 PG）。
- 群組其他曾要求：MariaDB、MSSQL、SQL Server、Oracle（可視為「任一 DBMS 深度經驗」）。
- 必備技能：
  - Schema / 索引設計，複合索引與覆蓋索引
  - Slow Query 分析與 `EXPLAIN` 調優
  - 交易隔離等級、悲觀鎖 / 樂觀鎖（版本號）
  - **Multi-DB 存取架構**：讀寫分離、分庫分表、水平切分
  - 金額欄位使用 DECIMAL / 整數最小單位，**嚴禁 float**

### 2.2 NoSQL 與快取
- **Redis**（必備）：
  - Cache-Aside / Write-Through 快取策略、TTL 與快取雪崩/擊穿/穿透防護
  - **分散式鎖**（Redlock 或 Lua 原子腳本）— 下注扣款、防重複請求的關鍵
  - 原子計數器、Sorted Set 排行榜、Stream、Pub/Sub
- **MongoDB**：遊戲局紀錄、日誌類半結構化資料。
- **Memcached**：純快取備選。
- **Elasticsearch**：注單查詢、遊戲日誌全文檢索與行為分析。

### 2.3 訊息佇列與分散式協調
- **Kafka**（+ Zookeeper / KRaft）：非同步事件流主幹——注單事件、帳變事件、風控事件、資料倉儲同步。
- **NATS**：輕量、低延遲內部事件（適合遊戲房間廣播）。
- **RabbitMQ**：任務型佇列備選（銀行職缺另提 IBM MQ，本專案不需要）。
- 必備觀念：事件冪等、Exactly-once 近似處理、消費者 Rebalance、死信佇列、消費積壓監控。

---

## 3. 高併發與效能工程（本專案的生死線）

- 高併發系統架構設計經驗，能有效優化**吞吐量（Throughput）與穩定性**。
- 效能瓶頸定位能力：Memory bound / CPU bound / **Lock Contention** / I/O 等待。
- 連線池管理：DB pool、Redis pool、HTTP client 複用。
- 限流與保護：Token Bucket / Leaky Bucket 限流、熔斷（Circuit Breaker）、降級、排隊機制。
- 冷熱資料分離、快取預熱、批次寫入（Batch / Buffer Flush）。
- 網路程式設計：Socket、TCP / UDP、長連線管理、心跳與斷線重連（低延遲交易職缺要求，同樣適用即時遊戲）。
- Linux 系統層調優：核心參數（`somaxconn`、`file-max`、TIME_WAIT）、系統效能調校基礎。
- 容量規劃（Capacity Planning）與壓力測試（k6 / wrk / Locust / vegeta）。

---

## 4. 虛擬幣 / 區塊鏈能力

- 對 **Bitcoin / Ethereum / 數位貨幣 / 區塊鏈行業**有一定理解。
- **P2P 網路**原理、節點通訊。
- **密碼學原理**：雜湊、非對稱加密、簽章驗證、HMAC。
- 曾參與區塊鏈開源開發、對區塊鏈資產應用有熱忱。
- 本專案延伸必要能力（由上述基礎推導）：
  - 鏈上入帳監聽：Block Scanner、區塊重組（Reorg）處理、確認數策略
  - 穩定幣通道：USDT ERC-20 / TRC-20 / BEP-20 之地址派生與歸集
  - 冷熱錢包分離架構、提幣審核流程與多簽（**私鑰管理須由具權限的人員以 HSM / KMS 獨立操作，不入程式碼庫**）
  - 幣價匯率服務、法幣計價換算與滑價控制

---

## 5. 帳務與交易正確性（娛樂城核心）

- 目標定義（引用共通要求）：確保**資料處理的正確性、系統運行的穩定性與高擴展性**，並提供安全可靠的 API。
- 複式記帳（Double-Entry Ledger）帳本設計，餘額由帳變流水推導、可完整對帳。
- 全鏈路**冪等性**：請求 ID / 注單 ID 唯一鍵，重送不重複扣款。
- 分散式一致性：本地訊息表 / Transactional Outbox、Saga 補償交易、最終一致性。
- 下注→結算→派彩流程的原子性設計（Redis 分散式鎖 + DB 交易 + 事件補償）。
- 意外事件應對處理與資料修補（Reconciliation）機制。

---

## 6. 安全性（Security）

- 常見 Web 漏洞防護：**SQL Injection、XSS、CSRF、CORS** 設定。
- 認證授權：**JWT**（含 Refresh Token 輪替）、**OTP / 2FA**、OAuth 2.0。
- API 安全：簽章驗簽、時間戳 + Nonce 防重放、IP 白名單、Rate Limit。
- 敏感資料加密儲存、金鑰輪替、稽核日誌（Audit Log）不可篡改。
- 風控模組：異常投注偏好偵測、對賭/套利偵測、多帳號關聯、提幣風險評分。
- 版本控管紀律（Git）、Secret 不進 Repo（Vault / KMS / Sealed Secrets）。

---

## 7. DevOps、雲端與可觀測性

### 7.1 容器與部署
- **Docker**（必備）、**Kubernetes / GKE**（進階，本專案建議直上 K8s）。
- 任一雲端服務經驗：GCP / AWS / Azure。
- **CI/CD**：GitLab CI/CD、Jenkins、GitHub Actions；IaC 使用 Terraform、Ansible。
- 藍綠部署 / 金絲雀發布、服務發布與系統升級流程、**高可用性（HA）規劃**。
- 自動化維運工具與核心模組自動化佈署工具之開發維護。

### 7.2 監控與追蹤
- **Prometheus + Grafana**：Metrics 與告警儀表板。
- **ELK / Elastic Stack**：Log 集中收集、處理與分析。
- **OpenTelemetry + 分散式追蹤（Jaeger / Tempo）**：跨微服務請求鏈路。
- 結構化 Logging（zap / zerolog）、Trace ID 貫穿、生產環境故障排除與效能瓶頸定位。
- **遊戲日誌管理與分析**：注單、玩家行為、RTP 統計、金流稽核軌跡。
- API Gateway 治理：Kong 或 APISIX（統一認證、限流、路由、灰度）。

### 7.3 作業系統
- Linux 實務經驗（CentOS / Ubuntu / RHEL）、shell 排錯、系統資源觀測。

---

## 8. AI 輔助開發（多家已列為必要條件）

- 熟練 **Claude Code、Cursor、GitHub Copilot** 等 AI 協作開發工具，並整合進日常工作流。
- 應用場景：程式碼重構、單元測試自動生成、API 文件維護、架構情境分析。
- 了解 **MCP（Model Context Protocol）**，能建立 AI 與外部工具的串接。
- 目的：把人力集中在高價值的架構設計與業務邏輯上。

---

## 9. 工程流程與軟實力

- 規格制訂與文件統整、產品功能流程圖繪製、系統分析與需求分析能力。
- Sprint 交付定義與驗收標準、Daily Standup、OKR / Roadmap 對齊。
- Code Review 主持能力、PR 顆粒度控制、測試策略制定。
- 系統設計時能理性權衡**效能、成本與穩定性**。
- 追查 Root Cause 到底的責任感；主動提出優化建議。
- 跨部門協作（PM / 企劃 / 前端 / QA / SRE）與技術表達能力。
- 英文讀寫中等以上（閱讀官方文件與國際金流/鏈上 API 規格）。

---

## 10. 建議技術棧總表（本專案定案建議）

| 層級 | 選型 | 理由 |
|------|------|------|
| 語言 | Go 1.22+ | 高併發、部署簡單、生態成熟 |
| HTTP | Gin | 群組職缺最主流 |
| 內部通訊 | gRPC + Protobuf | 低延遲、強型別 |
| 即時推播 | WebSocket（gorilla / melody） | 牌局、餘額、聊天 |
| ORM | GORM（讀寫分離）| 生態完整 |
| 主資料庫 | PostgreSQL（帳務）+ MySQL（業務） | 帳務精度 / 通用性 |
| 快取與鎖 | Redis Cluster | 快取 + 分散式鎖 + 排行榜 |
| 文件庫 | MongoDB | 局紀錄、事件明細 |
| 搜尋 | Elasticsearch | 注單查詢、行為分析 |
| 事件流 | Kafka | 帳變、風控、數倉 |
| 內部事件 | NATS | 房間廣播、低延遲 |
| 容器 | Docker + Kubernetes（GKE） | 彈性擴縮 |
| CI/CD | GitLab CI + Terraform | 自動化佈署 |
| 監控 | Prometheus + Grafana + Loki | 指標與告警 |
| 追蹤 | OpenTelemetry + Jaeger | 全鏈路追蹤 |
| Gateway | Kong / APISIX | 認證、限流、灰度 |
| 鏈上 | 自建節點 / 第三方 RPC + Scanner | 入帳監聽 |

---

## 11. 招募分級建議（依群組年資門檻歸納）

- **Junior（1 年）**：Go + Gin + MySQL + Redis + Git + RESTful；能在 mentor 指導下交付模組。
- **Mid（2–3 年）**：獨立設計分層架構、Redis 快取策略、Kafka 消費者、Docker 化、Slow Query 調優。
- **Senior（3 年+）**：高併發架構主導、goroutine/GC 層級調優、分散式一致性與帳務正確性設計、K8s 與可觀測性落地、Code Review 與 Mentorship。

---

## 12. 合規備註

線上博彩與虛擬貨幣支付在各地法規差異極大（含執照、KYC/AML、資金存管、稅務、RNG 公平性認證）。本文件僅涵蓋技術架構層面，實際營運前務必取得當地法律與合規意見；私鑰、金流帳戶與客戶身分資料之處理須由具權責之人員依合規流程獨立執行。