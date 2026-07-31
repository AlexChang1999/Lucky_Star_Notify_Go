# 跨語言黑箱契約測試

**同一份測試，對 Java 版（8087）與 Go 版（8088）都必須通過。**

任何人都可以說「我用 Go 重寫了一個服務」。這個資料夾是用來證明
**重寫出來的東西跟原版行為一樣**——而且是有證據的一樣，不是宣稱的一樣。

測試本身是 Node.js 寫的，用的是前端實際在用的 [`@stomp/stompjs`](https://stomp-js.github.io/)
與 `kafkajs`。刻意不用 Go 寫：用 Go 的 client 測 Go 的 server，證明的只是
「我的兩份實作互相相容」；用業界標準 client 測，證明的才是**與規格相容**。
測試碼裡也沒有任何一行認得出對面是 Java 還是 Go——只要出現一句
「如果是 Go 就……」，這份測試的證明力就沒了。

---

## 前置

- **Node.js ≥ 22.4**（需要內建的全域 `WebSocket`，不必額外裝 `ws`）
- 目標服務已經在跑，而且它的 Kafka 從宿主連得到

```bash
cd test/contract
npm ci
```

## 跑 Go 版

```bash
# 專案根目錄
docker compose -f deploy/docker-compose.yml up --build -d --wait

cd test/contract
CONTRACT_TARGET=go npm test
```

PowerShell：

```powershell
$env:CONTRACT_TARGET='go'; npm test
```

## 跑 Java 版（對照組）

需要團隊 repo 在本機。**唯讀使用：把它跑起來就好，不提交任何 commit。**

```bash
# 團隊 repo 根目錄。只起 notification-service，它會自己帶起 kafka + kafka-init
docker compose up -d notification-service

cd <本 repo>/test/contract
CONTRACT_TARGET=java JWT_SECRET=<團隊 repo .env 裡的 JWT_SECRET> npm test
```

PowerShell（順便從團隊 repo 的 `.env` 讀 secret）：

```powershell
$teamSecret = (Select-String -Path 'H:\Lucky_Star_Casino\.env' -Pattern '^JWT_SECRET=' |
               Select-Object -First 1).Line -replace '^JWT_SECRET=',''
$env:CONTRACT_TARGET='java'; $env:JWT_SECRET=$teamSecret; npm test
```

> ⚠️ 團隊 repo 的路徑**會隨機器變動**，開工前先確認（見 `AGENTS.md` §2.8）。

## 環境變數

| 變數 | 預設 | 說明 |
|---|---|---|
| `CONTRACT_TARGET` | `go` | `go` / `java`，只用來填下面兩個的**預設埠號**，不影響任何斷言 |
| `WS_TARGET` | 依 preset | WebSocket 端點，例如 `ws://localhost:8088/ws` |
| `KAFKA_BROKER` | 依 preset | 從**宿主**連得到的 broker，例如 `localhost:9094` |
| `JWT_SECRET` | 本 repo demo 那把 | 簽 token 用的原始字串（**不是 base64**） |
| `CONTRACT_TIMEOUT_MS` | `15000` | 等一則推播的上限 |
| `CONTRACT_DEBUG` | `false` | 印出 STOMP 帧與 kafkajs 日誌 |

`CONTRACT_TARGET` 的 preset 只有埠號、**沒有 secret**——團隊 repo 的那把是
別人 repo 的東西，抄進這個公開 repo 等於把它散佈出去。

## 埠號為什麼是 9094

本 repo 的 demo Kafka 對宿主開的是 **9094**，團隊 repo 的是 9092。
兩套要能**同時跑**才做得成 A/B 對照，所以每一個對外埠都刻意錯開
（服務 8088 vs 8087、Prometheus 9091 vs 9090、Grafana 3001 vs 3000）。

---

## 測了什麼

| # | 案例 | 為什麼值得測 |
|---|---|---|
| 1 | CONNECT 成功後回 STOMP 1.2 的 CONNECTED | 握手的最低門檻 |
| 2 | `game.result` → 私人推播，body 是**重組過**的通知 | 三個 topic 只有這個不是原事件回吐；原樣轉推是最直覺也最容易犯的錯 |
| 3 | 同上：**不含 `playerId`**、`type` 在第一個 | `playerId` 是路由用的不是內容；前端靠 `type` 分流 |
| 4 | `game.result` 缺 `playerId` → 略過，不廣播 | 上游的合法變異，不該變成「推給所有人」 |
| 5 | 私人推播**不會外洩**給其他玩家 | 推播服務最基本的安全性質 |
| 6 | `notification.push` 帶 / 不帶 `targetPlayerId` → 私人 / 廣播 | 兩種路由 |
| 7 | `null` 欄位是「存在且為 null」，不是消失 | 前端看不出差別，但那是**逐位元組等價**的一部分 |
| 8 | `rank.update` → `/topic/rank`，原事件回吐、`updatedAt` 不溢位 | Java 那邊是 `long` |
| 9 | MESSAGE 帧一定帶 `subscription` 與 `message-id` | 漏了的話 `@stomp/stompjs` **靜默丟棄**，兩邊都不報錯 |
| 10 | JSON **不做 HTML 轉義**，非 ASCII 也不轉義 | Go 的 `encoding/json` 預設會轉，Jackson 不會；解析後相同、位元組不同 |
| 11~15 | 簽章錯 / 過期 / `alg: none` / 不是 JWT / 沒帶憑證 → ERROR 且斷線 | `alg: none` 是最經典的 JWT 漏洞 |

**刻意不斷言**的東西也一樣重要：CONNECTED 的 `heart-beat` 值（兩邊的心跳
能力本來就可以不同，規格就是為此才要協商）、session id 格式、ERROR 帧的
文字內容（Java 是 Spring 產生的英文、Go 是自己寫的中文）。把那些寫進斷言
等於**把不是契約的東西當成契約**，會讓測試在無害的差異上變紅。

## 實測結果

2026-07-31，同一台機器、同一份 `contract.test.js`：

```
契約測試目標：go　WS=ws://localhost:8088/ws　Kafka=localhost:9094
ℹ tests 14
ℹ pass 14
ℹ fail 0

契約測試目標：java　WS=ws://localhost:8087/ws　Kafka=localhost:9092
ℹ tests 14
ℹ pass 14
ℹ fail 0
```

Go 版這一組也**跑在 CI 裡**（`.github/workflows/ci.yml` 的 `compose 煙霧測試`
job），所以它不會因為之後某次改動而無聲失效。Java 版那一組沒進 CI——
那需要團隊 repo 的原始碼，而本 repo 對它是唯讀參考。
