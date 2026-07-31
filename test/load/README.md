# 壓測 client

建立 N 條 WebSocket/STOMP 長連線 → 對 Kafka 灌 R 輪廣播 → 量連線建立延遲與扇出延遲。

**同一支程式可以壓 Java 版與 Go 版**（`-target` 換埠號就好）。這是效能對照
能成立的前提：兩邊用同一支施壓程式、同一組參數，否則量出來的差異可能
只是兩支壓測工具的差異。

產出的數字進 [`docs/效能對照報告.md`](../../docs/效能對照報告.md)。

---

## 跑法

```bash
export JWT_SECRET='lucky-star-demo-secret-do-not-use-in-production'

# Go 版（預設值就是它）
go run ./test/load -conns 1000 -rounds 10

# Java 版
go run ./test/load -target ws://localhost:8087/ws -kafka localhost:9092 -conns 1000 -rounds 10

# 保持連線 60 秒，另開一個視窗量記憶體
go run ./test/load -conns 1000 -hold 60s
#   docker stats --no-stream
```

PowerShell 用 `$env:JWT_SECRET='...'`。

## 旗標

| 旗標 | 預設 | 說明 |
|---|---|---|
| `-target` | `ws://localhost:8088/ws` | WebSocket 端點 |
| `-kafka` | `localhost:9094` | 從**宿主**連得到的 broker（團隊 repo 的是 9092） |
| `-secret` | `$JWT_SECRET` | 簽 token 用的原始字串 |
| `-conns` | 100 | 連線數 |
| `-rate` | 200 | 每秒建立幾條；`<=0` 不限速 |
| `-rounds` | 10 | 廣播輪數 |
| `-interval` | 1s | 兩輪之間的間隔 |
| `-settle` | 2s | 全部連上後、開始廣播前的等待 |
| `-drain` | 3s | 最後一輪之後的收訊等待 |
| `-hold` | 0 | 連上後多停留一段時間，方便量 `docker stats` |
| `-out` | — | 把**全部原始樣本**寫成 JSON（`test/load/results/` 已被 gitignore） |

## 量的是什麼

**握手延遲** = `dial` 到收到 `CONNECTED` 為止，包含 TCP、WebSocket upgrade、
STOMP 握手與 JWT 驗章。也就是「一個玩家從按下連線到可以收訊息」的實際成本。

**扇出延遲** = 事件寫進 Kafka **前一刻**的時鐘，到 client 收到 `MESSAGE` 為止。
這一段包含 Kafka 的傳輸與消費，不是只有伺服器內部的扇出——刻意如此，
因為那才是玩家實際等到的時間。兩邊測的是同一條路徑，所以可以比較。

**應收 / 實收** 一定會印出來。扇出漏了就是漏了，不會被平均值蓋掉。

## 三個容易量錯的地方

1. **`kafka-go` 的 `Writer.BatchTimeout` 預設是 1 秒。**
   單獨寫一則訊息時它會在 producer 端等滿那一秒才送出，而延遲是用
   「寫入前一刻的時鐘」算的——那一秒會**整個算進扇出延遲**，量出來的
   p50 變成 1 秒出頭，看起來像伺服器很慢。這裡設了 `BatchSize: 1`。

2. **奈秒不是毫秒。** 事件的 `updatedAt` 放的是 `UnixNano`。本機的扇出延遲
   是個位數毫秒，用毫秒解析度會有一大半樣本擠在同一格。

3. **同機施壓會污染結果。** 施壓端自己要吃 CPU，尤其在幾千條連線時。
   報告裡必須標明是 co-located，而且**不可當作對外可引用的容量上限**。

## 為什麼百分位數不做插值

用的是**最近排名法**（第 `ceil(p/100 × N)` 個樣本）。插值會產生一個資料裡
不存在的數字；對延遲報告來說，「有一個請求真的花了這麼久」比「理論上的
分位點」誠實。Prometheus 的 `histogram_quantile` 會插值，所以兩邊的數字
本來就不該期待完全一致。

也**刻意不印平均值**：長尾一拉高它就跟著跑，既不代表典型體感（p50），
也不代表最糟情況（p99）。
