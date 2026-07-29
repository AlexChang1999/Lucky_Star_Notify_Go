# Lucky Star Notify (Go)

> ⚠️ **建置中（Phase 1）。** 這份 README 目前只是骨架——
> 對外門面在 Phase 5 才會寫完（架構圖、demo GIF、效能對照、一行啟動）。
> 現階段請看 [`計畫說明書.md`](計畫說明書.md)。

用 Go 重寫 [Lucky Star Casino](https://github.com/AlexChang1999) 的即時推播服務：
**消費 Kafka 事件 → 透過 WebSocket/STOMP 推給線上玩家**。

原版是 Java 21 / Spring Boot 3.3.5。本專案的目標不是「換個語言寫一遍」，
而是證明兩件事：

1. **協議層自己能寫** — Go 生態沒有現成的 STOMP *伺服器* 實作，這裡照規格實作了需要的子集。
2. **兩個實作行為等價** — 同一份跨語言黑箱契約測試，對 Java 版與 Go 版都要通過。

## 目前進度

| Phase | 內容 | 狀態 |
|---|---|---|
| 0 | 前端脫離 SockJS（改原生 WebSocket） | ⬜ 未開始 |
| 1 | Go 骨架 + STOMP 帧編解碼 + JWT 驗章 + 握手 | 🔶 進行中 |
| 2 | Hub / 訂閱 / 推播 / 心跳 | ⬜ |
| 3 | Kafka 三個 consumer | ⬜ |
| 4 | 容器化 / 觀測性 / 獨立 compose / CI | ⬜ |
| 5 | 契約測試 / 效能對照 / ADR / README | ⬜ |

逐項 DoD 見 [`計畫說明書.md`](計畫說明書.md) §6。

## 開發

```bash
go vet ./...
go test -race ./...
go test -race -cover ./internal/stomp/
```

需要 **Go 1.24+**。其餘約定見 [`AGENTS.md`](AGENTS.md)。
