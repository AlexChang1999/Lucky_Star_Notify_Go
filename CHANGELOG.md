# CHANGELOG

本專案所有「會影響行為」的變更都記在這裡（規則見 `AGENTS.md` §3）。
最新的在最上面。

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
