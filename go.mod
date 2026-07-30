module github.com/AlexChang1999/lucky-star-notify-go

// 這是「最低要求」不是「鎖定版本」——裝更新的 toolchain 一樣能編。
//
// 原本寫 1.24，Phase 1 引入 prometheus/client_golang v1.24.1 時被它的
// go directive 推到 1.25.0（Go 的模組規則：主模組的版本不得低於任一依賴）。
// 這代表最低 toolchain 要求跟著提高，不是我們主動選的。
//
// ★ STOMP 協議層仍然完全用標準庫實作（internal/stomp 零外部依賴），
// 這是本專案的價值所在，不要為了省事換成第三方套件。
// kafka-go 在 Phase 3 才會加進來。
go 1.25.0

require (
	github.com/golang-jwt/jwt/v5 v5.3.1
	github.com/gorilla/websocket v1.5.3
	github.com/prometheus/client_golang v1.24.1
	github.com/segmentio/kafka-go v0.4.51
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/klauspost/compress v1.19.1 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.70.1 // indirect
	github.com/prometheus/procfs v0.21.1 // indirect
	golang.org/x/sys v0.47.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
