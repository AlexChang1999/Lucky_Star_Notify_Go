module github.com/AlexChang1999/lucky-star-notify-go

// 1.24 是「最低要求」不是「鎖定版本」——裝更新的 toolchain 一樣能編。
// 目前無外部依賴：STOMP 協議層完全用標準庫實作（見 AGENTS.md §0）。
// gorilla/websocket、kafka-go、golang-jwt 等依賴在 Phase 1~3 才會加進來。
go 1.24
