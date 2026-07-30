package main

import (
	_ "embed"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

// indexHTML 是 demo 頁面，用 go:embed 編進執行檔。
//
// # 為什麼要 embed 而不是 COPY 進映像
//
// 執行檔自帶頁面，映像就還是「只有一個檔案」，Dockerfile 兩個 cmd 可以
// 共用同一份（見根目錄 Dockerfile 的 ARG CMD）。若改成 COPY 一個 static/
// 目錄，就得為 demo 另外寫一份 Dockerfile，還多一個「忘記 COPY 就 404」
// 的失敗模式。
//
// 代價是改 HTML 要重新編譯——對一頁 demo 來說完全可以接受。
//
//go:embed index.html
var indexHTML []byte

// newMux 組出 demo 的三個端點。
func newMux(cfg demoConfig, logger *slog.Logger) *http.ServeMux {
	mux := http.NewServeMux()

	// "GET /{$}" 的 {$} 表示「只匹配正好是 /」。
	// 沒有它，"GET /" 會變成萬用前綴，任何打錯的路徑都會拿到首頁而不是 404。
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexHTML)
	})

	mux.HandleFunc("GET /api/config", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{
			"wsUrl":   cfg.WSURL,
			"players": cfg.playersForPage(),
		})
	})

	mux.HandleFunc("GET /api/token", func(w http.ResponseWriter, r *http.Request) {
		handleToken(w, r, cfg, logger)
	})

	return mux
}

// playerView 是給頁面用的玩家資料。
//
// 不直接把 player 丟出去，是因為那個型別沒有 json tag，序列化出來會是
// 大寫開頭的 ID / Nickname。與其為了 demo 在事件那邊加 tag（那會讓人以為
// 那些 tag 與 Kafka 契約有關），不如在這裡明確轉一次。
type playerView struct {
	ID       string `json:"id"`
	Nickname string `json:"nickname"`
}

// playersForPage 把玩家名單轉成頁面要的形狀。
//
// ID 轉成字串是刻意的：JavaScript 的 number 是 float64，超過 2^53 的
// 玩家 ID 會被無聲地改值。這裡的 1001 當然沒事，但頁面上的 ID 是要拿去
// 跟推播內容比對的，型別從一開始就選字串可以讓這個問題永遠不會發生。
func (c demoConfig) playersForPage() []playerView {
	views := make([]playerView, 0, len(demoPlayers))
	for _, p := range demoPlayers {
		views = append(views, playerView{
			ID:       strconv.FormatInt(p.ID, 10),
			Nickname: p.Nickname,
		})
	}
	return views
}

// handleToken 簽一張 token 給頁面。
//
// ⚠️ 沒有任何鑑權——誰打這個端點都能拿到任意玩家的 token。
// 這是 demo 容器，不是服務的一部分，見 token.go 的說明。
func handleToken(w http.ResponseWriter, r *http.Request, cfg demoConfig, logger *slog.Logger) {
	raw := r.URL.Query().Get("playerId")
	playerID, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		http.Error(w, "playerId 必須是整數", http.StatusBadRequest)
		return
	}

	// 只發給名單上的玩家。
	//
	// 這不是安全措施（這個端點本來就沒有安全性可言），而是**除錯輔助**：
	// 打錯 ID 時立刻拿到 400，而不是拿到一張有效但沒有任何事件會送達的
	// token，然後對著一片空白的頁面猜哪裡壞了。
	if !isDemoPlayer(playerID) {
		http.Error(w, "playerId 不在 demo 名單內", http.StatusBadRequest)
		return
	}

	token, err := mintToken(cfg.JWTSecret, playerID, cfg.TokenTTL, time.Now())
	if err != nil {
		logger.Error("簽發 token 失敗", "playerID", playerID, "err", err)
		http.Error(w, "簽發 token 失敗", http.StatusInternalServerError)
		return
	}

	logger.Info("簽發 demo token", "playerID", playerID, "ttl", cfg.TokenTTL.String())
	writeJSON(w, map[string]string{"token": token})
}

func isDemoPlayer(id int64) bool {
	for _, p := range demoPlayers {
		if p.ID == id {
			return true
		}
	}
	return false
}

// writeJSON 送出 JSON 回應。
//
// Encode 的錯誤刻意忽略：走到這裡 header 已經送出去了，除了記 log 之外
// 什麼都做不了，而這是 demo 程式，log 的價值不足以換一層額外的包裝。
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(v)
}
