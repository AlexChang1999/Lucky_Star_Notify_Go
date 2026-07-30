package server

import (
	"encoding/json"
	"net/http"

	"github.com/AlexChang1999/lucky-star-notify-go/internal/hub"
)

// maxDebugBodySize 是 /debug/push 請求本體的上限。
//
// 與 STOMP 的 MaxBodySize 對齊（1 MiB）：這個端點模擬的就是 Kafka 事件，
// 讓它能灌的東西比真實路徑還大沒有意義，只會多一個記憶體耗盡的入口。
const maxDebugBodySize = 1 << 20

// debugPushRequest 是 POST /debug/push 的請求格式。
//
//	{
//	  "destination": "/topic/rank",   // 選填，PlayerID 有值時預設為私人頻道
//	  "playerId":    "42",            // 選填，有值 = 只推給這個玩家
//	  "body":        { ... }          // 必填，原樣當成 MESSAGE 的 body 送出
//	}
//
// 三條推播路徑都用同一個形狀表達，不必為每一種各開一個端點：
//
//	私人（game.result）        → playerId 有值
//	系統公告                    → destination=/topic/notifications
//	排行榜                      → destination=/topic/rank
type debugPushRequest struct {
	Destination string `json:"destination"`
	PlayerID    string `json:"playerId"`
	// Body 用 json.RawMessage 而不是 any：原樣搬運對方給的 JSON，
	// 不做 decode → encode 的來回。那個來回會改變欄位順序與數字表示法
	// （123 會變成 1.23e+02），而契約測試是逐位元組比對的。
	Body json.RawMessage `json:"body"`
}

// handleDebugPush 手動灌一則訊息，Phase 3 之前用來代替 Kafka。
//
// ⚠️ 這個端點**沒有鑑權**，由 DEBUG_PUSH 旗標控制是否註冊（見 config.Config）。
func (s *Server) handleDebugPush(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxDebugBodySize)

	var req debugPushRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "請求不是合法的 JSON："+err.Error(), http.StatusBadRequest)
		return
	}
	if len(req.Body) == 0 {
		http.Error(w, "缺少 body 欄位", http.StatusBadRequest)
		return
	}

	destination := req.Destination
	if destination == "" {
		if req.PlayerID == "" {
			http.Error(w, "destination 與 playerId 至少要給一個", http.StatusBadRequest)
			return
		}
		destination = hub.PrivateDestination
	}

	// 有 playerId 就走私人索引，否則廣播給所有訂閱該 destination 的連線。
	// 這正好對應 Java 版 notification.push 的分流規則：
	// targetPlayerId 非空 → convertAndSendToUser，空 → convertAndSend。
	var delivered int
	if req.PlayerID != "" {
		delivered = s.hub.PushToPlayer(req.PlayerID, destination, req.Body)
	} else {
		delivered = s.hub.Broadcast(destination, req.Body)
	}

	s.log.Info("除錯推播",
		"destination", destination, "playerID", req.PlayerID, "delivered", delivered)

	w.Header().Set("Content-Type", "application/json")
	// 回傳送出則數，讓手動驗證與整合測試都能直接斷言「有沒有推到人」，
	// 不必去翻日誌。0 則不算錯誤——沒人訂閱本來就是合法狀態。
	_ = json.NewEncoder(w).Encode(map[string]int{"delivered": delivered})
}
