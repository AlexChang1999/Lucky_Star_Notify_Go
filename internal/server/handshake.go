package server

import (
	"errors"
	"strings"

	"github.com/AlexChang1999/lucky-star-notify-go/internal/auth"
	"github.com/AlexChang1999/lucky-star-notify-go/internal/stomp"
)

// StompVersion 是本服務支援的唯一 STOMP 版本。
//
// 只做 1.2 是刻意的取捨（見 ADR-002）：1.0 沒有心跳、1.1 的 header 轉義規則
// 與 1.2 不同，同時支援三個版本要在編解碼層帶著版本旗標到處跑。
// 前端用的 @stomp/stompjs 一定支援 1.2，所以支援單一版本沒有實際損失。
const StompVersion = "1.2"

// serverHeaderValue 是 CONNECTED 帧的 server header。
// 規格說 SHOULD 帶，格式是 name/version。除錯時能一眼看出對面是哪一版。
const serverHeaderValue = "lucky-star-notify-go/0.1.0"

// handshakeResult 是握手的結論。
//
// 不論成功或失敗都有 Reply（CONNECTED 或 ERROR），呼叫端只要「送出 Reply，
// 然後看 OK 決定要不要繼續」。把「決定」與「I/O」分開，是為了讓協議邏輯
// 能被純函式測試——不必開一條真的 WebSocket 才測得到版本協商。
type handshakeResult struct {
	// PlayerID 是驗章後取到的 JWT subject；失敗時為空字串。
	PlayerID string
	// Reply 是要送回 client 的帧。
	Reply *stomp.Frame
	// OK 為 true 表示握手成功，連線可以繼續。
	OK bool
}

// handshake 檢視 client 的第一個帧，決定要回 CONNECTED 還是 ERROR。
//
// 檢查順序是刻意的：先確認「這是不是一個 CONNECT」、再確認「版本談得成」、
// 最後才驗章。理由是**便宜的檢查先做**，而且版本談不成時就算 token 有效
// 也沒意義。
func handshake(verifier *auth.Verifier, frame *stomp.Frame, sessionID string) handshakeResult {
	// STOMP 是 CONNECT 的 1.1+ 別名，兩個都收。
	if frame.Command != stomp.CommandConnect && frame.Command != stomp.CommandStomp {
		return handshakeResult{Reply: errorFrame(
			"Expected CONNECT frame",
			"連線建立後的第一個帧必須是 CONNECT 或 STOMP，收到的是 "+string(frame.Command),
		)}
	}

	if !acceptsVersion(frame.Headers.Get(stomp.HeaderAcceptVersion), StompVersion) {
		// 規格要求版本談不成時，ERROR 要帶上伺服器支援的版本清單，
		// client 才知道該降級到什麼。
		return handshakeResult{Reply: errorFrame(
			"Unsupported STOMP version",
			"本服務只支援 STOMP "+StompVersion,
			stomp.Header{Name: stomp.HeaderVersion, Value: StompVersion},
		)}
	}

	// 刻意**不**檢查 host header。規格 1.2 說 CONNECT MUST 帶 host，
	// 但 Java 版（Spring）不檢查，強制檢查會讓 Go 版拒絕 Java 版接受的
	// client。行為一致優先於規格潔癖——這個取捨要寫在註解裡，
	// 否則下一個人會以為是漏掉了。

	playerID, err := verifier.PlayerID(auth.StripBearer(frame.Headers.Get(stomp.HeaderAuthorization)))
	if err != nil {
		// ⚠️ 對外一律回同一句 "Unauthorized"，不區分「沒帶 token」、
		// 「過期」、「簽章錯」。差異只寫進伺服器的 log。
		// 理由：把失敗原因告訴對方，等於免費幫攻擊者做預言機
		// （能分辨「這個 token 曾經有效但過期了」是有價值的情報）。
		detail := "缺少或無效的 Authorization header"
		if errors.Is(err, auth.ErrNoSubject) {
			// 這一種例外：簽章是對的，代表對面是我們自己的發證端，
			// 但 token 形狀變了。這種錯誤要讓對方看得到，否則沒人查得出來。
			detail = "token 通過驗章但缺少 subject"
		}
		return handshakeResult{Reply: errorFrame("Unauthorized", detail)}
	}

	return handshakeResult{
		PlayerID: playerID,
		OK:       true,
		Reply: &stomp.Frame{
			Command: stomp.CommandConnected,
			Headers: stomp.Headers{
				{Name: stomp.HeaderVersion, Value: StompVersion},
				{Name: "session", Value: sessionID},
				{Name: "server", Value: serverHeaderValue},
				// heart-beat:0,0 = 「我不送心跳，也不要求你送」。
				//
				// Phase 1 刻意這樣宣告，因為心跳的收發還沒實作。
				// 如果這裡照著 client 的要求回一個非零值，client 會在
				// 等不到心跳時主動斷線——那是一個「握手看起來成功、
				// 幾秒後莫名斷線」的 bug，比誠實回 0,0 難查太多。
				// Phase 2 實作心跳後才改成真正的協商結果。
				{Name: stomp.HeaderHeartBeat, Value: "0,0"},
			},
		},
	}
}

// acceptsVersion 回報 client 的 accept-version 清單是否包含 want。
//
// accept-version 的格式是逗號分隔的遞增版本清單，例如 "1.0,1.1,1.2"。
// header 不存在時回傳 false：那代表對面是 STOMP 1.0 的 client
// （1.0 沒有這個 header），而我們不支援 1.0。
func acceptsVersion(header, want string) bool {
	for version := range strings.SplitSeq(header, ",") {
		if strings.TrimSpace(version) == want {
			return true
		}
	}
	return false
}

// errorFrame 組出一個 ERROR 帧。
//
// message header 放**簡短、給機器看**的原因；body 放給人看的細節。
// 規格建議 ERROR 帶 body 說明，而 @stomp/stompjs 會把 message header
// 丟進 onStompError 的 callback，前端能直接顯示。
//
// 注意 ERROR **不在**免轉義的例外清單裡（只有 CONNECT/CONNECTED 是），
// 所以 message 裡若含冒號會被 Marshal 自動轉成 \c——這是對的，不必自己處理。
func errorFrame(message, detail string, extra ...stomp.Header) *stomp.Frame {
	headers := stomp.Headers{
		{Name: stomp.HeaderMessage, Value: message},
		{Name: stomp.HeaderContentType, Value: "text/plain;charset=UTF-8"},
	}
	headers = append(headers, extra...)
	return &stomp.Frame{
		Command: stomp.CommandError,
		Headers: headers,
		Body:    []byte(detail),
	}
}
