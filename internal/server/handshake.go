package server

import (
	"errors"
	"strconv"
	"strings"
	"time"

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

// 伺服器對心跳的**自我宣告**（規格 §heart-beating 的 <sx>,<sy>）。
//
//   - serverHeartbeatSend（sx）＝「我最快能每 N 毫秒送一次」
//   - serverHeartbeatWant（sy）＝「我希望你每 N 毫秒送一次」
//
// 10 秒是對齊 @stomp/stompjs 的預設值（10000/10000），談出來的結果就會是
// 雙向各 10 秒，不會因為兩邊期望不同而變成奇怪的間隔。
//
// ⚠️ 這兩個值宣告出去就**必須**做到：宣告了非零的 sx 卻不送心跳，
// client 等不到就會自己斷線，症狀是「握手成功、幾秒後莫名斷線」（AGENTS.md §2.13）。
// 真正在送的是 hub.Session 的 write goroutine——改動這裡之前先確認那邊還在送。
const (
	serverHeartbeatSend = 10 * time.Second
	serverHeartbeatWant = 10 * time.Second
)

// heartbeat 是協商後的結果，單位是實際的時間間隔。
//
// 零值代表「該方向不啟用」——規格用 0 表達這件事，Go 這邊剛好
// time.Duration(0) 同義，不必另外用 bool 標記。
type heartbeat struct {
	// Send 是伺服器要送心跳給 client 的間隔；0 = 不送。
	Send time.Duration
	// Recv 是伺服器期待 client 送心跳過來的間隔；0 = 不要求（讀取不設逾時）。
	Recv time.Duration
}

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
	// Heartbeat 是協商後的雙向心跳間隔；握手失敗時為零值。
	Heartbeat heartbeat
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
		PlayerID:  playerID,
		OK:        true,
		Heartbeat: negotiateHeartbeat(frame.Headers.Get(stomp.HeaderHeartBeat)),
		Reply: &stomp.Frame{
			Command: stomp.CommandConnected,
			Headers: stomp.Headers{
				{Name: stomp.HeaderVersion, Value: StompVersion},
				{Name: "session", Value: sessionID},
				{Name: "server", Value: serverHeaderValue},
				// CONNECTED 帶的是**伺服器自己的** sx,sy，不是協商結果。
				// 協商是兩邊各自用同一個公式算出來的（見 negotiateHeartbeat），
				// 規格沒有「把結果告訴對方」這一步。
				{Name: stomp.HeaderHeartBeat, Value: serverHeartbeatHeader()},
			},
		},
	}
}

// serverHeartbeatHeader 產生 CONNECTED 用的 "sx,sy" 字串。
func serverHeartbeatHeader() string {
	return strconv.FormatInt(serverHeartbeatSend.Milliseconds(), 10) + "," +
		strconv.FormatInt(serverHeartbeatWant.Milliseconds(), 10)
}

// negotiateHeartbeat 依規格算出雙向的實際心跳間隔。
//
// client 在 CONNECT 送 "cx,cy"：
//
//   - cx = client 保證最快能送的間隔（client → server）
//   - cy = client 希望收到的間隔（server → client）
//
// 規格的公式是「任一邊為 0 就關掉該方向，否則取兩者較大值」：
//
//	server → client： cy==0 || sx==0 ? 0 : max(cy, sx)
//	client → server： cx==0 || sy==0 ? 0 : max(cx, sy)
//
// 取 max 而不是 min 的理由值得記住：兩個數字的語意不同（一邊是「我最快
// 只能這麼快」、一邊是「我希望這麼頻繁」），能同時滿足雙方的只有較大值。
//
// header 缺漏或格式不正確時一律當成 "0,0"（＝不啟用）。規格把格式錯誤
// 算成協議錯誤，但這裡選擇寬容：斷線的代價遠大於少一組心跳，而且
// 「談不成心跳」本身不會造成任何不一致。
func negotiateHeartbeat(header string) heartbeat {
	cx, cy, ok := parseHeartbeatHeader(header)
	if !ok {
		return heartbeat{}
	}
	return heartbeat{
		Send: pickInterval(cy, serverHeartbeatSend),
		Recv: pickInterval(cx, serverHeartbeatWant),
	}
}

// pickInterval 實作規格的 "0 → 關閉，否則取 max" 規則。
func pickInterval(theirs, ours time.Duration) time.Duration {
	if theirs <= 0 || ours <= 0 {
		return 0
	}
	return max(theirs, ours)
}

// parseHeartbeatHeader 解析 "cx,cy"，值的單位是毫秒。
//
// ok 為 false 代表格式不合（缺逗號、不是數字、負數），呼叫端當成 0,0 處理。
func parseHeartbeatHeader(header string) (cx, cy time.Duration, ok bool) {
	rawX, rawY, found := strings.Cut(header, ",")
	if !found {
		return 0, 0, false
	}
	msX, errX := strconv.Atoi(strings.TrimSpace(rawX))
	msY, errY := strconv.Atoi(strings.TrimSpace(rawY))
	if errX != nil || errY != nil || msX < 0 || msY < 0 {
		return 0, 0, false
	}
	return time.Duration(msX) * time.Millisecond, time.Duration(msY) * time.Millisecond, true
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

// errorFrame 是 stomp.ErrorFrame 的套件內短名。
//
// 本檔與 server.go 用它的次數夠多，留一個薄包裝比每處寫全名好讀；
// 實作在 stomp 是因為 hub 也要組 ERROR 帧（見 stomp.ErrorFrame 的說明）。
func errorFrame(message, detail string, extra ...stomp.Header) *stomp.Frame {
	return stomp.ErrorFrame(message, detail, extra...)
}
