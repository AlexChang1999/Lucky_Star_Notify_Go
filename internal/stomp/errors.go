package stomp

import "errors"

// 協議錯誤的哨兵值。用 errors.Is 比對，不要比對字串。
//
// 規格對「致命協議錯誤」的要求是：伺服器回一個 ERROR 帧然後關閉連線。
// 所以這些錯誤在上層都會導向同一個處置，分這麼細的意義在於**日誌可診斷**——
// 出事時要能一眼看出是 client 送了什麼樣的壞東西。
var (
	// ErrInvalidCommand：指令不是規格定義的任何一個。
	ErrInvalidCommand = errors.New("stomp: 非法指令")

	// ErrMalformedHeader：header 行缺冒號，或名稱為空。
	ErrMalformedHeader = errors.New("stomp: header 格式錯誤")

	// ErrUnknownEscape：header 含規格未定義的轉義序列（如 \t）。
	// 規格明訂這是致命錯誤，不可寬容處理。
	ErrUnknownEscape = errors.New("stomp: 未定義的轉義序列")

	// ErrFrameTooLarge：帧超過 Limits 設定的上限。
	// 這是阻擋記憶體耗盡攻擊的那道門——沒有它，一個 client 送個
	// 沒有 NULL 結尾的無限長 body 就能把伺服器吃垮。
	ErrFrameTooLarge = errors.New("stomp: 帧超過大小上限")

	// ErrMissingNull：帧沒有以 NULL 位元組結尾（含串流提早結束）。
	ErrMissingNull = errors.New("stomp: 帧未以 NULL 結尾")

	// ErrInvalidContentLength：content-length 不是非負整數。
	ErrInvalidContentLength = errors.New("stomp: content-length 非法")

	// ErrMultipleFrames：一則 WebSocket 訊息裡塞了不只一個帧。
	// 只有 Unmarshal 會回傳它——Decoder 是串流式的，多個帧本來就合法。
	ErrMultipleFrames = errors.New("stomp: 單一訊息含多個帧")
)

// ErrHeartbeat 表示讀到的是心跳（單獨的 EOL）而非帧。
//
// 這不是錯誤，是一個**訊號**：呼叫端應該把它當作「對方還活著」，
// 重設讀取逾時後繼續，而不是關閉連線。用 error 傳遞是因為它出現在
// 回傳 (*Frame, error) 的位置上，且必須與「解析失敗」明確區分。
var ErrHeartbeat = errors.New("stomp: 心跳")

// ProtocolError 包裝一個哨兵錯誤並附上診斷細節。
//
// 上層可以用 errors.As 攔截它來決定「回 ERROR 帧並斷線」，
// 同時用 errors.Is 比對 Kind 判斷是哪一類問題。
type ProtocolError struct {
	Kind   error  // 上面的哨兵之一
	Detail string // 人看的細節，會進日誌
}

func newProtocolError(kind error, detail string) *ProtocolError {
	return &ProtocolError{Kind: kind, Detail: detail}
}

func (e *ProtocolError) Error() string {
	if e.Detail == "" {
		return e.Kind.Error()
	}
	return e.Kind.Error() + "：" + e.Detail
}

// Unwrap 讓 errors.Is(err, ErrUnknownEscape) 之類的比對成立。
func (e *ProtocolError) Unwrap() error { return e.Kind }
