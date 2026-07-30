// Package stomp 實作 STOMP 1.2 協議中「純推播伺服器」所需的子集。
//
// # 為什麼自己寫
//
// Go 生態沒有成熟的 STOMP *伺服器* 實作（現有套件多為 client 端），
// 而本服務只需要一小塊子集：接受連線、處理訂閱、把訊息推出去。
// 與其硬套一個不合身的第三方套件，不如照規格實作需要的部分。
//
// # 範圍
//
// 解析端接受所有合法的 STOMP 指令（讓「不支援」與「格式錯誤」是兩種不同的錯誤，
// 由上層決定回什麼 ERROR 訊息）；產生端只用得到 CONNECTED / MESSAGE / RECEIPT / ERROR。
//
// 規格：https://stomp.github.io/stomp-specification-1.2.html
package stomp

import (
	"bytes"
	"strconv"
	"strings"
)

// Command 是 STOMP 帧的指令名稱。規格定義它區分大小寫、一律大寫。
type Command string

// Client → Server 指令。
const (
	CommandConnect     Command = "CONNECT"
	CommandStomp       Command = "STOMP" // 1.1 起的 CONNECT 別名
	CommandSend        Command = "SEND"
	CommandSubscribe   Command = "SUBSCRIBE"
	CommandUnsubscribe Command = "UNSUBSCRIBE"
	CommandAck         Command = "ACK"
	CommandNack        Command = "NACK"
	CommandBegin       Command = "BEGIN"
	CommandCommit      Command = "COMMIT"
	CommandAbort       Command = "ABORT"
	CommandDisconnect  Command = "DISCONNECT"
)

// Server → Client 指令。
const (
	CommandConnected Command = "CONNECTED"
	CommandMessage   Command = "MESSAGE"
	CommandReceipt   Command = "RECEIPT"
	CommandError     Command = "ERROR"
)

// validCommands 是所有合法指令的集合。
//
// 這裡刻意收錄「本服務不支援」的指令（SEND / ACK / BEGIN …）：
// 解析層只負責判斷「這是不是一個合法的 STOMP 帧」，
// 「支不支援」是上層的策略決定——分開才能回出有意義的錯誤訊息。
var validCommands = map[Command]struct{}{
	CommandConnect: {}, CommandStomp: {}, CommandSend: {},
	CommandSubscribe: {}, CommandUnsubscribe: {},
	CommandAck: {}, CommandNack: {},
	CommandBegin: {}, CommandCommit: {}, CommandAbort: {},
	CommandDisconnect: {},
	CommandConnected:  {}, CommandMessage: {}, CommandReceipt: {}, CommandError: {},
}

// Valid 回報 c 是否為規格定義的指令。
func (c Command) Valid() bool {
	_, ok := validCommands[c]
	return ok
}

// 常用 header 名稱。STOMP 的 header 名稱**區分大小寫**。
const (
	HeaderAcceptVersion = "accept-version"
	HeaderVersion       = "version"
	HeaderHeartBeat     = "heart-beat"
	HeaderHost          = "host"
	HeaderDestination   = "destination"
	HeaderID            = "id"
	HeaderSubscription  = "subscription"
	HeaderMessageID     = "message-id"
	HeaderContentType   = "content-type"
	HeaderContentLength = "content-length"
	HeaderReceipt       = "receipt"
	HeaderReceiptID     = "receipt-id"
	HeaderMessage       = "message"

	// HeaderAuthorization 不是 STOMP 規格的一部分，是應用層自訂的鑑權 header。
	// 大寫 A 是刻意的：Java 版讀的是 getNativeHeader("Authorization")，
	// 而前端 @stomp/stompjs 的 connectHeaders 會原樣送出。**改成小寫會驗不到。**
	HeaderAuthorization = "Authorization"
)

// Header 是單一 header 條目。
//
// 用 slice of struct 而不是 map，有兩個理由：
//  1. 規格允許重複的 header 名稱，且明訂「以第一個為準」——map 會把重複的蓋掉。
//  2. 產生帧時 header 的順序是穩定的，測試才好比對。
type Header struct {
	Name  string
	Value string
}

// Headers 是有序的 header 清單。
type Headers []Header

// Lookup 回傳第一個名為 name 的 header 值。
//
// 「第一個」是規格要求（1.2 §2.1）：重複的 header 只有第一個有效。
// 這不只是細節——它是一個安全性質：惡意 client 若補送第二個
// Authorization，也蓋不掉第一個。
func (h Headers) Lookup(name string) (string, bool) {
	for _, entry := range h {
		if entry.Name == name {
			return entry.Value, true
		}
	}
	return "", false
}

// Get 回傳第一個名為 name 的 header 值，不存在時回傳空字串。
func (h Headers) Get(name string) string {
	value, _ := h.Lookup(name)
	return value
}

// Has 回報是否存在名為 name 的 header。
func (h Headers) Has(name string) bool {
	_, ok := h.Lookup(name)
	return ok
}

// Add 附加一個 header 條目，允許重複名稱。
func (h *Headers) Add(name, value string) {
	*h = append(*h, Header{Name: name, Value: value})
}

// Frame 是一個 STOMP 帧。
//
// Body 只有 SEND / MESSAGE / ERROR 用得到；其餘指令規格禁止帶 body。
// 這裡**不強制**該限制——編解碼層保持「照實搬運」，
// 語意驗證屬於上層 handler 的職責（分層乾淨，也讓測試能構造畸形帧）。
type Frame struct {
	Command Command
	Headers Headers
	Body    []byte
}

// escapesHeaders 回報該指令的 header 是否需要轉義。
//
// 規格 1.2 §3.1 的例外條款：CONNECT 與 CONNECTED 兩種帧**不**轉義
// CR / LF / 冒號，以維持與 STOMP 1.0 的相容性。
//
// STOMP 指令（CONNECT 的 1.1+ 別名）不在例外之列——它本來就只存在於
// 1.1 之後，沒有向下相容的包袱，所以照常轉義。
func escapesHeaders(c Command) bool {
	return c != CommandConnect && c != CommandConnected
}

// escapeHeaderOctets 依規格 1.2 §3.1 轉義 header 的 name 或 value。
//
// 只有這四個字元需要轉義，且**只在 header，不在 body**：
//
//	\  →  \\      (必須最先處理，否則會把自己產生的反斜線再轉一次)
//	\r →  \r
//	\n →  \n
//	:  →  \c
//
// 逐 byte 走訪而非逐 rune 是正確的：這四個字元都是 ASCII，
// 而 UTF-8 的後續位元組一律 >= 0x80，會原封不動落入 default 分支。
func escapeHeaderOctets(s string) string {
	if !strings.ContainsAny(s, "\\\r\n:") {
		return s // 快路徑：絕大多數 header 不含這些字元，免去配置
	}

	var b strings.Builder
	b.Grow(len(s) + 8)
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			b.WriteString(`\\`)
		case '\r':
			b.WriteString(`\r`)
		case '\n':
			b.WriteString(`\n`)
		case ':':
			b.WriteString(`\c`)
		default:
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

// unescapeHeaderOctets 還原 escapeHeaderOctets 的轉義。
//
// 規格明訂：**未定義的轉義序列是致命協議錯誤**（例如 \t）。
// 所以這裡不能「看不懂就原樣保留」——那會讓惡意 client 能繞過轉義規則。
func unescapeHeaderOctets(s string) (string, error) {
	if !strings.Contains(s, `\`) {
		return s, nil // 快路徑
	}

	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' {
			b.WriteByte(s[i])
			continue
		}

		i++
		if i >= len(s) {
			return "", newProtocolError(ErrUnknownEscape, "header 以落單的反斜線結尾")
		}
		switch s[i] {
		case 'r':
			b.WriteByte('\r')
		case 'n':
			b.WriteByte('\n')
		case 'c':
			b.WriteByte(':')
		case '\\':
			b.WriteByte('\\')
		default:
			return "", newProtocolError(ErrUnknownEscape, `未定義的轉義序列 \`+string(s[i]))
		}
	}
	return b.String(), nil
}

// Marshal 把帧序列化成可直接送出的位元組。
//
// 格式：
//
//	COMMAND\n
//	header:value\n
//	...
//	\n
//	body\x00
//
// body 非空且呼叫端未自行指定時，會自動補上 content-length。
// 規格建議帶（SHOULD），而且帶了之後 body 就能合法包含 NULL 位元組。
func (f *Frame) Marshal() []byte {
	var b bytes.Buffer
	// 預估容量：指令 + 每個 header 約 32 bytes + body + 結尾，減少重新配置
	b.Grow(len(f.Command) + len(f.Headers)*32 + len(f.Body) + 8)

	b.WriteString(string(f.Command))
	b.WriteByte('\n')

	escape := escapesHeaders(f.Command)
	for _, entry := range f.Headers {
		if escape {
			b.WriteString(escapeHeaderOctets(entry.Name))
			b.WriteByte(':')
			b.WriteString(escapeHeaderOctets(entry.Value))
		} else {
			b.WriteString(entry.Name)
			b.WriteByte(':')
			b.WriteString(entry.Value)
		}
		b.WriteByte('\n')
	}

	if len(f.Body) > 0 && !f.Headers.Has(HeaderContentLength) {
		b.WriteString(HeaderContentLength)
		b.WriteByte(':')
		b.WriteString(strconv.Itoa(len(f.Body)))
		b.WriteByte('\n')
	}

	b.WriteByte('\n') // header 區與 body 的分隔空行
	b.Write(f.Body)
	b.WriteByte(0) // NULL 結尾，不是換行
	return b.Bytes()
}

// String 讓帧在測試失敗訊息與除錯日誌裡看得懂。
// NULL 位元組印成 ^@，免得終端機吃掉它。
func (f *Frame) String() string {
	return strings.ReplaceAll(string(f.Marshal()), "\x00", "^@")
}

// ErrorFrame 組出一個 ERROR 帧。
//
// message header 放**簡短、給機器看**的原因；body 放給人看的細節。
// 規格建議 ERROR 帶 body 說明，而 @stomp/stompjs 會把 message header
// 丟進 onStompError 的 callback，前端能直接顯示。
//
// 注意 ERROR **不在**免轉義的例外清單裡（只有 CONNECT/CONNECTED 是），
// 所以 message 裡若含冒號會被 Marshal 自動轉成 \c——這是對的，不必自己處理。
//
// 放在 stomp 而不是各自的呼叫端，是因為 server（握手失敗）與 hub
// （連線建立後的協議錯誤、關機通知）都要用；ERROR 帧的形狀是協議層的事。
func ErrorFrame(message, detail string, extra ...Header) *Frame {
	headers := Headers{
		{Name: HeaderMessage, Value: message},
		{Name: HeaderContentType, Value: "text/plain;charset=UTF-8"},
	}
	headers = append(headers, extra...)
	return &Frame{
		Command: CommandError,
		Headers: headers,
		Body:    []byte(detail),
	}
}
