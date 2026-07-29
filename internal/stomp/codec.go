package stomp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Limits 是解析時的資源上限。
//
// 這不是效能調校參數，是**安全邊界**：STOMP 的 body 預設讀到第一個 NULL 為止，
// 所以一個惡意 client 只要不送 NULL 就能讓伺服器無限配置記憶體。
// header 數量與行長同理。
type Limits struct {
	MaxLineLength int // 指令行與每一行 header 的長度上限
	MaxHeaders    int // 單一帧的 header 數量上限
	MaxBodySize   int // body 位元組上限
}

// DefaultLimits 對本服務的用途足夠寬鬆。
//
// 依據：最大的推播是 rank.update 的排行榜（100 筆 entries），
// 實測 JSON 約數十 KB，1 MiB 有充分餘裕。
var DefaultLimits = Limits{
	MaxLineLength: 8 * 1024,
	MaxHeaders:    64,
	MaxBodySize:   1 << 20, // 1 MiB
}

// Decoder 從位元組串流讀取 STOMP 帧。
//
// 它是**串流式**的：不假設一次讀取剛好等於一個帧，
// 所以同樣能處理「一次收到多個帧」與「帧被切成兩半」。
// 心跳（單獨的 EOL）會被自動略過。
type Decoder struct {
	r      *bufio.Reader
	limits Limits
}

// NewDecoder 用 DefaultLimits 建立解析器。
func NewDecoder(r io.Reader) *Decoder {
	return NewDecoderWithLimits(r, DefaultLimits)
}

// NewDecoderWithLimits 用指定的上限建立解析器（測試用得到）。
func NewDecoderWithLimits(r io.Reader, limits Limits) *Decoder {
	return &Decoder{r: bufio.NewReader(r), limits: limits}
}

// ReadFrame 讀出下一個帧。
//
// 心跳會被靜默略過（串流語意：呼叫端要的是下一個「帧」）。
// 串流正常結束時回傳 io.EOF。
func (d *Decoder) ReadFrame() (*Frame, error) {
	// 略過帧與帧之間的心跳。規格允許在帧之間出現任意數量的 EOL。
	var command string
	for {
		line, err := d.readLine()
		if err != nil {
			return nil, err
		}
		if line != "" {
			command = line
			break
		}
	}

	cmd := Command(command)
	if !cmd.Valid() {
		return nil, newProtocolError(ErrInvalidCommand, strconv.Quote(command))
	}

	headers, err := d.readHeaders(cmd)
	if err != nil {
		return nil, err
	}

	body, err := d.readBody(headers)
	if err != nil {
		return nil, err
	}

	return &Frame{Command: cmd, Headers: headers, Body: body}, nil
}

// readLine 讀一行並去掉行尾的 \n（以及選用的 \r）。
//
// 用 ReadSlice 迴圈而非 ReadString，是為了在**讀取過程中**就能擋住超長行；
// ReadString 會先把整行配置出來才輪到我們檢查，那時記憶體已經吃掉了。
func (d *Decoder) readLine() (string, error) {
	var buf []byte
	for {
		chunk, err := d.r.ReadSlice('\n')
		if len(buf)+len(chunk) > d.limits.MaxLineLength {
			return "", newProtocolError(ErrFrameTooLarge,
				fmt.Sprintf("單行超過 %d bytes", d.limits.MaxLineLength))
		}
		// ReadSlice 回傳的是內部緩衝的切片，下次讀取就失效，所以必須複製
		buf = append(buf, chunk...)

		if err == nil {
			break
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue // 這一行比 bufio 緩衝長，繼續讀
		}
		return "", err // io.EOF 或真正的 I/O 錯誤，原樣往上拋
	}

	buf = buf[:len(buf)-1] // 去掉 \n
	if len(buf) > 0 && buf[len(buf)-1] == '\r' {
		buf = buf[:len(buf)-1] // 去掉 \r（規格允許 \r\n 作為 EOL）
	}
	return string(buf), nil
}

// readHeaders 讀取 header 區，直到遇到空行。
func (d *Decoder) readHeaders(cmd Command) (Headers, error) {
	unescape := escapesHeaders(cmd)

	var headers Headers
	for {
		line, err := d.readLine()
		if err != nil {
			return nil, err
		}
		if line == "" {
			return headers, nil // 空行 = header 區結束
		}
		if len(headers) >= d.limits.MaxHeaders {
			return nil, newProtocolError(ErrFrameTooLarge,
				fmt.Sprintf("header 數量超過 %d", d.limits.MaxHeaders))
		}

		// 第一個冒號就是分隔符：name/value 裡的字面冒號在傳輸時已被轉義成 \c，
		// 所以不會有歧義。（CONNECT/CONNECTED 不轉義，那是規格自己承認的
		// 向下相容缺口——名稱含冒號在那兩種帧本來就無法表達。）
		name, value, found := strings.Cut(line, ":")
		if !found {
			return nil, newProtocolError(ErrMalformedHeader, "缺少冒號："+strconv.Quote(line))
		}
		if name == "" {
			return nil, newProtocolError(ErrMalformedHeader, "名稱為空："+strconv.Quote(line))
		}

		if !unescape {
			headers.Add(name, value)
			continue
		}
		decodedName, err := unescapeHeaderOctets(name)
		if err != nil {
			return nil, err
		}
		decodedValue, err := unescapeHeaderOctets(value)
		if err != nil {
			return nil, err
		}
		headers.Add(decodedName, decodedValue)
	}
}

// readBody 讀取 body。
//
// 兩種模式：
//   - 有 content-length：精準讀 N 個位元組，body 因此可以合法包含 NULL。
//   - 沒有 content-length：讀到第一個 NULL 為止。
func (d *Decoder) readBody(headers Headers) ([]byte, error) {
	if raw, ok := headers.Lookup(HeaderContentLength); ok {
		return d.readBodyWithLength(raw)
	}
	return d.readBodyUntilNull()
}

func (d *Decoder) readBodyWithLength(raw string) ([]byte, error) {
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return nil, newProtocolError(ErrInvalidContentLength, strconv.Quote(raw))
	}
	if n > d.limits.MaxBodySize {
		return nil, newProtocolError(ErrFrameTooLarge,
			fmt.Sprintf("content-length %d 超過 %d", n, d.limits.MaxBodySize))
	}

	body := make([]byte, n)
	if _, err := io.ReadFull(d.r, body); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return nil, newProtocolError(ErrMissingNull, "body 讀到一半串流就結束了")
		}
		return nil, err
	}

	terminator, err := d.r.ReadByte()
	if err != nil {
		return nil, newProtocolError(ErrMissingNull, "content-length 之後沒有 NULL")
	}
	if terminator != 0 {
		return nil, newProtocolError(ErrMissingNull,
			fmt.Sprintf("content-length 之後是 %#x 而非 NULL", terminator))
	}
	return body, nil
}

func (d *Decoder) readBodyUntilNull() ([]byte, error) {
	var buf []byte
	for {
		chunk, err := d.r.ReadSlice(0)
		// +1 是替結尾的 NULL 位元組留的
		if len(buf)+len(chunk) > d.limits.MaxBodySize+1 {
			return nil, newProtocolError(ErrFrameTooLarge,
				fmt.Sprintf("body 超過 %d bytes", d.limits.MaxBodySize))
		}
		buf = append(buf, chunk...)

		if err == nil {
			break
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) {
			return nil, newProtocolError(ErrMissingNull, "串流結束前未出現 NULL")
		}
		return nil, err
	}
	return buf[:len(buf)-1], nil // 去掉結尾的 NULL
}

// Unmarshal 從**一則完整訊息**解析出單一帧，供 WebSocket 路徑使用。
//
// STOMP over WebSocket 的慣例是一則 WS 訊息裝一個帧，
// 而心跳是一則只含 EOL 的訊息——後者回傳 ErrHeartbeat，
// 呼叫端據此重設讀取逾時即可，不必當成錯誤。
//
// 訊息尾端多餘的 EOL 會被容忍（部分 client 會補），
// 但若後面還跟著第二個帧則視為錯誤：那代表對端沒有遵守
// 一訊息一帧的慣例，靜默忽略掉第二個帧會變成很難查的漏訊息 bug。
func Unmarshal(data []byte) (*Frame, error) {
	if isOnlyEOL(data) {
		return nil, ErrHeartbeat
	}

	d := NewDecoder(bytes.NewReader(data))
	frame, err := d.ReadFrame()
	if err != nil {
		return nil, err
	}

	rest, err := io.ReadAll(d.r)
	if err != nil {
		return nil, err
	}
	if !isOnlyEOL(rest) {
		return nil, newProtocolError(ErrMultipleFrames,
			fmt.Sprintf("第一個帧之後還有 %d 個位元組", len(rest)))
	}
	return frame, nil
}

// isOnlyEOL 回報 data 是否只由 CR / LF 組成（空 slice 也算）。
func isOnlyEOL(data []byte) bool {
	for _, b := range data {
		if b != '\n' && b != '\r' {
			return false
		}
	}
	return true
}
