package stomp

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

// decodeOne 從字串解析單一帧，測試用。
func decodeOne(t *testing.T, input string) *Frame {
	t.Helper()
	frame, err := NewDecoder(strings.NewReader(input)).ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame(%q) 非預期錯誤：%v", input, err)
	}
	return frame
}

func TestDecoderReadFrame(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantCommand Command
		wantHeaders Headers
		wantBody    string
	}{
		{
			name:        "沒有 header 也沒有 body",
			input:       "DISCONNECT\n\n\x00",
			wantCommand: CommandDisconnect,
			wantHeaders: nil,
			wantBody:    "",
		},
		{
			name:        "CONNECT 握手",
			input:       "CONNECT\naccept-version:1.2\nhost:localhost\n\n\x00",
			wantCommand: CommandConnect,
			wantHeaders: Headers{
				{Name: HeaderAcceptVersion, Value: "1.2"},
				{Name: HeaderHost, Value: "localhost"},
			},
		},
		{
			// 規格允許 \r\n 當 EOL；某些 client 會用
			name:        "CRLF 行尾",
			input:       "CONNECT\r\naccept-version:1.2\r\n\r\n\x00",
			wantCommand: CommandConnect,
			wantHeaders: Headers{{Name: HeaderAcceptVersion, Value: "1.2"}},
		},
		{
			name:        "沒有 content-length 就讀到 NULL 為止",
			input:       "MESSAGE\ndestination:/topic/rank\n\nhello world\x00",
			wantCommand: CommandMessage,
			wantHeaders: Headers{{Name: HeaderDestination, Value: "/topic/rank"}},
			wantBody:    "hello world",
		},
		{
			// 有 content-length 時 body 可以合法包含 NULL——
			// 這正是規格要這個 header 的理由
			name:        "有 content-length 時 body 可含 NULL",
			input:       "MESSAGE\ncontent-length:3\n\na\x00b\x00",
			wantCommand: CommandMessage,
			wantHeaders: Headers{{Name: HeaderContentLength, Value: "3"}},
			wantBody:    "a\x00b",
		},
		{
			name:        "content-length 為 0",
			input:       "MESSAGE\ncontent-length:0\n\n\x00",
			wantCommand: CommandMessage,
			wantHeaders: Headers{{Name: HeaderContentLength, Value: "0"}},
			wantBody:    "",
		},
		{
			name:        "body 含換行",
			input:       "MESSAGE\n\nline1\nline2\x00",
			wantCommand: CommandMessage,
			wantBody:    "line1\nline2",
		},
		{
			// 重複的 header 全部保留，Lookup 才能實現「第一個為準」
			name:        "重複的 header 全部保留",
			input:       "SEND\nid:first\nid:second\n\n\x00",
			wantCommand: CommandSend,
			wantHeaders: Headers{
				{Name: "id", Value: "first"},
				{Name: "id", Value: "second"},
			},
		},
		{
			name:        "MESSAGE 的 header 會還原轉義",
			input:       "MESSAGE\nnick:玩家\\c暱稱\n\n\x00",
			wantCommand: CommandMessage,
			wantHeaders: Headers{{Name: "nick", Value: "玩家:暱稱"}},
		},
		{
			name:        "header 的名稱也會還原轉義",
			input:       "MESSAGE\na\\cb:v\n\n\x00",
			wantCommand: CommandMessage,
			wantHeaders: Headers{{Name: "a:b", Value: "v"}},
		},
		{
			// §3.1 例外：CONNECT 不做轉義，反斜線原樣保留
			name:        "CONNECT 的 header 不還原轉義",
			input:       "CONNECT\nlogin:a\\cb\n\n\x00",
			wantCommand: CommandConnect,
			wantHeaders: Headers{{Name: "login", Value: `a\cb`}},
		},
		{
			name:        "CONNECT 的 header 值可含原始冒號",
			input:       "CONNECT\nhost:example.com:61613\n\n\x00",
			wantCommand: CommandConnect,
			wantHeaders: Headers{{Name: HeaderHost, Value: "example.com:61613"}},
		},
		{
			name:        "header 值可為空",
			input:       "SEND\nreceipt:\n\n\x00",
			wantCommand: CommandSend,
			wantHeaders: Headers{{Name: HeaderReceipt, Value: ""}},
		},
		{
			name:        "前端送的 Authorization 是大寫 A",
			input:       "CONNECT\nAuthorization:Bearer abc.def.ghi\n\n\x00",
			wantCommand: CommandConnect,
			wantHeaders: Headers{{Name: HeaderAuthorization, Value: "Bearer abc.def.ghi"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := decodeOne(t, tt.input)

			if frame.Command != tt.wantCommand {
				t.Errorf("Command = %q，want %q", frame.Command, tt.wantCommand)
			}
			if !reflect.DeepEqual(frame.Headers, tt.wantHeaders) {
				t.Errorf("Headers\n got = %#v\nwant = %#v", frame.Headers, tt.wantHeaders)
			}
			if string(frame.Body) != tt.wantBody {
				t.Errorf("Body\n got = %q\nwant = %q", frame.Body, tt.wantBody)
			}
		})
	}
}

// 帧與帧之間的心跳（單獨的 EOL）必須被略過，不能被當成畸形帧。
func TestDecoderSkipsHeartbeats(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"帧之前有一個 LF", "\nDISCONNECT\n\n\x00"},
		{"帧之前有多個 LF", "\n\n\nDISCONNECT\n\n\x00"},
		{"帧之前有 CRLF", "\r\nDISCONNECT\n\n\x00"},
		{"LF 與 CRLF 混雜", "\n\r\n\nDISCONNECT\n\n\x00"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if frame := decodeOne(t, tt.input); frame.Command != CommandDisconnect {
				t.Errorf("Command = %q，want DISCONNECT", frame.Command)
			}
		})
	}
}

// 串流式解析：一次餵進多個帧要能逐一取出，最後乾淨地拿到 io.EOF。
func TestDecoderReadsSuccessiveFrames(t *testing.T) {
	input := "CONNECT\naccept-version:1.2\n\n\x00" +
		"\n" + // 中間夾一個心跳
		"SUBSCRIBE\nid:sub-0\ndestination:/topic/rank\n\n\x00" +
		"DISCONNECT\nreceipt:bye\n\n\x00"

	decoder := NewDecoder(strings.NewReader(input))
	want := []Command{CommandConnect, CommandSubscribe, CommandDisconnect}

	for i, wantCmd := range want {
		frame, err := decoder.ReadFrame()
		if err != nil {
			t.Fatalf("第 %d 個帧讀取失敗：%v", i+1, err)
		}
		if frame.Command != wantCmd {
			t.Errorf("第 %d 個帧的 Command = %q，want %q", i+1, frame.Command, wantCmd)
		}
	}

	if _, err := decoder.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Errorf("讀完之後 err = %v，want io.EOF", err)
	}
}

// 帧被切成兩半（跨兩次網路讀取）也要能正確拼回來。
func TestDecoderHandlesSplitFrame(t *testing.T) {
	full := "MESSAGE\ndestination:/topic/rank\ncontent-length:5\n\nhello\x00"

	for split := 1; split < len(full); split++ {
		reader := io.MultiReader(
			strings.NewReader(full[:split]),
			strings.NewReader(full[split:]),
		)
		frame, err := NewDecoder(reader).ReadFrame()
		if err != nil {
			t.Fatalf("在第 %d 個位元組切開時解析失敗：%v", split, err)
		}
		if string(frame.Body) != "hello" {
			t.Fatalf("在第 %d 個位元組切開時 Body = %q，want \"hello\"", split, frame.Body)
		}
	}
}

func TestDecoderErrors(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		limits  Limits
		wantErr error
	}{
		{
			name:    "指令不存在",
			input:   "FOO\n\n\x00",
			wantErr: ErrInvalidCommand,
		},
		{
			name:    "指令大小寫錯誤",
			input:   "connect\n\n\x00",
			wantErr: ErrInvalidCommand,
		},
		{
			name:    "header 缺冒號",
			input:   "SEND\nbadheader\n\n\x00",
			wantErr: ErrMalformedHeader,
		},
		{
			name:    "header 名稱為空",
			input:   "SEND\n:value\n\n\x00",
			wantErr: ErrMalformedHeader,
		},
		{
			name:    "未定義的轉義序列",
			input:   "SEND\nkey:a\\tb\n\n\x00",
			wantErr: ErrUnknownEscape,
		},
		{
			name:    "header 以落單反斜線結尾",
			input:   "SEND\nkey:a\\\n\n\x00",
			wantErr: ErrUnknownEscape,
		},
		{
			name:    "沒有 NULL 結尾",
			input:   "SEND\n\nhello",
			wantErr: ErrMissingNull,
		},
		{
			name:    "content-length 之後不是 NULL",
			input:   "SEND\ncontent-length:2\n\nabcd\x00",
			wantErr: ErrMissingNull,
		},
		{
			name:    "content-length 的 body 被截斷",
			input:   "SEND\ncontent-length:10\n\nabc",
			wantErr: ErrMissingNull,
		},
		{
			name:    "content-length 不是數字",
			input:   "SEND\ncontent-length:abc\n\n\x00",
			wantErr: ErrInvalidContentLength,
		},
		{
			name:    "content-length 為負數",
			input:   "SEND\ncontent-length:-1\n\n\x00",
			wantErr: ErrInvalidContentLength,
		},
		{
			name:    "body 超過上限（讀到 NULL 模式）",
			input:   "SEND\n\nhello\x00",
			limits:  Limits{MaxLineLength: 64, MaxHeaders: 8, MaxBodySize: 4},
			wantErr: ErrFrameTooLarge,
		},
		{
			// 這條是防記憶體耗盡的關鍵：宣告一個巨大的 content-length
			// 必須在配置記憶體「之前」就被擋下來
			name:    "content-length 宣告超過上限",
			input:   "SEND\ncontent-length:999999\n\n\x00",
			limits:  Limits{MaxLineLength: 64, MaxHeaders: 8, MaxBodySize: 4},
			wantErr: ErrFrameTooLarge,
		},
		{
			name:    "單行超過上限",
			input:   "SEND\nkey:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n\n\x00",
			limits:  Limits{MaxLineLength: 16, MaxHeaders: 8, MaxBodySize: 64},
			wantErr: ErrFrameTooLarge,
		},
		{
			name:    "header 數量超過上限",
			input:   "SEND\na:1\nb:2\nc:3\n\n\x00",
			limits:  Limits{MaxLineLength: 64, MaxHeaders: 2, MaxBodySize: 64},
			wantErr: ErrFrameTooLarge,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limits := tt.limits
			if limits == (Limits{}) {
				limits = DefaultLimits
			}

			_, err := NewDecoderWithLimits(strings.NewReader(tt.input), limits).ReadFrame()
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v，want errors.Is(..., %v)", err, tt.wantErr)
			}

			// 協議錯誤要能被上層用 errors.As 攔下來決定「回 ERROR 帧並斷線」
			var protoErr *ProtocolError
			if !errors.As(err, &protoErr) {
				t.Errorf("err 型別 = %T，want 可轉成 *ProtocolError", err)
			}
			if protoErr != nil && protoErr.Detail == "" {
				t.Error("ProtocolError.Detail 為空——出事時日誌會看不出 client 送了什麼")
			}
		})
	}
}

func TestUnmarshal(t *testing.T) {
	t.Run("正常帧", func(t *testing.T) {
		frame, err := Unmarshal([]byte("SEND\ndestination:/app/x\n\nbody\x00"))
		if err != nil {
			t.Fatalf("非預期錯誤：%v", err)
		}
		if frame.Command != CommandSend {
			t.Errorf("Command = %q，want SEND", frame.Command)
		}
		if string(frame.Body) != "body" {
			t.Errorf("Body = %q，want \"body\"", frame.Body)
		}
	})

	t.Run("尾端多餘的換行可容忍", func(t *testing.T) {
		if _, err := Unmarshal([]byte("DISCONNECT\n\n\x00\n")); err != nil {
			t.Fatalf("非預期錯誤：%v", err)
		}
	})

	heartbeats := []struct {
		name string
		data []byte
	}{
		{"單一 LF", []byte("\n")},
		{"CRLF", []byte("\r\n")},
		{"多個 LF", []byte("\n\n")},
		{"空訊息", []byte{}},
	}
	for _, tt := range heartbeats {
		t.Run("心跳／"+tt.name, func(t *testing.T) {
			frame, err := Unmarshal(tt.data)
			if !errors.Is(err, ErrHeartbeat) {
				t.Fatalf("err = %v，want ErrHeartbeat", err)
			}
			if frame != nil {
				t.Errorf("frame = %v，want nil", frame)
			}
		})
	}

	t.Run("一則訊息裡兩個帧要報錯", func(t *testing.T) {
		// 靜默忽略第二個帧會變成「偶爾漏訊息」這種極難查的 bug，所以寧可明確失敗
		_, err := Unmarshal([]byte("DISCONNECT\n\n\x00SEND\n\n\x00"))
		if !errors.Is(err, ErrMultipleFrames) {
			t.Fatalf("err = %v，want ErrMultipleFrames", err)
		}
	})

	t.Run("畸形帧照樣回傳協議錯誤", func(t *testing.T) {
		if _, err := Unmarshal([]byte("NOPE\n\n\x00")); !errors.Is(err, ErrInvalidCommand) {
			t.Fatalf("err = %v，want ErrInvalidCommand", err)
		}
	})
}

// Marshal 產生的位元組必須能被自己解析回來，且指令與 body 不變。
//
// header 不做全等比對是刻意的：body 非空時 Marshal 會自動補上
// content-length，解析回來自然會多這一個 header。
func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		frame Frame
	}{
		{
			name:  "只有指令",
			frame: Frame{Command: CommandDisconnect},
		},
		{
			name: "CONNECTED 握手回應",
			frame: Frame{Command: CommandConnected, Headers: Headers{
				{Name: HeaderVersion, Value: "1.2"},
				{Name: HeaderHeartBeat, Value: "10000,10000"},
			}},
		},
		{
			name: "推播給玩家的 MESSAGE",
			frame: Frame{Command: CommandMessage, Headers: Headers{
				{Name: HeaderDestination, Value: "/user/queue/notifications"},
				{Name: HeaderSubscription, Value: "sub-0"},
				{Name: HeaderMessageID, Value: "msg-1"},
				{Name: HeaderContentType, Value: "application/json"},
			}, Body: []byte(`{"type":"GAME_RESULT","payout":1200}`)},
		},
		{
			name: "header 與 body 都含需轉義的字元",
			frame: Frame{Command: CommandMessage, Headers: Headers{
				{Name: "nick", Value: "玩家:暱稱\n第二行"},
			}, Body: []byte("含換行\n的內容")},
		},
		{
			name: "ERROR 帶中文說明",
			frame: Frame{Command: CommandError, Headers: Headers{
				{Name: HeaderMessage, Value: "unauthorized"},
			}, Body: []byte("JWT 驗證失敗")},
		},
		{
			name: "CONNECT 不轉義也要能來回",
			frame: Frame{Command: CommandConnect, Headers: Headers{
				{Name: HeaderAuthorization, Value: "Bearer a.b.c"},
			}},
		},
	}

	for _, tt := range tests {
		original := tt.frame
		t.Run(tt.name, func(t *testing.T) {
			decoded, err := Unmarshal(original.Marshal())
			if err != nil {
				t.Fatalf("來回一趟失敗：%v\n原始帧：%s", err, original.String())
			}

			if decoded.Command != original.Command {
				t.Errorf("Command = %q，want %q", decoded.Command, original.Command)
			}
			if !bytes.Equal(decoded.Body, original.Body) {
				t.Errorf("Body\n got = %q\nwant = %q", decoded.Body, original.Body)
			}
			for _, want := range original.Headers {
				if got := decoded.Headers.Get(want.Name); got != want.Value {
					t.Errorf("header %q\n got = %q\nwant = %q", want.Name, got, want.Value)
				}
			}
		})
	}
}

// FuzzReadFrame 用隨機輸入撞解析器。
//
// 性質有二：
//  1. **絕不 panic**——解析器直接面對網路上的任意位元組，
//     一個 panic 就是一次遠端拒絕服務。
//  2. 成功解析出來的帧，重新編碼後必須能再解析回來且語意不變。
//
// 跑法：go test -fuzz=FuzzReadFrame -fuzztime=30s ./internal/stomp/
func FuzzReadFrame(f *testing.F) {
	seeds := []string{
		"CONNECT\naccept-version:1.2\nhost:x\n\n\x00",
		"CONNECT\nAuthorization:Bearer a.b.c\n\n\x00",
		"SUBSCRIBE\nid:sub-0\ndestination:/topic/rank\n\n\x00",
		"MESSAGE\ndestination:/topic/rank\ncontent-length:2\n\nhi\x00",
		"MESSAGE\nnick:a\\cb\n\n\x00",
		"SEND\n\nbody\x00",
		"DISCONNECT\n\n\x00",
		"\n",                    // 心跳
		"",                      // 空輸入
		"FOO\n\n\x00",           // 非法指令
		"SEND\nbad\n\n\x00",     // 缺冒號
		"SEND\nk:a\\tb\n\n\x00", // 未定義轉義
		"SEND\ncontent-length:99\n\nshort",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	limits := Limits{MaxLineLength: 256, MaxHeaders: 8, MaxBodySize: 4096}

	f.Fuzz(func(t *testing.T, data []byte) {
		frame, err := NewDecoderWithLimits(bytes.NewReader(data), limits).ReadFrame()
		if err != nil {
			return // 拒絕畸形輸入是正確行為，不是失敗
		}

		again, err := Unmarshal(frame.Marshal())
		if err != nil {
			t.Fatalf("自己編出來的帧自己解不回來：%v\n帧：%s", err, frame.String())
		}
		if again.Command != frame.Command {
			t.Errorf("來回後 Command 變了：%q → %q", frame.Command, again.Command)
		}
		if !bytes.Equal(again.Body, frame.Body) {
			t.Errorf("來回後 Body 變了：%q → %q", frame.Body, again.Body)
		}
	})
}
