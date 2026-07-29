package stomp

import (
	"errors"
	"testing"
)

func TestEscapeHeaderOctets(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"沒有特殊字元就原樣回傳", "destination", "destination"},
		{"空字串", "", ""},
		{"反斜線", `a\b`, `a\\b`},
		{"歸位", "a\rb", `a\rb`},
		{"換行", "a\nb", `a\nb`},
		{"冒號", "a:b", `a\cb`},
		{"四種一起來", "a\\b:c\nd", `a\\b\cc\nd`},
		{"連續反斜線不可互相吃掉", `\\`, `\\\\`},
		{"UTF-8 多位元組不受影響", "玩家暱稱", "玩家暱稱"},
		{"UTF-8 夾冒號", "玩家:暱稱", `玩家\c暱稱`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := escapeHeaderOctets(tt.in); got != tt.want {
				t.Errorf("escapeHeaderOctets(%q)\n got = %q\nwant = %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestUnescapeHeaderOctets(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    string
		wantErr error
	}{
		{name: "沒有反斜線就原樣回傳", in: "destination", want: "destination"},
		{name: "空字串", in: "", want: ""},
		{name: `\\ 還原成反斜線`, in: `a\\b`, want: `a\b`},
		{name: `\r 還原成歸位`, in: `a\rb`, want: "a\rb"},
		{name: `\n 還原成換行`, in: `a\nb`, want: "a\nb"},
		{name: `\c 還原成冒號`, in: `a\cb`, want: "a:b"},
		{name: "四種一起來", in: `a\\b\cc\nd`, want: "a\\b:c\nd"},

		// 規格明訂未定義的轉義序列是致命錯誤，不可寬容處理
		{name: `未定義的 \t 要報錯`, in: `a\tb`, wantErr: ErrUnknownEscape},
		{name: "落單的反斜線要報錯", in: `a\`, wantErr: ErrUnknownEscape},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := unescapeHeaderOctets(tt.in)

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("unescapeHeaderOctets(%q) err = %v，want errors.Is(..., %v)", tt.in, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unescapeHeaderOctets(%q) 非預期錯誤：%v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("unescapeHeaderOctets(%q)\n got = %q\nwant = %q", tt.in, got, tt.want)
			}
		})
	}
}

// 轉義與還原必須互為反函式，否則帧在來回一趟後會變形。
func TestEscapeUnescapeRoundTrip(t *testing.T) {
	values := []string{
		"", "plain", `a\b`, "a\rb", "a\nb", "a:b",
		"a\\b:c\nd", `\\`, "玩家:暱稱", ":::", `\\\`,
	}

	for _, v := range values {
		t.Run(v, func(t *testing.T) {
			got, err := unescapeHeaderOctets(escapeHeaderOctets(v))
			if err != nil {
				t.Fatalf("還原 %q 的轉義時出錯：%v", v, err)
			}
			if got != v {
				t.Errorf("來回一趟後不相等\n got = %q\nwant = %q", got, v)
			}
		})
	}
}

func TestHeadersLookupUsesFirstEntry(t *testing.T) {
	// 規格 §2.1：重複的 header 只有第一個有效。
	// 這不只是相容性細節——它擋掉「補送第二個 Authorization 去蓋掉第一個」的招式。
	headers := Headers{
		{Name: "id", Value: "first"},
		{Name: "id", Value: "second"},
		{Name: HeaderAuthorization, Value: "Bearer real"},
		{Name: HeaderAuthorization, Value: "Bearer forged"},
	}

	if got := headers.Get("id"); got != "first" {
		t.Errorf(`Get("id") = %q，want "first"`, got)
	}
	if got := headers.Get(HeaderAuthorization); got != "Bearer real" {
		t.Errorf("Get(Authorization) = %q，want 第一個值", got)
	}

	if _, ok := headers.Lookup("nope"); ok {
		t.Error(`Lookup("nope") 回報存在，want 不存在`)
	}
	if got := headers.Get("nope"); got != "" {
		t.Errorf(`Get("nope") = %q，want ""`, got)
	}
	if headers.Has("nope") {
		t.Error(`Has("nope") = true，want false`)
	}
}

func TestHeadersAdd(t *testing.T) {
	var headers Headers
	headers.Add("a", "1")
	headers.Add("b", "2")
	headers.Add("a", "3") // 允許重複

	if len(headers) != 3 {
		t.Fatalf("len = %d，want 3（Add 不該去重）", len(headers))
	}
	if got := headers.Get("a"); got != "1" {
		t.Errorf(`Get("a") = %q，want "1"`, got)
	}
}

func TestCommandValid(t *testing.T) {
	valid := []Command{
		CommandConnect, CommandStomp, CommandSend, CommandSubscribe,
		CommandUnsubscribe, CommandAck, CommandNack, CommandBegin,
		CommandCommit, CommandAbort, CommandDisconnect,
		CommandConnected, CommandMessage, CommandReceipt, CommandError,
	}
	for _, c := range valid {
		if !c.Valid() {
			t.Errorf("Command(%q).Valid() = false，want true", c)
		}
	}

	invalid := []Command{"", "connect", "Connect", "SUBSCRIBE ", "FOO", "MESSAGE\n"}
	for _, c := range invalid {
		if c.Valid() {
			t.Errorf("Command(%q).Valid() = true，want false（指令區分大小寫且不含空白）", c)
		}
	}
}

func TestFrameMarshal(t *testing.T) {
	tests := []struct {
		name  string
		frame Frame
		want  string
	}{
		{
			name:  "沒有 header 也沒有 body",
			frame: Frame{Command: CommandDisconnect},
			want:  "DISCONNECT\n\n\x00",
		},
		{
			name: "CONNECTED 握手回應",
			frame: Frame{
				Command: CommandConnected,
				Headers: Headers{
					{Name: HeaderVersion, Value: "1.2"},
					{Name: HeaderHeartBeat, Value: "10000,10000"},
				},
			},
			want: "CONNECTED\nversion:1.2\nheart-beat:10000,10000\n\n\x00",
		},
		{
			name: "有 body 時自動補上 content-length",
			frame: Frame{
				Command: CommandMessage,
				Headers: Headers{
					{Name: HeaderDestination, Value: "/topic/rank"},
					{Name: HeaderSubscription, Value: "sub-0"},
					{Name: HeaderMessageID, Value: "1"},
					{Name: HeaderContentType, Value: "application/json"},
				},
				Body: []byte(`{"a":1}`),
			},
			want: "MESSAGE\ndestination:/topic/rank\nsubscription:sub-0\nmessage-id:1\n" +
				"content-type:application/json\ncontent-length:7\n\n{\"a\":1}\x00",
		},
		{
			name: "呼叫端已指定 content-length 就不重複加",
			frame: Frame{
				Command: CommandMessage,
				Headers: Headers{{Name: HeaderContentLength, Value: "5"}},
				Body:    []byte("hello"),
			},
			want: "MESSAGE\ncontent-length:5\n\nhello\x00",
		},
		{
			name: "body 為空就不加 content-length",
			frame: Frame{
				Command: CommandMessage,
				Headers: Headers{{Name: HeaderDestination, Value: "/topic/x"}},
			},
			want: "MESSAGE\ndestination:/topic/x\n\n\x00",
		},
		{
			name: "MESSAGE 的 header 要轉義",
			frame: Frame{
				Command: CommandMessage,
				Headers: Headers{{Name: "nick", Value: "玩家:暱稱"}},
			},
			want: "MESSAGE\nnick:玩家\\c暱稱\n\n\x00",
		},
		{
			// 規格 §3.1 的向下相容例外：CONNECT 不轉義 CR/LF/冒號。
			// 做錯這裡會導致與 @stomp/stompjs 握手失敗。
			name: "CONNECT 的 header 不轉義",
			frame: Frame{
				Command: CommandConnect,
				Headers: Headers{{Name: "login", Value: "a:b"}},
			},
			want: "CONNECT\nlogin:a:b\n\n\x00",
		},
		{
			name: "CONNECTED 的 header 同樣不轉義",
			frame: Frame{
				Command: CommandConnected,
				Headers: Headers{{Name: "server", Value: "notify:1.0"}},
			},
			want: "CONNECTED\nserver:notify:1.0\n\n\x00",
		},
		{
			name: "ERROR 帶說明 body",
			frame: Frame{
				Command: CommandError,
				Headers: Headers{{Name: HeaderMessage, Value: "unauthorized"}},
				Body:    []byte("bad token"),
			},
			want: "ERROR\nmessage:unauthorized\ncontent-length:9\n\nbad token\x00",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(tt.frame.Marshal())
			if got != tt.want {
				t.Errorf("Marshal()\n got = %q\nwant = %q", got, tt.want)
			}
		})
	}
}

// Marshal 出來的位元組尾端必須是 NULL，不是換行。
// 這條單獨測是因為它是最容易寫錯、而且錯了會讓 client 卡住等不到帧結束的地方。
func TestFrameMarshalTerminatesWithNull(t *testing.T) {
	frames := []Frame{
		{Command: CommandDisconnect},
		{Command: CommandMessage, Body: []byte("x")},
		{Command: CommandConnected, Headers: Headers{{Name: "version", Value: "1.2"}}},
	}

	for _, f := range frames {
		out := f.Marshal()
		if len(out) == 0 || out[len(out)-1] != 0 {
			t.Errorf("%s 的最後一個位元組不是 NULL：%q", f.Command, out)
		}
	}
}
