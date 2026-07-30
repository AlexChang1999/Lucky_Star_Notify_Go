package server

import (
	"strings"
	"testing"
	"time"

	"github.com/AlexChang1999/lucky-star-notify-go/internal/auth"
	"github.com/AlexChang1999/lucky-star-notify-go/internal/stomp"
	"github.com/golang-jwt/jwt/v5"
)

const testSecret = "0123456789abcdef0123456789abcdef"

// testToken 簽一個與 member-service 同形狀的 access token。
func testToken(t *testing.T, subject string, ttl time.Duration) string {
	t.Helper()
	claims := jwt.MapClaims{
		"sub":      subject,
		"username": "player01",
		"role":     "PLAYER",
		"type":     "access",
		"exp":      time.Now().Add(ttl).Unix(),
	}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(testSecret))
	if err != nil {
		t.Fatalf("簽 token 失敗：%v", err)
	}
	return signed
}

// connectFrame 組一個 CONNECT 帧。headers 會覆蓋預設值。
func connectFrame(headers stomp.Headers) *stomp.Frame {
	return &stomp.Frame{Command: stomp.CommandConnect, Headers: headers}
}

func TestHandshake(t *testing.T) {
	t.Parallel()

	verifier := auth.NewVerifier([]byte(testSecret))
	validToken := testToken(t, "42", time.Hour)
	expiredToken := testToken(t, "42", -time.Minute)

	tests := []struct {
		name string
		// frame 是 client 送來的第一個帧
		frame *stomp.Frame
		// wantOK 為 true 時檢查 CONNECTED；否則檢查 ERROR
		wantOK bool
		// wantPlayerID 只在 wantOK 時檢查
		wantPlayerID string
		// wantMessage 是 ERROR 帧的 message header
		wantMessage string
	}{
		{
			name: "正常握手",
			frame: connectFrame(stomp.Headers{
				{Name: stomp.HeaderAcceptVersion, Value: "1.0,1.1,1.2"},
				{Name: stomp.HeaderHost, Value: "localhost"},
				{Name: stomp.HeaderAuthorization, Value: "Bearer " + validToken},
			}),
			wantOK:       true,
			wantPlayerID: "42",
		},
		{
			name: "STOMP 指令是 CONNECT 的別名，一樣接受",
			frame: &stomp.Frame{Command: stomp.CommandStomp, Headers: stomp.Headers{
				{Name: stomp.HeaderAcceptVersion, Value: "1.2"},
				{Name: stomp.HeaderAuthorization, Value: "Bearer " + validToken},
			}},
			wantOK:       true,
			wantPlayerID: "42",
		},
		{
			name: "沒有 Bearer 前綴也接受（與 Java 版一致）",
			frame: connectFrame(stomp.Headers{
				{Name: stomp.HeaderAcceptVersion, Value: "1.2"},
				{Name: stomp.HeaderAuthorization, Value: validToken},
			}),
			wantOK:       true,
			wantPlayerID: "42",
		},
		{
			// ★ 規格 §2.1：重複的 header 以第一個為準。
			// 攻擊者補送第二個 Authorization 蓋不掉第一個。
			frame: connectFrame(stomp.Headers{
				{Name: stomp.HeaderAcceptVersion, Value: "1.2"},
				{Name: stomp.HeaderAuthorization, Value: "Bearer " + validToken},
				{Name: stomp.HeaderAuthorization, Value: "Bearer " + expiredToken},
			}),
			name:         "重複的 Authorization 以第一個為準",
			wantOK:       true,
			wantPlayerID: "42",
		},
		{
			name: "缺 host header 仍接受（Java 版也不檢查）",
			frame: connectFrame(stomp.Headers{
				{Name: stomp.HeaderAcceptVersion, Value: "1.2"},
				{Name: stomp.HeaderAuthorization, Value: "Bearer " + validToken},
			}),
			wantOK:       true,
			wantPlayerID: "42",
		},
		{
			name: "第一個帧不是 CONNECT",
			frame: &stomp.Frame{Command: stomp.CommandSubscribe, Headers: stomp.Headers{
				{Name: stomp.HeaderDestination, Value: "/topic/rank"},
				{Name: stomp.HeaderID, Value: "sub-0"},
			}},
			wantMessage: "Expected CONNECT frame",
		},
		{
			name: "accept-version 不含 1.2",
			frame: connectFrame(stomp.Headers{
				{Name: stomp.HeaderAcceptVersion, Value: "1.0,1.1"},
				{Name: stomp.HeaderAuthorization, Value: "Bearer " + validToken},
			}),
			wantMessage: "Unsupported STOMP version",
		},
		{
			name: "完全沒有 accept-version（STOMP 1.0 的 client）",
			frame: connectFrame(stomp.Headers{
				{Name: stomp.HeaderAuthorization, Value: "Bearer " + validToken},
			}),
			wantMessage: "Unsupported STOMP version",
		},
		{
			name: "缺 Authorization",
			frame: connectFrame(stomp.Headers{
				{Name: stomp.HeaderAcceptVersion, Value: "1.2"},
			}),
			wantMessage: "Unauthorized",
		},
		{
			name: "token 過期",
			frame: connectFrame(stomp.Headers{
				{Name: stomp.HeaderAcceptVersion, Value: "1.2"},
				{Name: stomp.HeaderAuthorization, Value: "Bearer " + expiredToken},
			}),
			wantMessage: "Unauthorized",
		},
		{
			name: "token 亂填",
			frame: connectFrame(stomp.Headers{
				{Name: stomp.HeaderAcceptVersion, Value: "1.2"},
				{Name: stomp.HeaderAuthorization, Value: "Bearer 不是一個 token"},
			}),
			wantMessage: "Unauthorized",
		},
		{
			// 版本檢查排在驗章之前：版本談不成時，token 有效也沒意義。
			name: "版本與 token 同時有問題時，先報版本",
			frame: connectFrame(stomp.Headers{
				{Name: stomp.HeaderAcceptVersion, Value: "1.0"},
				{Name: stomp.HeaderAuthorization, Value: "Bearer " + expiredToken},
			}),
			wantMessage: "Unsupported STOMP version",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := handshake(verifier, tt.frame, "test-session")

			if got.OK != tt.wantOK {
				t.Fatalf("OK = %v，預期 %v（Reply=%v）", got.OK, tt.wantOK, got.Reply)
			}

			if !tt.wantOK {
				if got.Reply.Command != stomp.CommandError {
					t.Errorf("失敗時的 Reply 指令 = %s，預期 ERROR", got.Reply.Command)
				}
				if msg := got.Reply.Headers.Get(stomp.HeaderMessage); msg != tt.wantMessage {
					t.Errorf("message header = %q，預期 %q", msg, tt.wantMessage)
				}
				if got.PlayerID != "" {
					t.Errorf("失敗時 PlayerID = %q，應為空", got.PlayerID)
				}
				return
			}

			if got.PlayerID != tt.wantPlayerID {
				t.Errorf("PlayerID = %q，預期 %q", got.PlayerID, tt.wantPlayerID)
			}
			if got.Reply.Command != stomp.CommandConnected {
				t.Fatalf("Reply 指令 = %s，預期 CONNECTED", got.Reply.Command)
			}
			if v := got.Reply.Headers.Get(stomp.HeaderVersion); v != StompVersion {
				t.Errorf("version header = %q，預期 %q", v, StompVersion)
			}
			// CONNECTED 帶的是伺服器自己的 sx,sy，不是協商結果——
			// 所以不論 client 送什麼，這個 header 都一樣。
			if hb := got.Reply.Headers.Get(stomp.HeaderHeartBeat); hb != "10000,10000" {
				t.Errorf("heart-beat header = %q，預期 %q", hb, "10000,10000")
			}
			if got.Reply.Headers.Get("session") != "test-session" {
				t.Errorf("session header = %q，預期 test-session", got.Reply.Headers.Get("session"))
			}
			if len(got.Reply.Body) != 0 {
				t.Errorf("CONNECTED 不該有 body，卻有 %q", got.Reply.Body)
			}
		})
	}
}

// TestUnsupportedVersionErrorAdvertisesSupportedVersion 確認版本談不成時，
// ERROR 帧帶著我們支援的版本——client 才知道該降級到什麼。
func TestUnsupportedVersionErrorAdvertisesSupportedVersion(t *testing.T) {
	t.Parallel()

	got := handshake(auth.NewVerifier([]byte(testSecret)),
		connectFrame(stomp.Headers{{Name: stomp.HeaderAcceptVersion, Value: "1.0"}}),
		"test-session")

	if v := got.Reply.Headers.Get(stomp.HeaderVersion); v != StompVersion {
		t.Errorf("ERROR 的 version header = %q，預期 %q", v, StompVersion)
	}
}

// TestAuthFailuresAreIndistinguishable 是安全性測試：
// 「沒帶 token」、「過期」、「簽章錯」對外必須長得一模一樣。
//
// 為什麼重要：能分辨「這個 token 曾經有效但過期了」對攻擊者是有用的情報
// （代表帳號存在、代表 secret 沒換過）。差異只能留在伺服器的 log 裡。
func TestAuthFailuresAreIndistinguishable(t *testing.T) {
	t.Parallel()

	verifier := auth.NewVerifier([]byte(testSecret))
	otherKeyToken, err := jwt.NewWithClaims(jwt.SigningMethodHS256,
		jwt.MapClaims{"sub": "42", "exp": time.Now().Add(time.Hour).Unix()}).
		SignedString([]byte("ffffffffffffffffffffffffffffffff"))
	if err != nil {
		t.Fatalf("簽 token 失敗：%v", err)
	}

	cases := map[string]string{
		"沒帶":  "",
		"過期":  "Bearer " + testToken(t, "42", -time.Minute),
		"簽章錯": "Bearer " + otherKeyToken,
		"亂填":  "Bearer aaa.bbb.ccc",
	}

	var seen []string
	for name, authValue := range cases {
		headers := stomp.Headers{{Name: stomp.HeaderAcceptVersion, Value: "1.2"}}
		if authValue != "" {
			headers.Add(stomp.HeaderAuthorization, authValue)
		}
		got := handshake(verifier, connectFrame(headers), "test-session")
		if got.OK {
			t.Fatalf("%s：預期握手失敗", name)
		}
		seen = append(seen, string(got.Reply.Marshal()))
	}

	for i := 1; i < len(seen); i++ {
		if seen[i] != seen[0] {
			t.Errorf("不同的驗章失敗回了不同的 ERROR 帧：\n%s\n---\n%s",
				strings.ReplaceAll(seen[0], "\x00", "^@"),
				strings.ReplaceAll(seen[i], "\x00", "^@"))
		}
	}
}

// TestNegotiateHeartbeat 是心跳協商的表格驅動測試。
//
// 規格的公式：任一邊為 0 就關掉該方向，否則取兩者**較大值**。
// 取 max 而不是 min，是因為兩個數字的語意不同（一邊是「我最快只能這麼快」、
// 一邊是「我希望這麼頻繁」），能同時滿足雙方的只有較大值。
//
// 伺服器端固定是 sx=sy=10000ms，所以下面每一列都以此為前提。
func TestNegotiateHeartbeat(t *testing.T) {
	t.Parallel()

	const s = 10 * time.Second

	tests := []struct {
		name     string
		header   string
		wantSend time.Duration
		wantRecv time.Duration
	}{
		{"雙方都要 10 秒", "10000,10000", s, s},
		{"client 要得比我們快，取我們的下限", "1000,1000", s, s},
		// cx=30000（它保證每 30s 送一次）、cy=20000（它希望每 20s 收到一次）
		//   server→client = max(cy=20s, sx=10s) = 20s
		//   client→server = max(cx=30s, sy=10s) = 30s
		{"client 要得比我們慢，取它的", "30000,20000", 20 * time.Second, 30 * time.Second},
		{"client 說它不送（cx=0）→ 關掉 client→server", "0,10000", s, 0},
		{"client 說它不要收（cy=0）→ 關掉 server→client", "10000,0", 0, s},
		{"雙向都關", "0,0", 0, 0},
		{"沒帶 header（STOMP 1.0 行為）", "", 0, 0},
		{"格式壞掉：沒有逗號", "10000", 0, 0},
		{"格式壞掉：不是數字", "abc,10000", 0, 0},
		{"格式壞掉：負數", "-1,10000", 0, 0},
		{"容忍空白", " 10000 , 10000 ", s, s},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := negotiateHeartbeat(tt.header)
			if got.Send != tt.wantSend {
				t.Errorf("Send = %v，預期 %v", got.Send, tt.wantSend)
			}
			if got.Recv != tt.wantRecv {
				t.Errorf("Recv = %v，預期 %v", got.Recv, tt.wantRecv)
			}
		})
	}
}
