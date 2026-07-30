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
			if hb := got.Reply.Headers.Get(stomp.HeaderHeartBeat); hb != "0,0" {
				// Phase 1 沒實作心跳，宣告 0,0 才不會讓 client 等不到心跳就斷線。
				t.Errorf("heart-beat header = %q，Phase 1 應為 %q", hb, "0,0")
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
