package auth

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// testSecret 模擬 member-service 的 jwt.secret（32 bytes）。
const testSecret = "0123456789abcdef0123456789abcdef"

// signWith 用指定的方法與金鑰簽出一個 token，供測試構造各種情境。
func signWith(t *testing.T, method jwt.SigningMethod, key []byte, claims jwt.Claims) string {
	t.Helper()
	signed, err := jwt.NewWithClaims(method, claims).SignedString(key)
	if err != nil {
		t.Fatalf("簽 token 失敗：%v", err)
	}
	return signed
}

// playerClaims 產生一份與 member-service JwtTokenProvider 同形狀的 claims。
//
// Java 那邊是：subject = String.valueOf(memberId)、另有 username / role / type
// 三個自訂 claim。我們只在意 subject，但保留其他欄位才能證明
// 「多出來的 claim 不會讓解析失敗」。
func playerClaims(subject string, ttl time.Duration) jwt.MapClaims {
	now := time.Now()
	return jwt.MapClaims{
		"sub":      subject,
		"username": "player01",
		"role":     "PLAYER",
		"type":     "access",
		"iat":      now.Unix(),
		"exp":      now.Add(ttl).Unix(),
	}
}

// unsignedToken 手工組出 alg:none 的 token。
//
// 不能用 golang-jwt 簽——它刻意讓 "none" 很難用。手工組正好模擬攻擊者：
// 攻擊者本來就不會用你的函式庫，他直接拼 base64。
func unsignedToken(t *testing.T, subject string) string {
	t.Helper()
	enc := func(s string) string {
		return base64.RawURLEncoding.EncodeToString([]byte(s))
	}
	header := enc(`{"alg":"none","typ":"JWT"}`)
	payload := enc(`{"sub":"` + subject + `","exp":9999999999}`)
	return header + "." + payload + "." // 簽章欄位留空
}

func TestVerifierPlayerID(t *testing.T) {
	t.Parallel()

	key := []byte(testSecret)
	verifier := NewVerifier(key)

	tests := []struct {
		name    string
		token   string
		want    string
		wantErr error
	}{
		{
			name:  "正常的 access token",
			token: signWith(t, jwt.SigningMethodHS256, key, playerClaims("42", time.Hour)),
			want:  "42",
		},
		{
			// Java 版沒有檢查 type claim，refresh token 一樣過得了 CONNECT。
			// 這裡明確測出這個行為，是為了讓「兩版行為相同」是被驗證過的事實，
			// 而不是碰巧。要不要改成拒絕，是產品決策（見 CHANGELOG 的討論）。
			name: "refresh token 也會通過（與 Java 版一致）",
			token: signWith(t, jwt.SigningMethodHS256, key, jwt.MapClaims{
				"sub": "42", "type": "refresh", "exp": time.Now().Add(time.Hour).Unix(),
			}),
			want: "42",
		},
		{
			name: "沒有 exp 的 token 被接受（JJWT 也不強制 exp）",
			token: signWith(t, jwt.SigningMethodHS256, key, jwt.MapClaims{
				"sub": "7",
			}),
			want: "7",
		},
		{
			name:    "空字串",
			token:   "",
			wantErr: ErrNoToken,
		},
		{
			name:    "只有空白（對齊 Java 的 token.isBlank()）",
			token:   "   \t ",
			wantErr: ErrNoToken,
		},
		{
			name:    "格式不是三段",
			token:   "not.a.valid.jwt.at.all",
			wantErr: ErrInvalidToken,
		},
		{
			name:    "已過期",
			token:   signWith(t, jwt.SigningMethodHS256, key, playerClaims("42", -time.Minute)),
			wantErr: ErrInvalidToken,
		},
		{
			name:    "換了別把 secret 簽的",
			token:   signWith(t, jwt.SigningMethodHS256, []byte("ffffffffffffffffffffffffffffffff"), playerClaims("42", time.Hour)),
			wantErr: ErrInvalidToken,
		},
		{
			// ★ 安全測試：alg 混淆攻擊。攻擊者把 alg 改成 none 並清空簽章，
			// 沒帶 WithValidMethods 的解析器會直接放過去。
			name:    "alg:none 必須被拒",
			token:   unsignedToken(t, "999"),
			wantErr: ErrInvalidToken,
		},
		{
			// ★ 安全測試：同一把 key 但換成 HS512。簽章其實是「對的」，
			// 但我們只宣告接受 HS256，就不該接受任何其他演算法。
			name:    "同 key 的 HS512 也要被拒（只接受 HS256）",
			token:   signWith(t, jwt.SigningMethodHS512, key, playerClaims("42", time.Hour)),
			wantErr: ErrInvalidToken,
		},
		{
			name: "nbf 還沒到",
			token: signWith(t, jwt.SigningMethodHS256, key, jwt.MapClaims{
				"sub": "42",
				"nbf": time.Now().Add(time.Hour).Unix(),
				"exp": time.Now().Add(2 * time.Hour).Unix(),
			}),
			wantErr: ErrInvalidToken,
		},
		{
			name: "缺 sub",
			token: signWith(t, jwt.SigningMethodHS256, key, jwt.MapClaims{
				"username": "player01",
				"exp":      time.Now().Add(time.Hour).Unix(),
			}),
			wantErr: ErrNoSubject,
		},
		{
			name: "sub 是空字串",
			token: signWith(t, jwt.SigningMethodHS256, key, jwt.MapClaims{
				"sub": "",
				"exp": time.Now().Add(time.Hour).Unix(),
			}),
			wantErr: ErrNoSubject,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := verifier.PlayerID(tt.token)

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("PlayerID() error = %v，預期包含 %v", err, tt.wantErr)
				}
				if got != "" {
					t.Errorf("失敗時仍回傳了 playerID %q，應為空字串", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("PlayerID() 意外失敗：%v", err)
			}
			if got != tt.want {
				t.Errorf("PlayerID() = %q，預期 %q", got, tt.want)
			}
		})
	}
}

// TestVerifierAcceptsExternallyGeneratedToken 是**跨實作**的驗證。
//
// 這個 token 與 secret 出自 jwt.io 的官方範例，不是我們自己簽的，
// 所以它證明的是「我們對 HS256 的解讀跟外界一致」：
//
//	secret（原始字串）= "your-256-bit-secret"
//	sub               = "1234567890"
//
// ★ 重點：secret 是**直接取字串的位元組**。如果照多數 Go 教學那樣寫
// base64.StdEncoding.DecodeString(secret) 再當 key，這個測試就會失敗——
// 而這正是與 Java 版（Keys.hmacShaKeyFor(secret.getBytes(UTF_8))）互通的關鍵。
func TestVerifierAcceptsExternallyGeneratedToken(t *testing.T) {
	t.Parallel()

	const (
		externalSecret = "your-256-bit-secret"
		externalToken  = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9." +
			"eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ." +
			"SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
	)

	got, err := NewVerifier([]byte(externalSecret)).PlayerID(externalToken)
	if err != nil {
		t.Fatalf("外部 token 驗不過：%v（secret 是不是被 base64 解碼了？）", err)
	}
	if got != "1234567890" {
		t.Errorf("PlayerID() = %q，預期 %q", got, "1234567890")
	}
}

// TestVerifierIsSafeForConcurrentUse 確認同一個 Verifier 能被多條連線共用。
//
// 為什麼要測：每條 WebSocket 連線都會在自己的 goroutine 裡驗章。
// 如果 Verifier 內部藏了可變狀態（例如共用同一個 claims 物件），
// 這裡會在 -race 下爆出來。
func TestVerifierIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()

	key := []byte(testSecret)
	verifier := NewVerifier(key)
	token := signWith(t, jwt.SigningMethodHS256, key, playerClaims("42", time.Hour))

	const goroutines = 32
	errs := make(chan error, goroutines)
	for range goroutines {
		go func() {
			id, err := verifier.PlayerID(token)
			if err == nil && id != "42" {
				err = errors.New("playerID 不是 42：" + id)
			}
			errs <- err
		}()
	}
	for range goroutines {
		if err := <-errs; err != nil {
			t.Errorf("併發驗章失敗：%v", err)
		}
	}
}

func TestStripBearer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"標準 Bearer 前綴", "Bearer abc.def.ghi", "abc.def.ghi"},
		{"沒有前綴時整串當 token（與 Java 版一致）", "abc.def.ghi", "abc.def.ghi"},
		{"大小寫不同不算前綴（Java 用 startsWith，區分大小寫）", "bearer abc.def.ghi", "bearer abc.def.ghi"},
		{"少了空格不算前綴", "Bearerabc.def.ghi", "Bearerabc.def.ghi"},
		{"只有 Bearer 沒有 token", "Bearer ", ""},
		{"空字串", "", ""},
		{"前綴後多餘空白會被 PlayerID 當空 token 處理", "Bearer    ", "   "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := StripBearer(tt.raw); got != tt.want {
				t.Errorf("StripBearer(%q) = %q，預期 %q", tt.raw, got, tt.want)
			}
		})
	}
}

// TestErrorMessagesDoNotLeakTokenContent 確認錯誤訊息不會把 token 原文吐出來。
//
// 為什麼在意：這些錯誤最終會進 log。token 是憑證，寫進 log 等於把憑證
// 留在檔案裡（而且 log 常常被集中收集、權限比資料庫寬鬆）。
func TestErrorMessagesDoNotLeakTokenContent(t *testing.T) {
	t.Parallel()

	key := []byte(testSecret)
	token := signWith(t, jwt.SigningMethodHS256, []byte("ffffffffffffffffffffffffffffffff"),
		playerClaims("42", time.Hour))

	_, err := NewVerifier(key).PlayerID(token)
	if err == nil {
		t.Fatal("預期驗章失敗")
	}
	// token 的簽章段（最後一段）是最敏感的部分，錯誤訊息裡不該出現。
	signature := token[strings.LastIndex(token, ".")+1:]
	if strings.Contains(err.Error(), signature) {
		t.Errorf("錯誤訊息含 token 簽章：%v", err)
	}
}
