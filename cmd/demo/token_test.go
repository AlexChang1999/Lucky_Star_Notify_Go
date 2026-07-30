package main

import (
	"errors"
	"testing"
	"time"

	"github.com/AlexChang1999/lucky-star-notify-go/internal/auth"
)

// demoSecret 至少 32 bytes，對齊 config.MinSecretLen。
const demoSecret = "demo-secret-至少要三十二個位元組才行"

// TestMintTokenAcceptedByRealVerifier 用**推播服務真正在用的**驗證器驗簽。
//
// # 為什麼要 import internal/auth，而不是在這裡自己 jwt.Parse 一次
//
// 自己解一次只能證明「我簽的東西我自己解得開」——那是同義反覆。
// 這個 demo 存在的唯一理由是「簽出來的 token 要能連上推播服務」，
// 所以要驗的對象必須是服務那一端的程式碼。
// 兩邊只要有一邊對 secret 的解讀不同（例如有人在某一側加了 base64 解碼，
// AGENTS.md §2.1），這條測試會當場失敗；否則那個錯誤要到瀏覽器上
// 才會現形，而訊息只會說 signature is invalid。
func TestMintTokenAcceptedByRealVerifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		playerID int64
		wantSub  string
	}{
		{name: "名單上的第一位玩家", playerID: 1001, wantSub: "1001"},
		{name: "名單上的第二位玩家", playerID: 1002, wantSub: "1002"},
		{
			// subject 必須是**十進位字串**：hub 的私人索引 key 來自 JWT 的
			// subject，而 consumer 那邊是用 strconv.FormatInt 產生的。
			// 表示法不一致的話推播會靜默送不到人。
			name: "大到超過 JavaScript 安全整數的 ID 仍是十進位字串",
			// 2^53 + 1。這個值在 JS 的 number 裡會被改成 9007199254740992，
			// 所以頁面那邊玩家 ID 一律用字串傳（見 http.go 的 playerView）。
			playerID: 9007199254740993,
			wantSub:  "9007199254740993",
		},
	}

	verifier := auth.NewVerifier([]byte(demoSecret))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			token, err := mintToken([]byte(demoSecret), tt.playerID, time.Hour, time.Now())
			if err != nil {
				t.Fatalf("mintToken() 意外失敗：%v", err)
			}

			got, err := verifier.PlayerID(token)
			if err != nil {
				t.Fatalf("服務端的驗證器拒絕了 demo 簽的 token：%v", err)
			}
			if got != tt.wantSub {
				t.Errorf("subject = %q，預期 %q", got, tt.wantSub)
			}
		})
	}
}

// TestMintTokenRejectedCases 確認簽出來的 token 在該被拒絕時真的被拒絕。
//
// 這幾條看起來像在測 golang-jwt 而不是測我們的程式碼，但它們釘住的是
// **我們有沒有正確地把 exp 與 secret 交出去**——例如漏傳 ExpiresAt，
// 過期的 token 就會永遠有效，而那種 bug 不會有任何症狀，直到被利用。
func TestMintTokenRejectedCases(t *testing.T) {
	t.Parallel()

	verifier := auth.NewVerifier([]byte(demoSecret))

	t.Run("過期的 token 會被拒絕", func(t *testing.T) {
		t.Parallel()

		// 把「現在」往回推兩小時、TTL 只有一小時 → 簽出來就已經過期。
		// 這比 time.Sleep 一小時務實，也是把 now 做成參數的另一個好處。
		past := time.Now().Add(-2 * time.Hour)
		token, err := mintToken([]byte(demoSecret), 1001, time.Hour, past)
		if err != nil {
			t.Fatalf("mintToken() 意外失敗：%v", err)
		}

		if _, err := verifier.PlayerID(token); !errors.Is(err, auth.ErrInvalidToken) {
			t.Errorf("預期 ErrInvalidToken，實際 %v", err)
		}
	})

	t.Run("用別的 secret 簽的 token 會被拒絕", func(t *testing.T) {
		t.Parallel()

		token, err := mintToken([]byte("另一把完全不同的-secret-也要夠長才行"), 1001, time.Hour, time.Now())
		if err != nil {
			t.Fatalf("mintToken() 意外失敗：%v", err)
		}

		if _, err := verifier.PlayerID(token); !errors.Is(err, auth.ErrInvalidToken) {
			t.Errorf("預期 ErrInvalidToken，實際 %v", err)
		}
	})
}

// TestIsDemoPlayer 確認 token 端點的名單檢查。
func TestIsDemoPlayer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		id   int64
		want bool
	}{
		{name: "名單上的玩家", id: 1001, want: true},
		{name: "名單外的玩家", id: 9999, want: false},
		// 0 是 int64 的零值。特別測它是因為「解析失敗回傳 0」這種寫法很常見，
		// 若哪天有人把 ParseInt 的錯誤忽略掉，這裡會擋下來。
		{name: "零值不算合法玩家", id: 0, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isDemoPlayer(tt.id); got != tt.want {
				t.Errorf("isDemoPlayer(%d) = %v，預期 %v", tt.id, got, tt.want)
			}
		})
	}
}
