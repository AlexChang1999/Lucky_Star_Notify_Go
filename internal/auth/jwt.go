// Package auth 驗證玩家的 access token（HS256），成功後取出 playerID。
//
// # 與 Java 版的對應
//
// 對照 notification-service 的 PlayerJwtVerifier：同一把 jwt.secret、同樣的
// HS256、playerID 同樣取 JWT 的 subject。差別只有錯誤處理的風格——
// Java 把所有失敗折成 null，Go 這裡回具體的 error 讓上層能決定要 log 什麼。
//
// # 兩個一定要記住的地雷
//
//  1. secret 是**字串的原始 UTF-8 位元組**，不是 base64。Java 用
//     Keys.hmacShaKeyFor(secret.getBytes(UTF_8))；多數 Go 教學會示範
//     base64 解碼，照抄就永遠驗不過，而錯誤訊息只會說 signature is invalid。
//
//  2. jwt.Parse 必須帶 WithValidMethods。不帶等於接受 alg:none 與
//     演算法混淆攻擊——這是 JWT 最經典的漏洞。
package auth

import (
	"errors"
	"fmt"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// bearerPrefix 與 Java 版的 BEARER_PREFIX 完全一致（含尾端空格、區分大小寫）。
const bearerPrefix = "Bearer "

// 驗章失敗的哨兵。
//
// 三種分開是因為上層要的反應不同：ErrNoToken 是「client 根本沒帶」，
// 屬於用法錯誤；ErrInvalidToken 是「帶了但不對」，可能是攻擊；
// ErrNoSubject 是「簽章對但 token 內容不是我們認得的形狀」，
// 通常代表發 token 的那一端改了格式——這個要能一眼看出來。
var (
	// ErrNoToken 表示沒有提供 token（缺 header，或值是空白）。
	ErrNoToken = errors.New("未提供 token")
	// ErrInvalidToken 表示簽章、演算法、或有效期不通過。
	ErrInvalidToken = errors.New("token 無效")
	// ErrNoSubject 表示簽章通過但缺少 subject（playerID）。
	ErrNoSubject = errors.New("token 缺少 subject")
)

// Verifier 驗證玩家 token。
//
// 建一次、全程共用即可：內部沒有可變狀態，可安全地被多個 goroutine 併發使用
// （每條 WebSocket 連線都會在自己的 goroutine 裡呼叫）。
type Verifier struct {
	key []byte
	// parser 預先組好解析選項。
	//
	// 為什麼不每次呼叫 jwt.Parse(...) 現組選項：那會在每條連線的握手
	// 路徑上多配置幾個物件。jwt.Parser 本身是唯讀的，共用是安全的。
	parser *jwt.Parser
}

// NewVerifier 用 secret 的原始位元組建立驗證器。
//
// ⚠️ secret 必須是「member-service 的 jwt.secret 字串直接轉成 bytes」，
// 呼叫端**不要**先做 base64 解碼。長度檢查在 config 那層做（見 config.MinSecretLen），
// 這裡不重複檢查——測試需要能用短 key 構造情境。
func NewVerifier(secret []byte) *Verifier {
	return &Verifier{
		key: secret,
		parser: jwt.NewParser(
			// ★ 安全要求，不是選配：明確宣告只接受 HS256。
			// 不帶這個選項，攻擊者把 header 的 alg 改成 "none" 並清空簽章
			// 就能偽造任何身分。
			//
			// 另一種常見寫法是在 keyfunc 裡檢查
			// token.Method.(*jwt.SigningMethodHMAC)，但那只擋掉非 HMAC，
			// HS512 還是會過。WithValidMethods 是完整的做法。
			jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),

			// 刻意**不加** WithExpirationRequired()：JJWT 在 exp 不存在時
			// 也不檢查有效期，加了會讓 Go 版拒絕 Java 版接受的 token，
			// 破壞 Phase 5 的跨語言契約等價。
			// 這是「對齊既有行為」勝過「更嚴格」的一個取捨。
		),
	}
}

// PlayerID 驗證 token 並回傳 playerID（JWT subject）。
//
// 失敗時 playerID 一定是空字串——呼叫端不必額外判斷「有 error 但 id 也有值」
// 這種曖昧狀態。
func (v *Verifier) PlayerID(token string) (string, error) {
	if strings.TrimSpace(token) == "" {
		// 對齊 Java 的 token.isBlank()：只有空白也算沒帶。
		return "", ErrNoToken
	}

	// claims 每次呼叫都新配置一份。
	// 若把它提升成 Verifier 的欄位來「省配置」，多條連線就會寫同一塊記憶體——
	// 那是不折不扣的 data race，而且症狀是「偶爾把 A 玩家的訊息推給 B」。
	claims := &jwt.RegisteredClaims{}

	// keyfunc 的職責只有「這個 token 該用哪把 key 驗」。
	// 演算法檢查交給 WithValidMethods，不在這裡重複。
	_, err := v.parser.ParseWithClaims(token, claims, func(*jwt.Token) (any, error) {
		return v.key, nil
	})
	if err != nil {
		// 用 %w 包起來：上層想細分（過期？簽章錯？）時可以繼續 errors.Is
		// 到 golang-jwt 的哨兵（jwt.ErrTokenExpired 等），
		// 同時我們對外仍然只有一個 ErrInvalidToken 的分類。
		//
		// 注意這裡帶的是 golang-jwt 的錯誤，**不含 token 原文**——
		// token 是憑證，不該進 log。
		return "", fmt.Errorf("%w：%w", ErrInvalidToken, err)
	}

	if claims.Subject == "" {
		return "", ErrNoSubject
	}
	return claims.Subject, nil
}

// StripBearer 去掉 "Bearer " 前綴；沒有前綴時原樣回傳。
//
// 「沒有前綴就整串當 token」是刻意對齊 Java 版的 extractBearerToken——
// 它也是這個寬鬆行為。前綴比對區分大小寫（Java 用 startsWith），
// 所以 "bearer xxx" 不會被剝掉，會整串進去驗然後失敗。
// 這不是我們喜歡的設計，但**行為一致優先**：兩版對同一個輸入要有同樣的結果。
func StripBearer(raw string) string {
	if after, ok := strings.CutPrefix(raw, bearerPrefix); ok {
		return after
	}
	return raw
}
