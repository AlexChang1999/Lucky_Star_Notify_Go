// Package config 把環境變數讀成一個 Config，並在設定不合法時讓程序**啟動失敗**。
//
// # 為什麼要 fail-fast，而不是「用預設值撐過去」
//
// Java 版的 application.yml 寫 ${JWT_SECRET:?...}，缺值時 Spring 直接拒絕啟動。
// 這個選擇很重要：如果 secret 缺了還讓服務起來，症狀會是「服務活著，但每個
// 玩家都連不上」——而且錯誤訊息只會說 signature is invalid，指不到真因。
// 設定錯誤要在部署當下就炸，不要延後到第一個使用者身上。
//
// Go 沒有 Spring 那套 placeholder 機制，所以這件事得自己做。
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
)

// DefaultPort 是本服務的監聽埠。
//
// 刻意與 Java 版（8087）錯開：兩版要能同時跑在同一台機器上做 A/B 對照。
const DefaultPort = 8088

// MinSecretLen 是 HS256 secret 的最小長度（**bytes，不是字元數**）。
//
// 這個數字不是我們發明的，是為了對齊 Java：JJWT 的 Keys.hmacShaKeyFor()
// 對短於 256 bit（32 bytes）的 key 會丟 WeakKeyException，服務起不來。
// golang-jwt 不做這個檢查，所以我們自己補——否則就會出現「Go 版接受了
// Java 版拒絕的設定」這種跨語言行為不一致，而 Phase 5 的契約測試正是
// 要證明兩版行為相同。
//
// 密碼學上的理由：HMAC-SHA256 的安全強度上限就是 256 bit，key 比它短
// 只是白白削弱簽章強度，沒有任何好處。
const MinSecretLen = 32

// 設定錯誤的哨兵（sentinel）。
//
// 為什麼用哨兵而不是各自 fmt.Errorf 出一段字串：測試要能用 errors.Is
// 判斷「是哪一類錯誤」，而不是去比對錯誤訊息的文字——訊息會改，
// 用文字比對的測試很脆。
var (
	// ErrMissingRequired 表示必填的環境變數沒給（或給了空字串）。
	ErrMissingRequired = errors.New("缺少必填的環境變數")
	// ErrInvalidValue 表示環境變數有給，但值格式不對或超出範圍。
	ErrInvalidValue = errors.New("環境變數的值不合法")
	// ErrWeakSecret 表示 JWT_SECRET 太短，見 MinSecretLen 的說明。
	ErrWeakSecret = errors.New("JWT_SECRET 太短")
)

// LookupFunc 與 os.LookupEnv 同型：回傳值與「有沒有設定」。
//
// 把環境查詢做成參數（而不是在 Load 裡直接呼叫 os.LookupEnv），
// 是為了讓測試能餵假環境。替代方案 t.Setenv 改的是行程層級的全域狀態，
// 會讓子測試不能 t.Parallel()。一個函式型別就解決，不需要 interface。
type LookupFunc func(key string) (string, bool)

// Config 是本服務啟動所需的全部設定。
//
// 欄位刻意存「已經解析好的型別」（int / []byte / slog.Level）而不是字串：
// 解析與驗證集中在 Load 一處完成，之後的程式碼就不必再處理格式錯誤。
type Config struct {
	// Port 是 HTTP/WebSocket 的監聽埠。
	Port int
	// JWTSecret 是 HS256 的簽章金鑰，**secret 字串的原始 UTF-8 位元組**。
	//
	// ⚠️ 絕對不要 base64 解碼。Java 版用的是 secret.getBytes(UTF_8)，
	// 網路上多數 Go JWT 教學會示範 base64.StdEncoding.DecodeString，
	// 照抄的話驗章永遠失敗，而錯誤訊息只會說 signature is invalid。
	JWTSecret []byte
	// AllowedOrigins 是允許的 WebSocket 來源；單一元素 "*" 代表全部允許。
	//
	// 預設是 "*"，對齊 Java 版的 setAllowedOriginPatterns("*")。
	AllowedOrigins []string
	// LogLevel 是 slog 的最低輸出等級。
	LogLevel slog.Level
	// DebugPush 決定要不要註冊 POST /debug/push 這個手動灌訊息的端點。
	//
	// ⚠️ 這個端點沒有鑑權，開著等於任何能連到本服務的人都能對任意玩家
	// 推任意內容。**Phase 3 接上 Kafka、以及任何對外部署，都必須設成 false。**
	//
	// 預設 true 是刻意的權衡：Phase 2 還沒有 Kafka，關掉的話這個服務
	// 完全沒有訊息來源，連不上任何可以驗收的東西。啟動時會印一行
	// WARN 提醒，不會靜默地開著。
	DebugPush bool
}

// FromEnv 從真正的行程環境載入設定。
func FromEnv() (Config, error) {
	return Load(os.LookupEnv)
}

// Load 從 lookup 提供的環境載入設定。
//
// 所有問題會**一次全部回報**（errors.Join），而不是遇到第一個就返回：
// 部署時一輪就能把設定改對，不用「修一個、重啟、再炸下一個」。
func Load(lookup LookupFunc) (Config, error) {
	cfg := Config{
		Port:           DefaultPort,
		AllowedOrigins: []string{"*"},
		LogLevel:       slog.LevelInfo,
		DebugPush:      true,
	}

	// problems 蒐集所有錯誤，最後用 errors.Join 併成一個。
	// errors.Join 產生的錯誤對每一個成員都能通過 errors.Is，
	// 所以測試可以分別檢查「缺必填」與「值不合法」都有被回報。
	var problems []error

	secret, ok := lookup("JWT_SECRET")
	switch {
	case !ok || secret == "":
		problems = append(problems, fmt.Errorf("%w：JWT_SECRET", ErrMissingRequired))
	case len(secret) < MinSecretLen:
		// len() 數的是 bytes，這正是我們要的——和 JJWT 一致。
		problems = append(problems, fmt.Errorf("%w：目前 %d bytes，至少要 %d bytes",
			ErrWeakSecret, len(secret), MinSecretLen))
	default:
		cfg.JWTSecret = []byte(secret)
	}

	if raw, ok := lookup("PORT"); ok && raw != "" {
		port, err := strconv.Atoi(raw)
		switch {
		case err != nil:
			problems = append(problems, fmt.Errorf("%w：PORT=%q 不是整數", ErrInvalidValue, raw))
		case port < 1 || port > 65535:
			// 0 也拒絕：Go 的 net.Listen 會把 0 解讀成「隨機挑一個空閒埠」，
			// 那對一個要被 compose healthcheck 打的服務來說是壞事，不是方便。
			problems = append(problems, fmt.Errorf("%w：PORT=%d 不在 1~65535", ErrInvalidValue, port))
		default:
			cfg.Port = port
		}
	}

	if raw, ok := lookup("ALLOWED_ORIGINS"); ok {
		if origins := splitAndTrim(raw); len(origins) > 0 {
			cfg.AllowedOrigins = origins
		}
	}

	if raw, ok := lookup("LOG_LEVEL"); ok && raw != "" {
		// slog.Level 自己就實作了 encoding.TextUnmarshaler，
		// 認得 DEBUG / INFO / WARN / ERROR（不分大小寫）與 "DEBUG+2" 這種偏移寫法。
		// 標準庫已經有的東西就不要自己寫 switch。
		var level slog.Level
		if err := level.UnmarshalText([]byte(raw)); err != nil {
			problems = append(problems, fmt.Errorf("%w：LOG_LEVEL=%q 認不出來", ErrInvalidValue, raw))
		} else {
			cfg.LogLevel = level
		}
	}

	if raw, ok := lookup("DEBUG_PUSH"); ok && raw != "" {
		// ParseBool 認得 1/t/T/true/TRUE/True 與 0/f/F/false/FALSE/False。
		// 不自己寫 raw == "true" 的比對，那會把 DEBUG_PUSH=1 誤判成關閉。
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			problems = append(problems, fmt.Errorf("%w：DEBUG_PUSH=%q 不是布林值", ErrInvalidValue, raw))
		} else {
			cfg.DebugPush = enabled
		}
	}

	if len(problems) > 0 {
		return Config{}, errors.Join(problems...)
	}
	return cfg, nil
}

// AllowsOrigin 回報是否接受來自 origin 的 WebSocket 連線。
//
// origin 為空字串時放行：非瀏覽器的 client（wscat、Go 的測試 client、
// 契約測試腳本）根本不送 Origin header，把它們擋掉只會擋到自己人。
// 瀏覽器一定會送，所以真正的跨站攻擊仍然被白名單擋著。
func (c Config) AllowsOrigin(origin string) bool {
	if origin == "" {
		return true
	}
	for _, allowed := range c.AllowedOrigins {
		if allowed == "*" || allowed == origin {
			return true
		}
	}
	return false
}

// splitAndTrim 把逗號分隔的清單切開，去掉前後空白與空項目。
func splitAndTrim(raw string) []string {
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
