package config

import (
	"errors"
	"log/slog"
	"strings"
	"testing"
)

// validSecret 是「剛好 32 bytes」的 secret，用來當合法基準。
const validSecret = "0123456789abcdef0123456789abcdef"

// fakeEnv 把 map 包成 LookupFunc。
//
// 用假環境而不是 t.Setenv：後者改的是行程層級的全域狀態，
// 會讓子測試無法 t.Parallel()，而且測試之間可能互相污染。
func fakeEnv(vars map[string]string) LookupFunc {
	return func(key string) (string, bool) {
		v, ok := vars[key]
		return v, ok
	}
}

func TestLoad(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		env     map[string]string
		want    Config
		wantErr error // 用 errors.Is 比對；nil 表示預期成功
	}{
		{
			name: "只給必填值，其餘走預設",
			env:  map[string]string{"JWT_SECRET": validSecret},
			want: Config{
				Port:           DefaultPort,
				JWTSecret:      []byte(validSecret),
				AllowedOrigins: []string{"*"},
				LogLevel:       slog.LevelInfo,
			},
		},
		{
			name: "全部覆寫",
			env: map[string]string{
				"JWT_SECRET":      validSecret,
				"PORT":            "9090",
				"ALLOWED_ORIGINS": "http://localhost:5173,https://luckystar.example",
				"LOG_LEVEL":       "debug",
			},
			want: Config{
				Port:           9090,
				JWTSecret:      []byte(validSecret),
				AllowedOrigins: []string{"http://localhost:5173", "https://luckystar.example"},
				LogLevel:       slog.LevelDebug,
			},
		},
		{
			name:    "缺 JWT_SECRET 即啟動失敗",
			env:     map[string]string{},
			wantErr: ErrMissingRequired,
		},
		{
			name:    "JWT_SECRET 為空字串也算缺",
			env:     map[string]string{"JWT_SECRET": ""},
			wantErr: ErrMissingRequired,
		},
		{
			// 對齊 Java：JJWT 的 Keys.hmacShaKeyFor 對 < 256 bit 的 key 丟
			// WeakKeyException，服務根本起不來。Go 若放過，就會出現
			// 「Go 版接受了 Java 版拒絕的設定」的跨語言不一致。
			name:    "JWT_SECRET 少於 32 bytes 被拒",
			env:     map[string]string{"JWT_SECRET": strings.Repeat("a", 31)},
			wantErr: ErrWeakSecret,
		},
		{
			// 中文字在 UTF-8 下一字 3 bytes，11 字 = 33 bytes → 過關。
			// 這個案例確認我們數的是 **bytes 不是字元數**（和 JJWT 一致）。
			name: "JWT_SECRET 用中文，長度以 bytes 計",
			env:  map[string]string{"JWT_SECRET": "幸運星推播服務密鑰壹貳"},
			want: Config{
				Port:           DefaultPort,
				JWTSecret:      []byte("幸運星推播服務密鑰壹貳"),
				AllowedOrigins: []string{"*"},
				LogLevel:       slog.LevelInfo,
			},
		},
		{
			name:    "PORT 不是數字",
			env:     map[string]string{"JWT_SECRET": validSecret, "PORT": "八千零八十八"},
			wantErr: ErrInvalidValue,
		},
		{
			name:    "PORT 超出範圍",
			env:     map[string]string{"JWT_SECRET": validSecret, "PORT": "70000"},
			wantErr: ErrInvalidValue,
		},
		{
			name:    "PORT 為 0（不接受，要明確指定或走預設）",
			env:     map[string]string{"JWT_SECRET": validSecret, "PORT": "0"},
			wantErr: ErrInvalidValue,
		},
		{
			name:    "LOG_LEVEL 認不出來",
			env:     map[string]string{"JWT_SECRET": validSecret, "LOG_LEVEL": "吵一點"},
			wantErr: ErrInvalidValue,
		},
		{
			name: "ALLOWED_ORIGINS 的空白與空項目被清掉",
			env: map[string]string{
				"JWT_SECRET":      validSecret,
				"ALLOWED_ORIGINS": " http://a.example , ,http://b.example ,",
			},
			want: Config{
				Port:           DefaultPort,
				JWTSecret:      []byte(validSecret),
				AllowedOrigins: []string{"http://a.example", "http://b.example"},
				LogLevel:       slog.LevelInfo,
			},
		},
		{
			name: "ALLOWED_ORIGINS 全是空白 → 退回預設的 *",
			env: map[string]string{
				"JWT_SECRET":      validSecret,
				"ALLOWED_ORIGINS": "  ,  ",
			},
			want: Config{
				Port:           DefaultPort,
				JWTSecret:      []byte(validSecret),
				AllowedOrigins: []string{"*"},
				LogLevel:       slog.LevelInfo,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Load(fakeEnv(tt.env))

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Load() error = %v，預期包含 %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() 意外失敗：%v", err)
			}

			if got.Port != tt.want.Port {
				t.Errorf("Port = %d，預期 %d", got.Port, tt.want.Port)
			}
			if string(got.JWTSecret) != string(tt.want.JWTSecret) {
				t.Errorf("JWTSecret = %q，預期 %q", got.JWTSecret, tt.want.JWTSecret)
			}
			if got.LogLevel != tt.want.LogLevel {
				t.Errorf("LogLevel = %v，預期 %v", got.LogLevel, tt.want.LogLevel)
			}
			if strings.Join(got.AllowedOrigins, "|") != strings.Join(tt.want.AllowedOrigins, "|") {
				t.Errorf("AllowedOrigins = %v，預期 %v", got.AllowedOrigins, tt.want.AllowedOrigins)
			}
		})
	}
}

// TestLoadReportsAllProblemsAtOnce 確認多個設定錯誤會一次全部回報。
//
// 為什麼在意這件事：Java 的 ${VAR:?...} 只會炸第一個缺的值，
// 修一個、重啟、再炸下一個。用 errors.Join 一次講完，部署時少繞好幾圈。
func TestLoadReportsAllProblemsAtOnce(t *testing.T) {
	t.Parallel()

	_, err := Load(fakeEnv(map[string]string{"PORT": "-1", "LOG_LEVEL": "nope"}))
	if err == nil {
		t.Fatal("預期失敗，卻成功了")
	}
	if !errors.Is(err, ErrMissingRequired) {
		t.Errorf("錯誤裡少了 ErrMissingRequired：%v", err)
	}
	if !errors.Is(err, ErrInvalidValue) {
		t.Errorf("錯誤裡少了 ErrInvalidValue：%v", err)
	}
}

func TestAllowsOrigin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		origins []string
		origin  string
		want    bool
	}{
		{"星號允許任何來源", []string{"*"}, "http://evil.example", true},
		{"星號也允許空來源（非瀏覽器 client）", []string{"*"}, "", true},
		{"白名單命中", []string{"http://a.example"}, "http://a.example", true},
		{"白名單沒命中", []string{"http://a.example"}, "http://b.example", false},
		{"沒帶 Origin 時放行（wscat / Go client 不送這個 header）", []string{"http://a.example"}, "", true},
		{"比對區分大小寫以外的一致性：scheme 不同就不算命中", []string{"https://a.example"}, "http://a.example", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := Config{AllowedOrigins: tt.origins}
			if got := c.AllowsOrigin(tt.origin); got != tt.want {
				t.Errorf("AllowsOrigin(%q) = %v，預期 %v", tt.origin, got, tt.want)
			}
		})
	}
}
