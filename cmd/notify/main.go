// Command notify 是 Lucky Star 推播服務的入口。
//
// 它只做「組裝」：讀設定 → 建指標 → 建 server → 起 HTTP → 等關閉訊號。
// 任何業務邏輯都不該出現在這裡，這樣 internal/ 底下的每個套件都能被
// 獨立測試，不必啟動整個程序。
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AlexChang1999/lucky-star-notify-go/internal/config"
	"github.com/AlexChang1999/lucky-star-notify-go/internal/metrics"
	"github.com/AlexChang1999/lucky-star-notify-go/internal/server"
)

const (
	// shutdownTimeout 是收到關閉訊號後，允許收尾的時間上限。
	//
	// 5 秒是對齊計畫書的 DoD（Ctrl+C 後 5 秒內乾淨結束）。
	// 超過就放棄等待直接退出——關不掉比慢一點更糟，
	// k8s 的 terminationGracePeriodSeconds 到了會直接 SIGKILL。
	shutdownTimeout = 5 * time.Second

	// readHeaderTimeout 限制讀完請求 header 的時間。
	//
	// 不設的話，一個只送半個 header 就不動的連線可以永久佔住資源
	// （slowloris）。net/http 預設沒有上限，這是一定要自己補的洞。
	readHeaderTimeout = 10 * time.Second
)

func main() {
	if err := run(); err != nil {
		// 用 slog + os.Exit 而不是 log.Fatal：語意一樣，但訊息格式與
		// 服務其他部分一致（都是結構化日誌），方便日誌收集器解析。
		slog.Error("服務結束於錯誤", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.FromEnv()
	if err != nil {
		// 設定錯誤發生在 logger 建立之前，所以這裡回傳讓 main 印。
		return fmt.Errorf("載入設定失敗：%w", err)
	}

	// JSON 格式輸出到 stdout：容器的日誌就是 stdout，
	// 而結構化欄位（playerID、remote…）讓之後接日誌系統時不必重新解析文字。
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(logger)

	m := metrics.New()
	srv := server.New(cfg, logger, m)

	httpServer := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           srv.Handler(),
		ReadHeaderTimeout: readHeaderTimeout,
	}

	// signal.NotifyContext 把「收到訊號」變成「context 被 cancel」。
	//
	// 這是 Go 的招牌寫法，Java 沒有等價物（那邊要用 @PreDestroy +
	// SmartLifecycle 拼湊）。好處是同一個 ctx 之後可以一路往下傳給
	// Kafka consumer（Phase 3），關閉訊號一次通知到所有元件。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 緩衝 1：ListenAndServe 失敗時，即使沒人在 select 這條 channel
	// 也不會讓那個 goroutine 永久卡在寫入而洩漏。
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("服務啟動",
			"port", cfg.Port,
			"allowedOrigins", cfg.AllowedOrigins,
			"logLevel", cfg.LogLevel.String())
		// Shutdown 被呼叫時 ListenAndServe 會回 ErrServerClosed，
		// 那是正常結束，不是錯誤。
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	select {
	case err := <-serveErr:
		return fmt.Errorf("HTTP 伺服器錯誤：%w", err)
	case <-ctx.Done():
	}

	logger.Info("收到關閉訊號，開始優雅關閉")
	// 還原訊號的預設處理：關閉過程中再按一次 Ctrl+C 就直接中止，
	// 不會變成「按了沒反應」。
	stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	// 順序有意義：先讓 HTTP 停止接受新連線並等現有請求做完，
	// 再收掉 WebSocket。反過來的話，剛關掉的連線可能又有新的接進來。
	//
	// ⚠️ Shutdown 不會處理被 Hijack 的連線（WebSocket 就是），
	// 所以 srv.Close() 不是多餘的——少了它，程序會等到 goroutine
	// 自己結束為止，也就是永遠。詳見 server.Server.Close 的說明。
	shutdownErr := httpServer.Shutdown(shutdownCtx)
	closeErr := srv.Close()

	logger.Info("已停止")
	return errors.Join(shutdownErr, closeErr)
}
