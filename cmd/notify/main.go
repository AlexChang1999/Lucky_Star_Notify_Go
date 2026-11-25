// Command notify 是 Lucky Star 推播服務的入口。
//
// 它只做「組裝」：讀設定 → 建指標 → 建 server → 起 HTTP → 等關閉訊號。
// 任何業務邏輯都不該出現在這裡，這樣 internal/ 底下的每個套件都能被
// 獨立測試，不必啟動整個程序。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/AlexChang1999/lucky-star-notify-go/internal/config"
	"github.com/AlexChang1999/lucky-star-notify-go/internal/consumer"
	"github.com/AlexChang1999/lucky-star-notify-go/internal/metrics"
	"github.com/AlexChang1999/lucky-star-notify-go/internal/server"
)

const (
	// shutdownTimeout 是收到關閉訊號後，允許收尾的時間上限。
	//
	// # 為什麼是 10 秒而不是計畫書原訂的 5 秒
	//
	// 5 秒是量測**之前**訂的目標。Phase 4 在真的接上 Kafka 之後實測，
	// 5 秒不夠——而且原因不在我們的程式碼：
	//
	// 三個 topic 各有一個 kafka.Reader，它們是**同一個 consumer group 的
	// 三個 member**。關閉時每個 member 送 LeaveGroup，而 coordinator 一次
	// 只處理一個、每次都觸發一輪 rebalance，所以三個 member 是**排隊**離開的
	// （即使我們這邊三條 goroutine 是並行關的）。
	//
	// 實測收工時間恰好每 3.000 秒一個。
	// ⚠️ 也就是說這個成本**隨 topic 數線性成長**，10 秒的餘裕不算多：
	// 要加第四個 topic 的話，這個值得一起調大。
	// （試過用 kafka-go 的 HeartbeatInterval 縮短它，無效，
	// 原因見 internal/consumer 的說明。）
	//
	// # 為什麼選擇「等它」而不是「砍掉重練」
	//
	// 把預算縮小、不等 LeaveGroup 送完，代價是 Kafka 要等到 session timeout
	// （預設 30 秒）才知道我們走了。下一次啟動的 rebalance 會被那段時間卡住，
	// 也就是**滾動更新時消費會出現一個 30 秒的空窗**。
	// 拿「關機慢 5 秒」換「重啟後空窗 30 秒」是划不來的。
	//
	// 10 秒仍然在兩個外部限制之內：docker stop 預設等 10 秒（compose 那邊
	// 已明確設 stop_grace_period: 15s），k8s 的 terminationGracePeriodSeconds
	// 預設 30 秒。
	shutdownTimeout = 10 * time.Second

	// readHeaderTimeout 限制讀完請求 header 的時間。
	//
	// 不設的話，一個只送半個 header 就不動的連線可以永久佔住資源
	// （slowloris）。net/http 預設沒有上限，這是一定要自己補的洞。
	readHeaderTimeout = 10 * time.Second

	// healthcheckTimeout 是 -healthcheck 模式等回應的上限。
	//
	// 要明顯短於 compose healthcheck 的 interval，否則探針還沒回覆
	// 下一次就開始了，會疊出一堆卡住的行程。
	healthcheckTimeout = 2 * time.Second
)

func main() {
	// ⚠️ 旗標要在 config 載入**之前**處理。-healthcheck 是在容器內被
	// docker 的探針呼叫的，那個行程拿不到 JWT_SECRET 之類的必填設定
	// （compose 的 healthcheck 不繼承 environment 以外的東西也就算了，
	// 更重要的是「檢查服務活著」本來就不該需要完整設定）。
	// 先 Load 再判斷旗標的話，健康檢查會因為設定不全而永遠失敗。
	healthcheck := flag.Bool("healthcheck", false,
		"對本機的 /actuator/health 發一次請求；正常回 0、異常回 1。"+
			"給 distroless 映像的容器探針用（映像裡沒有 curl 或 shell）")
	flag.Parse()

	if *healthcheck {
		if err := runHealthcheck(); err != nil {
			// 探針的輸出會進 docker 的 health log，是唯一能查失敗原因的地方，
			// 所以寫 stderr 而不是靜默退出。
			fmt.Fprintln(os.Stderr, "健康檢查失敗：", err)
			os.Exit(1)
		}
		return
	}

	if err := run(); err != nil {
		// 用 slog + os.Exit 而不是 log.Fatal：語意一樣，但訊息格式與
		// 服務其他部分一致（都是結構化日誌），方便日誌收集器解析。
		slog.Error("服務結束於錯誤", "err", err)
		os.Exit(1)
	}
}

// runHealthcheck 打自己的 /actuator/health，成功回 nil。
//
// # 為什麼要把探針做進同一個執行檔
//
// distroless / scratch 映像裡**沒有 curl、wget，也沒有 shell**，
// 所以 `HEALTHCHECK CMD curl ...` 那種寫法完全用不了。
// 唯一保證存在的執行檔就是服務自己——那就讓它多認一個旗標。
// 代價幾乎是零（net/http 本來就已經連進去了），換到的是 compose 可以用
// `depends_on: condition: service_healthy` 正確排序。
//
// 這個做法在 Go 容器圈是標準解法，Java 那邊沒有等價物：jar 不能自己跑，
// 一定要有 JRE，而有了 JRE 就順便有 shell，於是根本不會遇到這個問題。
func runHealthcheck() error {
	// 只讀 PORT，不走 config.Load——那個會要求 JWT_SECRET。
	// 讀不到或不合法就退回預設埠，理由是「探針不該因為設定解析而失敗」：
	// 設定真的錯了，服務本身根本起不來，探針自然也連不上，一樣會回報異常。
	port := config.DefaultPort
	if raw := os.Getenv("PORT"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			port = parsed
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), healthcheckTimeout)
	defer cancel()

	// 用 127.0.0.1 而不是 localhost：後者要查名稱解析，而在只有一個靜態
	// 執行檔的映像裡，多一個可能失敗的環節沒有任何好處。
	url := fmt.Sprintf("http://127.0.0.1:%d/actuator/health", port)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	// 一定要關，否則 -healthcheck 每跑一次就漏一個連線。這個行程隨即結束
	// 所以影響不大，但被探針每 10 秒呼叫一次的東西不該養成壞習慣。
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s 回應 %s", url, resp.Status)
	}
	return nil
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
	// SmartLifecycle 拼湊）。好處是同一個 ctx 一路往下傳給 Kafka consumer，
	// 關閉訊號一次通知到所有元件——這就是計畫書 §5.3「context 貫穿全程」。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// srv.Hub() 傳進去時會被收窄成 consumer.Pusher（只有兩個方法）。
	// main 因此完全不必 import hub——介面由使用端定義的好處在這裡具體化了。
	consumers := startConsumers(ctx, cfg, srv.Hub(), logger, m)

	// 緩衝 1：ListenAndServe 失敗時，即使沒人在 select 這條 channel
	// 也不會讓那個 goroutine 永久卡在寫入而洩漏。
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("服務啟動",
			"port", cfg.Port,
			"allowedOrigins", cfg.AllowedOrigins,
			"logLevel", cfg.LogLevel.String(),
			"debugPush", cfg.DebugPush)
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

	// 先等 Kafka consumer 收工：它們是訊息來源，停掉之後才不會有新的推播
	// 灌進正要關閉的連線。正常情況下這只花幾毫秒（FetchMessage 一收到取消
	// 就返回），但它與底下兩步**共用** shutdownTimeout 的 5 秒預算——
	// Kafka 掛掉時最壞會吃掉 commit 的那段逾時。
	waitConsumers(shutdownCtx, consumers, logger)

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

// startConsumers 為每個 topic 起一條 goroutine，回傳等它們結束用的 WaitGroup。
//
// Kafka 沒設定（`KAFKA_BOOTSTRAP_SERVERS=` 明確給空值）時什麼都不起，只印
// 一行 WARN——服務的 WebSocket 那一半照常可用，適合只想驗推播路徑的本機開發。
//
// 這裡收 consumer.Pusher 而不是 *hub.Hub：main 只是把東西接起來，不需要
// 認識 hub 的其他十幾個方法。
func startConsumers(
	ctx context.Context,
	cfg config.Config,
	pusher consumer.Pusher,
	logger *slog.Logger,
	m *metrics.Metrics,
) *sync.WaitGroup {
	var wg sync.WaitGroup

	if !cfg.KafkaEnabled() {
		logger.Warn("KAFKA_BOOTSTRAP_SERVERS 為空，不啟動 Kafka consumer；" +
			"此時唯一的訊息來源是 POST /debug/push")
		return &wg
	}

	// 不拒絕啟動、只警告：Java 版沒在跑的時候沿用它的 group（接續 offset）
	// 是合法用法。但兩版同時跑就會互搶 partition，症狀是「Go 版隨機漏訊息」，
	// 而真因完全看不出來——所以一定要留下這行日誌（AGENTS.md §2.4）。
	if cfg.KafkaGroupID == config.JavaKafkaGroupID {
		logger.Warn("consumer group id 與 Java 版相同：兩版同時執行時，"+
			"每則事件只會進其中一版", "groupID", cfg.KafkaGroupID)
	}

	logger.Info("啟動 Kafka consumer",
		"brokers", cfg.KafkaBrokers,
		"groupID", cfg.KafkaGroupID,
		"topics", consumer.AllTopics)

	for _, topic := range consumer.AllTopics {
		c := consumer.New(topic, cfg.KafkaBrokers, cfg.KafkaGroupID, pusher, logger, m)
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Run 只在關閉時回傳 nil；回傳錯誤代表是真的壞了，不是關機。
			if err := c.Run(ctx); err != nil {
				logger.Error("Kafka consumer 異常結束", "topic", topic, "err", err)
			}
		}()
	}
	return &wg
}

// waitConsumers 等 consumer 收工，最多等到 ctx 逾時。
//
// WaitGroup.Wait() 沒有帶逾時的版本，所以包一條 goroutine 把它轉成可以
// select 的 channel——這是 Go 裡把「阻塞呼叫」變成「事件」的標準手法，
// 值得記住（Java 那邊對應的是 CountDownLatch.await(timeout)）。
func waitConsumers(ctx context.Context, wg *sync.WaitGroup, logger *slog.Logger) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		logger.Warn("等 Kafka consumer 收工逾時，不再等待")
	}
}
