// Command demo 是 docker compose 展示用的假上游服務。
//
// 它扮演真實環境裡 game-service / rank-service / 各種業務服務的角色，
// 做三件事：
//
//  1. 定期把假事件灌進三個 Kafka topic
//  2. 提供一頁 HTML，用 @stomp/stompjs 連上推播服務並把收到的訊息顯示出來
//  3. 簽發玩家 token（正式環境是 member-service 的工作）
//
// # ⚠️ 這個程式只能在 compose 的 demo 環境裡跑
//
// 它會用共用的 JWT secret 簽出**任意玩家身分**的 token，而且沒有任何鑑權。
// 這正是它必須與 cmd/notify 分成兩個執行檔、兩個映像的理由：
// 正式服務的映像裡不存在這段程式碼，就不可能被誤開。
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	defaultPort          = 8090
	defaultBrokers       = "localhost:9092"
	defaultWSURL         = "ws://localhost:8088/ws"
	defaultEventInterval = 3 * time.Second
	defaultTokenTTL      = time.Hour

	// readHeaderTimeout 與 cmd/notify 同理，見那裡的說明。
	readHeaderTimeout = 10 * time.Second
	shutdownTimeout   = 5 * time.Second

	// topicCreateTimeout 是啟動時建立 topic 的時間上限，見 ensureTopics。
	topicCreateTimeout = 10 * time.Second

	// produceRetryDelay 是寫入 Kafka 失敗後的退避時間。
	//
	// compose 啟動時 Kafka 通常比這個程式晚幾秒才準備好，前幾則必然失敗。
	// 這是**正常現象不是故障**，所以退避重試而不是結束程序。
	produceRetryDelay = 2 * time.Second
)

// demoConfig 是這個程式的全部設定。
//
// 刻意不重用 internal/config：那一份的必填欄位、預設值與驗證規則都是為
// 推播服務設計的（例如它要求 JWT_SECRET 至少 32 bytes、預設埠 8088）。
// 為了共用而把兩個程式的設定攪在一起，會讓推播服務的設定被 demo 的需求
// 汙染——那是本末倒置。三十行的重複遠比一個「什麼都支援」的設定套件便宜。
type demoConfig struct {
	Port          int
	JWTSecret     []byte
	Brokers       []string
	WSURL         string
	EventInterval time.Duration
	TokenTTL      time.Duration
}

func loadConfig() (demoConfig, error) {
	cfg := demoConfig{
		Port:          defaultPort,
		Brokers:       splitAndTrim(defaultBrokers),
		WSURL:         defaultWSURL,
		EventInterval: defaultEventInterval,
		TokenTTL:      defaultTokenTTL,
	}

	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		// 唯一的必填項。沒有它，簽出來的 token 推播服務驗不過，
		// 而症狀是「頁面一直連不上但沒說為什麼」——寧可現在就起不來。
		return demoConfig{}, errors.New("缺少必填的環境變數 JWT_SECRET")
	}
	// 與推播服務一樣取字串的原始 UTF-8 位元組，**不做 base64 解碼**。
	// 兩邊只要有一邊解讀方式不同，簽章就永遠對不起來（AGENTS.md §2.1）。
	cfg.JWTSecret = []byte(secret)

	if raw := os.Getenv("PORT"); raw != "" {
		port, err := strconv.Atoi(raw)
		if err != nil || port < 1 || port > 65535 {
			return demoConfig{}, fmt.Errorf("PORT=%q 不是合法的埠號", raw)
		}
		cfg.Port = port
	}

	if raw, ok := os.LookupEnv("KAFKA_BOOTSTRAP_SERVERS"); ok {
		cfg.Brokers = splitAndTrim(raw)
		if len(cfg.Brokers) == 0 {
			return demoConfig{}, errors.New("KAFKA_BOOTSTRAP_SERVERS 是空的，沒有 Kafka 就沒有事件可發")
		}
	}

	// 瀏覽器要連的 WebSocket 位址。
	//
	// 為什麼不能直接用容器名（ws://notify-go:8088/ws）：這個字串是給
	// **瀏覽器**用的，而瀏覽器跑在宿主上，解析不到 compose 內部的 DNS 名稱。
	// 這是容器化 demo 最常見的坑之一，所以做成可設定並預設指向 localhost。
	if raw := os.Getenv("NOTIFY_WS_URL"); raw != "" {
		cfg.WSURL = raw
	}

	if raw := os.Getenv("EVENT_INTERVAL"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return demoConfig{}, fmt.Errorf("EVENT_INTERVAL=%q 不是合法的正時距（例：3s、500ms）", raw)
		}
		cfg.EventInterval = d
	}

	return cfg, nil
}

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

func main() {
	if err := run(); err != nil {
		slog.Error("demo 結束於錯誤", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	writer := &kafka.Writer{
		Addr: kafka.TCP(cfg.Brokers...),
		// Balancer 只在沒給 Key 時決定 partition。Hash 讓同一個玩家的事件
		// 永遠落在同一個 partition，也就保住了該玩家事件之間的順序。
		Balancer: &kafka.Hash{},
		// 讓 writer 自己建 topic。少了它，第一次 `docker compose up` 會因為
		// topic 不存在而失敗，使用者得手動跑 kafka-topics.sh——
		// 那就不叫「clone 完直接跑起來」了。
		AllowAutoTopicCreation: true,
		// ⚠️ kafka-go 的 BatchTimeout **預設是 1 秒**（BatchSize 預設 100）。
		// 這個產生器每次只寫一則，於是每一則都會在 producer 端整整壓一秒
		// 才送出——demo 頁面上看到的「即時推播」因此無端慢一秒，
		// 而且日誌與指標都看不出來（`notify_push_latency_seconds` 量的是
		// Kafka 訊息時間戳之後的事，這一秒發生在它之前）。
		// 一則一批就立刻送出。Phase 5 做扇出延遲量測時才發現這件事。
		BatchSize:    1,
		BatchTimeout: 5 * time.Millisecond,
	}
	defer func() {
		if err := writer.Close(); err != nil {
			logger.Error("關閉 Kafka writer 失敗", "err", err)
		}
	}()

	// 先把 topic 建好再開始發。
	//
	// 兩邊的 AllowAutoTopicCreation 其實就夠讓 topic 被建出來，但那是
	// 「第一次寫入失敗 → 退避 2 秒 → 再試」的路徑，而 consumer 那邊還要
	// 等 partition watcher（5 秒）偵測到才會 rebalance——冷啟動因此要
	// 三十秒左右才看得到第一則推播。CI 的煙霧測試就是這樣超時的。
	//
	// 明確先建，順便也消掉啟動時那行看起來很像壞掉的 WARN。
	ensureTopics(ctx, cfg, logger)

	go produceLoop(ctx, cfg, writer, logger)

	httpServer := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           newMux(cfg, logger),
		ReadHeaderTimeout: readHeaderTimeout,
	}

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("demo 啟動",
			"port", cfg.Port,
			"brokers", cfg.Brokers,
			"wsURL", cfg.WSURL,
			"eventInterval", cfg.EventInterval.String())
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	select {
	case err := <-serveErr:
		return fmt.Errorf("HTTP 伺服器錯誤：%w", err)
	case <-ctx.Done():
	}

	logger.Info("收到關閉訊號")
	stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	return httpServer.Shutdown(shutdownCtx)
}

// ensureTopics 建立三個 topic，**best-effort**。
//
// # 為什麼失敗只記 WARN 而不讓程序結束
//
// 它是「加速器」不是「前提」：writer 帶了 AllowAutoTopicCreation，
// broker 也開了自動建立，所以就算這一步整個失敗，topic 仍然會在第一次
// 寫入時被建出來，只是慢個幾十秒。為了一個純粹的最佳化而讓 demo 起不來
// 是不划算的——尤其在權限受限的 Kafka 上（沒有 CreateTopics 權限）
// 這一步注定會失敗，但 demo 其實還是能跑。
//
// 分區數固定 1：demo 只有一個 consumer，多分區不會更快，
// 反而會讓「同一個玩家的事件保持順序」這件事需要額外解釋。
func ensureTopics(ctx context.Context, cfg demoConfig, logger *slog.Logger) {
	client := &kafka.Client{Addr: kafka.TCP(cfg.Brokers...), Timeout: topicCreateTimeout}

	topics := make([]kafka.TopicConfig, 0, len(allTopics))
	for _, topic := range allTopics {
		topics = append(topics, kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		})
	}

	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: topics})
	if err != nil {
		logger.Warn("建立 topic 失敗，改由第一次寫入時自動建立", "err", err)
		return
	}

	for topic, topicErr := range resp.Errors {
		// 已經存在不是錯誤——重複啟動 demo 是完全正常的用法。
		// kafka-go 對 TopicAlreadyExists 回傳的就是這個 error 值，
		// 所以用 errors.Is 比對而不是去看字串。
		if topicErr != nil && !errors.Is(topicErr, kafka.TopicAlreadyExists) {
			logger.Warn("建立 topic 失敗", "topic", topic, "err", topicErr)
		}
	}
	logger.Info("topic 已就緒", "topics", allTopics)
}

// produceLoop 每隔 EventInterval 發一則假事件，直到 ctx 被取消。
func produceLoop(ctx context.Context, cfg demoConfig, writer *kafka.Writer, logger *slog.Logger) {
	gen := newGenerator()

	ticker := time.NewTicker(cfg.EventInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("停止產生事件")
			return
		case now := <-ticker.C:
			event, err := gen.next(now)
			if err != nil {
				// 編碼失敗只可能是程式碼寫錯（欄位型別不對），不是執行期問題。
				logger.Error("產生事件失敗", "err", err)
				continue
			}

			if err := writer.WriteMessages(ctx, kafka.Message{
				Topic: event.Topic,
				Key:   event.Key,
				Value: event.Value,
			}); err != nil {
				// ctx 被取消時 WriteMessages 也會回錯，那是關機不是故障。
				if ctx.Err() != nil {
					return
				}
				// 這裡刻意用 Warn 而不是 Error：compose 剛啟動時 Kafka 還沒
				// 準備好，前幾則失敗是預期中的事，用 Error 會讓第一眼看起來
				// 像壞掉了。
				logger.Warn("寫入 Kafka 失敗，稍後重試",
					"topic", event.Topic, "err", err, "retryIn", produceRetryDelay.String())

				// 退避。用 select 而不是 time.Sleep，否則關機訊號要等滿
				// 這段時間才會被看到。
				select {
				case <-ctx.Done():
					return
				case <-time.After(produceRetryDelay):
				}
				continue
			}

			logger.Debug("已送出事件", "topic", event.Topic, "key", string(event.Key))
		}
	}
}
