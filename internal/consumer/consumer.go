package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/AlexChang1999/lucky-star-notify-go/internal/metrics"
	"github.com/segmentio/kafka-go"
)

const (
	// fetchBackoff 是取訊息失敗後的等待時間。
	//
	// 沒有這個等待，Kafka 連不上時迴圈會以 CPU 全速空轉、日誌每秒噴幾萬行。
	// 服務看起來還活著，但機器等於掛了——而且真正的原因（broker 連不上）
	// 會被自己刷出來的日誌埋掉。
	fetchBackoff = 2 * time.Second

	// commitTimeout 是提交 offset 的時間上限，見 Consumer.commit。
	commitTimeout = 5 * time.Second

	// maxFetchBytes 是單次 fetch 的回傳上限。
	maxFetchBytes = 10 << 20

	// partitionWatchInterval 是檢查 partition 數有沒有變的間隔。
	//
	// 這個機制真正在防的是「topic 比 consumer 晚出現」，見 New 裡的說明。
	// 5 秒是 kafka-go 的預設值，明寫出來是因為它與那個坑直接相關——
	// 留白會讓讀的人以為沒有在監看。
	partitionWatchInterval = 5 * time.Second
)

// 關機為什麼是「每個 topic 約 3 秒」——一個試過並排除的假設，記在這裡
// 免得下次有人重走一遍。
//
// 實測（Phase 4，compose 上三個 topic）：三個 reader 的收工時間**恰好
// 相隔 3.000 秒**，總關機時間 ≈ topic 數 × 3 秒。
//
// 直覺的嫌疑犯是 kafka-go 的 HeartbeatInterval（預設剛好 3 秒）——
// Reader.Close() 要等自己的 heartbeat goroutine 收到取消。
// **試過了，把它設成 1 秒，間隔還是 3.000 秒。假設不成立。**
// 真正的原因在 broker 端：coordinator 一次只處理一個 member 的
// LeaveGroup，每次都要跑完一輪 rebalance，所以三個 member 是排隊離開的，
// 與 client 這邊的設定無關。
//
// 結論：這個成本改不掉，只能把關機預算訂得夠大（見 cmd/notify 的
// shutdownTimeout）。⚠️ 它會**隨 topic 數線性成長**——之後要加第四個
// topic，先回去把那個預算一起調大。

// 處理結果，直接當成 metrics.KafkaMessages 的 result 標籤值。
const (
	resultOK      = "ok"
	resultDropped = "dropped"
	resultSkipped = "skipped"
)

// Pusher 是 consumer 需要 hub 提供的兩個動作。
//
// # 為什麼介面定義在使用端而不是實作端
//
// 這是 Java 與 Go 最容易講反的一個慣例。Java 的習慣是先在實作端設計
// interface 再 implements；Go 的慣例是**在使用端定義自己需要的小介面**，
// 實作端一行都不用改（隱式滿足介面）。
//
// 好處很實際：hub.Hub 有十幾個方法，consumer 只用到兩個。介面只列這兩個，
// 測試替身就只要實作兩個方法，而不是為了 mock 去複製整個 Hub。
type Pusher interface {
	Broadcast(dest string, body []byte) int
	PushToPlayer(playerID, dest string, body []byte) int
}

// reader 是 kafka.Reader 在本套件用到的部分。
//
// 抽成介面只為了讓測試能替換掉真的 Kafka。這符合「有兩個實作才抽介面」
// 的原則——kafka.Reader 與測試替身就是兩個。
type reader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// Consumer 消費單一 topic，一個 topic 一條 goroutine。
type Consumer struct {
	topic   string
	reader  reader
	pusher  Pusher
	log     *slog.Logger
	metrics *metrics.Metrics

	// backoff 可注入，讓測試不必真的等 fetchBackoff 那 2 秒。
	backoff time.Duration
}

// New 為單一 topic 建立 consumer。
func New(topic string, brokers []string, groupID string, p Pusher, log *slog.Logger, m *metrics.Metrics) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		GroupID: groupID,
		Topic:   topic,
		// 對齊 Java 的 auto-offset-reset: latest。
		//
		// ⚠️ 它**只在這個 group 還沒有已提交 offset 時**生效。已經有 offset
		// 就從那裡續下去，這正是我們要的：重啟不會重播整個 topic。
		StartOffset: kafka.LastOffset,
		// MinBytes 1 = 有一則就回傳，不為了攢批次而增加延遲。
		// 這是即時推播，吞吐讓給延遲。
		MinBytes: 1,
		MaxBytes: maxFetchBytes,
		// ★ 少了這兩行，consumer 比 topic 早啟動就會**永遠**收不到訊息。
		//
		// kafka-go 預設 WatchPartitionChanges = false：加入 group 那一刻
		// topic 不存在 → 分配到 0 個 partition → 之後就一直維持那個狀態，
		// 因為沒有任何事件會觸發 rebalance。服務看起來完全正常
		// （健康檢查過、日誌無錯誤、group 也在），但一則都不會消費。
		// 這是 2026-07-30 在 Phase 4 的 compose 上實測到的：topic 由
		// producer 自動建立，而 producer 比 consumer 晚幾秒，就中了。
		//
		// Java 版沒有這個問題——Spring Kafka 靠 metadata.max.age.ms
		// （預設 5 分鐘）定期刷新中繼資料並重新訂閱，最多五分鐘會自己好。
		// 開啟這個選項就是補上等價的行為，代價只是每 5 秒一次中繼資料查詢。
		WatchPartitionChanges:  true,
		PartitionWatchInterval: partitionWatchInterval,
		// kafka-go 內部的訊息（連線、rebalance）導進 slog。
		// 不接的話它會直接寫 stderr，變成日誌裡唯一不是 JSON 的那幾行，
		// 收集器解析不了。
		Logger: kafka.LoggerFunc(func(msg string, args ...any) {
			log.Debug(fmt.Sprintf(msg, args...), "topic", topic)
		}),
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...any) {
			log.Warn(fmt.Sprintf(msg, args...), "topic", topic)
		}),
	})
	return newWithReader(topic, r, p, log, m)
}

// newWithReader 是 New 與測試共用的建構子。
func newWithReader(topic string, r reader, p Pusher, log *slog.Logger, m *metrics.Metrics) *Consumer {
	return &Consumer{
		topic:   topic,
		reader:  r,
		pusher:  p,
		log:     log,
		metrics: m,
		backoff: fetchBackoff,
	}
}

// Run 跑消費迴圈，直到 ctx 被取消才回傳。
//
// 回傳 nil 代表「因為關閉而正常結束」。它**不會**因為單一則訊息有問題就
// 回傳錯誤——那正是 Java 版 try/catch + finally ack 的語意：壞訊息丟棄、
// offset 前進、繼續處理下一則。一則壞訊息不該讓整個 topic 停擺。
func (c *Consumer) Run(ctx context.Context) error {
	c.log.Info("Kafka consumer 啟動", "topic", c.topic)
	defer func() {
		// Close 會收乾淨 kafka-go 內部的 goroutine，並送出 LeaveGroup。
		// 少了它，Kafka 要等到 session timeout 才知道我們離開了，
		// 下一次啟動的 rebalance 會被白白拖慢好幾秒。
		if err := c.reader.Close(); err != nil {
			c.log.Warn("關閉 Kafka reader 失敗", "topic", c.topic, "err", err)
		}
		c.log.Info("Kafka consumer 已停止", "topic", c.topic)
	}()

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				// 關機造成的失敗，不是錯誤。
				return nil
			}
			c.log.Warn("取 Kafka 訊息失敗，稍後重試",
				"topic", c.topic, "err", err, "backoff", c.backoff)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(c.backoff):
			}
			continue
		}
		c.handle(ctx, msg)
	}
}

// handle 處理一則訊息，**結束時一定 commit**。
func (c *Consumer) handle(ctx context.Context, msg kafka.Message) {
	result := resultOK

	switch p, err := plan(c.topic, msg.Value); {
	case err != nil:
		result = resultDropped
		// 與 Java 版一樣把 payload 一起記下來。推播是 best-effort，
		// 訊息丟掉就沒了，日誌是事後唯一能追的東西。
		c.log.Warn("丟棄無法處理的訊息",
			"topic", c.topic, "offset", msg.Offset, "err", err, "payload", string(msg.Value))
	case p == nil:
		result = resultSkipped
		c.log.Warn("事件缺少路由需要的欄位，略過推播",
			"topic", c.topic, "offset", msg.Offset)
	default:
		var delivered int
		if p.PlayerID != "" {
			delivered = c.pusher.PushToPlayer(p.PlayerID, p.Destination, p.Body)
		} else {
			delivered = c.pusher.Broadcast(p.Destination, p.Body)
		}
		c.metrics.PushLatency.Observe(time.Since(msg.Time).Seconds())
		// delivered 為 0 不是錯誤，只是「現在沒人訂閱那個 destination」。
		// 推播不存、不重送——與 Java 版一致。
		c.log.Debug("已推播",
			"topic", c.topic, "destination", p.Destination,
			"playerID", p.PlayerID, "delivered", delivered)
	}

	c.metrics.KafkaMessages.WithLabelValues(c.topic, result).Inc()
	c.commit(ctx, msg)
}

// commit 提交 offset。
//
// ⚠️ **不論推播成功與否都要 commit**，這是對齊 Java 版的
// `finally { ack.acknowledge(); }`。壞訊息重試一萬次還是壞的，不 commit
// 只會讓 offset 永遠卡在那一則，後面所有事件都收不到——**一則壞訊息
// 就能讓整個 topic 停擺**，而且外部看起來只是「推播突然沒了」。
//
// 用 context.WithoutCancel：關機時 root ctx 已經被取消，但剛處理完的這一則
// 還是該提交，否則下次啟動會重播它。再包一層 WithTimeout，避免關機被一個
// 連不上的 broker 無限期拖住（Java 那邊對應的是 shutdown 的 ack 逾時）。
func (c *Consumer) commit(ctx context.Context, msg kafka.Message) {
	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
	defer cancel()

	if err := c.reader.CommitMessages(commitCtx, msg); err != nil {
		// 不重試：下次啟動會從舊 offset 重播，而重播對 best-effort 推播
		// 是可接受的（玩家最多多收到一則舊通知，不會算錯錢）。
		c.log.Warn("提交 offset 失敗", "topic", c.topic, "offset", msg.Offset, "err", err)
	}
}
