// Package metrics 集中管理本服務的 Prometheus 指標。
//
// # 為什麼用自己的 Registry，而不是 prometheus.DefaultRegisterer
//
// 用預設的全域 registry 時，指標是「行程唯一」的：同一個測試檔跑兩次
// New() 就會 panic（duplicate metrics collector registration attempted）。
// 自己開一個 Registry 之後，每個測試可以有自己乾淨的一組指標——
// 這跟 auth.Verifier 不把 claims 存成欄位是同一個原則：**別用全域可變狀態**。
//
// # 免費得到的指標
//
// 註冊 Go 與 process collector 就會拿到 go_goroutines、go_memstats_*、GC 統計、
// process_resident_memory_bytes。最後那個就是 Java vs Go 記憶體對照的數據來源。
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// namespace 是所有自訂指標的前綴，對齊 AGENTS.md §3 約定的 notify_ 命名。
const namespace = "notify"

// Metrics 持有 registry 與所有自訂指標。
//
// 指標本身（Gauge/Counter）是併發安全的，可以直接被多個 goroutine 更新，
// 不需要額外加鎖。
type Metrics struct {
	registry *prometheus.Registry

	// WSConnections 是當前的 WebSocket 連線數。
	//
	// 用 Gauge 而不是 Counter：它會上下浮動。Counter 只能遞增，
	// 拿來數「當前數量」是常見的新手錯誤。
	WSConnections prometheus.Gauge

	// OutboundDropped 是因為 outbound channel 塞滿而被踢掉的連線數。
	//
	// 這是**背壓策略的可觀測性出口**：扇出時絕不阻塞，慢速客戶端一律
	// fail-fast 斷線（AGENTS.md §2.6）。沒有這個計數器的話，「被踢掉」
	// 這件事在外部完全看不見——只會表現成使用者那端莫名重連。
	//
	// 用 Counter 而不是 Gauge：它只會遞增，而且真正有意義的是
	// rate()（每秒被踢幾條），不是當下的絕對值。
	OutboundDropped prometheus.Counter

	// KafkaMessages 是消費過的 Kafka 訊息數，依 topic 與處理結果分類。
	//
	// result 的三個值刻意分開，因為它們的意義完全不同：
	//
	//	ok       成功轉成推播（不代表有人收到——沒人訂閱時送出 0 則，那是正常狀態）
	//	dropped  JSON 壞掉、解析不出來，記錄後照樣 commit（與 Java 版一致）
	//	skipped  格式合法但缺了路由需要的欄位（例如 game.result 沒有 playerId）
	//
	// 把 dropped 與 skipped 併成一個「失敗」就看不出差別了：前者是上游的 bug，
	// 後者是上游的正常變異。rate(dropped) 竄高值得叫人起床，skipped 不值得。
	KafkaMessages *prometheus.CounterVec

	// PushLatency 是「Kafka 訊息時間戳 → 訊息送進連線的 outbound channel」的延遲。
	//
	// ⚠️ 兩個誠實性上的保留，引用這個指標時要一起註明：
	//
	//  1. 時間戳是 **producer 那一端**打的，所以這個數字含兩台機器的時鐘偏移。
	//     同機（compose 之內）可以忽略，跨機量測就必須標明。
	//  2. 量到的是「進佇列」不是「TCP 寫出」——真正寫出由 write goroutine 負責。
	//     刻意如此：把客戶端的網路狀況算進來，就量不出服務本身的表現了。
	PushLatency prometheus.Histogram
}

// New 建立指標集合並註冊 collector。
func New() *Metrics {
	registry := prometheus.NewRegistry()

	// Go runtime 與 process 的內建指標。用 MustRegister 是合理的：
	// 這裡失敗代表程式碼寫錯（重複註冊），不是執行期可以復原的狀況，
	// 讓它在啟動時就 panic 比默默少了指標好。
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{
		registry: registry,
		WSConnections: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "ws_connections",
			Help:      "目前建立中的 WebSocket 連線數",
		}),
		OutboundDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "outbound_dropped_total",
			Help:      "因送出佇列塞滿而被斷線的慢速客戶端次數",
		}),
		KafkaMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "kafka_messages_total",
			Help:      "消費過的 Kafka 訊息數（result：ok／dropped／skipped）",
		}, []string{"topic", "result"}),
		PushLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "push_latency_seconds",
			Help:      "Kafka 訊息時間戳到訊息進入連線送出佇列的延遲（秒）",
			// 用預設分桶（5ms ~ 10s）。這個服務的正常值應該落在最前面幾個桶，
			// 自訂分桶要等真的量過一輪、知道分布長什麼樣子再調——先猜只會猜錯。
			Buckets: prometheus.DefBuckets,
		}),
	}
	registry.MustRegister(m.WSConnections, m.OutboundDropped, m.KafkaMessages, m.PushLatency)
	return m
}

// Handler 回傳 /metrics 用的 HTTP handler。
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		// 抓取指標時若有 collector 出錯，回 500 而不是靜默送出不完整的內容——
		// 「指標少了一半」比「抓取失敗」難察覺得多。
		ErrorHandling: promhttp.HTTPErrorOnError,
	})
}
