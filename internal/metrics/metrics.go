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
	}
	registry.MustRegister(m.WSConnections)
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
