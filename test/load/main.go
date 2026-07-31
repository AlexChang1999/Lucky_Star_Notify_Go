// Command load 是本專案的壓測 client：建立 N 條 WebSocket/STOMP 長連線，
// 對 Kafka 灌 R 輪廣播事件，量測連線建立延遲與扇出延遲。
// 輸出是 docs/效能對照報告.md 的原始素材。
//
// # 為什麼用 Go 寫壓測工具本身就是展示
//
// 5000 條長連線 = 5000 個 goroutine，每個用最直觀的阻塞式
// for { ReadMessage } 就寫完了，沒有 callback、沒有 event loop。
// 同樣的事在 Java 要嘛用 Netty 的非同步模型（複雜度整個換一階），
// 要嘛開 5000 條 OS 執行緒——光是預設 1MB 的執行緒堆疊就是 5GB。
//
// # 為什麼它可以同時壓 Java 版與 Go 版
//
// 它講的是 STOMP，不是「Go 版的協議」。-target 換成 8087 就是壓 Java 版。
// 這是效能對照能成立的前提：**同一支施壓程式、同一組參數**，
// 否則比出來的差異可能只是兩支壓測工具的差異。
//
// # ⚠️ 這個程式會簽 JWT
//
// 與 cmd/demo 同理：簽章能力只能存在於開發/測試工具，不能進正式映像。
// test/ 底下的東西不會被任何 Dockerfile 打包，所以這個性質是靠
// **目錄位置**保證的，不是靠註解。
//
// 用法見同目錄的 README.md。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	// basePlayerID 是第一條連線用的玩家 ID。
	//
	// 避開 demo 產生器的 1001/1002 與契約測試的 70001/70002：
	// 三者用同一個 Kafka 時，各自的訊息不該互相干擾。
	basePlayerID = 800000

	// loadTestType 是壓測事件的 type 值，用來從別人的流量裡認出自己的。
	// demo 產生器同時也在發 rank.update（type=GLOBAL_COINS），
	// 不過濾的話那些會被算進延遲樣本，而它們的 updatedAt 是別人的時鐘。
	loadTestType = "LOADTEST"

	// tokenTTL 只要活得比整場壓測久就夠。
	tokenTTL = 2 * time.Hour
)

type options struct {
	target    string
	kafkaAddr string
	topic     string
	dest      string
	secret    string
	conns     int
	rate      int
	rounds    int
	interval  time.Duration
	settle    time.Duration
	drain     time.Duration
	hold      time.Duration
	timeout   time.Duration
	out       string
}

func parseFlags() options {
	var o options
	flag.StringVar(&o.target, "target", "ws://localhost:8088/ws", "WebSocket 端點（Java 版是 8087）")
	flag.StringVar(&o.kafkaAddr, "kafka", "localhost:9094", "從宿主連得到的 Kafka broker（團隊 repo 的是 9092）")
	flag.StringVar(&o.topic, "topic", "rank.update", "廣播事件寫入的 topic")
	flag.StringVar(&o.dest, "dest", "/topic/rank", "訂閱的 destination")
	flag.StringVar(&o.secret, "secret", os.Getenv("JWT_SECRET"), "簽 token 用的原始字串（預設讀 JWT_SECRET）")
	flag.IntVar(&o.conns, "conns", 100, "連線數")
	flag.IntVar(&o.rate, "rate", 200, "每秒建立幾條連線；<=0 表示不限速")
	flag.IntVar(&o.rounds, "rounds", 10, "廣播輪數")
	flag.DurationVar(&o.interval, "interval", time.Second, "兩輪廣播之間的間隔")
	flag.DurationVar(&o.settle, "settle", 2*time.Second, "全部連上之後、開始廣播之前的等待（讓伺服器端的訂閱登記完成）")
	flag.DurationVar(&o.drain, "drain", 3*time.Second, "最後一輪廣播之後的收訊等待")
	flag.DurationVar(&o.hold, "hold", 0, "全部連上之後多停留一段時間，方便另開視窗量 docker stats")
	flag.DurationVar(&o.timeout, "timeout", 15*time.Second, "單條連線的握手上限")
	flag.StringVar(&o.out, "out", "", "把原始樣本寫成 JSON 到這個路徑（預設不寫）")
	flag.Parse()
	return o
}

func main() {
	o := parseFlags()
	if o.secret == "" {
		fmt.Fprintln(os.Stderr, "缺少 -secret（或環境變數 JWT_SECRET）：沒有它簽不出 token，所有連線都會被拒絕")
		os.Exit(2)
	}
	if err := run(o); err != nil {
		fmt.Fprintf(os.Stderr, "壓測失敗：%v\n", err)
		os.Exit(1)
	}
}

func run(o options) error {
	fmt.Printf("目標 %s　連線 %d　廣播 %d 輪（每 %s）\n\n", o.target, o.conns, o.rounds, o.interval)

	clients, connectElapsed := establish(o)
	connected := report(o, clients, connectElapsed)
	if connected == 0 {
		return errors.New("一條連線都沒建立成功，後面的量測沒有意義")
	}

	if o.hold > 0 {
		fmt.Printf("\n連線保持中，%s 後開始廣播——現在可以量記憶體：\n", o.hold)
		fmt.Println("  docker stats --no-stream")
		time.Sleep(o.hold)
	}

	// settle：SUBSCRIBE 送出去之後，伺服器端還要把它登記進訂閱表，
	// 那是非同步的。不等就開始廣播的話，前幾輪會有一部分連線漏收，
	// 而症狀看起來像「扇出不可靠」——其實是施壓端太急。
	// 漏收本身仍然會誠實反映在「應收 / 實收」那一行，不會被藏起來。
	time.Sleep(o.settle)

	if err := broadcastRounds(o); err != nil {
		return err
	}

	// 最後一輪還在路上，等它到齊再收工。
	time.Sleep(o.drain)

	for _, c := range clients {
		c.close()
	}
	waitAll(clients)

	fanout := reportFanout(o, clients, connected)

	if o.out != "" {
		if err := writeJSON(o, clients, connectElapsed, fanout); err != nil {
			return fmt.Errorf("寫出原始樣本失敗：%w", err)
		}
		fmt.Printf("\n原始樣本已寫入 %s\n", o.out)
	}
	return nil
}

// establish 依 -rate 逐條建立連線，回傳全部 client 與實際耗時。
//
// 為什麼要限速：一次同時發起幾千條 TCP 連線，瓶頸會落在施壓端自己
// （以及 Windows 的動態埠配置）而不是被測服務，量出來的「握手延遲」
// 就變成在量施壓端。限速讓這個數字有意義。
func establish(o options) ([]*client, time.Duration) {
	clients := make([]*client, o.conns)
	ready := make(chan struct{}, o.conns)

	var pacer <-chan time.Time
	if o.rate > 0 {
		ticker := time.NewTicker(time.Second / time.Duration(o.rate))
		defer ticker.Stop()
		pacer = ticker.C
	}

	start := time.Now()
	for i := range clients {
		if pacer != nil {
			<-pacer
		}
		c := &client{id: i, playerID: int64(basePlayerID + i)}
		clients[i] = c
		go c.run(o, ready)
	}

	// 等每一條都回報結果（成功或失敗都算）。
	//
	// ⚠️ 這個 channel 收訊同時也是**記憶體可見性**的保證：收到訊號之後，
	// 主 goroutine 讀 c.conn / c.err 才是安全的（channel 建立 happens-before）。
	// 少了它就是資料競賽，而 -race 不一定每次都抓得到。
	for range clients {
		<-ready
	}
	return clients, time.Since(start)
}

func waitAll(clients []*client) {
	var wg sync.WaitGroup
	for _, c := range clients {
		if c.done == nil {
			continue
		}
		wg.Add(1)
		go func(c *client) {
			defer wg.Done()
			<-c.done
		}(c)
	}
	wg.Wait()
}

// broadcastRounds 每隔 -interval 對 Kafka 送一則 rank.update。
func broadcastRounds(o options) error {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(o.kafkaAddr),
		Topic:                  o.topic,
		AllowAutoTopicCreation: true,
		// ⚠️ 這兩行是量測正確性的關鍵，不是效能微調。
		//
		// kafka-go 的 Writer 預設 BatchSize=100、BatchTimeout=**1 秒**：
		// 單獨寫一則訊息時，它會在 producer 端等滿一秒才送出。
		// 而我們的延遲是用事件裡的 updatedAt（寫入前一刻的時鐘）算的，
		// 所以那一秒會**整個算進扇出延遲**——量出來的 p50 會是 1 秒出頭，
		// 而且看起來像伺服器很慢。BatchSize=1 讓它立刻送出。
		BatchSize:    1,
		BatchTimeout: 5 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}
	defer func() { _ = writer.Close() }()

	fmt.Printf("\n開始廣播 %d 輪…\n", o.rounds)
	for round := range o.rounds {
		event := newLoadEvent()
		value, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("編碼廣播事件失敗：%w", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), o.timeout)
		err = writer.WriteMessages(ctx, kafka.Message{Value: value})
		cancel()
		if err != nil {
			return fmt.Errorf("第 %d 輪寫入 Kafka 失敗：%w", round+1, err)
		}
		if round < o.rounds-1 {
			time.Sleep(o.interval)
		}
	}
	return nil
}

// rankEntry / loadEvent 對齊 rank.update 的事件契約。
//
// 帶三筆 entries 而不是空陣列：body 大小會影響扇出成本，用空 body 量出來
// 的數字漂亮但不代表真實情況。三筆是團隊前端排行榜面板的實際量級。
type rankEntry struct {
	PlayerID int64  `json:"playerId"`
	Nickname string `json:"nickname"`
	Score    int64  `json:"score"`
	Rank     int    `json:"rank"`
}

type loadEvent struct {
	Type    string      `json:"type"`
	Entries []rankEntry `json:"entries"`
	// UpdatedAt 放的是**寫入前一刻的 UnixNano**，不是毫秒。
	//
	// 毫秒對這個量測太粗：本機的扇出延遲是個位數毫秒，用毫秒解析度會有
	// 一大半樣本落在同一格。Java 那邊的欄位是 long，奈秒（約 1.8e18）
	// 遠在範圍內，兩邊都會原樣回吐。
	UpdatedAt int64 `json:"updatedAt"`
}

func newLoadEvent() loadEvent {
	return loadEvent{
		Type: loadTestType,
		Entries: []rankEntry{
			{PlayerID: 1001, Nickname: "阿明", Score: 128000, Rank: 1},
			{PlayerID: 1002, Nickname: "小美", Score: 96500, Rank: 2},
			{PlayerID: 1003, Nickname: "老王", Score: 41200, Rank: 3},
		},
		UpdatedAt: time.Now().UnixNano(),
	}
}

// ── 報告 ──────────────────────────────────────────────────────────────

func report(o options, clients []*client, elapsed time.Duration) int {
	var connected int
	var handshake []time.Duration
	failures := map[string]int{}
	for _, c := range clients {
		if c.err != nil {
			failures[c.failureKey()]++
			continue
		}
		connected++
		handshake = append(handshake, c.connectDur)
	}

	fmt.Println("== 連線建立 ==")
	fmt.Printf("  連線數      %d（成功 %d / 失敗 %d）\n", o.conns, connected, o.conns-connected)
	fmt.Printf("  建立耗時    %s（設定速率 %d/s）\n", elapsed.Round(time.Millisecond), o.rate)
	fmt.Printf("  握手延遲    %s\n", summarize(handshake))
	printFailures(failures)
	return connected
}

// printFailures 依數量由多到少列出失敗原因。
//
// map 的走訪順序在 Go 是刻意隨機的，直接印會讓同一場壓測跑兩次的輸出
// 順序不同——比對兩份報告時很煩。排序不是美觀問題。
func printFailures(failures map[string]int) {
	keys := make([]string, 0, len(failures))
	for key := range failures {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b string) int {
		if d := failures[b] - failures[a]; d != 0 {
			return d
		}
		return strings.Compare(a, b)
	})
	for _, key := range keys {
		fmt.Printf("  ✗ %d 條：%s\n", failures[key], key)
	}
}

func reportFanout(o options, clients []*client, connected int) summary {
	var all []time.Duration
	for _, c := range clients {
		all = append(all, c.latencies...)
	}
	expected := connected * o.rounds
	s := summarize(all)

	fmt.Println("\n== 扇出延遲（事件寫入 Kafka 前一刻 → client 收到）==")
	fmt.Printf("  廣播輪數    %d（間隔 %s）\n", o.rounds, o.interval)
	if expected > 0 {
		fmt.Printf("  應收 / 實收 %d / %d（%.2f%%）\n", expected, len(all), float64(len(all))*100/float64(expected))
	}
	fmt.Printf("  延遲        %s\n", s)
	return s
}

// jsonReport 是 -out 寫出的內容。
//
// 存的是**摘要加上全部原始樣本**，不是只有百分位數：報告寫完之後若想換一組
// 百分位、或畫個分佈圖，不必重跑一次壓測。
type jsonReport struct {
	Target         string    `json:"target"`
	Kafka          string    `json:"kafka"`
	Conns          int       `json:"conns"`
	Connected      int       `json:"connected"`
	Rate           int       `json:"rate"`
	Rounds         int       `json:"rounds"`
	IntervalMS     int64     `json:"intervalMs"`
	ConnectElapsed string    `json:"connectElapsed"`
	Handshake      summary   `json:"handshake"`
	Fanout         summary   `json:"fanout"`
	FanoutSamples  []int64   `json:"fanoutSamplesNs"`
	HandshakeNs    []int64   `json:"handshakeSamplesNs"`
	RecordedAt     time.Time `json:"recordedAt"`
}

func writeJSON(o options, clients []*client, connectElapsed time.Duration, fanout summary) error {
	rep := jsonReport{
		Target:         o.target,
		Kafka:          o.kafkaAddr,
		Conns:          o.conns,
		Rate:           o.rate,
		Rounds:         o.rounds,
		IntervalMS:     o.interval.Milliseconds(),
		ConnectElapsed: connectElapsed.String(),
		Fanout:         fanout,
		RecordedAt:     time.Now(),
	}
	var handshake []time.Duration
	for _, c := range clients {
		if c.err != nil {
			continue
		}
		rep.Connected++
		handshake = append(handshake, c.connectDur)
		for _, d := range c.latencies {
			rep.FanoutSamples = append(rep.FanoutSamples, d.Nanoseconds())
		}
	}
	for _, d := range handshake {
		rep.HandshakeNs = append(rep.HandshakeNs, d.Nanoseconds())
	}
	rep.Handshake = summarize(handshake)

	data, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(o.out, data, 0o644)
}
