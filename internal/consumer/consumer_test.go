package consumer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/AlexChang1999/lucky-star-notify-go/internal/hub"
	"github.com/AlexChang1999/lucky-star-notify-go/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/segmentio/kafka-go"
)

// ---- 測試替身 ----

// fetchResult 是 fakeReader 腳本裡的一步：回一則訊息，或回一個錯誤。
type fetchResult struct {
	msg kafka.Message
	err error
}

// fakeReader 照腳本吐東西，腳本跑完就阻塞到 ctx 結束（模擬 Kafka 的 long poll）。
type fakeReader struct {
	mu        sync.Mutex
	script    []fetchResult
	next      int
	committed []kafka.Message
	closed    bool

	// drained 在腳本吐完最後一項時關閉，讓測試知道可以取消 ctx 了。
	//
	// 用 channel 而不是 time.Sleep：靠睡覺同步的測試在忙碌的機器上會偶發
	// 失敗，而偶發失敗的測試比沒有測試更糟——它會訓練你忽略紅燈。
	drained chan struct{}
}

func newFakeReader(script ...fetchResult) *fakeReader {
	r := &fakeReader{script: script, drained: make(chan struct{})}
	if len(script) == 0 {
		// 空腳本代表「一開始就沒有訊息」，直接視為已吐完。
		close(r.drained)
	}
	return r
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	r.mu.Lock()
	if r.next < len(r.script) {
		item := r.script[r.next]
		r.next++
		if r.next == len(r.script) {
			close(r.drained)
		}
		r.mu.Unlock()
		return item.msg, item.err
	}
	r.mu.Unlock()

	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (r *fakeReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.committed = append(r.committed, msgs...)
	return nil
}

func (r *fakeReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return nil
}

// committedOffsets 回傳已提交的 offset。
func (r *fakeReader) committedOffsets() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	offsets := make([]int64, 0, len(r.committed))
	for _, m := range r.committed {
		offsets = append(offsets, m.Offset)
	}
	return offsets
}

func (r *fakeReader) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

// recordedPush 是 fakePusher 記下來的一次推播。
type recordedPush struct {
	playerID    string
	destination string
	body        string
}

// fakePusher 只實作 Pusher 的兩個方法——這就是「介面定義在使用端」的好處：
// 不必為了 mock 去複製整個 hub.Hub。
type fakePusher struct {
	mu     sync.Mutex
	pushes []recordedPush
}

func (p *fakePusher) Broadcast(dest string, body []byte) int {
	return p.record("", dest, body)
}

func (p *fakePusher) PushToPlayer(playerID, dest string, body []byte) int {
	return p.record(playerID, dest, body)
}

func (p *fakePusher) record(playerID, dest string, body []byte) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pushes = append(p.pushes, recordedPush{playerID: playerID, destination: dest, body: string(body)})
	return 1
}

func (p *fakePusher) recorded() []recordedPush {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.pushes)
}

// ---- 輔助函式 ----

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func message(topic string, offset int64, value string) kafka.Message {
	return kafka.Message{Topic: topic, Offset: offset, Value: []byte(value), Time: time.Now()}
}

// runUntilDrained 跑 Run，等腳本吐完後取消 ctx，回傳 Run 的錯誤。
//
// 為什麼「腳本吐完就取消」不會漏掉最後一則的處理：handle 是同步呼叫的，
// Run 一定是先處理完手上這一則，才回到迴圈頂端去 FetchMessage 並看到取消。
// 而 commit 用的是 context.WithoutCancel，取消後照樣提交得成。
func runUntilDrained(t *testing.T, c *Consumer, r *fakeReader) error {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	select {
	case <-r.drained:
	case <-time.After(2 * time.Second):
		t.Fatal("等 fakeReader 吐完腳本逾時")
	}
	cancel()

	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("Run 沒有在 ctx 取消後結束（goroutine 洩漏）")
	}
	return nil
}

// ---- 測試 ----

func TestRunPushesAndCommits(t *testing.T) {
	t.Parallel()

	r := newFakeReader(fetchResult{
		msg: message(TopicGameResult, 7, `{"playerId":42,"roundId":"r-1","win":true}`),
	})
	p := &fakePusher{}
	m := metrics.New()
	c := newWithReader(TopicGameResult, r, p, discardLogger(), m)

	if err := runUntilDrained(t, c, r); err != nil {
		t.Fatalf("Run() = %v，預期 nil", err)
	}

	pushes := p.recorded()
	if len(pushes) != 1 {
		t.Fatalf("推播了 %d 則，預期 1 則：%+v", len(pushes), pushes)
	}
	if pushes[0].playerID != "42" {
		t.Errorf("playerID = %q，預期 42", pushes[0].playerID)
	}
	if pushes[0].destination != hub.PrivateDestination {
		t.Errorf("destination = %q，預期 %q", pushes[0].destination, hub.PrivateDestination)
	}
	wantBody := `{"type":"GAME_RESULT","roundId":"r-1","gameType":null,"bet":null,"payout":null,"win":true,"settledAt":null}`
	if pushes[0].body != wantBody {
		t.Errorf("body 不符\n  實際：%s\n  預期：%s", pushes[0].body, wantBody)
	}

	if got := r.committedOffsets(); !slices.Equal(got, []int64{7}) {
		t.Errorf("已提交的 offset = %v，預期 [7]", got)
	}
	if !r.isClosed() {
		t.Error("Run 結束後應該關掉 reader（否則 LeaveGroup 不會送出）")
	}
	if got := testutil.ToFloat64(m.KafkaMessages.WithLabelValues(TopicGameResult, resultOK)); got != 1 {
		t.Errorf("ok 計數 = %v，預期 1", got)
	}
}

// TestRunCommitsBadMessageAndContinues 是 Phase 3 DoD 的核心那一條：
// 灌一則格式錯誤的 JSON，服務不崩潰、有記錄、**offset 照樣前進（不卡住）**。
//
// 順便把 dropped 與 skipped 分開驗證：前者是上游 bug，後者是上游的正常變異，
// 混成一種計數就看不出差別了。
func TestRunCommitsBadMessageAndContinues(t *testing.T) {
	t.Parallel()

	r := newFakeReader(
		fetchResult{msg: message(TopicGameResult, 0, `{"playerId":`)},                     // 壞 JSON
		fetchResult{msg: message(TopicGameResult, 1, `{"roundId":"r-2","gameType":"x"}`)}, // 缺 playerId
		fetchResult{msg: message(TopicGameResult, 2, `{"playerId":9,"roundId":"r-3"}`)},   // 正常
	)
	p := &fakePusher{}
	m := metrics.New()
	c := newWithReader(TopicGameResult, r, p, discardLogger(), m)

	if err := runUntilDrained(t, c, r); err != nil {
		t.Fatalf("Run() = %v，預期 nil（壞訊息不該讓 Run 失敗）", err)
	}

	if pushes := p.recorded(); len(pushes) != 1 || pushes[0].playerID != "9" {
		t.Errorf("推播結果 = %+v，預期只有 playerID=9 那一則", pushes)
	}
	// 三則都要 commit——包含壞的那兩則。這就是「offset 不會卡住」。
	if got := r.committedOffsets(); !slices.Equal(got, []int64{0, 1, 2}) {
		t.Errorf("已提交的 offset = %v，預期 [0 1 2]", got)
	}

	for _, tc := range []struct {
		result string
		want   float64
	}{
		{resultDropped, 1},
		{resultSkipped, 1},
		{resultOK, 1},
	} {
		if got := testutil.ToFloat64(m.KafkaMessages.WithLabelValues(TopicGameResult, tc.result)); got != tc.want {
			t.Errorf("%s 計數 = %v，預期 %v", tc.result, got, tc.want)
		}
	}
}

// TestRunBacksOffOnFetchError 確認取訊息失敗之後迴圈會繼續，而且不空轉。
func TestRunBacksOffOnFetchError(t *testing.T) {
	t.Parallel()

	r := newFakeReader(
		fetchResult{err: errors.New("dial tcp: broker 連不上")},
		fetchResult{msg: message(TopicRankUpdate, 3, `{"type":"X","entries":[]}`)},
	)
	p := &fakePusher{}
	c := newWithReader(TopicRankUpdate, r, p, discardLogger(), metrics.New())
	// 注入極短的退避，否則這個測試要等 fetchBackoff 那 2 秒。
	c.backoff = 10 * time.Millisecond

	if err := runUntilDrained(t, c, r); err != nil {
		t.Fatalf("Run() = %v，預期 nil", err)
	}

	if pushes := p.recorded(); len(pushes) != 1 || pushes[0].destination != RankDestination {
		t.Errorf("推播結果 = %+v，預期錯誤之後仍推出 rank 那一則", pushes)
	}
	// 取訊息失敗的那一次沒有訊息可以 commit，所以只該有一個 offset。
	if got := r.committedOffsets(); !slices.Equal(got, []int64{3}) {
		t.Errorf("已提交的 offset = %v，預期 [3]", got)
	}
}

func TestRunStopsWhenContextCancelled(t *testing.T) {
	t.Parallel()

	r := newFakeReader() // 沒有任何訊息，FetchMessage 會一直阻塞
	c := newWithReader(TopicNotificationPush, r, &fakePusher{}, discardLogger(), metrics.New())

	if err := runUntilDrained(t, c, r); err != nil {
		t.Fatalf("Run() = %v，預期 nil", err)
	}
	if !r.isClosed() {
		t.Error("Run 結束後應該關掉 reader")
	}
}

// TestNoGoroutineLeakAfterShutdown 對齊 Phase 3 的 DoD：關閉後 goroutine 回到基線。
//
// ⚠️ 刻意**不呼叫 t.Parallel()**：runtime.NumGoroutine() 是行程層級的數字，
// 與其他測試並行跑就量不準。Go 的測試排程會讓非並行測試獨佔執行，
// 所以只要不標 Parallel，這個數字就是可信的。
func TestNoGoroutineLeakAfterShutdown(t *testing.T) {
	baseline := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	m := metrics.New()
	p := &fakePusher{}

	// 照 main 的樣子起三個 consumer，一個 topic 一條 goroutine。
	var wg sync.WaitGroup
	for _, topic := range AllTopics {
		c := newWithReader(topic, newFakeReader(), p, discardLogger(), m)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.Run(ctx); err != nil {
				t.Errorf("%s 的 Run() = %v，預期 nil", topic, err)
			}
		}()
	}

	cancel()
	wg.Wait()

	// goroutine 不會在 Run 回傳的同一瞬間就從排程器消失，所以輪詢一小段時間，
	// 而不是量一次就判死。
	for range 200 {
		if runtime.NumGoroutine() <= baseline {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("goroutine 數 = %d，未回到基線 %d", runtime.NumGoroutine(), baseline)
}
