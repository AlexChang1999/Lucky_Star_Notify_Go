package hub

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AlexChang1999/lucky-star-notify-go/internal/metrics"
	"github.com/AlexChang1999/lucky-star-notify-go/internal/stomp"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// ioTimeout 是測試裡每次讀寫的上限。
// 比正式碼的 10 秒短很多：測試卡住時要快點失敗，而不是等 go test 超時。
const ioTimeout = 3 * time.Second

// testDestination 是大部分測試用的廣播頻道。
const testDestination = "/topic/rank"

// ---------------------------------------------------------------------------
// 測試環境
// ---------------------------------------------------------------------------

// hubEnv 是一個跑在真 HTTP 服務上的 Hub。
//
// 為什麼要開真的 WebSocket 而不是用假物件：Session 的整個併發模型
// （兩個 goroutine、心跳 ticker、讀寫 deadline）都建立在 *websocket.Conn
// 的真實行為上，用介面抽掉它就等於什麼都沒測到。
type hubEnv struct {
	hub     *Hub
	metrics *metrics.Metrics
	http    *httptest.Server
}

func newHubEnv(t *testing.T) *hubEnv {
	t.Helper()

	m := metrics.New()
	// 日誌丟掉：測試輸出要乾淨，失敗訊息才看得見。
	h := New(slog.New(slog.NewTextHandler(io.Discard, nil)), m)

	upgrader := websocket.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true },
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		query := r.URL.Query()
		// 握手（CONNECT 驗章、心跳協商）是 server 套件的事，這裡直接用
		// query 參數把結果餵進來——hub 測的是「握手之後」。
		h.Serve(SessionConfig{
			Conn:         conn,
			ID:           query.Get("session"),
			PlayerID:     query.Get("player"),
			SendInterval: durationMillis(query.Get("send")),
			RecvInterval: durationMillis(query.Get("recv")),
		})
	}))

	t.Cleanup(func() {
		h.Close(ioTimeout)
		ts.Close()
	})
	return &hubEnv{hub: h, metrics: m, http: ts}
}

func durationMillis(raw string) time.Duration {
	ms, err := strconv.Atoi(raw)
	if err != nil {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

// client 是測試用的 STOMP client 端連線。
type client struct {
	conn      *websocket.Conn
	sessionID string
}

// dial 建立一條連線並等到 hub 真的登記完成。
//
// 等待是必要的：WebSocket 握手完成不代表 Hub.Serve 已經跑到 h.add，
// 不等就 Broadcast 會偶發地推不到人——這種 flaky test 比沒有測試更糟。
func (e *hubEnv) dial(t *testing.T, playerID string) *client {
	t.Helper()
	return e.dialWithHeartbeat(t, playerID, 0, 0)
}

func (e *hubEnv) dialWithHeartbeat(t *testing.T, playerID string, send, recv time.Duration) *client {
	t.Helper()

	before := e.hub.SessionCount()
	sessionID := fmt.Sprintf("sess-%s-%d", playerID, before)

	url := fmt.Sprintf("ws%s/?player=%s&session=%s&send=%d&recv=%d",
		strings.TrimPrefix(e.http.URL, "http"), playerID, sessionID,
		send.Milliseconds(), recv.Milliseconds())

	dialer := &websocket.Dialer{HandshakeTimeout: ioTimeout}
	conn, resp, err := dialer.Dial(url, nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		t.Fatalf("WebSocket 連線失敗：%v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	waitFor(t, "連線被 hub 登記", func() bool { return e.hub.SessionCount() > before })
	return &client{conn: conn, sessionID: sessionID}
}

// waitFor 輪詢等待某個條件成立，逾時就讓測試失敗。
//
// 用輪詢而不是固定 sleep：固定 sleep 要嘛太短（flaky）要嘛太長（測試變慢），
// 而且失敗時的訊息會是「不知道為什麼沒發生」。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(ioTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("等待「%s」逾時", what)
}

// ---------------------------------------------------------------------------
// client 端的帧收發
// ---------------------------------------------------------------------------

func (c *client) send(t *testing.T, frame *stomp.Frame) {
	t.Helper()
	if err := c.conn.SetWriteDeadline(time.Now().Add(ioTimeout)); err != nil {
		t.Fatalf("設定寫入 deadline 失敗：%v", err)
	}
	if err := c.conn.WriteMessage(websocket.TextMessage, frame.Marshal()); err != nil {
		t.Fatalf("送出 %s 失敗：%v", frame.Command, err)
	}
}

// receive 讀一個帧，心跳會被略過。
func (c *client) receive(t *testing.T) *stomp.Frame {
	t.Helper()
	for {
		data := c.receiveRaw(t)
		frame, err := stomp.Unmarshal(data)
		if err == stomp.ErrHeartbeat {
			continue
		}
		if err != nil {
			t.Fatalf("解析回應失敗：%v（原文 %q）", err, data)
		}
		return frame
	}
}

// receiveRaw 讀一則原始 WebSocket 訊息，並斷言它是文字帧。
func (c *client) receiveRaw(t *testing.T) []byte {
	t.Helper()
	if err := c.conn.SetReadDeadline(time.Now().Add(ioTimeout)); err != nil {
		t.Fatalf("設定讀取 deadline 失敗：%v", err)
	}
	msgType, data, err := c.conn.ReadMessage()
	if err != nil {
		t.Fatalf("讀取失敗：%v", err)
	}
	// ★ 必須是文字帧。送二進位的話 @stomp/stompjs 會靜默忽略（AGENTS.md §2.12）。
	if msgType != websocket.TextMessage {
		t.Fatalf("訊息型別 = %d，預期 TextMessage(%d)", msgType, websocket.TextMessage)
	}
	return data
}

func (c *client) expectClosed(t *testing.T) {
	t.Helper()
	if err := c.conn.SetReadDeadline(time.Now().Add(ioTimeout)); err != nil {
		t.Fatalf("設定讀取 deadline 失敗：%v", err)
	}
	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			return // 如預期：連線已被伺服器關閉
		}
		t.Fatalf("預期連線已關閉，卻還讀得到 %q", data)
	}
}

// subscribe 送 SUBSCRIBE 並等 RECEIPT，回傳時訂閱**已經生效**。
//
// 用 receipt 做同步點，而不是 sleep 幾毫秒：這是規格提供的機制，
// 既精準又順便測到 RECEIPT 有沒有正確回覆。
func (c *client) subscribe(t *testing.T, id, dest string) {
	t.Helper()
	receipt := "r-sub-" + id
	c.send(t, &stomp.Frame{
		Command: stomp.CommandSubscribe,
		Headers: stomp.Headers{
			{Name: stomp.HeaderID, Value: id},
			{Name: stomp.HeaderDestination, Value: dest},
			{Name: stomp.HeaderReceipt, Value: receipt},
		},
	})
	c.expectReceipt(t, receipt)
}

func (c *client) unsubscribe(t *testing.T, id string) {
	t.Helper()
	receipt := "r-unsub-" + id
	c.send(t, &stomp.Frame{
		Command: stomp.CommandUnsubscribe,
		Headers: stomp.Headers{
			{Name: stomp.HeaderID, Value: id},
			{Name: stomp.HeaderReceipt, Value: receipt},
		},
	})
	c.expectReceipt(t, receipt)
}

// expectReceipt 等一個 RECEIPT，途中先到的 MESSAGE 會被略過。
//
// 略過而不是直接失敗，是因為在有併發廣播的測試裡，推播完全可能
// 插隊到 RECEIPT 前面——那是合法的，RECEIPT 只保證「這個帧處理完了」，
// 沒有保證它是下一個抵達的東西。
func (c *client) expectReceipt(t *testing.T, want string) {
	t.Helper()
	for {
		frame := c.receive(t)
		if frame.Command == stomp.CommandMessage {
			continue
		}
		if frame.Command != stomp.CommandReceipt {
			t.Fatalf("指令 = %s，預期 RECEIPT（帧內容：%v）", frame.Command, frame)
		}
		if got := frame.Headers.Get(stomp.HeaderReceiptID); got != want {
			t.Fatalf("receipt-id = %q，預期 %q", got, want)
		}
		return
	}
}

// expectMessage 讀一個 MESSAGE 帧並檢查必備 header。
func (c *client) expectMessage(t *testing.T, wantSub, wantDest, wantBody string) *stomp.Frame {
	t.Helper()

	frame := c.receive(t)
	if frame.Command != stomp.CommandMessage {
		t.Fatalf("指令 = %s，預期 MESSAGE（帧內容：%v）", frame.Command, frame)
	}
	// ★ subscription 漏掉的話 @stomp/stompjs 會靜默丟棄（AGENTS.md §2.3），
	//   沒有錯誤訊息，症狀只有「怎麼沒反應」。這個斷言就是防線。
	if got := frame.Headers.Get(stomp.HeaderSubscription); got != wantSub {
		t.Errorf("subscription = %q，預期 %q", got, wantSub)
	}
	if got := frame.Headers.Get(stomp.HeaderMessageID); got == "" {
		t.Error("message-id 是空的，client 端會無法識別訊息")
	}
	if got := frame.Headers.Get(stomp.HeaderDestination); got != wantDest {
		t.Errorf("destination = %q，預期 %q", got, wantDest)
	}
	if got := frame.Headers.Get(stomp.HeaderContentType); got != contentTypeJSON {
		t.Errorf("content-type = %q，預期 %q", got, contentTypeJSON)
	}
	if got := string(frame.Body); got != wantBody {
		t.Errorf("body = %q，預期 %q", got, wantBody)
	}
	return frame
}

// expectError 讀一個 ERROR 帧並檢查 message header。
func (c *client) expectError(t *testing.T, wantMessage string) {
	t.Helper()
	frame := c.receive(t)
	if frame.Command != stomp.CommandError {
		t.Fatalf("指令 = %s，預期 ERROR（帧內容：%v）", frame.Command, frame)
	}
	if got := frame.Headers.Get(stomp.HeaderMessage); got != wantMessage {
		t.Errorf("message = %q，預期 %q", got, wantMessage)
	}
}

// ---------------------------------------------------------------------------
// 訂閱與扇出
// ---------------------------------------------------------------------------

// TestBroadcastReachesAllSubscribers 是 Phase 2 DoD：/topic/rank 廣播兩個都收到。
func TestBroadcastReachesAllSubscribers(t *testing.T) {
	t.Parallel()

	env := newHubEnv(t)
	alice := env.dial(t, "alice")
	bob := env.dial(t, "bob")
	alice.subscribe(t, "sub-a", testDestination)
	bob.subscribe(t, "sub-b", testDestination)

	body := `{"type":"RANK_UPDATE","entries":[]}`
	if sent := env.hub.Broadcast(testDestination, []byte(body)); sent != 2 {
		t.Fatalf("Broadcast 回報送出 %d 則，預期 2", sent)
	}

	alice.expectMessage(t, "sub-a", testDestination, body)
	bob.expectMessage(t, "sub-b", testDestination, body)
}

// TestPrivatePushOnlyReachesTargetPlayer 是 Phase 2 DoD：
// 兩個帳號各自登入，私人推播只進正確的那一個。
func TestPrivatePushOnlyReachesTargetPlayer(t *testing.T) {
	t.Parallel()

	env := newHubEnv(t)
	alice := env.dial(t, "alice")
	bob := env.dial(t, "bob")
	alice.subscribe(t, "s1", PrivateDestination)
	bob.subscribe(t, "s1", PrivateDestination)

	body := `{"type":"GAME_RESULT","win":true}`
	if sent := env.hub.PushToPlayer("alice", PrivateDestination, []byte(body)); sent != 1 {
		t.Fatalf("PushToPlayer 回報送出 %d 則，預期 1", sent)
	}
	alice.expectMessage(t, "s1", PrivateDestination, body)

	// bob 不該收到 alice 的私人訊息。用「再廣播一則、確認 bob 讀到的是它」
	// 來驗證，比 sleep 一段時間然後斷言「沒東西」可靠得多——後者只是
	// 在賭時間差，訊息晚一點到就抓不到 bug 了。
	marker := `{"marker":true}`
	env.hub.Broadcast(PrivateDestination, []byte(marker))
	bob.expectMessage(t, "s1", PrivateDestination, marker)
}

// TestSamePlayerOnTwoConnections 確認同一個玩家開兩個分頁時，兩邊都收得到。
// Java 版的 convertAndSendToUser 也是這個行為。
func TestSamePlayerOnTwoConnections(t *testing.T) {
	t.Parallel()

	env := newHubEnv(t)
	tab1 := env.dial(t, "alice")
	tab2 := env.dial(t, "alice")
	tab1.subscribe(t, "s1", PrivateDestination)
	tab2.subscribe(t, "s1", PrivateDestination)

	body := `{"n":1}`
	if sent := env.hub.PushToPlayer("alice", PrivateDestination, []byte(body)); sent != 2 {
		t.Fatalf("送出 %d 則，預期 2（同一玩家的兩條連線）", sent)
	}
	tab1.expectMessage(t, "s1", PrivateDestination, body)
	tab2.expectMessage(t, "s1", PrivateDestination, body)
}

// TestPushToUnknownTargets 確認推給不存在的對象是安靜的 no-op，不是錯誤。
// 推播是 best-effort：玩家沒上線就是收不到，這與 Java 版一致。
func TestPushToUnknownTargets(t *testing.T) {
	t.Parallel()

	env := newHubEnv(t)
	conn := env.dial(t, "alice")
	conn.subscribe(t, "s1", testDestination)

	tests := []struct {
		name string
		got  int
	}{
		{"沒人訂閱的 destination", env.hub.Broadcast("/topic/nobody", []byte(`{}`))},
		{"沒上線的玩家", env.hub.PushToPlayer("ghost", PrivateDestination, []byte(`{}`))},
		{"有上線但沒訂那個 destination", env.hub.PushToPlayer("alice", PrivateDestination, []byte(`{}`))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != 0 {
				t.Errorf("送出 %d 則，預期 0", tt.got)
			}
		})
	}
}

// TestDuplicateDestinationSubscriptions 確認同一條連線用兩個 id 訂同一個
// destination 時會收到兩則——這是規格的行為，subscription header 各自對應。
func TestDuplicateDestinationSubscriptions(t *testing.T) {
	t.Parallel()

	env := newHubEnv(t)
	conn := env.dial(t, "alice")
	conn.subscribe(t, "s1", testDestination)
	conn.subscribe(t, "s2", testDestination)

	body := `{"n":1}`
	if sent := env.hub.Broadcast(testDestination, []byte(body)); sent != 2 {
		t.Fatalf("送出 %d 則，預期 2（同一連線的兩個訂閱）", sent)
	}

	// map 的走訪順序不固定，所以只檢查「兩則的 subscription 剛好是 s1 與 s2」。
	seen := map[string]bool{}
	for range 2 {
		frame := conn.receive(t)
		seen[frame.Headers.Get(stomp.HeaderSubscription)] = true
	}
	if !seen["s1"] || !seen["s2"] {
		t.Errorf("收到的 subscription = %v，預期同時有 s1 與 s2", seen)
	}
}

// TestUnsubscribeStopsDelivery 確認退訂之後就收不到了，而且同 destination
// 的其他訂閱不受影響。
func TestUnsubscribeStopsDelivery(t *testing.T) {
	t.Parallel()

	env := newHubEnv(t)
	conn := env.dial(t, "alice")
	conn.subscribe(t, "s1", testDestination)
	conn.subscribe(t, "s2", testDestination)

	conn.unsubscribe(t, "s1")

	body := `{"n":2}`
	if sent := env.hub.Broadcast(testDestination, []byte(body)); sent != 1 {
		t.Fatalf("送出 %d 則，預期 1（s1 已退訂，s2 還在）", sent)
	}
	conn.expectMessage(t, "s2", testDestination, body)

	conn.unsubscribe(t, "s2")
	if sent := env.hub.Broadcast(testDestination, []byte(body)); sent != 0 {
		t.Errorf("全部退訂後仍送出 %d 則，預期 0", sent)
	}
}

// TestUnsubscribeUnknownIDIsIgnored 確認退訂不存在的 id 不會斷線。
//
// 這在前端很常見：React StrictMode 的 effect cleanup 會跑兩次，
// 嚴格照規格回 ERROR 的話開發模式下每次都會斷線。
func TestUnsubscribeUnknownIDIsIgnored(t *testing.T) {
	t.Parallel()

	env := newHubEnv(t)
	conn := env.dial(t, "alice")

	conn.unsubscribe(t, "從來沒訂過")

	// 連線還活著：訂閱得起來，也收得到訊息。
	conn.subscribe(t, "s1", testDestination)
	env.hub.Broadcast(testDestination, []byte(`{}`))
	conn.expectMessage(t, "s1", testDestination, `{}`)
}

// TestProtocolErrorsSendErrorAndClose 是協議錯誤的表格驅動測試。
//
// 每一種都必須「先送出可讀的 ERROR 帧，再關閉連線」——只關不說的話，
// client 端只會看到 1006，完全查不出原因。
func TestProtocolErrorsSendErrorAndClose(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		frame       *stomp.Frame
		wantMessage string
	}{
		{
			name: "SUBSCRIBE 缺 id",
			frame: &stomp.Frame{Command: stomp.CommandSubscribe, Headers: stomp.Headers{
				{Name: stomp.HeaderDestination, Value: testDestination},
			}},
			wantMessage: "Invalid SUBSCRIBE",
		},
		{
			name: "SUBSCRIBE 缺 destination",
			frame: &stomp.Frame{Command: stomp.CommandSubscribe, Headers: stomp.Headers{
				{Name: stomp.HeaderID, Value: "s1"},
			}},
			wantMessage: "Invalid SUBSCRIBE",
		},
		{
			name:        "UNSUBSCRIBE 缺 id",
			frame:       &stomp.Frame{Command: stomp.CommandUnsubscribe},
			wantMessage: "Invalid UNSUBSCRIBE",
		},
		{
			name: "握手後又送 CONNECT",
			frame: &stomp.Frame{Command: stomp.CommandConnect, Headers: stomp.Headers{
				{Name: stomp.HeaderAcceptVersion, Value: "1.2"},
			}},
			wantMessage: "Already connected",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newHubEnv(t)
			conn := env.dial(t, "alice")

			conn.send(t, tt.frame)
			conn.expectError(t, tt.wantMessage)
			conn.expectClosed(t)
		})
	}
}

// TestDuplicateSubscriptionID 確認同一條連線重複用同一個 id 會被拒絕。
// 允許重複的話，UNSUBSCRIBE 就不知道該退訂哪一個。
func TestDuplicateSubscriptionID(t *testing.T) {
	t.Parallel()

	env := newHubEnv(t)
	conn := env.dial(t, "alice")
	conn.subscribe(t, "s1", testDestination)

	conn.send(t, &stomp.Frame{Command: stomp.CommandSubscribe, Headers: stomp.Headers{
		{Name: stomp.HeaderID, Value: "s1"},
		{Name: stomp.HeaderDestination, Value: "/topic/other"},
	}})
	conn.expectError(t, "Duplicate subscription id")
	conn.expectClosed(t)
}

// TestSubscriptionLimit 確認訂閱數上限有生效。
// 沒有上限的話，一個 client 不斷訂新 destination 就能把 hub 的 map 撐爆。
func TestSubscriptionLimit(t *testing.T) {
	t.Parallel()

	env := newHubEnv(t)
	conn := env.dial(t, "alice")

	for i := range maxSubscriptions {
		conn.subscribe(t, "s"+strconv.Itoa(i), "/topic/"+strconv.Itoa(i))
	}

	conn.send(t, &stomp.Frame{Command: stomp.CommandSubscribe, Headers: stomp.Headers{
		{Name: stomp.HeaderID, Value: "one-too-many"},
		{Name: stomp.HeaderDestination, Value: "/topic/overflow"},
	}})
	conn.expectError(t, "Too many subscriptions")
	conn.expectClosed(t)
}

// TestUnsupportedCommandsAreIgnored 確認 SEND / ACK 之類的指令被靜默忽略。
//
// 對齊 Java 版：Spring 的 simple broker 收到沒有 @MessageMapping 的 SEND
// 只記一行 log，不會斷線。回 ERROR 的話 Go 版就比 Java 版嚴格，
// Phase 5 的契約測試會對不上。
func TestUnsupportedCommandsAreIgnored(t *testing.T) {
	t.Parallel()

	env := newHubEnv(t)
	conn := env.dial(t, "alice")
	conn.subscribe(t, "s1", testDestination)

	for _, command := range []stomp.Command{
		stomp.CommandSend, stomp.CommandAck, stomp.CommandNack,
		stomp.CommandBegin, stomp.CommandCommit, stomp.CommandAbort,
	} {
		conn.send(t, &stomp.Frame{
			Command: command,
			Headers: stomp.Headers{{Name: stomp.HeaderDestination, Value: "/app/whatever"}},
			Body:    []byte(`{"ignored":true}`),
		})
	}

	// 連線仍然活著。
	env.hub.Broadcast(testDestination, []byte(`{"alive":true}`))
	conn.expectMessage(t, "s1", testDestination, `{"alive":true}`)
}

// TestMalformedFrameIsRejected 確認畸形帧會拿到 ERROR 並斷線。
func TestMalformedFrameIsRejected(t *testing.T) {
	t.Parallel()

	env := newHubEnv(t)
	conn := env.dial(t, "alice")

	// 沒有 NULL 結尾，解析器一定會擋下來。
	if err := conn.conn.WriteMessage(websocket.TextMessage, []byte("SUBSCRIBE\nid:s1\n\n")); err != nil {
		t.Fatalf("送出畸形帧失敗：%v", err)
	}
	conn.expectError(t, "Malformed frame")
	conn.expectClosed(t)
}

// ---------------------------------------------------------------------------
// 收尾
// ---------------------------------------------------------------------------

// TestDisconnectRepliesReceiptThenCloses 確認 DISCONNECT 的收尾握手。
//
// 這是 Phase 1 做不到的：那時沒有 write goroutine，RECEIPT 還沒寫出去
// 連線就被關掉了。
func TestDisconnectRepliesReceiptThenCloses(t *testing.T) {
	t.Parallel()

	env := newHubEnv(t)
	conn := env.dial(t, "alice")

	conn.send(t, &stomp.Frame{
		Command: stomp.CommandDisconnect,
		Headers: stomp.Headers{{Name: stomp.HeaderReceipt, Value: "bye"}},
	})
	conn.expectReceipt(t, "bye")
	conn.expectClosed(t)

	waitFor(t, "hub 移除已斷開的連線", func() bool { return env.hub.SessionCount() == 0 })
}

// TestSessionIsRemovedFromAllIndexes 確認連線斷開後，訂閱索引也跟著清掉。
// 沒清乾淨的話會是記憶體洩漏，而且廣播會對著死連線做白工。
func TestSessionIsRemovedFromAllIndexes(t *testing.T) {
	t.Parallel()

	env := newHubEnv(t)
	conn := env.dial(t, "alice")
	conn.subscribe(t, "s1", testDestination)

	_ = conn.conn.Close()
	waitFor(t, "hub 移除已斷開的連線", func() bool { return env.hub.SessionCount() == 0 })

	if sent := env.hub.Broadcast(testDestination, []byte(`{}`)); sent != 0 {
		t.Errorf("斷線後廣播仍送出 %d 則，預期 0", sent)
	}
	if sent := env.hub.PushToPlayer("alice", PrivateDestination, []byte(`{}`)); sent != 0 {
		t.Errorf("斷線後私人推播仍送出 %d 則，預期 0", sent)
	}

	// 內層 map 空了要把整個 key 刪掉，否則索引會隨 destination 數量單向成長。
	env.hub.mu.RLock()
	subsSize, playersSize := len(env.hub.subs), len(env.hub.players)
	env.hub.mu.RUnlock()
	if subsSize != 0 || playersSize != 0 {
		t.Errorf("索引沒清乾淨：subs=%d players=%d，預期都是 0", subsSize, playersSize)
	}
}

// TestCloseSendsFarewellError 確認優雅關閉會先送 ERROR 再關。
//
// 直接關的話 client 收到 1006（異常關閉），會判定成網路問題並進入
// 指數退避的重連迴圈；收到 ERROR + 正常關閉則可以立刻重連。
func TestCloseSendsFarewellError(t *testing.T) {
	t.Parallel()

	env := newHubEnv(t)
	conn := env.dial(t, "alice")

	if closed := env.hub.Close(ioTimeout); closed != 1 {
		t.Fatalf("Close 回報關閉 %d 條連線，預期 1", closed)
	}
	conn.expectError(t, "Server shutting down")
	conn.expectClosed(t)
}

// TestClosedHubRejectsNewSessions 確認關閉後不再收新連線。
func TestClosedHubRejectsNewSessions(t *testing.T) {
	t.Parallel()

	env := newHubEnv(t)
	env.hub.Close(ioTimeout)

	// 這裡不能用 env.dial（它會等 SessionCount 增加，而這次不會增加）。
	url := "ws" + strings.TrimPrefix(env.http.URL, "http") + "/?player=late&session=late"
	conn, resp, err := (&websocket.Dialer{HandshakeTimeout: ioTimeout}).Dial(url, nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		return // 連 WebSocket 都沒升級成功也算合格
	}
	t.Cleanup(func() { _ = conn.Close() })

	(&client{conn: conn}).expectClosed(t)
	if got := env.hub.SessionCount(); got != 0 {
		t.Errorf("關閉後仍登記了 %d 條連線，預期 0", got)
	}
}

// ---------------------------------------------------------------------------
// 心跳
// ---------------------------------------------------------------------------

// TestHeartbeatIsSent 確認伺服器真的會依協商間隔送心跳。
//
// 宣告了非零的 heart-beat 卻不送，client 等不到就會自己斷線——
// 症狀是「握手成功、幾秒後莫名斷線」（AGENTS.md §2.13）。
func TestHeartbeatIsSent(t *testing.T) {
	t.Parallel()

	env := newHubEnv(t)
	conn := env.dialWithHeartbeat(t, "alice", 30*time.Millisecond, 0)

	for range 3 {
		if data := conn.receiveRaw(t); string(data) != "\n" {
			t.Fatalf("收到 %q，預期心跳（單一 EOL）", data)
		}
	}
}

// TestHeartbeatTimerResetsOnRealTraffic 確認有流量時不會多送心跳。
// 規格：任何送出的帧都算一次心跳。
func TestHeartbeatTimerResetsOnRealTraffic(t *testing.T) {
	t.Parallel()

	env := newHubEnv(t)
	// 心跳間隔設得比測試持續時間長：只要 ticker 每次寫出都被 Reset，
	// 整段期間就不該出現任何心跳。
	conn := env.dialWithHeartbeat(t, "alice", 200*time.Millisecond, 0)
	conn.subscribe(t, "s1", testDestination)

	for i := range 5 {
		env.hub.Broadcast(testDestination, []byte(`{}`))
		conn.expectMessage(t, "s1", testDestination, `{}`)
		if i < 4 {
			time.Sleep(50 * time.Millisecond) // 累計 200ms，剛好跨過一個未重設的間隔
		}
	}
}

// TestReadTimeoutClosesSilentClient 是 Phase 2 DoD 的「斷網」情境：
// 談好心跳之後對方不再送任何東西，伺服器要主動斷線。
//
// 沒有這個機制的話，被拔網路線的連線會永遠掛在註冊表裡——
// TCP 自己要幾十分鐘才會發現。
func TestReadTimeoutClosesSilentClient(t *testing.T) {
	t.Parallel()

	env := newHubEnv(t)
	conn := env.dialWithHeartbeat(t, "alice", 0, 30*time.Millisecond)

	// 寬容倍數是 2，所以 60ms 之後就該被踢掉。
	conn.expectClosed(t)
	waitFor(t, "hub 移除逾時的連線", func() bool { return env.hub.SessionCount() == 0 })
}

// TestNoHeartbeatMeansNoReadTimeout 確認沒談成心跳時不會因為對方安靜而斷線。
// 這必須與 CONNECTED 宣告的內容一致：沒要求對方送，就不能因此斷線。
func TestNoHeartbeatMeansNoReadTimeout(t *testing.T) {
	t.Parallel()

	env := newHubEnv(t)
	conn := env.dial(t, "alice")
	conn.subscribe(t, "s1", testDestination)

	time.Sleep(150 * time.Millisecond)

	env.hub.Broadcast(testDestination, []byte(`{"alive":true}`))
	conn.expectMessage(t, "s1", testDestination, `{"alive":true}`)
}

// ---------------------------------------------------------------------------
// 背壓
// ---------------------------------------------------------------------------

// parkedConn 回傳一條已升級、但**沒有任何 goroutine 在讀寫**的伺服器端連線。
//
// 用它組出一個「write goroutine 沒在跑」的 Session，就能百分之百確定地
// 塞爆 outbound——不必靠灌大量資料去賭 TCP 緩衝什麼時候滿。
// 那種寫法在不同作業系統上的緩衝大小差很多，是 flaky test 的經典來源。
func parkedConn(t *testing.T) *websocket.Conn {
	t.Helper()

	conns := make(chan *websocket.Conn, 1)
	release := make(chan struct{})
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		conns <- conn
		<-release // 撐住 handler，httptest 的 Close 才不會提早收掉連線
	}))

	url := "ws" + strings.TrimPrefix(ts.URL, "http")
	clientConn, resp, err := (&websocket.Dialer{HandshakeTimeout: ioTimeout}).Dial(url, nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		ts.Close()
		t.Fatalf("建立測試連線失敗：%v", err)
	}
	t.Cleanup(func() {
		close(release)
		_ = clientConn.Close()
		ts.Close()
	})

	select {
	case conn := <-conns:
		return conn
	case <-time.After(ioTimeout):
		t.Fatal("等待伺服器端連線逾時")
		return nil
	}
}

// TestSlowClientIsDroppedWithoutBlockingOthers 是 Phase 2 DoD 的慢速客戶端測試，
// 也是整個服務最關鍵的一條性質。
//
// 一旦扇出寫成阻塞的 `ch <- msg`，一個讀不動的 client 就會卡住整個廣播迴圈，
// 拖垮所有人（AGENTS.md §2.6）。這個測試同時驗證三件事：
//  1. 慢速連線被踢掉；
//  2. notify_outbound_dropped_total 有增加（背壓可觀測）；
//  3. 同一輪廣播裡的正常連線**全部**收到，一則不漏。
func TestSlowClientIsDroppedWithoutBlockingOthers(t *testing.T) {
	t.Parallel()

	env := newHubEnv(t)
	healthy := env.dial(t, "healthy")
	healthy.subscribe(t, "s1", testDestination)

	// 手工組一個「write goroutine 沒在跑」的 session，模擬完全讀不動的 client。
	slow := newSession(env.hub, SessionConfig{
		Conn:     parkedConn(t),
		ID:       "slow",
		PlayerID: "slow",
	})
	if !env.hub.add(slow) {
		t.Fatal("加入慢速 session 失敗")
	}
	slow.subs["s1"] = testDestination
	env.hub.subscribe(testDestination, slow)
	// 這個 session 沒有 write goroutine，writerDone 永遠不會關；留著的話
	// 收尾時的 Hub.Close 會白等滿一個 drainTimeout。測試自己收掉它。
	t.Cleanup(func() { env.hub.remove(slow) })

	// 廣播的則數要超過緩衝，才會真的觸發丟棄。
	const rounds = outboundBuffer + 10
	received := make(chan int, 1)
	go func() {
		count := 0
		for range rounds {
			frame := healthy.receive(t)
			if frame.Command == stomp.CommandMessage {
				count++
			}
		}
		received <- count
	}()

	for i := range rounds {
		env.hub.Broadcast(testDestination, fmt.Appendf(nil, `{"n":%d}`, i))
	}

	// 1. 正常連線一則都沒少。
	select {
	case got := <-received:
		if got != rounds {
			t.Errorf("正常連線收到 %d 則，預期 %d", got, rounds)
		}
	case <-time.After(ioTimeout):
		t.Fatal("正常連線收訊逾時——扇出很可能被慢速連線卡住了")
	}

	// 2. 慢速連線被關掉。
	select {
	case <-slow.done:
	default:
		t.Error("慢速連線沒有被斷開")
	}

	// 3. 背壓在指標上看得見。
	if dropped := testutil.ToFloat64(env.metrics.OutboundDropped); dropped < 1 {
		t.Errorf("notify_outbound_dropped_total = %v，預期至少 1", dropped)
	}
}

// ---------------------------------------------------------------------------
// 併發
// ---------------------------------------------------------------------------

// TestConcurrentSubscribeAndBroadcast 是給 -race 跑的壓力測試。
//
// 同時做「連線建立/斷開」（寫鎖）與「廣播」（讀鎖），把鎖的順序問題
// 與資料競爭逼出來。這種 bug 用眼睛看程式碼是找不到的。
func TestConcurrentSubscribeAndBroadcast(t *testing.T) {
	t.Parallel()

	env := newHubEnv(t)

	const clients = 8
	var wg sync.WaitGroup

	// 一批連線不斷訂閱、退訂、斷開。
	for i := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn := env.dial(t, "player-"+strconv.Itoa(i))
			conn.subscribe(t, "s1", testDestination)

			// ⚠️ gorilla 同時只允許一個 reader。開了這條讀取 goroutine 之後，
			// 這個 goroutine 就**不能**再呼叫任何會讀取的 helper——
			// 所以下面的退訂不帶 receipt（否則要等回覆＝要讀）。
			drained := make(chan struct{})
			go func() {
				defer close(drained)
				for {
					if _, _, err := conn.conn.ReadMessage(); err != nil {
						return
					}
				}
			}()

			conn.send(t, &stomp.Frame{
				Command: stomp.CommandUnsubscribe,
				Headers: stomp.Headers{{Name: stomp.HeaderID, Value: "s1"}},
			})
			_ = conn.conn.Close()
			<-drained
		}()
	}

	// 同時不斷廣播。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 200 {
			env.hub.Broadcast(testDestination, []byte(`{"x":1}`))
			env.hub.PushToPlayer("player-0", PrivateDestination, []byte(`{"x":1}`))
			env.hub.SessionCount()
		}
	}()

	wg.Wait()
}

// TestMessageIDsAreUnique 確認同一條連線上的 message-id 不重複。
// 重複的話 client 端做去重時會把後來的訊息當成重播丟掉。
func TestMessageIDsAreUnique(t *testing.T) {
	t.Parallel()

	env := newHubEnv(t)
	conn := env.dial(t, "alice")
	conn.subscribe(t, "s1", testDestination)

	const rounds = 20
	for range rounds {
		env.hub.Broadcast(testDestination, []byte(`{}`))
	}

	seen := make(map[string]struct{}, rounds)
	for range rounds {
		id := conn.receive(t).Headers.Get(stomp.HeaderMessageID)
		if _, duplicate := seen[id]; duplicate {
			t.Fatalf("message-id %q 重複出現", id)
		}
		seen[id] = struct{}{}
	}
}
