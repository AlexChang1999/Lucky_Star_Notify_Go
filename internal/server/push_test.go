package server

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/AlexChang1999/lucky-star-notify-go/internal/config"
	"github.com/AlexChang1999/lucky-star-notify-go/internal/hub"
	"github.com/AlexChang1999/lucky-star-notify-go/internal/stomp"
	"github.com/gorilla/websocket"
)

// newPushEnv 起一個開著 /debug/push 的服務。
func newPushEnv(t *testing.T) *testEnv {
	t.Helper()
	return newTestEnvWithConfig(t, config.Config{
		Port:           config.DefaultPort,
		JWTSecret:      []byte(testSecret),
		AllowedOrigins: []string{"*"},
		LogLevel:       slog.LevelError,
		DebugPush:      true,
	})
}

// connectAs 完成一次完整握手，回傳連線與 CONNECTED 帧。
func connectAs(t *testing.T, env *testEnv, playerID string, heartBeat string) (*websocket.Conn, *stomp.Frame) {
	t.Helper()

	conn := env.dial(t, nil)
	headers := stomp.Headers{
		{Name: stomp.HeaderAcceptVersion, Value: "1.0,1.1,1.2"},
		{Name: stomp.HeaderHost, Value: "localhost"},
		{Name: stomp.HeaderAuthorization, Value: "Bearer " + testToken(t, playerID, time.Hour)},
	}
	if heartBeat != "" {
		headers = append(headers, stomp.Header{Name: stomp.HeaderHeartBeat, Value: heartBeat})
	}
	send(t, conn, connectFrame(headers))

	connected := receive(t, conn)
	if connected.Command != stomp.CommandConnected {
		t.Fatalf("回應指令 = %s，預期 CONNECTED（body=%q）", connected.Command, connected.Body)
	}
	return conn, connected
}

// subscribeWithReceipt 送 SUBSCRIBE 並等 RECEIPT，回傳時訂閱已生效。
func subscribeWithReceipt(t *testing.T, conn *websocket.Conn, id, dest string) {
	t.Helper()

	send(t, conn, &stomp.Frame{
		Command: stomp.CommandSubscribe,
		Headers: stomp.Headers{
			{Name: stomp.HeaderID, Value: id},
			{Name: stomp.HeaderDestination, Value: dest},
			{Name: stomp.HeaderReceipt, Value: "r-" + id},
		},
	})
	reply := receive(t, conn)
	if reply.Command != stomp.CommandReceipt {
		t.Fatalf("指令 = %s，預期 RECEIPT", reply.Command)
	}
}

// debugPush 打 POST /debug/push，回傳送出的則數。
func debugPush(t *testing.T, env *testEnv, payload string) int {
	t.Helper()

	resp, err := env.http.Client().Post(
		env.http.URL+"/debug/push", "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("請求失敗：%v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("狀態碼 = %d，預期 200", resp.StatusCode)
	}
	var result struct {
		Delivered int `json:"delivered"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("解析回應失敗：%v", err)
	}
	return result.Delivered
}

// TestDebugPushBroadcast 是 Phase 2 DoD 的端到端路徑：
// 真的握手 → 真的訂閱 → /debug/push 灌的訊息看得到。
func TestDebugPushBroadcast(t *testing.T) {
	t.Parallel()

	env := newPushEnv(t)
	alice, _ := connectAs(t, env, "alice", "")
	bob, _ := connectAs(t, env, "bob", "")
	subscribeWithReceipt(t, alice, "sub-rank", "/topic/rank")
	subscribeWithReceipt(t, bob, "sub-rank", "/topic/rank")

	body := `{"type":"RANK_UPDATE","entries":[]}`
	if got := debugPush(t, env, `{"destination":"/topic/rank","body":`+body+`}`); got != 2 {
		t.Fatalf("delivered = %d，預期 2", got)
	}

	for name, conn := range map[string]*websocket.Conn{"alice": alice, "bob": bob} {
		frame := receive(t, conn)
		if frame.Command != stomp.CommandMessage {
			t.Fatalf("%s 收到 %s，預期 MESSAGE", name, frame.Command)
		}
		if got := string(frame.Body); got != body {
			t.Errorf("%s 的 body = %q，預期 %q", name, got, body)
		}
	}
}

// TestDebugPushPrivateOnlyReachesTarget 是 Phase 2 DoD：
// 兩個帳號登入，私人推播只進正確的那一個。
func TestDebugPushPrivateOnlyReachesTarget(t *testing.T) {
	t.Parallel()

	env := newPushEnv(t)
	alice, _ := connectAs(t, env, "alice", "")
	bob, _ := connectAs(t, env, "bob", "")
	subscribeWithReceipt(t, alice, "sub-me", hub.PrivateDestination)
	subscribeWithReceipt(t, bob, "sub-me", hub.PrivateDestination)

	// 不帶 destination，只帶 playerId → 預設走私人頻道。
	if got := debugPush(t, env, `{"playerId":"alice","body":{"type":"GAME_RESULT"}}`); got != 1 {
		t.Fatalf("delivered = %d，預期 1", got)
	}

	frame := receive(t, alice)
	if frame.Command != stomp.CommandMessage {
		t.Fatalf("alice 收到 %s，預期 MESSAGE", frame.Command)
	}

	// bob 那條連線上，下一個抵達的必須是後來這則廣播，而不是 alice 的私訊。
	if got := debugPush(t, env, `{"destination":"`+hub.PrivateDestination+`","body":{"marker":true}}`); got != 2 {
		t.Fatalf("廣播 delivered = %d，預期 2", got)
	}
	if got := string(receive(t, bob).Body); got != `{"marker":true}` {
		t.Errorf("bob 收到的 body = %q，預期 marker——alice 的私訊漏給 bob 了", got)
	}
}

// TestDebugPushRejectsBadRequests 檢查參數驗證。
func TestDebugPushRejectsBadRequests(t *testing.T) {
	t.Parallel()

	env := newPushEnv(t)

	tests := []struct {
		name    string
		payload string
	}{
		{"不是 JSON", `這不是 JSON`},
		{"缺 body", `{"destination":"/topic/rank"}`},
		{"destination 與 playerId 都沒給", `{"body":{}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, err := env.http.Client().Post(
				env.http.URL+"/debug/push", "application/json", strings.NewReader(tt.payload))
			if err != nil {
				t.Fatalf("請求失敗：%v", err)
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("狀態碼 = %d，預期 400", resp.StatusCode)
			}
		})
	}
}

// TestDebugPushNotRegisteredWhenDisabled 確認旗標關掉時整個路徑不存在。
//
// 回 404 而不是 403：讓掃描器連「有這個端點」都看不出來。
func TestDebugPushNotRegisteredWhenDisabled(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t) // DebugPush 預設 false
	resp, err := env.http.Client().Post(
		env.http.URL+"/debug/push", "application/json", strings.NewReader(`{"body":{}}`))
	if err != nil {
		t.Fatalf("請求失敗：%v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("狀態碼 = %d，預期 404", resp.StatusCode)
	}
}

// TestConnectedAdvertisesHeartbeat 確認 CONNECTED 帶的是伺服器自己的 sx,sy。
//
// 這個 header 一旦宣告非零就**必須**真的送心跳，否則 client 等不到會自己
// 斷線——症狀是「握手成功、幾秒後莫名斷線」（AGENTS.md §2.13）。
func TestConnectedAdvertisesHeartbeat(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	_, connected := connectAs(t, env, "alice", "10000,10000")

	if got := connected.Headers.Get(stomp.HeaderHeartBeat); got != "10000,10000" {
		t.Errorf("heart-beat = %q，預期 %q", got, "10000,10000")
	}
}

// TestGracefulShutdownSendsErrorFrame 確認關機時 client 收得到 STOMP ERROR。
//
// 這是 Phase 1 做不到的事：當時沒有 write goroutine，關機那條 goroutine
// 不能直接寫連線（AGENTS.md §2.5），只能硬關，client 端看到的是 1006。
func TestGracefulShutdownSendsErrorFrame(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	conn, _ := connectAs(t, env, "alice", "")

	if err := env.server.Close(); err != nil {
		t.Fatalf("Close() 失敗：%v", err)
	}

	frame := receive(t, conn)
	if frame.Command != stomp.CommandError {
		t.Fatalf("指令 = %s，預期 ERROR", frame.Command)
	}
	if got := frame.Headers.Get(stomp.HeaderMessage); got != "Server shutting down" {
		t.Errorf("message = %q，預期 %q", got, "Server shutting down")
	}
	expectClosed(t, conn)
}
