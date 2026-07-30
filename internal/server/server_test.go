package server

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/AlexChang1999/lucky-star-notify-go/internal/config"
	"github.com/AlexChang1999/lucky-star-notify-go/internal/metrics"
	"github.com/AlexChang1999/lucky-star-notify-go/internal/stomp"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// ioTimeout 是整合測試裡每次讀寫的上限。
// 設得比正式碼的 10 秒短很多：測試卡住時要快點失敗，而不是等到 go test 超時。
const ioTimeout = 3 * time.Second

// testEnv 是一組跑起來的服務加上它的依賴，方便測試取用。
type testEnv struct {
	http    *httptest.Server
	server  *Server
	metrics *metrics.Metrics
}

// newTestEnv 起一個真的 HTTP 服務（含真的 WebSocket 升級）。
//
// 用 httptest.NewServer 而不是直接呼叫 handler：WebSocket 需要 Hijack
// 底層連線，httptest.NewRecorder 沒有實作 http.Hijacker，測不到升級。
func newTestEnv(t *testing.T, origins ...string) *testEnv {
	t.Helper()

	if len(origins) == 0 {
		origins = []string{"*"}
	}
	return newTestEnvWithConfig(t, config.Config{
		Port:           config.DefaultPort,
		JWTSecret:      []byte(testSecret),
		AllowedOrigins: origins,
		LogLevel:       slog.LevelError,
	})
}

// newTestEnvWithConfig 是 newTestEnv 的完整版，用在需要動預設設定的測試
// （例如打開 DebugPush）。
func newTestEnvWithConfig(t *testing.T, cfg config.Config) *testEnv {
	t.Helper()

	m := metrics.New()
	// 日誌丟掉：測試輸出要乾淨，失敗訊息才看得見。
	srv := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), m)

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		_ = srv.Close()
		ts.Close()
	})
	return &testEnv{http: ts, server: srv, metrics: m}
}

// wsURL 把 httptest 的 http:// 位址換成 ws://。
func (e *testEnv) wsURL() string {
	return "ws" + strings.TrimPrefix(e.http.URL, "http") + "/ws"
}

// dial 建立 WebSocket 連線。header 可為 nil。
func (e *testEnv) dial(t *testing.T, header http.Header) *websocket.Conn {
	t.Helper()

	dialer := &websocket.Dialer{
		// 宣告 v12.stomp，測試子協議協商真的有生效。
		Subprotocols:     []string{"v12.stomp"},
		HandshakeTimeout: ioTimeout,
	}
	conn, resp, err := dialer.Dial(e.wsURL(), header)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		t.Fatalf("WebSocket 連線失敗：%v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// send 把帧當文字訊息送出。
func send(t *testing.T, conn *websocket.Conn, frame *stomp.Frame) {
	t.Helper()
	if err := conn.SetWriteDeadline(time.Now().Add(ioTimeout)); err != nil {
		t.Fatalf("設定寫入 deadline 失敗：%v", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, frame.Marshal()); err != nil {
		t.Fatalf("送出 %s 失敗：%v", frame.Command, err)
	}
}

// receive 讀一個帧。
func receive(t *testing.T, conn *websocket.Conn) *stomp.Frame {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(ioTimeout)); err != nil {
		t.Fatalf("設定讀取 deadline 失敗：%v", err)
	}
	msgType, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("讀取失敗：%v", err)
	}
	// ★ 必須是文字帧。送二進位的話 @stomp/stompjs 會靜默忽略，
	// 這個斷言就是防止有人把 TextMessage 改成 BinaryMessage。
	if msgType != websocket.TextMessage {
		t.Fatalf("訊息型別 = %d，預期 TextMessage(%d)", msgType, websocket.TextMessage)
	}
	frame, err := stomp.Unmarshal(data)
	if err != nil {
		t.Fatalf("解析回應失敗：%v（原文 %q）", err, data)
	}
	return frame
}

// expectClosed 確認連線已被伺服器關閉。
func expectClosed(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(ioTimeout)); err != nil {
		t.Fatalf("設定讀取 deadline 失敗：%v", err)
	}
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("預期連線已關閉，卻還讀得到訊息")
	}
}

// validConnect 組一個能過關的 CONNECT 帧。
func validConnect(t *testing.T) *stomp.Frame {
	t.Helper()
	return connectFrame(stomp.Headers{
		{Name: stomp.HeaderAcceptVersion, Value: "1.0,1.1,1.2"},
		{Name: stomp.HeaderHost, Value: "localhost"},
		{Name: stomp.HeaderAuthorization, Value: "Bearer " + testToken(t, "42", time.Hour)},
	})
}

// TestConnectWithValidToken 是 Phase 1 DoD 的正向案例：
// 帶有效 token 送 CONNECT → 拿到 CONNECTED。
func TestConnectWithValidToken(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	conn := env.dial(t, nil)

	if got := conn.Subprotocol(); got != "v12.stomp" {
		t.Errorf("協商到的子協議 = %q，預期 v12.stomp", got)
	}

	send(t, conn, validConnect(t))
	reply := receive(t, conn)

	if reply.Command != stomp.CommandConnected {
		t.Fatalf("回應指令 = %s，預期 CONNECTED（body=%q）", reply.Command, reply.Body)
	}
	if v := reply.Headers.Get(stomp.HeaderVersion); v != StompVersion {
		t.Errorf("version = %q，預期 %q", v, StompVersion)
	}
	if reply.Headers.Get("session") == "" {
		t.Error("CONNECTED 缺少 session header")
	}
	if s := reply.Headers.Get("server"); s != serverHeaderValue {
		t.Errorf("server = %q，預期 %q", s, serverHeaderValue)
	}

	// 握手成功後連線要活著，不能立刻被關掉。
	if err := conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
		t.Fatalf("設定讀取 deadline 失敗：%v", err)
	}
	if _, _, err := conn.ReadMessage(); !isTimeout(err) {
		t.Errorf("握手成功後連線應保持開啟，卻收到：%v", err)
	}
}

// TestConnectRejectsBadTokens 是 Phase 1 DoD 的反向案例：
// 爛 token → 拿到 ERROR **並且連線被關閉**。
func TestConnectRejectsBadTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		authHeader  string // 空字串表示不帶這個 header
		wantMessage string
	}{
		{"沒帶 Authorization", "", "Unauthorized"},
		{"token 亂填", "Bearer 這不是 token", "Unauthorized"},
		{"簽章對不上", "Bearer " + strings.Repeat("a", 20), "Unauthorized"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newTestEnv(t)
			conn := env.dial(t, nil)

			headers := stomp.Headers{{Name: stomp.HeaderAcceptVersion, Value: "1.2"}}
			if tt.authHeader != "" {
				headers.Add(stomp.HeaderAuthorization, tt.authHeader)
			}
			send(t, conn, connectFrame(headers))

			reply := receive(t, conn)
			if reply.Command != stomp.CommandError {
				t.Fatalf("回應指令 = %s，預期 ERROR", reply.Command)
			}
			if msg := reply.Headers.Get(stomp.HeaderMessage); msg != tt.wantMessage {
				t.Errorf("message = %q，預期 %q", msg, tt.wantMessage)
			}
			// 規格要求：送出 ERROR 之後必須關閉連線。
			expectClosed(t, conn)
		})
	}
}

// TestExpiredTokenIsRejected 單獨測過期，因為它需要簽一個過期的 token。
func TestExpiredTokenIsRejected(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	conn := env.dial(t, nil)

	send(t, conn, connectFrame(stomp.Headers{
		{Name: stomp.HeaderAcceptVersion, Value: "1.2"},
		{Name: stomp.HeaderAuthorization, Value: "Bearer " + testToken(t, "42", -time.Minute)},
	}))

	reply := receive(t, conn)
	if reply.Command != stomp.CommandError {
		t.Fatalf("回應指令 = %s，預期 ERROR", reply.Command)
	}
	// 對外不能透露「這個 token 過期了」，只能是通用的 Unauthorized。
	if msg := reply.Headers.Get(stomp.HeaderMessage); msg != "Unauthorized" {
		t.Errorf("message = %q，預期 Unauthorized（不可洩漏是過期）", msg)
	}
	expectClosed(t, conn)
}

// TestMalformedFirstFrameIsRejected 送一段不是合法 STOMP 帧的資料。
func TestMalformedFirstFrameIsRejected(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	conn := env.dial(t, nil)

	if err := conn.SetWriteDeadline(time.Now().Add(ioTimeout)); err != nil {
		t.Fatalf("設定寫入 deadline 失敗：%v", err)
	}
	// 缺少結尾的 NULL，而且指令也不合法。
	if err := conn.WriteMessage(websocket.TextMessage, []byte("HELLO\nfoo:bar\n\n")); err != nil {
		t.Fatalf("寫入失敗：%v", err)
	}

	reply := receive(t, conn)
	if reply.Command != stomp.CommandError {
		t.Fatalf("回應指令 = %s，預期 ERROR", reply.Command)
	}
	if msg := reply.Headers.Get(stomp.HeaderMessage); msg != "Malformed frame" {
		t.Errorf("message = %q，預期 Malformed frame", msg)
	}
	expectClosed(t, conn)
}

// TestHeartbeatBeforeConnectIsIgnored 確認 CONNECT 之前的心跳不會把握手弄壞。
//
// 規格允許帧與帧之間出現任意數量的 EOL，包含第一個帧之前。
func TestHeartbeatBeforeConnectIsIgnored(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	conn := env.dial(t, nil)

	if err := conn.SetWriteDeadline(time.Now().Add(ioTimeout)); err != nil {
		t.Fatalf("設定寫入 deadline 失敗：%v", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, []byte("\n")); err != nil {
		t.Fatalf("送心跳失敗：%v", err)
	}
	send(t, conn, validConnect(t))

	if reply := receive(t, conn); reply.Command != stomp.CommandConnected {
		t.Fatalf("回應指令 = %s，預期 CONNECTED（body=%q）", reply.Command, reply.Body)
	}
}

// TestDisconnectClosesConnection 確認 client 主動 DISCONNECT 後連線收掉。
func TestDisconnectClosesConnection(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	conn := env.dial(t, nil)

	send(t, conn, validConnect(t))
	if reply := receive(t, conn); reply.Command != stomp.CommandConnected {
		t.Fatalf("回應指令 = %s，預期 CONNECTED", reply.Command)
	}

	send(t, conn, &stomp.Frame{Command: stomp.CommandDisconnect})
	expectClosed(t, conn)
}

// TestOriginIsEnforced 確認白名單模式下，非白名單來源的升級會被擋在 HTTP 層。
//
// 這個測試同時是 gorilla 預設行為的備忘：若不自己設 CheckOrigin，
// 連合法的前端（不同 port）都會被 403 擋掉。
func TestOriginIsEnforced(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t, "http://localhost:5173")

	dialer := &websocket.Dialer{HandshakeTimeout: ioTimeout}
	conn, resp, err := dialer.Dial(env.wsURL(), http.Header{"Origin": []string{"http://evil.example"}})
	if err == nil {
		_ = conn.Close()
		t.Fatal("預期升級被拒，卻連上了")
	}
	if resp == nil {
		t.Fatalf("預期拿到 HTTP 回應，err = %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("狀態碼 = %d，預期 403", resp.StatusCode)
	}
}

// TestAllowedOriginPasses 確認白名單命中的來源可以連上。
func TestAllowedOriginPasses(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t, "http://localhost:5173")
	conn := env.dial(t, http.Header{"Origin": []string{"http://localhost:5173"}})

	send(t, conn, validConnect(t))
	if reply := receive(t, conn); reply.Command != stomp.CommandConnected {
		t.Fatalf("回應指令 = %s，預期 CONNECTED", reply.Command)
	}
}

// TestHealthEndpoint 確認健康檢查的形狀與 Java 版一致。
func TestHealthEndpoint(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	resp, err := env.http.Client().Get(env.http.URL + "/actuator/health")
	if err != nil {
		t.Fatalf("請求失敗：%v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("狀態碼 = %d，預期 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("讀取 body 失敗：%v", err)
	}
	// compose 的 healthcheck 會照抄 Java 版的設定，形狀必須一樣。
	if got := strings.TrimSpace(string(body)); got != `{"status":"UP"}` {
		t.Errorf("body = %q，預期 %q", got, `{"status":"UP"}`)
	}
}

// TestMetricsExposedOnBothPaths 確認 /metrics 與 /actuator/prometheus 都通。
//
// 後者是團隊 repo 的 prometheus.yml 寫死的路徑（整個 job 共用 metrics_path），
// 少了它就得為 Go 版另開一個 scrape job。
func TestMetricsExposedOnBothPaths(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	for _, path := range []string{"/metrics", "/actuator/prometheus"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()

			resp, err := env.http.Client().Get(env.http.URL + path)
			if err != nil {
				t.Fatalf("請求失敗：%v", err)
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("狀態碼 = %d，預期 200", resp.StatusCode)
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("讀取 body 失敗：%v", err)
			}
			text := string(body)
			for _, want := range []string{"notify_ws_connections", "go_goroutines", "process_"} {
				if !strings.Contains(text, want) {
					t.Errorf("指標輸出缺少 %q", want)
				}
			}
		})
	}
}

// TestConnectionGaugeTracksLifecycle 確認連線數指標會上去也會下來。
func TestConnectionGaugeTracksLifecycle(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	if got := testutil.ToFloat64(env.metrics.WSConnections); got != 0 {
		t.Fatalf("初始連線數 = %v，預期 0", got)
	}

	conn := env.dial(t, nil)
	// 先完成握手才檢查：拿到 CONNECTED 就保證伺服器端已經登記過這條連線，
	// 不會因為「dial 回來了但 server goroutine 還沒跑到 track」而偶發失敗。
	send(t, conn, validConnect(t))
	if reply := receive(t, conn); reply.Command != stomp.CommandConnected {
		t.Fatalf("回應指令 = %s，預期 CONNECTED", reply.Command)
	}
	if got := testutil.ToFloat64(env.metrics.WSConnections); got != 1 {
		t.Errorf("握手後連線數 = %v，預期 1", got)
	}

	_ = conn.Close()
	// 伺服器要等 ReadMessage 回錯誤才會 untrack，所以這裡輪詢等它降下來。
	waitFor(t, func() bool {
		return testutil.ToFloat64(env.metrics.WSConnections) == 0
	}, "連線關閉後連線數應回到 0")
}

// TestCloseTerminatesLiveConnections 確認優雅關閉真的收得掉 WebSocket。
//
// 這是 http.Server.Shutdown **做不到**的事——被 Hijack 的連線它管不著。
// 少了 Server.Close，程序會等 goroutine 自己結束，也就是等到永遠。
func TestCloseTerminatesLiveConnections(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	conn := env.dial(t, nil)
	send(t, conn, validConnect(t))
	if reply := receive(t, conn); reply.Command != stomp.CommandConnected {
		t.Fatalf("回應指令 = %s，預期 CONNECTED", reply.Command)
	}

	if err := env.server.Close(); err != nil {
		t.Fatalf("Close() 失敗：%v", err)
	}
	// 關機時會先送一個道別用的 ERROR 帧（見 TestGracefulShutdownSendsErrorFrame），
	// 先把它讀掉，剩下的才是「連線已關閉」。
	if frame := receive(t, conn); frame.Command != stomp.CommandError {
		t.Fatalf("關機時收到 %s，預期 ERROR", frame.Command)
	}
	expectClosed(t, conn)
}

// TestCloseRejectsNewConnections 確認關閉後新連線會被立刻收掉。
func TestCloseRejectsNewConnections(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	if err := env.server.Close(); err != nil {
		t.Fatalf("Close() 失敗：%v", err)
	}

	// HTTP 層還在（httptest 沒關），所以升級本身會成功，
	// 但伺服器會在 track 時發現自己正在關閉，立刻關掉這條連線。
	conn := env.dial(t, nil)
	expectClosed(t, conn)
}

// TestConcurrentConnections 讓多條連線同時握手，配合 -race 檢查共用狀態。
//
// 這一題就是 AGENTS.md §2.5 的守門測試：所有連線共用同一個 Server、
// 同一個 Verifier、同一個 Gauge，任何未受保護的共用狀態都會在這裡被抓到。
func TestConcurrentConnections(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)

	const clients = 16
	done := make(chan error, clients)
	for i := range clients {
		go func(i int) {
			dialer := &websocket.Dialer{HandshakeTimeout: ioTimeout}
			conn, resp, err := dialer.Dial(env.wsURL(), nil)
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			if err != nil {
				done <- err
				return
			}
			defer func() { _ = conn.Close() }()

			// 每個 client 用不同的 playerID，順便確認驗章沒有共用可變狀態。
			claims := connectFrame(stomp.Headers{
				{Name: stomp.HeaderAcceptVersion, Value: "1.2"},
				{Name: stomp.HeaderAuthorization, Value: "Bearer " + testToken(t, playerIDOf(i), time.Hour)},
			})
			if err := conn.SetWriteDeadline(time.Now().Add(ioTimeout)); err != nil {
				done <- err
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, claims.Marshal()); err != nil {
				done <- err
				return
			}
			if err := conn.SetReadDeadline(time.Now().Add(ioTimeout)); err != nil {
				done <- err
				return
			}
			_, data, err := conn.ReadMessage()
			if err != nil {
				done <- err
				return
			}
			frame, err := stomp.Unmarshal(data)
			if err != nil {
				done <- err
				return
			}
			if frame.Command != stomp.CommandConnected {
				done <- errUnexpectedFrame{frame}
				return
			}
			done <- nil
		}(i)
	}

	for range clients {
		if err := <-done; err != nil {
			t.Errorf("併發握手失敗：%v", err)
		}
	}
}

// errUnexpectedFrame 讓併發測試能把「收到什麼帧」帶回主 goroutine。
//
// 為什麼不直接在子 goroutine 裡 t.Fatalf：t.Fatal 只能在測試自己的
// goroutine 呼叫，在別的 goroutine 裡呼叫會讓測試行為未定義。
type errUnexpectedFrame struct{ frame *stomp.Frame }

func (e errUnexpectedFrame) Error() string {
	return "預期 CONNECTED，收到 " + string(e.frame.Command) + "：" + string(e.frame.Body)
}

func playerIDOf(i int) string {
	return "player-" + strconv.Itoa(i)
}

// isTimeout 判斷錯誤是不是 I/O 逾時。
//
// 用 errors.As 而不是型別斷言：逾時錯誤常常被包在別的錯誤裡
// （net.OpError 包 os.SyscallError…），直接斷言會漏掉。
func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// waitFor 輪詢等條件成立，逾時就讓測試失敗。
//
// 用輪詢而不是 time.Sleep 一個固定值：固定 sleep 在慢機器上會偶發失敗，
// 在快機器上又白白浪費時間。
func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(ioTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(msg)
}
