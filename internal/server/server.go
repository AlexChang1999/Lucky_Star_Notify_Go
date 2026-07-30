// Package server 組裝本服務的 HTTP 端點：
//
//	GET /ws                   WebSocket 升級 + STOMP 握手
//	GET /actuator/health      健康檢查（形狀對齊 Java 版，compose 設定可照抄）
//	GET /metrics              Prometheus 指標
//	GET /actuator/prometheus  同上，路徑對齊團隊 repo 的 prometheus.yml
//
// 另外，DEBUG_PUSH 開啟時多一個：
//
//	POST /debug/push          手動灌一則訊息（無鑑權，正式環境務必關閉）
//
// # 職責邊界
//
// 這個套件只負責「路由」與「握手」。握手成功之後整條連線就交給
// internal/hub 的 Session 接管——訂閱、推播、心跳、收尾都在那邊。
// 這樣切是為了讓 hub 完全不認識 HTTP，Phase 3 接 Kafka 時不必動它。
package server

import (
	"crypto/rand"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/AlexChang1999/lucky-star-notify-go/internal/auth"
	"github.com/AlexChang1999/lucky-star-notify-go/internal/config"
	"github.com/AlexChang1999/lucky-star-notify-go/internal/hub"
	"github.com/AlexChang1999/lucky-star-notify-go/internal/metrics"
	"github.com/AlexChang1999/lucky-star-notify-go/internal/stomp"
	"github.com/gorilla/websocket"
)

const (
	// handshakeTimeout 是「連線建立後多久內必須送出 CONNECT」。
	//
	// 沒有這個上限的話，只要開著 TCP 連線不說話，就能讓伺服器
	// 一直掛著一個 goroutine 與一份讀取緩衝——這是最省成本的
	// slowloris 型攻擊。
	handshakeTimeout = 10 * time.Second

	// writeTimeout 是單次寫出的上限。對方 TCP 視窗塞住時，
	// 沒有 deadline 的寫入會無限期阻塞。
	writeTimeout = 10 * time.Second

	// shutdownGrace 是關機時，等所有連線收到 ERROR 帧並乾淨關閉的上限。
	//
	// 3 秒是從 main 的 5 秒總預算裡切出來的：留 2 秒給 http.Server.Shutdown
	// 處理一般 HTTP 請求。超過就硬關——關不掉比慢一點更糟。
	shutdownGrace = 3 * time.Second
)

// maxMessageSize 是單一 WebSocket 訊息的上限。
//
// 這是 stomp.Limits 之外的**第二道**防線，而且更早生效：
// stomp 的上限要等資料進到解析器才擋，WebSocket 這層直接讓
// 超大訊息在讀取時就失敗。留 64 KiB 餘裕給 header 區。
//
// 是 var 而不是 const，因為 stomp.DefaultLimits 是變數（結構值無法當常數）。
var maxMessageSize = int64(stomp.DefaultLimits.MaxBodySize) + 64*1024

// stompSubprotocols 是 WebSocket 子協議協商用的清單，順序即優先序。
//
// @stomp/stompjs 會送 Sec-WebSocket-Protocol: v10.stomp, v11.stomp, v12.stomp。
// gorilla 會挑「伺服器清單裡第一個對方也支援的」，所以把 v12 放最前面。
// 只列 v12 會讓舊 client 談不成子協議；列三個但實際只講 1.2，
// 版本真正的把關是在 STOMP 的 accept-version（見 handshake）。
var stompSubprotocols = []string{"v12.stomp", "v11.stomp", "v10.stomp"}

// Server 持有所有連線共用的東西。
type Server struct {
	cfg      config.Config
	verifier *auth.Verifier
	metrics  *metrics.Metrics
	log      *slog.Logger
	upgrader websocket.Upgrader
	hub      *hub.Hub

	// mu 保護 conns 與 closed。
	//
	// 這裡用最單純的 Mutex 就好：只有「連線建立/斷開」兩種寫入，沒有
	// 高頻讀取。真正需要 RWMutex 的是 hub 的註冊表（廣播是高頻讀）。
	//
	// conns 追蹤的是**所有升級成功的連線**，包含還在握手、還沒交給 hub 的那些。
	// 那段空窗期 hub 不認識它們，關機時就只能靠這份清單把它們收掉。
	mu     sync.Mutex
	conns  map[*websocket.Conn]struct{}
	closed bool
}

// New 建立 Server。
func New(cfg config.Config, log *slog.Logger, m *metrics.Metrics) *Server {
	s := &Server{
		cfg:      cfg,
		verifier: auth.NewVerifier(cfg.JWTSecret),
		metrics:  m,
		log:      log,
		hub:      hub.New(log, m),
		conns:    make(map[*websocket.Conn]struct{}),
	}
	s.upgrader = websocket.Upgrader{
		Subprotocols: stompSubprotocols,
		// ⚠️ 一定要自己設 CheckOrigin。
		//
		// gorilla 的預設行為是「Origin 的 host 必須等於 Request 的 host」，
		// 也就是**同源才放行**。前端跑在 :5173、服務跑在 :8088 就是跨源，
		// 預設值會讓 upgrade 直接回 403——而瀏覽器 console 只會說
		// 「WebSocket connection failed」，指不到真因。
		// Java 版是 setAllowedOriginPatterns("*")，這裡用設定檔對齊。
		CheckOrigin: func(r *http.Request) bool {
			origin := r.Header.Get("Origin")
			if s.cfg.AllowsOrigin(origin) {
				return true
			}
			s.log.Warn("拒絕跨來源的 WebSocket 連線", "origin", origin)
			return false
		},
	}
	return s
}

// Hub 回傳連線註冊表，供 Kafka consumer 推播。
//
// 為什麼 hub 由 server 持有、而不是 main 建好再傳進來：hub 的生命週期
// 跟連線綁在一起，而連線是 server 收下來的。main 只需要能拿到它去接
// consumer——這也是為什麼回傳具體型別而不是包一層介面：介面該由
// **使用端**定義（見 consumer.Pusher），不是在這裡預先猜。
func (s *Server) Hub() *hub.Hub {
	return s.hub
}

// Handler 回傳整個服務的路由。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// "GET /path" 的寫法是 Go 1.22 起 net/http 內建的方法比對，
	// 不需要第三方 router。（GET 樣式同時也會匹配 HEAD。）
	mux.HandleFunc("GET /ws", s.handleWS)
	mux.HandleFunc("GET /actuator/health", handleHealth)

	// 同一個 handler 註冊兩個路徑：
	//   /metrics             Go/Prometheus 生態的慣例
	//   /actuator/prometheus 團隊 repo 的 prometheus.yml 整個 job 寫死這個路徑
	// 註冊兩次，團隊的 Prometheus 只要加一個 target 就抓得到，不必另開 job。
	metricsHandler := s.metrics.Handler()
	mux.Handle("GET /metrics", metricsHandler)
	mux.Handle("GET /actuator/prometheus", metricsHandler)

	// 只在旗標打開時才註冊——關掉時是「這個路徑不存在（404）」，
	// 而不是「存在但回 403」。少洩漏一個資訊給掃描器。
	if s.cfg.DebugPush {
		s.log.Warn("POST /debug/push 已啟用：這個端點沒有鑑權，正式環境請設 DEBUG_PUSH=false")
		mux.HandleFunc("POST /debug/push", s.handleDebugPush)
	}

	return mux
}

// Close 關閉所有進行中的 WebSocket 連線，並拒絕之後的新連線。
//
// # 為什麼需要這個方法
//
// http.Server.Shutdown **不會**處理被 Hijack 的連線，而 WebSocket 升級
// 正是靠 Hijack 拿走底層 TCP 連線。所以 Shutdown 會「立刻回報完成」，
// 而那些 WebSocket goroutine 還活著。優雅關閉必須自己收這一段。
//
// # 兩段式關閉
//
//  1. hub.Close：對**已握手**的連線送 STOMP ERROR 帧，等它們寫完再關。
//     禮貌斷線讓 client 收到 1000 而不是 1006，不會誤判成網路問題。
//  2. 剩下的連線（還在握手、還沒交給 hub 的）只能硬關。
//     這裡是 main 的 goroutine，而那些連線正卡在自己的 ReadMessage，
//     從這裡 WriteMessage 就是第二個 writer（AGENTS.md §2.5）。
//     conn.Close() 則是 gorilla 明確支援的併發操作，也是喚醒 reader 的標準手段。
//
// 第 2 步會重複關到已經被 hub 關掉的連線——那是無害的（回一個被忽略的 error），
// 換來的是「絕對不會漏掉任何一條」。
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	conns := make([]*websocket.Conn, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	s.mu.Unlock()

	s.hub.Close(shutdownGrace)

	for _, conn := range conns {
		_ = conn.Close()
	}
	s.log.Info("已關閉所有 WebSocket 連線", "count", len(conns))
	return nil
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		// Upgrade 失敗時 gorilla 已經自己回過 HTTP 錯誤了，這裡不能再寫 w。
		s.log.Debug("WebSocket upgrade 失敗", "remote", r.RemoteAddr, "err", err)
		return
	}
	// 關閉的 error 刻意忽略：連線已經要結束了，回報也無事可做。
	// 寫成 _ = 而不是裸呼叫，是為了讓「刻意忽略」是明示的（errcheck 會抓裸呼叫）。
	defer func() { _ = conn.Close() }()

	if !s.track(conn) {
		s.log.Debug("服務正在關閉，拒絕新連線", "remote", r.RemoteAddr)
		return
	}
	defer s.untrack(conn)

	s.serve(conn, r)
}

// track 登記連線；服務已在關閉中時回傳 false。
func (s *Server) track(conn *websocket.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.conns[conn] = struct{}{}
	s.metrics.WSConnections.Inc()
	return true
}

func (s *Server) untrack(conn *websocket.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.conns[conn]; ok {
		delete(s.conns, conn)
		s.metrics.WSConnections.Dec()
	}
}

// serve 跑完握手，成功後把連線交給 hub 接管。
func (s *Server) serve(conn *websocket.Conn, r *http.Request) {
	conn.SetReadLimit(maxMessageSize)

	frame, ok := s.readConnectFrame(conn, r)
	if !ok {
		return
	}

	sessionID := rand.Text()
	result := handshake(s.verifier, frame, sessionID)
	if !result.OK {
		// 失敗原因只留在伺服器端。對外的 ERROR 帧刻意含糊（見 handshake）。
		s.log.Info("STOMP 握手失敗",
			"remote", r.RemoteAddr,
			"reason", result.Reply.Headers.Get(stomp.HeaderMessage),
			"detail", string(result.Reply.Body))
		s.reject(conn, result.Reply)
		return
	}

	s.log.Info("STOMP 連線建立",
		"playerID", result.PlayerID,
		"sessionID", sessionID,
		"remote", r.RemoteAddr,
		"subprotocol", conn.Subprotocol(),
		"heartbeatSend", result.Heartbeat.Send,
		"heartbeatRecv", result.Heartbeat.Recv)

	// CONNECTED 交給 hub 用 write goroutine 送出，不在這裡直接寫——
	// 這樣「client 收到 CONNECTED」就保證「session 已登記進 hub」，
	// 中間不會有一段推播與關機通知都漏掉的空窗期。
	//
	// Serve 阻塞到連線結束；目前這條 goroutine 就變成該連線的 read goroutine。
	s.hub.Serve(hub.SessionConfig{
		Conn:         conn,
		ID:           sessionID,
		PlayerID:     result.PlayerID,
		Greeting:     result.Reply,
		SendInterval: result.Heartbeat.Send,
		RecvInterval: result.Heartbeat.Recv,
	})
}

// readConnectFrame 讀到第一個真正的帧為止。
//
// 帧與帧之間（含 CONNECT 之前）允許出現心跳，stomp.Unmarshal 會用
// ErrHeartbeat 回報，這裡略過它繼續讀——但**不重設 deadline**，
// 所以持續送心跳卻不送 CONNECT 一樣會在 handshakeTimeout 之後被踢掉。
func (s *Server) readConnectFrame(conn *websocket.Conn, r *http.Request) (*stomp.Frame, bool) {
	if err := conn.SetReadDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		s.log.Debug("設定握手逾時失敗", "remote", r.RemoteAddr, "err", err)
		return nil, false
	}

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			// 對方在握手前就關掉是常態（例如瀏覽器重新整理），用 Debug 不用 Warn。
			s.log.Debug("等待 CONNECT 時讀取失敗", "remote", r.RemoteAddr, "err", err)
			return nil, false
		}

		frame, err := stomp.Unmarshal(data)
		if errors.Is(err, stomp.ErrHeartbeat) {
			continue
		}
		if err != nil {
			s.log.Info("收到畸形帧", "remote", r.RemoteAddr, "err", err)
			s.reject(conn, errorFrame("Malformed frame", err.Error()))
			return nil, false
		}
		return frame, true
	}
}

// reject 送出 ERROR 帧後關閉連線。
//
// 規格明訂：伺服器送出 ERROR 之後**必須**關閉連線。先送 WebSocket 的
// Close 控制帧再關 TCP，client 才會收到 1000（正常關閉）而不是
// 1006（異常關閉）——後者會讓前端誤判成網路問題並進入重連迴圈。
func (s *Server) reject(conn *websocket.Conn, frame *stomp.Frame) {
	if err := writeFrame(conn, frame); err != nil {
		return
	}
	_ = conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(writeTimeout),
	)
}

// writeFrame 把帧當成一則 WebSocket **文字**訊息送出。
//
// ⚠️ 一定要 TextMessage。STOMP over WebSocket 的慣例是文字帧，
// 送 BinaryMessage 時 @stomp/stompjs 會直接忽略——沒有錯誤訊息，
// 只有「怎麼沒反應」。
func writeFrame(conn *websocket.Conn, frame *stomp.Frame) error {
	if err := conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, frame.Marshal())
}

// handleHealth 回傳與 Java 版同形狀的健康檢查結果。
//
// 形狀刻意對齊 Spring Actuator 的 {"status":"UP"}，這樣團隊 repo 的
// compose healthcheck 設定可以整段照抄，只改 port。
func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"UP"}`))
}
