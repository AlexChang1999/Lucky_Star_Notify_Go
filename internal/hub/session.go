package hub

import (
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AlexChang1999/lucky-star-notify-go/internal/stomp"
	"github.com/gorilla/websocket"
)

const (
	// outboundBuffer 是每條連線的送出佇列長度。
	//
	// 64 是一個「容得下突發、但容不下持續落後」的數字：排行榜廣播這種
	// 尖峰一次也就幾則，緩衝住不會誤殺；但一個真的讀不動的 client
	// 會在 64 則之內就被判定為慢速而踢掉，不會囤積記憶體。
	//
	// 緩衝設 0（無緩衝）會讓每一次扇出都直接失敗；設很大（例如 10000）
	// 則是把「慢速客戶端」的問題延後而不是解決——記憶體先爆。
	outboundBuffer = 64

	// maxSubscriptions 是單一連線的訂閱數上限。
	//
	// 前端實際只會訂 3 個 destination。設上限是防止惡意 client 用
	// 「不斷 SUBSCRIBE 不同 destination」把 hub 的 map 撐爆——
	// 那是一個不需要任何頻寬的記憶體耗盡攻擊。
	maxSubscriptions = 32

	// writeTimeout 是單次寫出的上限。對方 TCP 視窗塞住時，
	// 沒有 deadline 的寫入會無限期阻塞，write goroutine 就再也回不來。
	writeTimeout = 10 * time.Second

	// readGraceFactor 是讀取逾時相對於心跳間隔的寬容倍數。
	//
	// 談好 10 秒卻在第 10.001 秒斷線是不合理的——網路抖動、GC 暫停都會
	// 讓心跳晚到。規格建議「容許誤差」，業界慣例是 2 倍。
	readGraceFactor = 2

	// drainTimeout 是連線收尾時，等 write goroutine 把佇列寫完的上限。
	//
	// 比 writeTimeout 短很多是刻意的：收尾時對方常常已經不在了，
	// 沒必要為了送出一個 ERROR 帧而卡住呼叫端 10 秒。
	drainTimeout = 2 * time.Second

	// contentTypeJSON 是 MESSAGE 帧的 content-type。
	// 三個 Kafka 事件的 payload 都是 JSON，與 Java 版一致。
	contentTypeJSON = "application/json"
)

// heartbeatPayload 是 STOMP 心跳的內容：單一個 EOL 位元組。
//
// 是 var 而不是 const，因為它是 []byte。每次心跳都共用同一個 slice——
// 這是安全的，因為 gorilla 只會讀它，不會改。
var heartbeatPayload = []byte("\n")

// SessionConfig 是建立一條 Session 所需的參數。
//
// 用一個 struct 而不是 5 個位置參數，是因為其中三個都是字串／時間，
// 呼叫端寫錯順序時編譯器不會發現。
type SessionConfig struct {
	// Conn 是已完成 WebSocket 升級與 STOMP 握手的連線。
	Conn *websocket.Conn
	// ID 是這條連線的 session id（就是 CONNECTED 回給 client 的那個）。
	ID string
	// PlayerID 是 JWT 的 subject，私人推播的索引鍵。
	PlayerID string
	// Greeting 是連線建立後要送出的第一個帧（就是 CONNECTED）。
	//
	// 為什麼不讓呼叫端自己先寫出去再交過來：那樣「client 收到 CONNECTED」
	// 就不保證「session 已經登記進 hub」了——中間有一段空窗期，
	// 此時的推播與關機通知都會漏掉這條連線。交給 write goroutine 送，
	// 順序就變成 add → 送 CONNECTED，client 收到時註冊一定已完成。
	//
	// 附帶好處：這條連線從頭到尾只有 write goroutine 一個 writer，
	// 不必再去論證「握手那一次寫入為什麼是安全的」。
	Greeting *stomp.Frame
	// SendInterval 是伺服器送心跳的間隔；0 = 不送。
	SendInterval time.Duration
	// RecvInterval 是期待 client 送心跳的間隔；0 = 讀取不設逾時。
	RecvInterval time.Duration
}

// Session 是一條**已完成握手**的連線，以及它的併發模型。
//
// # 兩個 goroutine 的分工（AGENTS.md §2.5）
//
// gorilla/websocket 同時只允許一個 reader 與一個 writer，所以：
//
//   - read goroutine（就是 HTTP handler 那一條）：ReadMessage 迴圈，解析帧
//   - write goroutine：從 outbound 取資料寫出，並用同一個 select 處理心跳 ticker
//
// **所有**寫入都必須經過 outbound，任何其他 goroutine 都不可以直接呼叫
// WriteMessage。心跳也走 write goroutine 的 select，不另開 goroutine。
//
// # 為什麼心跳 ticker 要放在 write goroutine 的 select 裡
//
// 因為它本身就是一種「寫出」。另開一條 goroutine 定時寫，就是第二個 writer，
// 立刻違反上面的規則——而且 -race 不一定每次都抓得到，會變成偶發的資料損毀。
type Session struct {
	id       string
	playerID string
	conn     *websocket.Conn
	hub      *Hub
	log      *slog.Logger

	sendInterval time.Duration
	recvInterval time.Duration

	// outbound 是唯一的寫入通道。**永遠不 close**——因為送入端有很多個
	// （每個扇出的 goroutine 都會送），而對已關閉的 channel 送資料會 panic。
	// 「這條連線結束了」用 done 表達，不用關 channel 表達。
	outbound chan []byte

	// done 在連線終止時被 close，是所有 goroutine 的退出訊號。
	done      chan struct{}
	closeOnce sync.Once

	// draining 在「優雅收尾」時被 close：write goroutine 會把 outbound
	// 裡剩下的內容寫完、送出 WebSocket 關閉帧，然後才結束。
	//
	// 與 done 的差別：done 是「立刻停」，draining 是「寫完再停」。
	// 送 ERROR 帧給對方之後如果直接 close(done)，那個 ERROR 有很高的
	// 機率根本沒送出去——這正是 Phase 1 做不到「禮貌斷線」的原因。
	draining     chan struct{}
	drainingOnce sync.Once

	// writerDone 在 write goroutine 真的結束時被 close，供收尾時等待。
	writerDone chan struct{}

	// msgSeq 產生 message-id 的流水號。用 atomic 是因為扇出可能
	// 同時來自多個 goroutine（三個 Kafka consumer 各一條）。
	msgSeq atomic.Uint64

	// mu 保護 subs。讀多寫少：每次扇出都要讀，只有 SUBSCRIBE/UNSUBSCRIBE 才寫。
	mu sync.RWMutex
	// subs 是 subscription id → destination。
	//
	// ⚠️ 方向是 id → dest 而不是反過來，有兩個理由：
	//  1. UNSUBSCRIBE 帶的是 id，這個方向查得到；
	//  2. MESSAGE 帧必須帶 subscription header（＝client 自己的 id），
	//     漏了的話 @stomp/stompjs 會**靜默丟棄**（AGENTS.md §2.3）。
	//
	// 規格允許同一條連線用不同 id 訂同一個 destination，所以這是多對一。
	subs map[string]string
}

func newSession(h *Hub, cfg SessionConfig) *Session {
	return &Session{
		id:           cfg.ID,
		playerID:     cfg.PlayerID,
		conn:         cfg.Conn,
		hub:          h,
		log:          h.log.With("playerID", cfg.PlayerID, "sessionID", cfg.ID),
		sendInterval: cfg.SendInterval,
		recvInterval: cfg.RecvInterval,
		outbound:     make(chan []byte, outboundBuffer),
		done:         make(chan struct{}),
		draining:     make(chan struct{}),
		writerDone:   make(chan struct{}),
		subs:         make(map[string]string),
	}
}

// PlayerID 回傳這條連線綁定的玩家。
func (s *Session) PlayerID() string { return s.playerID }

// ID 回傳 session id。
func (s *Session) ID() string { return s.id }

// enqueue 把已序列化的帧送進送出佇列，**保證不阻塞**。
//
// # 這是整個服務最關鍵的六行
//
// 扇出迴圈會依序碰到每一條連線。只要有任何一條寫成阻塞的
// `s.outbound <- payload`，一個慢速或當掉的 client 就會卡住整個迴圈，
// 拖垮**所有其他人**（AGENTS.md §2.6）。所以這裡用 select + default：
// 佇列滿了就代表這個 client 消費不及，直接踢掉它保護其他人。
// 這叫 fail-fast 背壓，是即時系統的標準做法。
//
// # 為什麼這裡不呼叫 hub.remove
//
// 呼叫端（Hub.Broadcast）正持有 hub.mu 的讀鎖，在這裡去搶寫鎖會死鎖。
// close() 只動這條 session 自己的狀態；從 hub 註冊表移除是 read goroutine
// 被叫醒之後、在 Hub.Serve 的收尾階段做的。
func (s *Session) enqueue(payload []byte) {
	select {
	case <-s.done:
		return // 連線已結束，丟掉即可
	default:
	}

	select {
	case s.outbound <- payload:
	default:
		s.hub.metrics.OutboundDropped.Inc()
		s.log.Warn("送出佇列已滿，斷開慢速客戶端", "buffer", outboundBuffer)
		s.close()
	}
}

// deliver 把一則訊息送給這條連線上**所有訂閱了 dest 的訂閱**，回傳送出則數。
//
// 沒有訂閱就送出 0 則——這是對的：STOMP 的 client 只認得自己訂過的
// subscription id，硬推一個它沒訂的 destination 只會被丟掉。
//
// 在 s.mu 的讀鎖裡直接 enqueue 是安全的：enqueue 不會碰 s.mu，
// 而且它保證不阻塞，所以鎖不會被長時間持有。
func (s *Session) deliver(dest string, body []byte) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sent := 0
	for id, subscribed := range s.subs {
		if subscribed != dest {
			continue
		}
		s.enqueue(s.messageFrame(id, dest, body).Marshal())
		sent++
	}
	return sent
}

// messageFrame 組出一個 MESSAGE 帧。
//
// ⚠️ subscription 與 message-id 兩個 header **都是必備**：
// @stomp/stompjs 拿 subscription 去比對自己的訂閱表，對不上就靜默丟棄；
// 沒有錯誤訊息，症狀只有「怎麼沒反應」（AGENTS.md §2.3）。
// content-length 由 Marshal 自動補上。
func (s *Session) messageFrame(subID, dest string, body []byte) *stomp.Frame {
	return &stomp.Frame{
		Command: stomp.CommandMessage,
		Headers: stomp.Headers{
			{Name: stomp.HeaderSubscription, Value: subID},
			{Name: stomp.HeaderMessageID, Value: s.nextMessageID()},
			{Name: stomp.HeaderDestination, Value: dest},
			{Name: stomp.HeaderContentType, Value: contentTypeJSON},
		},
		Body: body,
	}
}

// nextMessageID 產生全服務唯一的 message-id。
//
// session id 本身已經是隨機且唯一的，接一個 per-session 流水號就夠了，
// 不需要 UUID——後者每次都要跟作業系統要亂數，在扇出熱路徑上是浪費。
func (s *Session) nextMessageID() string {
	return s.id + "-" + strconv.FormatUint(s.msgSeq.Add(1), 10)
}

// readLoop 是 read goroutine 的主體，回傳即代表連線該結束了。
func (s *Session) readLoop() {
	for {
		// 每一輪都重設讀取逾時。心跳與一般帧都算「對方還活著」，
		// 所以放在迴圈頂端統一處理，不必在每個 case 各寫一次。
		if !s.extendReadDeadline() {
			return
		}

		_, data, err := s.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				s.log.Info("連線非正常中斷", "err", err)
			} else {
				s.log.Debug("連線結束", "err", err)
			}
			return
		}

		frame, err := stomp.Unmarshal(data)
		if errors.Is(err, stomp.ErrHeartbeat) {
			continue
		}
		if err != nil {
			s.fail("Malformed frame", err.Error())
			return
		}

		if !s.handle(frame) {
			return
		}
	}
}

// extendReadDeadline 依協商的心跳間隔設定下一次讀取的期限。
//
// recvInterval 為 0（沒談成心跳）時設定零值 deadline，意思是「不逾時」。
// 這與 CONNECTED 宣告的內容必須一致：既然沒要求對方送心跳，
// 就不能反過來因為對方沉默而斷線。
func (s *Session) extendReadDeadline() bool {
	var deadline time.Time
	if s.recvInterval > 0 {
		deadline = time.Now().Add(s.recvInterval * readGraceFactor)
	}
	if err := s.conn.SetReadDeadline(deadline); err != nil {
		s.log.Debug("設定讀取逾時失敗", "err", err)
		return false
	}
	return true
}

// handle 處理一個收到的帧，回傳 false 代表連線要結束。
func (s *Session) handle(frame *stomp.Frame) bool {
	switch frame.Command {
	case stomp.CommandSubscribe:
		return s.handleSubscribe(frame)

	case stomp.CommandUnsubscribe:
		return s.handleUnsubscribe(frame)

	case stomp.CommandDisconnect:
		// 規格的收尾握手：client 送 DISCONNECT（可帶 receipt），
		// server 回 RECEIPT 後關閉。有了 receipt，client 才能確定
		// 「我送出去的東西伺服器都收到了」再安心關連線。
		s.replyReceipt(frame)
		s.log.Debug("對方主動 DISCONNECT")
		s.startDraining()
		return false

	case stomp.CommandConnect, stomp.CommandStomp:
		s.fail("Already connected", "一條連線只能握手一次")
		return false

	default:
		// SEND / ACK / NACK / BEGIN / COMMIT / ABORT：本服務不支援。
		//
		// 選擇**靜默忽略**而不是回 ERROR 斷線，是為了對齊 Java 版：
		// Spring 的 simple broker 收到沒有 @MessageMapping 的 SEND 時
		// 只會記一行 log，不會斷線。這裡回 ERROR 的話，Go 版就會拒絕
		// Java 版接受的 client，Phase 5 的契約測試會對不上。
		//
		// 「格式錯誤」與「不支援」是兩件事：前者在 stomp.Unmarshal 就擋掉了。
		s.log.Debug("忽略不支援的指令", "command", frame.Command)
		s.replyReceipt(frame)
		return true
	}
}

func (s *Session) handleSubscribe(frame *stomp.Frame) bool {
	id := frame.Headers.Get(stomp.HeaderID)
	dest := frame.Headers.Get(stomp.HeaderDestination)
	if id == "" || dest == "" {
		s.fail("Invalid SUBSCRIBE", "SUBSCRIBE 必須同時帶 id 與 destination")
		return false
	}

	// 刻意**不**檢查 ack header。規格的 client / client-individual 模式需要
	// ACK/NACK 與未確認訊息的重送，那是訊息佇列的職責；本服務是 best-effort
	// 推播（與 Java 版一致），一律當 auto 處理。
	//
	// 也刻意**不**限制 destination 的白名單：Spring 的 simple broker 對
	// /topic /queue 底下任何路徑都放行，加白名單會讓 Go 版比 Java 版嚴格。
	// 記憶體風險改用 maxSubscriptions 擋，那才是真正的攻擊面。

	s.mu.Lock()
	if _, exists := s.subs[id]; exists {
		s.mu.Unlock()
		// 規格：同一條連線上的 subscription id 必須唯一。允許重複的話，
		// UNSUBSCRIBE 就不知道該退訂哪一個。
		s.fail("Duplicate subscription id", "這條連線已經有 id 為 "+id+" 的訂閱")
		return false
	}
	if len(s.subs) >= maxSubscriptions {
		s.mu.Unlock()
		s.fail("Too many subscriptions",
			"單一連線最多 "+strconv.Itoa(maxSubscriptions)+" 個訂閱")
		return false
	}
	s.subs[id] = dest
	s.mu.Unlock()

	s.hub.subscribe(dest, s)
	s.log.Debug("訂閱成功", "id", id, "destination", dest)
	s.replyReceipt(frame)
	return true
}

func (s *Session) handleUnsubscribe(frame *stomp.Frame) bool {
	id := frame.Headers.Get(stomp.HeaderID)
	if id == "" {
		s.fail("Invalid UNSUBSCRIBE", "UNSUBSCRIBE 必須帶 id")
		return false
	}

	s.mu.Lock()
	dest, known := s.subs[id]
	if known {
		delete(s.subs, id)
	}
	// 同一個 destination 可能被訂了不只一次；還有別的訂閱在的話，
	// 就不能從 hub 的索引拿掉，否則其他訂閱會跟著收不到。
	stillSubscribed := known && s.hasDestinationLocked(dest)
	s.mu.Unlock()

	switch {
	case !known:
		// 退訂一個不存在的 id：記一行就好，不斷線。
		// 規格沒有明訂這種情況，而 Spring 是忽略的——重複退訂在
		// 前端 React 的 cleanup 裡很常見（StrictMode 會跑兩次）。
		s.log.Debug("退訂了不存在的 id", "id", id)
	case !stillSubscribed:
		s.hub.unsubscribe(dest, s)
		s.log.Debug("退訂成功", "id", id, "destination", dest)
	default:
		s.log.Debug("退訂成功，但同一個 destination 還有其他訂閱", "id", id, "destination", dest)
	}

	s.replyReceipt(frame)
	return true
}

// hasDestinationLocked 回報 subs 裡是否還有指向 dest 的訂閱。呼叫前必須持有 s.mu。
func (s *Session) hasDestinationLocked(dest string) bool {
	for _, subscribed := range s.subs {
		if subscribed == dest {
			return true
		}
	}
	return false
}

// replyReceipt 在 client 要求時回一個 RECEIPT 帧。
//
// 規格：除了 CONNECT 之外，任何 client 帧都可以帶 receipt header，
// 伺服器處理完之後要回 RECEIPT。沒帶就什麼都不做。
func (s *Session) replyReceipt(frame *stomp.Frame) {
	receipt := frame.Headers.Get(stomp.HeaderReceipt)
	if receipt == "" {
		return
	}
	s.enqueue((&stomp.Frame{
		Command: stomp.CommandReceipt,
		Headers: stomp.Headers{{Name: stomp.HeaderReceiptID, Value: receipt}},
	}).Marshal())
}

// fail 送出 ERROR 帧並要求 write goroutine 寫完後收工。
//
// 規格明訂伺服器送出 ERROR 之後**必須**關閉連線。用 startDraining 而不是
// 直接 close，是為了確保那個 ERROR 真的寫得出去——否則對方只會看到
// 一個沒有理由的斷線。
func (s *Session) fail(message, detail string) {
	s.log.Info("協議錯誤，送出 ERROR 並斷線", "reason", message, "detail", detail)
	s.enqueue(stomp.ErrorFrame(message, detail).Marshal())
	s.startDraining()
}

// writeLoop 是 write goroutine 的主體：這條連線**唯一**的寫入者。
func (s *Session) writeLoop() {
	defer close(s.writerDone)
	// 寫入端掛掉（對方斷線、逾時）時要叫醒還卡在 ReadMessage 的 read goroutine，
	// 否則它會一直等到讀取逾時才發現，最糟情況是沒談心跳時永遠等下去。
	defer s.close()

	var ticker *time.Ticker
	var tick <-chan time.Time
	if s.sendInterval > 0 {
		ticker = time.NewTicker(s.sendInterval)
		defer ticker.Stop()
		tick = ticker.C
	}

	write := func(payload []byte) bool {
		if err := s.writeMessage(websocket.TextMessage, payload); err != nil {
			s.log.Debug("寫出失敗，結束連線", "err", err)
			return false
		}
		// 規格：**任何**送出的帧都算一次心跳。剛寫過就把 ticker 往後推，
		// 有流量時就不會再多送無謂的心跳。
		if ticker != nil {
			ticker.Reset(s.sendInterval)
		}
		return true
	}

	for {
		select {
		case <-s.done:
			return

		case <-s.draining:
			// 優雅收尾：把佇列裡剩下的（通常是一個 ERROR 或 RECEIPT）寫完。
			for {
				select {
				case payload := <-s.outbound:
					if !write(payload) {
						return
					}
					continue
				default:
				}
				break
			}
			// 先送 WebSocket 的 Close 控制帧再關 TCP，client 才會收到
			// 1000（正常關閉）而不是 1006（異常關閉）——後者會讓前端
			// 誤判成網路問題並進入重連迴圈。
			_ = s.conn.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
				time.Now().Add(writeTimeout),
			)
			return

		case payload := <-s.outbound:
			if !write(payload) {
				return
			}

		case <-tick:
			if !write(heartbeatPayload) {
				return
			}
		}
	}
}

// writeMessage 寫出一則 WebSocket **文字**訊息。
//
// ⚠️ 一定要 TextMessage。STOMP over WebSocket 的慣例是文字帧，
// 送 BinaryMessage 時 @stomp/stompjs 會直接忽略——沒有錯誤訊息，
// 只有「怎麼沒反應」（AGENTS.md §2.12）。
func (s *Session) writeMessage(msgType int, payload []byte) error {
	if err := s.conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return err
	}
	return s.conn.WriteMessage(msgType, payload)
}

// startDraining 要求 write goroutine「把佇列寫完就收工」。可重複呼叫。
func (s *Session) startDraining() {
	s.drainingOnce.Do(func() { close(s.draining) })
}

// close 立刻終止連線。可重複呼叫、可從任何 goroutine 呼叫。
//
// conn.Close() 是喚醒阻塞中 ReadMessage 的標準手段，gorilla 明確支援
// 併發呼叫它（相對地，WriteMessage 就**不能**從別的 goroutine 呼叫）。
func (s *Session) close() {
	s.closeOnce.Do(func() {
		close(s.done)
		_ = s.conn.Close()
	})
}

// finish 收尾：等 write goroutine 把該寫的寫完（有上限），然後強制關閉。
func (s *Session) finish() {
	s.startDraining()

	timer := time.NewTimer(drainTimeout)
	defer timer.Stop()
	select {
	case <-s.writerDone:
	case <-timer.C:
		s.log.Debug("等待送出佇列排空逾時，直接關閉")
	}
	s.close()
}
