// Package hub 是連線註冊表與扇出中心。
//
// # 職責
//
// 記住「誰連著、誰訂了什麼」，然後把一則訊息送給該收到的每一條連線。
// 它不認識 Kafka，也不認識 HTTP——上游只要呼叫 Broadcast 或 PushToPlayer。
// 這個切法讓 Phase 3 接 Kafka 時完全不必動這個套件。
//
// # 為什麼用 sync.RWMutex 而不是「中央 hub goroutine + channel」
//
// gorilla 官方 chat 範例用的是中央 goroutine：註冊表由單一 goroutine 獨佔，
// 所有操作走 channel，完全沒有鎖。心智模型很乾淨，但**所有廣播都被序列化
// 通過同一個 goroutine**，連線數一多就是瓶頸。
//
// RWMutex 模型則是：廣播時 RLock，多個廣播可以並行讀取註冊表；
// 只有連線建立／斷開才需要 Lock。推播服務正是典型的讀多寫少場景，
// 所以這裡選 RWMutex。（決策理由寫進 ADR-002。）
//
// # 鎖的順序（避免死鎖的唯一規則）
//
// 永遠是 hub.mu → session.mu，不可以反過來。所以：
//   - Broadcast 持有 hub.mu.RLock 時可以呼叫 session.deliver（會拿 session.mu）
//   - Session.handleSubscribe 必須先放掉 session.mu，才能呼叫 hub.subscribe
package hub

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/AlexChang1999/lucky-star-notify-go/internal/metrics"
	"github.com/AlexChang1999/lucky-star-notify-go/internal/stomp"
)

// PrivateDestination 是玩家私人頻道的 destination。
//
// Spring 的做法是把它內部翻譯成 /user/{sessionId}/queue/notifications，
// 繞一圈是為了 Spring 自己的訊息匯流排。Go 版直接用 playerID 索引 session，
// **對 client 完全等價而且更快**——client 看到的 destination 才是契約。
const PrivateDestination = "/user/queue/notifications"

// Hub 持有所有連線與訂閱的索引。
type Hub struct {
	log     *slog.Logger
	metrics *metrics.Metrics

	// mu 保護底下三個 map 與 closed。
	mu sync.RWMutex
	// sessions 是所有存活的連線，關機時要逐一通知。
	sessions map[*Session]struct{}
	// subs 是 destination → 訂閱它的連線集合，廣播用。
	subs map[string]map[*Session]struct{}
	// players 是 playerID → 該玩家的連線集合，私人推播用。
	//
	// 是集合而不是單一 session：同一個玩家可能開兩個分頁，
	// 兩邊都該收到。Java 版的 convertAndSendToUser 也是這個行為。
	players map[string]map[*Session]struct{}
	closed  bool
}

// New 建立一個空的 Hub。
func New(log *slog.Logger, m *metrics.Metrics) *Hub {
	return &Hub{
		log:      log,
		metrics:  m,
		sessions: make(map[*Session]struct{}),
		subs:     make(map[string]map[*Session]struct{}),
		players:  make(map[string]map[*Session]struct{}),
	}
}

// Serve 接管一條**已握手完成**的連線，直到它結束才回傳。
//
// 呼叫端（HTTP handler）那一條 goroutine 就是 read goroutine；
// write goroutine 在這裡另外開。所以一條連線剛好兩個 goroutine——
// 這是整個服務的 goroutine 帳的基礎：10,000 連線 = 20,000 goroutine
// ≈ 40 MB 初始堆疊。
func (h *Hub) Serve(cfg SessionConfig) {
	session := newSession(h, cfg)
	if !h.add(session) {
		// 服務正在關閉，不要再收新連線——收了也只是馬上又要關掉，
		// 而且會漏掉 Close 的那一輪通知。
		session.close()
		return
	}

	// 先登記再送 CONNECTED（見 SessionConfig.Greeting 的說明）。
	// 這時 write goroutine 還沒起跑，但 outbound 有緩衝，放得進去。
	if cfg.Greeting != nil {
		session.enqueue(cfg.Greeting.Marshal())
	}

	go session.writeLoop()
	session.readLoop() // 阻塞到連線結束

	h.remove(session)
	session.finish()
}

// Broadcast 把 body 送給所有訂閱 dest 的連線，回傳實際送出的 MESSAGE 則數。
//
// 回傳「則數」而不是「連線數」，是因為同一條連線可能用兩個 id 訂了同一個
// destination，那就會收到兩則——這是規格的行為，計數要誠實反映。
func (h *Hub) Broadcast(dest string, body []byte) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	sent := 0
	for session := range h.subs[dest] {
		sent += session.deliver(dest, body)
	}
	return sent
}

// PushToPlayer 把 body 送給某個玩家在 dest 上的訂閱，回傳送出的則數。
//
// 玩家沒連線、或連著但沒訂閱 dest 時回傳 0。推播是 best-effort：
// 不存、不重送、不報錯——與 Java 版一致。
func (h *Hub) PushToPlayer(playerID, dest string, body []byte) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	sent := 0
	for session := range h.players[playerID] {
		sent += session.deliver(dest, body)
	}
	return sent
}

// SessionCount 回傳當前的連線數（測試與除錯用）。
func (h *Hub) SessionCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.sessions)
}

// Close 對所有連線送出 STOMP ERROR 帧，等它們寫完後關閉，回傳關掉的連線數。
//
// # 為什麼要送 ERROR 而不是直接關
//
// 直接關的話 client 收到的是 1006（異常關閉），會判定成網路問題並
// 進入指數退避的重連迴圈。送了 ERROR + 正常關閉帧，client 就知道
// 是伺服器主動收工，可以立刻重連到新的實例。
//
// Phase 1 做不到這件事，因為當時沒有 write goroutine——從關機那條
// goroutine 直接 WriteMessage 會變成第二個 writer（AGENTS.md §2.5）。
// 有了 outbound channel 之後就只是「送進佇列」，安全。
func (h *Hub) Close(timeout time.Duration) int {
	h.mu.Lock()
	h.closed = true
	sessions := make([]*Session, 0, len(h.sessions))
	for session := range h.sessions {
		sessions = append(sessions, session)
	}
	h.mu.Unlock()

	farewell := stomp.ErrorFrame("Server shutting down", "服務正在關閉，請稍後重新連線").Marshal()
	for _, session := range sessions {
		session.enqueue(farewell)
		session.startDraining()
	}

	// 用 context 而不是 time.After：time.After 的 channel 只會收到**一次**值，
	// 在迴圈裡 select 它的話，第一個逾時的 session 就把訊號吃掉了，
	// 後面每一個都會退化成無上限等待。ctx.Done() 關閉之後永遠是可讀的。
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for _, session := range sessions {
		select {
		case <-session.writerDone:
		case <-ctx.Done():
		}
	}

	// 逾時沒收乾淨的，到這裡一律硬關。
	for _, session := range sessions {
		session.close()
	}
	if len(sessions) > 0 {
		h.log.Info("已通知並關閉所有 STOMP 連線", "count", len(sessions))
	}
	return len(sessions)
}

// add 登記一條新連線；Hub 已關閉時回傳 false。
func (h *Hub) add(s *Session) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false
	}
	h.sessions[s] = struct{}{}
	addTo(h.players, s.playerID, s)
	return true
}

// remove 把連線從所有索引移除。
func (h *Hub) remove(s *Session) {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.sessions, s)
	removeFrom(h.players, s.playerID, s)

	// 鎖的順序是 hub.mu → session.mu（見套件說明），這裡符合。
	s.mu.RLock()
	destinations := make(map[string]struct{}, len(s.subs))
	for _, dest := range s.subs {
		destinations[dest] = struct{}{}
	}
	s.mu.RUnlock()

	for dest := range destinations {
		removeFrom(h.subs, dest, s)
	}
}

// subscribe 把連線登記到 destination 的索引。
func (h *Hub) subscribe(dest string, s *Session) {
	h.mu.Lock()
	defer h.mu.Unlock()
	addTo(h.subs, dest, s)
}

// unsubscribe 把連線從 destination 的索引移除。
func (h *Hub) unsubscribe(dest string, s *Session) {
	h.mu.Lock()
	defer h.mu.Unlock()
	removeFrom(h.subs, dest, s)
}

// addTo 往「key → 連線集合」的索引裡加一筆，需要時建立內層 map。
func addTo(index map[string]map[*Session]struct{}, key string, s *Session) {
	set, ok := index[key]
	if !ok {
		set = make(map[*Session]struct{})
		index[key] = set
	}
	set[s] = struct{}{}
}

// removeFrom 從索引移除一筆，**內層 map 空了就把整個 key 刪掉**。
//
// 最後那一步不是潔癖：不刪的話，一個惡意 client 只要不斷用新的
// destination 訂閱再斷線，外層 map 就會單向成長——Go 的 map 不會
// 因為刪除元素而縮小，這是一個很典型的隱性記憶體洩漏。
func removeFrom(index map[string]map[*Session]struct{}, key string, s *Session) {
	set, ok := index[key]
	if !ok {
		return
	}
	delete(set, s)
	if len(set) == 0 {
		delete(index, key)
	}
}
