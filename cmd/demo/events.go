package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// 三個 topic 的名稱。
//
// 這裡刻意**重新寫一次字面值**，而不是 import internal/consumer 的常數。
// 理由是這個程式扮演的是「上游服務」——真實環境裡發事件的是 game-service
// 與 rank-service，它們與本服務沒有共用程式碼。共用常數會讓「兩邊 topic
// 名稱不一致」這種真實故障在 demo 裡永遠不可能發生，反而失去驗證意義。
const (
	topicNotificationPush = "notification.push"
	topicGameResult       = "game.result"
	topicRankUpdate       = "rank.update"
)

// eventsPerCycle 是一輪循環包含幾則事件，見 generator.next 的說明。
const eventsPerCycle = 4

// player 是 demo 用的假玩家。
//
// ID 對應 JWT 的 subject，也就是推播路由的鍵；Nickname 只是讓畫面好看。
type player struct {
	ID       int64
	Nickname string
}

// demoPlayers 是固定的玩家名單。
//
// 兩個人是刻意的最小值：計畫書 Phase 2 的 DoD 要求「兩個分頁用不同帳號
// 登入，私人推播只進正確的那一個」。名單只有一個人的話，這件事在 demo 上
// 根本演示不出來。
var demoPlayers = []player{
	{ID: 1001, Nickname: "阿明"},
	{ID: 1002, Nickname: "小美"},
}

// demoEvent 是一則準備送進 Kafka 的假事件。
type demoEvent struct {
	Topic string
	// Key 決定 partition。真實 producer 用玩家 ID 當 key，好讓同一個玩家的
	// 事件保持順序；這裡照做，免得 demo 的行為與真實情境不同。
	Key   []byte
	Value []byte
}

// generator 依序產生假事件。
//
// # 為什麼不用 math/rand
//
// 隨機值會讓「產生器輸出什麼」變成不可測的東西，測試就只能斷言「有這個
// 欄位」而不是「值是什麼」——那種測試擋不住欄位順序或型別寫錯。
// 這裡改成**全部由 seq 推導**：畫面上看起來仍在變化（下注額、分數、輸贏
// 都會跳動），但同一個 seq 永遠產生同一則事件，測試可以逐位元組比對。
//
// 併發安全性：只有一條 goroutine（發送迴圈）會碰它，所以不加鎖。
// 這不是疏漏，是「不要為了不存在的併發付出代價」——真的多了第二個呼叫端，
// -race 會當場抓到。
type generator struct {
	players []player
	seq     int
}

func newGenerator() *generator {
	return &generator{players: demoPlayers}
}

// next 產生下一則事件。
//
// # 為什麼一輪是四則而不是三則
//
// 三個 topic 只能演示三條路徑，但 notification.push 自己就有**兩種**
// 路由行為（targetPlayerId 有值 = 私人、沒值 = 廣播），而這兩種在前端
// 是完全不同的顯示位置。四則一輪剛好把四條路徑都走過一遍：
//
//	0  game.result        → 私人 /user/queue/notifications
//	1  rank.update        → 廣播 /topic/rank
//	2  notification.push  → 私人 /user/queue/notifications
//	3  notification.push  → 廣播 /topic/notifications
//
// now 由呼叫端傳進來而不是在裡面呼叫 time.Now()：時間是外部依賴，
// 讓它變成參數，測試就不必想辦法凍結時鐘。
func (g *generator) next(now time.Time) (demoEvent, error) {
	seq := g.seq
	g.seq++

	// 每一輪換一個收件玩家，這樣開兩個分頁時兩邊會輪流收到私人訊息——
	// 「私人推播只進正確的那一個」才看得出來。
	target := g.players[(seq/eventsPerCycle)%len(g.players)]

	switch seq % eventsPerCycle {
	case 0:
		return marshalEvent(topicGameResult, target.ID, newGameResult(seq, target, now))
	case 1:
		return marshalEvent(topicRankUpdate, 0, newRankUpdate(seq, g.players, now))
	case 2:
		return marshalEvent(topicNotificationPush, target.ID, newPrivateNotification(seq, target))
	default:
		return marshalEvent(topicNotificationPush, 0, newBroadcastNotification(seq))
	}
}

// gameResultEvent 是 game-service 發出的結算事件。
//
// 欄位順序與 internal/consumer 的 gameResultEvent 對齊。順序在這裡沒有
// 正確性影響（消費端是靠 key 解析的），但兩邊看起來一致，日後對照 log
// 時省事。
type gameResultEvent struct {
	RoundID   string `json:"roundId"`
	PlayerID  int64  `json:"playerId"`
	GameType  string `json:"gameType"`
	Bet       int64  `json:"bet"`
	Payout    int64  `json:"payout"`
	Win       bool   `json:"win"`
	SettledAt string `json:"settledAt"`
}

// gameTypes 對應團隊專案實際有的三種遊戲。
var gameTypes = []string{"slot", "fishing", "baccarat"}

func newGameResult(seq int, target player, now time.Time) gameResultEvent {
	bet := int64(100 * (seq%5 + 1))
	// 三次贏一次。固定比例而不是隨機，是為了讓測試能斷言具體數值。
	win := seq%3 == 0
	var payout int64
	if win {
		payout = bet * 2
	}
	return gameResultEvent{
		RoundID:  fmt.Sprintf("r-%06d", seq),
		PlayerID: target.ID,
		GameType: gameTypes[(seq/eventsPerCycle)%len(gameTypes)],
		Bet:      bet,
		Payout:   payout,
		Win:      win,
		// RFC3339 對應 Java 那邊 Instant.toString() 的輸出形狀。
		// 用 UTC 是因為容器的時區是 UTC，本機跑時也才不會兩份不一樣。
		SettledAt: now.UTC().Format(time.RFC3339),
	}
}

// rankEntry 是排行榜的一列。
type rankEntry struct {
	PlayerID int64  `json:"playerId"`
	Nickname string `json:"nickname"`
	Score    int64  `json:"score"`
	Rank     int    `json:"rank"`
}

// rankUpdateEvent 是 rank-service 發出的排行榜更新。
type rankUpdateEvent struct {
	Type    string      `json:"type"`
	Entries []rankEntry `json:"entries"`
	// UpdatedAt 是 epoch 毫秒（對齊 Java 的 System.currentTimeMillis()），
	// 不是 RFC3339 字串——兩個 topic 的時間表示法本來就不同，
	// 統一它們會讓 demo 與真實契約不符。
	UpdatedAt int64 `json:"updatedAt"`
}

func newRankUpdate(seq int, players []player, now time.Time) rankUpdateEvent {
	entries := make([]rankEntry, 0, len(players))
	for i, p := range players {
		entries = append(entries, rankEntry{
			PlayerID: p.ID,
			Nickname: p.Nickname,
			// 讓分數隨 seq 成長，畫面上才看得出「排行榜在動」。
			Score: int64(10000 + seq*137 + i*911),
			Rank:  i + 1,
		})
	}
	return rankUpdateEvent{
		Type:      "GLOBAL_COINS",
		Entries:   entries,
		UpdatedAt: now.UTC().UnixMilli(),
	}
}

// notificationPushEvent 是 notification.push 的事件。
//
// TargetPlayerID 用指標而不是 int64：nil 才會序列化成 JSON 的 null，
// 而消費端正是靠「這個欄位是不是 null」決定私人推播還是廣播。
// 用 int64 的話缺值會變成 0，那會被當成「推給玩家 0」——一個不存在的人，
// 症狀是廣播訊息**誰都收不到而且沒有錯誤訊息**。
type notificationPushEvent struct {
	TargetPlayerID *int64          `json:"targetPlayerId"`
	Type           string          `json:"type"`
	Title          string          `json:"title"`
	Message        string          `json:"message"`
	Payload        json.RawMessage `json:"payload"`
}

func newPrivateNotification(seq int, target player) notificationPushEvent {
	id := target.ID
	return notificationPushEvent{
		TargetPlayerID: &id,
		Type:           "REWARD",
		Title:          "每日獎勵",
		Message:        fmt.Sprintf("%s，你領到了 %d 金幣", target.Nickname, 50*(seq%7+1)),
		Payload:        json.RawMessage(`{"coins":` + strconv.Itoa(50*(seq%7+1)) + `}`),
	}
}

// broadcastMessages 讓廣播內容有點變化，不然畫面上全是同一句。
var broadcastMessages = []string{
	"伺服器將於 03:00 進行例行維護",
	"限時活動開跑：老虎機加倍中",
	"新玩家禮包已發放",
}

func newBroadcastNotification(seq int) notificationPushEvent {
	return notificationPushEvent{
		// nil = 廣播。這一行就是整個 demo 裡最容易寫錯的地方，見型別的說明。
		TargetPlayerID: nil,
		Type:           "ANNOUNCEMENT",
		Title:          "系統公告",
		Message:        broadcastMessages[(seq/eventsPerCycle)%len(broadcastMessages)],
		Payload:        nil,
	}
}

// marshalEvent 把事件編成 Kafka 訊息。
//
// key 為 0 代表廣播事件，這時不指定 partition key（Kafka 會輪流分配）——
// 廣播沒有「同一個玩家的順序」要保，硬塞 key 只會讓負載集中在一個 partition。
func marshalEvent(topic string, key int64, payload any) (demoEvent, error) {
	value, err := json.Marshal(payload)
	if err != nil {
		return demoEvent{}, fmt.Errorf("編碼 %s 事件失敗：%w", topic, err)
	}

	event := demoEvent{Topic: topic, Value: value}
	if key != 0 {
		event.Key = []byte(strconv.FormatInt(key, 10))
	}
	return event, nil
}
