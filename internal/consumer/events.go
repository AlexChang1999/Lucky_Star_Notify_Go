// Package consumer 把三個 Kafka topic 的事件轉成 hub 的推播。
//
// # 職責
//
// 一個 topic 一個 Consumer，各自一條 goroutine 跑 FetchMessage 迴圈。
// 它只做三件事：解析事件 → 決定送去哪裡 → 交給 hub。連線管理、扇出、
// 心跳都是 hub 的事，這個套件完全不碰 WebSocket。
//
// # 與 Java 版的對應
//
//	@KafkaListener              → 自己開 goroutine 跑 for { FetchMessage }
//	ack-mode: MANUAL_IMMEDIATE  → FetchMessage + CommitMessages
//	                              （**不是** ReadMessage——那個會自動提交）
//	auto-offset-reset: latest   → kafka.ReaderConfig{StartOffset: kafka.LastOffset}
//	try/catch + finally ack     → handle 裡不論成功失敗都 commit
//
// # 三個 topic 的推播內容形狀**不一樣**
//
//	notification.push  原事件回吐（Java 是 convertAndSend(dest, event)）
//	rank.update        原事件回吐
//	game.result        **另外組一個 7 欄物件**：type 在最前面、且不含 playerId
//
// 把 game.result 原樣轉推是最容易犯的錯——前端拿不到 type 就會走進
// 「系統通知」分支，而且沒有任何錯誤訊息，只有「通知內容怎麼怪怪的」。
package consumer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/AlexChang1999/lucky-star-notify-go/internal/hub"
)

// 三個 topic 的名稱，與 Java 版的 @KafkaListener(topics = ...) 逐字一致。
const (
	TopicNotificationPush = "notification.push"
	TopicGameResult       = "game.result"
	TopicRankUpdate       = "rank.update"
)

// AllTopics 是本服務消費的全部 topic，main 用它建立 consumer。
//
// 用 var 而不是 const（Go 沒有常數陣列）。它不該被改動，但這是 internal
// 套件、只有自己人 import，為此包一層 getter 是 Java 習慣的過度防衛。
var AllTopics = []string{TopicNotificationPush, TopicGameResult, TopicRankUpdate}

const (
	// BroadcastDestination 是系統公告的廣播頻道，對齊 Java 版
	// NotificationConsumer.BROADCAST_DESTINATION。
	BroadcastDestination = "/topic/notifications"

	// RankDestination 是排行榜廣播頻道，對齊 Java 版 RankUpdateConsumer.DESTINATION。
	RankDestination = "/topic/rank"

	// GameResultType 是 game.result 推播內容的 type 欄位值，
	// 對齊 Java 版 GameResultConsumer.NOTIFICATION_TYPE。
	//
	// 前端用它決定要不要更新遊戲結果（useWebSocket.js 檢查
	// payload?.type === 'GAME_RESULT'），寫錯就只是「通知有跳但畫面沒更新」。
	GameResultType = "GAME_RESULT"
)

// push 描述「一則事件該送到哪裡」。
//
// PlayerID 為空代表廣播；有值代表只送給該玩家（走 hub 的 playerID 索引）。
type push struct {
	PlayerID    string
	Destination string
	Body        []byte
}

// notificationPushEvent 對齊 Java 的 NotificationPushEvent record。
//
// # 為什麼欄位順序重要
//
// encoding/json 照**宣告順序** marshal，Jackson 對 record 照 **component
// 順序** marshal。兩邊順序一致，送出去的位元組才會一致。順序在這裡不是
// 美觀問題，是契約的一部分。
//
// # 為什麼可空欄位用指標
//
// Java 用的是包裝型別（Long / String），值可以是 null，Jackson 預設會把
// null 照樣寫出來。Go 若用 string，缺值會變成 ""——那是**不同的值**，
// 前端的 `payload.title || '系統通知'` 判斷會走進不同分支。用 *string
// 才能忠實表達「這個欄位不存在」。
//
// # 為什麼 payload 用 json.RawMessage 而不是 map[string]any
//
// map 有兩個會破壞等價性的行為：Go marshal map 時會把 key **排序**
// （Jackson 保留輸入順序），而數字會全部變成 float64（大整數會失去精度、
// 也可能印成科學記號）。RawMessage 搬運的是輸入的原始位元組，兩個問題都沒有。
//
// 附帶好處：encoding/json 在 marshal RawMessage 時會順手 compact()，
// 所以輸入的多餘空白會被清掉，輸出與 Jackson 的緊湊形式一致——
// 不必自己再呼叫一次 json.Compact。
type notificationPushEvent struct {
	TargetPlayerID *int64          `json:"targetPlayerId"`
	Type           *string         `json:"type"`
	Title          *string         `json:"title"`
	Message        *string         `json:"message"`
	Payload        json.RawMessage `json:"payload"`
}

// gameResultEvent 對齊 Java 的 GameResultEvent record。
//
// game-service 對不同遊戲（slot / fishing / baccarat）送出的欄位略有差異，
// 所以除了 playerId 之外都可能缺——Java 那邊靠 @JsonIgnoreProperties 容忍，
// Go 的 encoding/json 預設就忽略未知欄位，行為天生一致，不必額外處理。
type gameResultEvent struct {
	RoundID   *string `json:"roundId"`
	PlayerID  *int64  `json:"playerId"`
	GameType  *string `json:"gameType"`
	Bet       *int64  `json:"bet"`
	Payout    *int64  `json:"payout"`
	Win       *bool   `json:"win"`
	SettledAt *string `json:"settledAt"`
}

// gameResultNotification 是 game.result **真正推出去**的形狀。
//
// ⚠️ 它跟 gameResultEvent 不一樣，兩個差異都是刻意的：
//
//	type 在第一個   對齊 Java 的 LinkedHashMap 插入順序
//	不含 playerId   playerId 是路由用的，不是內容——Java 版也沒放進去
type gameResultNotification struct {
	Type      string  `json:"type"`
	RoundID   *string `json:"roundId"`
	GameType  *string `json:"gameType"`
	Bet       *int64  `json:"bet"`
	Payout    *int64  `json:"payout"`
	Win       *bool   `json:"win"`
	SettledAt *string `json:"settledAt"`
}

// rankUpdateEvent 對齊 Java 的 RankUpdateEvent record。
//
// UpdatedAt 用 int64 而不是 *int64，因為 Java 那邊是**原始型別 long**：
// 欄位缺了或給 null，Jackson 都會填 0。用指標反而會寫出 null，與 Java 不同。
type rankUpdateEvent struct {
	Type      *string         `json:"type"`
	Entries   json.RawMessage `json:"entries"`
	UpdatedAt int64           `json:"updatedAt"`
}

// plan 把一則 Kafka 訊息轉成「要推去哪裡」。
//
// 三種回傳組合各有意義，呼叫端要分開處理（也分開計數）：
//
//	(*push, nil)  正常，照著推
//	(nil, nil)    格式合法但缺路由需要的欄位（例如 game.result 沒 playerId）→ 略過
//	(nil, err)    解析不出來 → 丟棄
//
// 後兩者都會 commit offset，但意義完全不同：前者是上游的正常變異，
// 後者是上游的 bug。混成一種就看不出差別了（見 metrics.KafkaMessages）。
func plan(topic string, value []byte) (*push, error) {
	switch topic {
	case TopicNotificationPush:
		return planNotificationPush(value)
	case TopicGameResult:
		return planGameResult(value)
	case TopicRankUpdate:
		return planRankUpdate(value)
	default:
		// 只可能是程式碼寫錯（訂了沒處理的 topic），不是執行期的資料問題。
		return nil, fmt.Errorf("未知的 topic %q", topic)
	}
}

// planNotificationPush 依 targetPlayerId 分流：有值 → 私人，無值 → 廣播。
//
// 與 Java 版 NotificationConsumer.dispatch 的判斷一致。
func planNotificationPush(value []byte) (*push, error) {
	var event notificationPushEvent
	if err := json.Unmarshal(value, &event); err != nil {
		return nil, fmt.Errorf("解析 %s 失敗：%w", TopicNotificationPush, err)
	}

	// 回吐整個事件——Java 是 convertAndSend(dest, event)，送出去的就是
	// 事件本身重新序列化的結果。順帶一個副作用要知道：未知欄位在這一趟
	// round-trip 裡會被丟掉（Java 因為 record 也一樣），這是等價的行為。
	body, err := marshalCompatible(event)
	if err != nil {
		return nil, fmt.Errorf("重新編碼 %s 失敗：%w", TopicNotificationPush, err)
	}

	if event.TargetPlayerID == nil {
		return &push{Destination: BroadcastDestination, Body: body}, nil
	}
	return &push{
		// Java 是 String.valueOf(Long)，也就是十進位字串；而 JWT 的 sub
		// 同樣是十進位字串，所以兩邊比對得起來。
		PlayerID:    strconv.FormatInt(*event.TargetPlayerID, 10),
		Destination: hub.PrivateDestination,
		Body:        body,
	}, nil
}

// planGameResult 把結算事件重組成前端要的通知，送給下注的玩家。
func planGameResult(value []byte) (*push, error) {
	var event gameResultEvent
	if err := json.Unmarshal(value, &event); err != nil {
		return nil, fmt.Errorf("解析 %s 失敗：%w", TopicGameResult, err)
	}

	if event.PlayerID == nil {
		// 沒有 playerId 就無法定位收件玩家。與 Java 版一樣記錄後略過，
		// **不是**錯誤——這是上游的合法變異，不該汙染 dropped 的計數。
		return nil, nil
	}

	body, err := marshalCompatible(gameResultNotification{
		Type:      GameResultType,
		RoundID:   event.RoundID,
		GameType:  event.GameType,
		Bet:       event.Bet,
		Payout:    event.Payout,
		Win:       event.Win,
		SettledAt: event.SettledAt,
	})
	if err != nil {
		return nil, fmt.Errorf("重新編碼 %s 失敗：%w", TopicGameResult, err)
	}

	return &push{
		PlayerID:    strconv.FormatInt(*event.PlayerID, 10),
		Destination: hub.PrivateDestination,
		Body:        body,
	}, nil
}

// planRankUpdate 把排行榜更新廣播到 /topic/rank。
func planRankUpdate(value []byte) (*push, error) {
	var event rankUpdateEvent
	if err := json.Unmarshal(value, &event); err != nil {
		return nil, fmt.Errorf("解析 %s 失敗：%w", TopicRankUpdate, err)
	}

	body, err := marshalCompatible(event)
	if err != nil {
		return nil, fmt.Errorf("重新編碼 %s 失敗：%w", TopicRankUpdate, err)
	}
	return &push{Destination: RankDestination, Body: body}, nil
}

// marshalCompatible 把 v 編成 JSON，且**不轉義 HTML 字元**。
//
// # 為什麼不直接用 json.Marshal
//
// json.Marshal 預設會把 `<`、`>`、`&` 換成 Unicode 轉義序列（碼位 u003c、
// u003e、u0026 的那種寫法），為的是讓 JSON 能安全地直接嵌進 HTML。
// Jackson 不做這件事：同一則公告「A&B」，Java 版的 body 留原字元，
// Go 版會變成轉義序列。
// 兩者 JSON.parse 之後完全相同，但位元組不同——本專案主打的正是跨語言
// 行為等價，這種差異能消掉就消掉。
//
// ⚠️ Encoder.Encode 會在結尾多補一個 '\n'，一定要砍掉。留著不會壞掉
// （content-length 由 Frame.Marshal 照算），但 body 就多了一個毫無意義的
// 換行，逐位元組比對 Java 與 Go 的輸出時會白白對不上。
func marshalCompatible(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}
