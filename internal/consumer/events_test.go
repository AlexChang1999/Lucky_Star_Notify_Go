package consumer

import (
	"testing"

	"github.com/AlexChang1999/lucky-star-notify-go/internal/hub"
)

// TestPlan 把「事件 → 推去哪裡、body 長什麼樣」逐位元組釘住。
//
// 為什麼要比對整個 body 字串而不是解析後挑幾個欄位：本專案主打的是
// 與 Java 版行為等價，而 body 的**欄位順序**與**缺值寫成 null 還是省略**
// 都是契約的一部分。只斷言「有 type 這個欄位」的話，欄位順序整個顛倒
// 也照樣過關。
func TestPlan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		topic string
		input string

		wantErr      bool   // 解析不出來 → 丟棄
		wantSkip     bool   // 格式合法但缺路由欄位 → 略過
		wantPlayerID string // 空字串 = 廣播
		wantDest     string
		wantBody     string
	}{
		// ---- notification.push ----
		{
			name:         "notification.push：targetPlayerId 有值 → 私人推播",
			topic:        TopicNotificationPush,
			input:        `{"targetPlayerId":42,"type":"SYSTEM","title":"維護公告","message":"01:00 起維護","payload":{"z":1,"a":2}}`,
			wantPlayerID: "42",
			wantDest:     hub.PrivateDestination,
			// payload 的 key 順序保持 z、a——這正是用 json.RawMessage 而不是
			// map[string]any 的理由：map 會被 Go 排序成 a、z，與 Java 不同。
			wantBody: `{"targetPlayerId":42,"type":"SYSTEM","title":"維護公告","message":"01:00 起維護","payload":{"z":1,"a":2}}`,
		},
		{
			name:     "notification.push：沒有 targetPlayerId → 廣播，缺的欄位寫成 null",
			topic:    TopicNotificationPush,
			input:    `{"type":"ANNOUNCEMENT","title":"公告","message":"開服"}`,
			wantDest: BroadcastDestination,
			wantBody: `{"targetPlayerId":null,"type":"ANNOUNCEMENT","title":"公告","message":"開服","payload":null}`,
		},
		{
			name:     "notification.push：targetPlayerId 明確給 null 也是廣播",
			topic:    TopicNotificationPush,
			input:    `{"targetPlayerId":null,"type":"A","title":null,"message":null,"payload":null}`,
			wantDest: BroadcastDestination,
			wantBody: `{"targetPlayerId":null,"type":"A","title":null,"message":null,"payload":null}`,
		},
		{
			// 對齊 Java 的 @JsonIgnoreProperties(ignoreUnknown = true)：
			// 未知欄位在 round-trip 裡被丟掉，兩版都是這個行為。
			name:         "notification.push：未知欄位被丟掉",
			topic:        TopicNotificationPush,
			input:        `{"targetPlayerId":7,"type":"T","extra":"忽略我","nested":{"a":1}}`,
			wantPlayerID: "7",
			wantDest:     hub.PrivateDestination,
			wantBody:     `{"targetPlayerId":7,"type":"T","title":null,"message":null,"payload":null}`,
		},
		{
			// 這一條釘住 marshalCompatible 的 SetEscapeHTML(false)。
			// 用 json.Marshal 的話 & 與 < > 會變成 Unicode 轉義，與 Jackson 不同。
			name:         "notification.push：& 與角括號保持原字元，不做 HTML 轉義",
			topic:        TopicNotificationPush,
			input:        `{"targetPlayerId":1,"type":"T","title":"A&B <c>"}`,
			wantPlayerID: "1",
			wantDest:     hub.PrivateDestination,
			wantBody:     `{"targetPlayerId":1,"type":"T","title":"A&B <c>","message":null,"payload":null}`,
		},
		{
			// encoding/json 在 marshal json.RawMessage 時會順手 compact()，
			// 所以輸入的空白會被清掉——結果剛好與 Jackson 的緊湊輸出一致，
			// 不必自己多做一次 json.Compact。**保留 key 的原始順序**則是
			// RawMessage 本來的行為（map 會被排序，那才是要避開的東西）。
			name:         "notification.push：payload 內部空白被 compact 掉，key 順序保留",
			topic:        TopicNotificationPush,
			input:        `{"targetPlayerId":1,"type":"T","payload":{"z": 1,  "a":2}}`,
			wantPlayerID: "1",
			wantDest:     hub.PrivateDestination,
			wantBody:     `{"targetPlayerId":1,"type":"T","title":null,"message":null,"payload":{"z":1,"a":2}}`,
		},
		{
			name:    "notification.push：JSON 沒收尾 → 丟棄",
			topic:   TopicNotificationPush,
			input:   `{"targetPlayerId":`,
			wantErr: true,
		},
		{
			// Kafka 的 tombstone（value 為空）會走到這裡。Java 那邊
			// readValue("") 也會丟例外並被 catch 起來，行為一致。
			name:    "notification.push：空的 value → 丟棄",
			topic:   TopicNotificationPush,
			input:   ``,
			wantErr: true,
		},
		{
			// ⚠️ 已知的跨語言差異，用測試釘住免得日後誤以為是 bug：
			// Jackson 預設會把字串 "42" 強制轉成 Long，Go 不會。
			// 現行 producer 都是 Java 服務、送的是數字，所以不值得為此
			// 自訂 UnmarshalJSON——但要知道差異存在。
			name:    "notification.push：targetPlayerId 給字串會被丟棄（Java 會強制轉型，已知差異）",
			topic:   TopicNotificationPush,
			input:   `{"targetPlayerId":"42","type":"T"}`,
			wantErr: true,
		},

		// ---- game.result ----
		{
			// 注意 body 與事件**不同形狀**：type 在最前面、且沒有 playerId。
			name:         "game.result：重組成 7 欄通知，type 在最前面且不含 playerId",
			topic:        TopicGameResult,
			input:        `{"roundId":"r-1","playerId":42,"gameType":"slot","bet":100,"payout":250,"win":true,"settledAt":"2026-07-30T12:00:00Z"}`,
			wantPlayerID: "42",
			wantDest:     hub.PrivateDestination,
			wantBody:     `{"type":"GAME_RESULT","roundId":"r-1","gameType":"slot","bet":100,"payout":250,"win":true,"settledAt":"2026-07-30T12:00:00Z"}`,
		},
		{
			name:     "game.result：缺 playerId → 略過（不是錯誤）",
			topic:    TopicGameResult,
			input:    `{"roundId":"r-2","gameType":"slot","bet":10}`,
			wantSkip: true,
		},
		{
			name:         "game.result：不同遊戲缺不同欄位，缺的寫成 null",
			topic:        TopicGameResult,
			input:        `{"roundId":"r-3","playerId":9,"gameType":"fishing","extra":{"hp":100}}`,
			wantPlayerID: "9",
			wantDest:     hub.PrivateDestination,
			wantBody:     `{"type":"GAME_RESULT","roundId":"r-3","gameType":"fishing","bet":null,"payout":null,"win":null,"settledAt":null}`,
		},
		{
			// 這一條防的是「順手加 omitempty」：那會讓 win:false 與 bet:0
			// 整個欄位消失，前端的 `payload.win` 就變成 undefined。
			name:         "game.result：win=false 與 0 值不可以被省略",
			topic:        TopicGameResult,
			input:        `{"playerId":5,"win":false,"bet":0,"payout":0}`,
			wantPlayerID: "5",
			wantDest:     hub.PrivateDestination,
			wantBody:     `{"type":"GAME_RESULT","roundId":null,"gameType":null,"bet":0,"payout":0,"win":false,"settledAt":null}`,
		},

		// ---- rank.update ----
		{
			// entries 裡那個 2^53+1 是刻意的：用 map[string]any 接的話會走
			// float64，這個值會被無聲地變成 9007199254740992。RawMessage 不會。
			name:     "rank.update：廣播，entries 原樣搬運（含超過 float64 精度的整數）",
			topic:    TopicRankUpdate,
			input:    `{"type":"GLOBAL_COINS","entries":[{"playerId":1,"nickname":"阿明","score":9007199254740993}],"updatedAt":1750000000000}`,
			wantDest: RankDestination,
			wantBody: `{"type":"GLOBAL_COINS","entries":[{"playerId":1,"nickname":"阿明","score":9007199254740993}],"updatedAt":1750000000000}`,
		},
		{
			// Java 的 updatedAt 是原始型別 long，缺值 Jackson 填 0。
			// 用 *int64 會寫出 null，與 Java 不同——所以刻意用 int64。
			name:     "rank.update：缺 updatedAt → 0（對齊 Java 的原始 long）",
			topic:    TopicRankUpdate,
			input:    `{"type":"X","entries":[]}`,
			wantDest: RankDestination,
			wantBody: `{"type":"X","entries":[],"updatedAt":0}`,
		},
		{
			name:     "rank.update：updatedAt 給 null 也是 0",
			topic:    TopicRankUpdate,
			input:    `{"type":"X","entries":[],"updatedAt":null}`,
			wantDest: RankDestination,
			wantBody: `{"type":"X","entries":[],"updatedAt":0}`,
		},
		{
			name:     "rank.update：空物件也能廣播（best-effort，不做欄位驗證）",
			topic:    TopicRankUpdate,
			input:    `{}`,
			wantDest: RankDestination,
			wantBody: `{"type":null,"entries":null,"updatedAt":0}`,
		},

		// ---- 其他 ----
		{
			name:    "未知的 topic 是程式碼寫錯，回報錯誤",
			topic:   "nope.topic",
			input:   `{}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := plan(tt.topic, []byte(tt.input))

			if tt.wantErr {
				if err == nil {
					t.Fatalf("預期失敗，卻得到 %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("plan() 意外失敗：%v", err)
			}
			if tt.wantSkip {
				if got != nil {
					t.Fatalf("預期略過，卻要推播 %+v", got)
				}
				return
			}
			if got == nil {
				t.Fatal("預期要推播，卻得到 nil")
			}
			if got.PlayerID != tt.wantPlayerID {
				t.Errorf("PlayerID = %q，預期 %q", got.PlayerID, tt.wantPlayerID)
			}
			if got.Destination != tt.wantDest {
				t.Errorf("Destination = %q，預期 %q", got.Destination, tt.wantDest)
			}
			if string(got.Body) != tt.wantBody {
				t.Errorf("Body 不符\n  實際：%s\n  預期：%s", got.Body, tt.wantBody)
			}
		})
	}
}
