package main

import (
	"testing"
	"time"
)

// fixedNow 是所有測試共用的時間點。
//
// 產生器把 now 當參數而不是自己呼叫 time.Now()，就是為了讓這件事成立：
// 時間固定下來，輸出才能逐位元組比對。
var fixedNow = time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)

// TestGeneratorCycle 把假事件的產出**逐位元組**釘住。
//
// # 為什麼要比對整個 JSON 字串
//
// 這個程式扮演的是上游 producer，它的輸出就是 internal/consumer 的輸入。
// 只斷言「有 playerId 這個欄位」的話，型別從數字寫成字串照樣過關——
// 而那正是 consumer 會直接丟棄的形狀（見 consumer 的「Jackson 會強制轉型、
// Go 不會」那條測試）。症狀是 demo 一則訊息都收不到，而且沒有錯誤訊息。
//
// 前兩輪（seq 0~4）就涵蓋了全部四條路徑，外加兩個容易寫錯的邊界：
// win=false 與 payout=0 不可以被省略。
func TestGeneratorCycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantTopic string
		wantKey   string // 空字串 = 不指定 partition key
		wantValue string
	}{
		{
			name:      "seq 0：game.result 給第一位玩家，贏局",
			wantTopic: topicGameResult,
			wantKey:   "1001",
			wantValue: `{"roundId":"r-000000","playerId":1001,"gameType":"slot",` +
				`"bet":100,"payout":200,"win":true,"settledAt":"2026-07-30T12:00:00Z"}`,
		},
		{
			name:      "seq 1：rank.update 廣播，updatedAt 是 epoch 毫秒",
			wantTopic: topicRankUpdate,
			wantKey:   "",
			wantValue: `{"type":"GLOBAL_COINS","entries":[` +
				`{"playerId":1001,"nickname":"阿明","score":10137,"rank":1},` +
				`{"playerId":1002,"nickname":"小美","score":11048,"rank":2}` +
				`],"updatedAt":1785412800000}`,
		},
		{
			name:      "seq 2：notification.push 有 targetPlayerId → 私人",
			wantTopic: topicNotificationPush,
			wantKey:   "1001",
			wantValue: `{"targetPlayerId":1001,"type":"REWARD","title":"每日獎勵",` +
				`"message":"阿明，你領到了 150 金幣","payload":{"coins":150}}`,
		},
		{
			// 這一條是整個檔案最重要的斷言：targetPlayerId 必須是 **null**。
			// 寫成 0 的話 consumer 會當成「推給玩家 0」，公告就誰都收不到，
			// 而且不會有任何錯誤訊息。
			name:      "seq 3：notification.push 沒有 targetPlayerId → 廣播，且該欄位是 null",
			wantTopic: topicNotificationPush,
			wantKey:   "",
			wantValue: `{"targetPlayerId":null,"type":"ANNOUNCEMENT","title":"系統公告",` +
				`"message":"伺服器將於 03:00 進行例行維護","payload":null}`,
		},
		{
			// 換一輪就換一位收件玩家——「私人推播只進正確的那個分頁」
			// 要在 demo 上看得出來，靠的就是這個輪替。
			// 同時這一則是輸局：win=false 與 payout=0 都**必須寫出來**，
			// 誰要是順手加了 omitempty，這裡會當場失敗。
			name:      "seq 4：換第二位玩家，輸局的 win=false 與 payout=0 不可省略",
			wantTopic: topicGameResult,
			wantKey:   "1002",
			wantValue: `{"roundId":"r-000004","playerId":1002,"gameType":"fishing",` +
				`"bet":500,"payout":0,"win":false,"settledAt":"2026-07-30T12:00:00Z"}`,
		},
	}

	// 刻意共用同一個 generator 依序取值，而不是每個子測試各建一個：
	// 要驗的正是「連續呼叫會照這個順序產出」。也因此這裡**不能** t.Parallel()
	// 到子測試層級——順序就是被測的東西。
	gen := newGenerator()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := gen.next(fixedNow)
			if err != nil {
				t.Fatalf("next() 意外失敗：%v", err)
			}
			if event.Topic != tt.wantTopic {
				t.Errorf("Topic = %q，預期 %q", event.Topic, tt.wantTopic)
			}
			if string(event.Key) != tt.wantKey {
				t.Errorf("Key = %q，預期 %q", event.Key, tt.wantKey)
			}
			if string(event.Value) != tt.wantValue {
				t.Errorf("Value 不符\n  實際：%s\n  預期：%s", event.Value, tt.wantValue)
			}
		})
	}
}

// TestGeneratorCoversAllRoutes 確認一輪循環真的走過全部四條推播路徑。
//
// 為什麼要有這一條：上面那個表格是「照順序比對」，若有人把 eventsPerCycle
// 從 4 改成 3（例如覺得「三個 topic 配三則比較整齊」），表格會被一起改成
// 新的期望值而看起來仍然全綠——但廣播公告那條路徑就再也不會被產生，
// demo 上就永遠看不到系統公告了。這條測試把「四條路徑都要有」講成一個
// 獨立的、不會被順手改掉的斷言。
func TestGeneratorCoversAllRoutes(t *testing.T) {
	t.Parallel()

	gen := newGenerator()

	topics := make(map[string]int)
	broadcastPushes := 0
	privatePushes := 0

	for range eventsPerCycle {
		event, err := gen.next(fixedNow)
		if err != nil {
			t.Fatalf("next() 意外失敗：%v", err)
		}
		topics[event.Topic]++
		if event.Topic == topicNotificationPush {
			if len(event.Key) == 0 {
				broadcastPushes++
			} else {
				privatePushes++
			}
		}
	}

	for _, topic := range []string{topicGameResult, topicRankUpdate, topicNotificationPush} {
		if topics[topic] == 0 {
			t.Errorf("一輪循環裡沒有產生任何 %s 事件", topic)
		}
	}
	if broadcastPushes == 0 {
		t.Error("一輪循環裡沒有廣播的 notification.push（targetPlayerId 為 null 的那種）")
	}
	if privatePushes == 0 {
		t.Error("一輪循環裡沒有私人的 notification.push")
	}
}

// TestGeneratorRotatesPlayers 確認收件玩家會輪替。
//
// 名單上兩個人卻永遠只推給第一個的話，計畫書 Phase 2 的 DoD
// 「兩個分頁用不同帳號登入，私人推播只進正確的那一個」就演示不出來——
// 而且看起來會像是推播服務的路由壞了，查錯方向。
func TestGeneratorRotatesPlayers(t *testing.T) {
	t.Parallel()

	gen := newGenerator()
	seen := make(map[string]bool)

	// 跑滿玩家數 × 每輪則數，理論上每位玩家都該當過一次收件人。
	for range len(demoPlayers) * eventsPerCycle {
		event, err := gen.next(fixedNow)
		if err != nil {
			t.Fatalf("next() 意外失敗：%v", err)
		}
		if event.Topic == topicGameResult {
			seen[string(event.Key)] = true
		}
	}

	if len(seen) != len(demoPlayers) {
		t.Errorf("game.result 只推給了 %d 位玩家（%v），名單上有 %d 位",
			len(seen), seen, len(demoPlayers))
	}
}
