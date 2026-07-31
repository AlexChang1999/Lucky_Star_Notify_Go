package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/AlexChang1999/lucky-star-notify-go/internal/stomp"
	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
)

// client 是一條長連線與它量到的東西。
//
// # 為什麼重用 internal/stomp 的編解碼
//
// 這裡的目的是**量延遲**，不是驗協議——協議正確性已經由 test/contract
// 用第三方的 @stomp/stompjs 證明過了。既然如此，用自家已經被表格驅動測試
// 與 fuzz 蓋過的編解碼器，比再手寫一份帧組裝可靠得多，也少一份會漂移的
// 重複實作。（反過來說，如果拿它去做正確性測試，那就是自己驗自己，沒有意義。）
type client struct {
	id       int
	playerID int64

	conn       *websocket.Conn
	connectDur time.Duration
	err        error
	// stage 記錄失敗發生在哪一步（dial / CONNECT / SUBSCRIBE …）。
	// 統計時要靠它區分「連不上」與「連上了但握手被拒」——這兩件事的
	// 成因完全不同，混在一起會查錯方向。
	stage string

	// latencies 只由自己的 goroutine 寫入，主 goroutine 在 done 關閉後才讀。
	latencies []time.Duration

	// done 在讀取迴圈結束時關閉。連線失敗時它是 nil——沒有迴圈要等。
	done chan struct{}
}

// run 建立連線、訂閱，然後一路讀到連線被關掉為止。
//
// 不論成功失敗都**恰好**往 ready 送一次訊號，主 goroutine 靠它知道
// 「這一條已經有結論了」。
func (c *client) run(o options, ready chan<- struct{}) {
	if err := c.connect(o); err != nil {
		c.err = err
		if c.conn != nil {
			_ = c.conn.Close()
		}
		ready <- struct{}{}
		return
	}

	c.done = make(chan struct{})
	ready <- struct{}{}

	defer close(c.done)
	defer func() { _ = c.conn.Close() }()
	c.readLoop()
}

// close 從**主** goroutine 叫，用來中斷卡在 ReadMessage 的讀取迴圈。
//
// gorilla 的規矩是「同時只能有一個 reader、一個 writer」，但 Close 是
// 明確支援的併發操作，也是喚醒 reader 的標準手段——這與伺服器端
// server.Server.Close 收拾被 Hijack 的連線是同一招。
func (c *client) close() {
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

func (c *client) connect(o options) error {
	token, err := mintToken(o.secret, c.playerID)
	if err != nil {
		return err
	}

	dialer := &websocket.Dialer{
		// 與 @stomp/stompjs 送的清單一致。伺服器挑它支援的第一個。
		Subprotocols:     []string{"v12.stomp", "v11.stomp", "v10.stomp"},
		HandshakeTimeout: o.timeout,
	}

	start := time.Now()
	c.stage = "dial"
	conn, resp, err := dialer.Dial(o.target, nil)
	if err != nil {
		// 帶上 HTTP 狀態碼：CheckOrigin 擋下來是 403，而 gorilla 的
		// error 只會說 "bad handshake"，沒有狀態碼就查不出是哪一種。
		if resp != nil {
			c.stage = fmt.Sprintf("dial（HTTP %d）", resp.StatusCode)
		}
		return err
	}
	c.conn = conn

	c.stage = "設定讀取逾時"
	if err := conn.SetReadDeadline(time.Now().Add(o.timeout)); err != nil {
		return err
	}

	c.stage = "送出 CONNECT"
	if err := writeFrame(conn, connectFrame(o.target, token)); err != nil {
		return err
	}
	c.stage = "等待 CONNECTED"
	frame, err := readFrame(conn)
	if err != nil {
		return err
	}
	if frame.Command != stomp.CommandConnected {
		return fmt.Errorf("期待 CONNECTED，收到 %s（%s）",
			frame.Command, frame.Headers.Get(stomp.HeaderMessage))
	}
	// 握手延遲量到 CONNECTED 為止：TCP + WebSocket upgrade + STOMP 握手
	// （含 JWT 驗章）。這是「一個玩家從按下連線到可以收訊息」的實際成本。
	c.connectDur = time.Since(start)

	c.stage = "送出 SUBSCRIBE"
	if err := writeFrame(conn, subscribeFrame(o.dest)); err != nil {
		return err
	}

	// 清掉讀取逾時。之後的訊息是稀疏的（每 -interval 才一則），
	// 留著逾時只會把正常的等待判成失敗；收工是靠主 goroutine 的 Close。
	c.stage = "清除讀取逾時"
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return err
	}
	c.stage = ""
	return nil
}

// failureKey 把一次失敗壓成可以統計的形狀。
//
// ⚠️ 不能直接拿 err.Error() 當 map 的 key：net 的錯誤字串裡帶著
// **本地埠號**，而每條連線的都不一樣。5000 條連線失敗 3000 條時，
// 統計就退化成「3000 種各一條」——資訊量是零，還把終端機洗掉三千行。
// （這是實測 5000 條那次真的發生的事。）
//
// 拆掉位址之後剩下的才是真正的分類依據：是 connection reset、
// 逾時，還是被伺服器拒絕。
func (c *client) failureKey() string {
	cause := c.err.Error()
	var opErr *net.OpError
	if errors.As(c.err, &opErr) {
		cause = fmt.Sprintf("%s %s：%v", opErr.Op, opErr.Net, opErr.Err)
	}
	if c.stage == "" {
		return cause
	}
	return c.stage + "：" + cause
}

// fanoutPayload 只挑出算延遲需要的兩個欄位。
//
// 其餘欄位刻意不解析：壓測的熱路徑上，少解析一點就少一點施壓端的成本，
// 而施壓端的成本會直接污染量到的延遲（co-located 壓測的老問題）。
type fanoutPayload struct {
	Type      string `json:"type"`
	UpdatedAt int64  `json:"updatedAt"`
}

func (c *client) readLoop() {
	for {
		frame, err := readFrame(c.conn)
		if err != nil {
			return // 連線被關掉（正常收工）或真的斷了，兩種都是結束
		}
		if frame.Command != stomp.CommandMessage {
			continue
		}

		var payload fanoutPayload
		if err := json.Unmarshal(frame.Body, &payload); err != nil {
			continue
		}
		// 濾掉 demo 產生器同時在發的 rank.update（type=GLOBAL_COINS）：
		// 它們的 updatedAt 是別人寫的，算進來就是在量別人的時鐘。
		if payload.Type != loadTestType {
			continue
		}
		c.latencies = append(c.latencies, time.Duration(time.Now().UnixNano()-payload.UpdatedAt))
	}
}

// ── 帧 ────────────────────────────────────────────────────────────────

func connectFrame(target, token string) *stomp.Frame {
	return &stomp.Frame{
		Command: stomp.CommandConnect,
		Headers: stomp.Headers{
			{Name: stomp.HeaderAcceptVersion, Value: "1.2"},
			{Name: stomp.HeaderHost, Value: hostOf(target)},
			// ⚠️ 一律談 0,0（不啟用心跳）。
			//
			// 談成非零的話，這條連線就必須有人定期送心跳出去，而那是**寫入**——
			// 於是每條連線要多一個 writer goroutine，同一條連線就變成兩個 writer
			// （讀取迴圈之外還有心跳），違反 gorilla 的規矩。
			// 壓測跑的時間遠短於任何合理的心跳逾時，關掉沒有代價。
			{Name: stomp.HeaderHeartBeat, Value: "0,0"},
			{Name: stomp.HeaderAuthorization, Value: "Bearer " + token},
		},
	}
}

func subscribeFrame(destination string) *stomp.Frame {
	return &stomp.Frame{
		Command: stomp.CommandSubscribe,
		Headers: stomp.Headers{
			{Name: stomp.HeaderID, Value: "sub-0"},
			{Name: stomp.HeaderDestination, Value: destination},
		},
	}
}

func writeFrame(conn *websocket.Conn, frame *stomp.Frame) error {
	// ⚠️ 一定要 TextMessage。送 BinaryMessage 的話，Spring 那邊會直接忽略，
	// 沒有錯誤訊息，只有「怎麼沒反應」。
	return conn.WriteMessage(websocket.TextMessage, frame.Marshal())
}

// readFrame 讀到一個真正的帧為止，中間的心跳（單一換行）略過。
func readFrame(conn *websocket.Conn) (*stomp.Frame, error) {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return nil, err
		}
		frame, err := stomp.Unmarshal(data)
		if errors.Is(err, stomp.ErrHeartbeat) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return frame, nil
	}
}

// hostOf 取出 URL 的 host，給 CONNECT 的 host header 用。
//
// 規格 1.2 說 CONNECT MUST 帶 host。兩邊實作其實都不檢查（Go 版刻意不查，
// 為的是與 Spring 行為一致），但施壓端沒有理由送一個不合規格的帧。
func hostOf(target string) string {
	u, err := url.Parse(target)
	if err != nil || u.Host == "" {
		return "localhost"
	}
	return u.Host
}

// ── JWT ───────────────────────────────────────────────────────────────

// mintToken 簽一張玩家 token，形狀與 cmd/demo 的 mintToken 一致。
//
// ⚠️ secret 取的是字串的**原始 UTF-8 位元組**，不做 base64 解碼——
// 兩邊實作都是這樣讀的（Java 是 secret.getBytes(UTF_8)）。
// 解讀方式差一點，簽章就永遠對不起來，而錯誤訊息只會說
// signature is invalid，完全指不到真因。
//
// subject 是十進位字串，理由同 cmd/demo：私人推播的索引鍵就是它。
func mintToken(secret string, playerID int64) (string, error) {
	now := time.Now()
	claims := jwt.RegisteredClaims{
		Subject:   strconv.FormatInt(playerID, 10),
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(tokenTTL)),
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
	if err != nil {
		return "", fmt.Errorf("簽發 token 失敗：%w", err)
	}
	return token, nil
}
