# Phase 0：前端脫離 SockJS

> **目的**：驗證整個計畫的前提——「Java 版的原生 WebSocket 端點接得住
> `@stomp/stompjs` 的 `brokerURL` 連線」。前提成立，Go 版就**不必實作
> SockJS 降級協議**（見 [ADR-002 §3](./ADR-002-STOMP-子集的取捨.md)）。
>
> 成本半天，避免的風險是整個專案——所以它被排在 Phase 0，寫任何 Go 之前。

---

## 1. 前提已驗證（不是推測）

團隊 repo 自己就有現成的證據：`WebSocketStompIntegrationTest` 使用
`StandardWebSocketClient`（**原生 WebSocket，無 SockJS**）連
`ws://localhost:{port}/ws`。

實測結果（2026-07-30，`mvn -B test -Dtest=WebSocketStompIntegrationTest`）：

```
Tests run: 3, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

三個案例涵蓋：

| 案例 | 驗到什麼 |
|---|---|
| `validJwt_connectsSubscribesAndReceivesPrivateMessage` | 原生 WS 握手 + JWT 鑑權 + 訂閱 `/user/queue/notifications` + **收到私人推播** |
| `missingJwt_connectionIsRejected` | 缺 JWT 被攔截器拒絕 |
| `invalidJwt_connectionIsRejected` | 爛 JWT 被拒絕 |

> 這個測試**不需要 Kafka**：`src/test/resources/application.yml` 設了
> `spring.kafka.listener.auto-startup: false`，所以可以完全離線重跑。

**結論**：Java 版的原生端點行為與 SockJS 端點等價，Phase 0 的前提成立。

---

## 2. 前端改動

改的是團隊 repo 的 `frontend/src/hooks/useWebSocket.js`。
**本 repo 不放團隊程式碼，只放 patch**：

```
docs/phase0-useWebSocket-native-ws.patch
```

### 套用方式

```bash
cd <團隊 repo 根目錄>
git apply docs/... 的絕對路徑/phase0-useWebSocket-native-ws.patch
```

patch 已在乾淨副本上驗證過 `git apply --check` 與 round-trip 位元組相同。

> ⚠️ 這個 patch 的 hunk 內容行帶著 **CRLF**（團隊 repo 是 Windows checkout）。
> 本 repo 的 `.gitattributes` 因此對 `*.patch` 設了 `-text`，避免 LF 正規化
> 把 CR 洗掉——被洗掉的話 `git apply` 只會說 "patch does not apply"，
> 完全指不到「行尾被改了」這個真因。

### 改了什麼

1. **新增 feature flag**：`VITE_WS_TRANSPORT`
   - 未設定或任何其他值 → **SockJS（維持原行為）**
   - `native` → 原生 WebSocket（`brokerURL`）

   預設維持 SockJS 是刻意的：**對團隊既有 demo 是零風險**，要切換才需要設
   環境變數。這也是計畫書 §9 風險表列的緩解措施。

2. **新增 `toWsUrl()`**，與既有的 `normalizeSockJsUrl()` 方向相反：

   | 傳輸 | 需要的 scheme | 既有／新增 |
   |---|---|---|
   | SockJS | `http(s)://`（傳輸協商走普通 HTTP 請求 `/ws/info`） | 既有 `normalizeSockJsUrl` |
   | 原生 WS | `ws(s)://` | 新增 `toWsUrl` |

3. **傳輸選項二選一**：`brokerURL` 或 `webSocketFactory`，其餘 Client 設定
   （`connectHeaders`、重連、訂閱）完全不動。

### 兩個容易踩的細節

1. **必須組出絕對 URL**。`VITE_WS_URL` 預設是相對路徑 `/ws`，而瀏覽器對
   相對路徑 WebSocket 的解析各家行為有差異——用 `window.location` 補上
   origin，不要賭。
2. **protocol 判斷要同時認 `https:` 與 `wss:`**。只判斷 `https:` 的話，
   已經是 `wss://` 的網址會被降級成 `ws://`，在 HTTPS 頁面下會被混合內容
   政策直接擋掉。

實測（Node 驗過的 5 個案例）：

| 輸入 | 頁面 origin | 輸出 |
|---|---|---|
| `/ws` | `http://localhost:5173` | `ws://localhost:5173/ws` |
| `/ws` | `https://app.example.com` | `wss://app.example.com/ws` |
| `ws://localhost:8088/ws` | `http://localhost:5173` | `ws://localhost:8088/ws` |
| `wss://x.example.com/ws` | `https://app.example.com` | `wss://x.example.com/ws` |
| `http://localhost:8087/ws` | `http://localhost:5173` | `ws://localhost:8087/ws` |

### 格式檢查

patch 是**格式中性**的：用團隊的 `.prettierrc`（`printWidth: 100`）檢查，
原檔與改後檔案得到**相同的**結果（原檔本身就不是 prettier-clean，唯一的差異
是既有的 import 換行，與本次改動無關）。

---

## 3. 尚未完成的部分

DoD 還有一項沒收：**「登入 → 玩一局 → 收得到 `GAME_RESULT` 推播」的完整
端到端驗證**。

- **卡在哪**：那條路徑需要整套 stack（gateway + member 登入 + game 下注 +
  Kafka + notification），而本機 Docker daemon 當時沒有執行。
- **已經有的替代證據**：上面 §1 的 Java 整合測試已證明「原生 WS + 訂閱 +
  收到私人推播」這條鏈；缺的只有「事件由真的 game-service 產生」這一段。
- **要補的話怎麼做**：

  ```bash
  # 1. 起團隊 repo 的 stack
  docker compose up -d

  # 2. 前端切成原生 WebSocket
  #    frontend/.env.local
  VITE_WS_TRANSPORT=native
  VITE_ENABLE_WS=true
  VITE_USE_MOCK_API=false
  VITE_USE_MOCK_WS=false

  # 3. 登入 → 玩一局 → 看通知是否跳出
  # 4. 對照組：把 VITE_WS_TRANSPORT 拿掉（回 SockJS），行為應相同
  ```

- **要換成 Go 版**（Phase 4/5 的 A/B 對照）：改團隊 repo 本機 `.env` 的
  `NOTIFICATION_SERVICE_URL=http://notify-go:8088`，然後
  `docker compose up -d gateway-service`。**不改任何 Java 程式碼、不 commit。**

---

## 4. 對團隊 repo 的紀律

團隊 repo 對本專案是**唯讀參考**（`AGENTS.md` §2.8）：

- 本次**沒有**修改團隊 repo 的任何檔案，改動只以 patch 形式留在本 repo
- 跑了它的測試（`mvn test`，只產生 gitignored 的 `target/`）
- 不提交 commit、不開 PR
