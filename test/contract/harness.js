/**
 * 契約測試的共用工具：發 token、連 STOMP、灌 Kafka、等訊息。
 *
 * # 這個檔案為什麼不能認識 Java 或 Go
 *
 * 整份契約測試的價值來自「同一份測試，兩個實作都通過」。只要出現一行
 * 「如果是 Go 就……」，證明力就沒了——那變成兩份測試，而不是一份契約。
 * 所以這裡只認三個環境變數（WS_TARGET / KAFKA_BROKER / JWT_SECRET），
 * CONTRACT_TARGET 只是幫忙填預設埠號的便利設定，不改變任何斷言。
 */

import { createHmac, randomUUID } from 'node:crypto';
import { Client } from '@stomp/stompjs';
import { Kafka, logLevel } from 'kafkajs';

// ── 目標設定 ──────────────────────────────────────────────────────────

/**
 * 兩個實作的**埠號**預設值。只有埠號，沒有 secret。
 *
 * secret 刻意不放進 preset：團隊 repo 的那把是別人 repo 的東西，
 * 抄進這個公開 repo 就是把它散佈出去。跑 Java 版時自己帶 JWT_SECRET。
 */
const PRESETS = {
  go: { wsTarget: 'ws://localhost:8088/ws', kafkaBroker: 'localhost:9094' },
  java: { wsTarget: 'ws://localhost:8087/ws', kafkaBroker: 'localhost:9092' },
};

const preset = PRESETS[process.env.CONTRACT_TARGET ?? 'go'];
if (!preset) {
  throw new Error(
    `CONTRACT_TARGET 只能是 ${Object.keys(PRESETS).join(' / ')}，收到 ${process.env.CONTRACT_TARGET}`,
  );
}

export const config = {
  target: process.env.CONTRACT_TARGET ?? 'go',
  wsTarget: process.env.WS_TARGET ?? preset.wsTarget,
  kafkaBroker: process.env.KAFKA_BROKER ?? preset.kafkaBroker,
  // 預設是本 repo demo compose 裡那把。跑 Java 版時**必須**自己帶。
  jwtSecret: process.env.JWT_SECRET ?? 'lucky-star-demo-secret-do-not-use-in-production',
  // 15 秒。Kafka → consumer → hub → WebSocket 這一整條在本機是毫秒級，
  // 但 CI 的容器冷啟動後第一則會慢一點，留寬一些不影響失敗案例的速度
  // （失敗案例等的是「不該收到」，用的是另一個更短的 window）。
  timeoutMs: Number(process.env.CONTRACT_TIMEOUT_MS ?? 15000),
  debug: process.env.CONTRACT_DEBUG === 'true',
};

/** runId 讓每次執行的事件內容都獨一無二，才能從別人的流量裡認出自己的。 */
export const runId = randomUUID().slice(0, 8);

/**
 * 契約測試專用的玩家 ID。
 *
 * 刻意避開 demo 產生器用的 1001 / 1002：那支程式每 3 秒就對它們推一則，
 * 私人頻道會混進不屬於這次測試的訊息。
 */
export const PLAYER_A = 70001;
export const PLAYER_B = 70002;

export const DEST_PRIVATE = '/user/queue/notifications';
export const DEST_BROADCAST = '/topic/notifications';
export const DEST_RANK = '/topic/rank';

export const TOPIC_NOTIFICATION_PUSH = 'notification.push';
export const TOPIC_GAME_RESULT = 'game.result';
export const TOPIC_RANK_UPDATE = 'rank.update';

// ── JWT ───────────────────────────────────────────────────────────────

const b64url = (input) => Buffer.from(input).toString('base64url');

/**
 * 簽一張 HS256 的玩家 token。
 *
 * ⚠️ secret 用的是**字串的原始 UTF-8 位元組**，不是 base64 解碼結果。
 * Java 版是 Keys.hmacShaKeyFor(secret.getBytes(UTF_8))，Go 版是
 * []byte(os.Getenv("JWT_SECRET"))。網路上多數 JWT 範例會示範
 * base64.decode(secret)，照抄的話兩邊都會驗不過，而錯誤訊息只會說
 * 「signature is invalid」，完全指不到真因。
 *
 * @param {object} opts
 * @param {number|string} opts.playerId  放進 sub 的玩家 ID
 * @param {number} [opts.ttlSeconds]     有效秒數；給負值就簽出一張過期的
 * @param {string} [opts.secret]         簽章用的 secret，預設用設定裡那把
 * @param {string} [opts.alg]            寫進 header 的 alg，攻擊案例用
 */
export function mintToken({ playerId, ttlSeconds = 300, secret = config.jwtSecret, alg = 'HS256' }) {
  const now = Math.floor(Date.now() / 1000);
  const header = b64url(JSON.stringify({ alg, typ: 'JWT' }));
  const payload = b64url(
    JSON.stringify({ sub: String(playerId), iat: now, exp: now + ttlSeconds }),
  );
  const signingInput = `${header}.${payload}`;

  // alg: none 的 token 依規格沒有簽章段，但那三個點分段仍然要在。
  // 這是最經典的 JWT 漏洞：驗證端若照著 header 說的 alg 走，就等於
  // 讓攻擊者自己決定「這張票要不要驗」。
  if (alg === 'none') {
    return `${signingInput}.`;
  }

  const signature = createHmac('sha256', Buffer.from(secret, 'utf8'))
    .update(signingInput)
    .digest();
  return `${signingInput}.${b64url(signature)}`;
}

// ── STOMP ─────────────────────────────────────────────────────────────

/**
 * 一條已握手的連線，外加一個「收到什麼」的信箱。
 *
 * 為什麼要自己做信箱而不是每個測試各自 subscribe：兩邊實作都是
 * best-effort 推播，**沒有補送**。訂閱一定要早於發事件，否則會偶發地
 * 抓不到——而那種失敗看起來會像實作的 bug。所以連線一建立就把三個
 * destination 都訂好，訊息先進信箱，測試再回頭撈。
 */
class Session {
  #waiters = new Set();

  constructor(name, client) {
    this.name = name;
    this.client = client;
    /** @type {{destination: string, headers: object, body: string, json: any}[]} */
    this.received = [];
  }

  /** @internal 由 stompjs 的訂閱回呼呼叫。 */
  _deliver(destination, frame) {
    let json;
    try {
      json = JSON.parse(frame.body);
    } catch {
      json = undefined; // 非 JSON 的 body 讓斷言自己去看 body 原文
    }
    const message = { destination, headers: frame.headers, body: frame.body, json };
    this.received.push(message);
    for (const waiter of [...this.#waiters]) {
      if (waiter.match(message)) {
        this.#waiters.delete(waiter);
        waiter.settle(message);
      }
    }
  }

  /**
   * 等一則符合 match 的訊息。已經收到的也算數（先掃信箱再掛等待）。
   *
   * @param {(m: {destination: string, headers: object, body: string, json: any}) => boolean} match
   * @param {object} [opts]
   * @param {string} [opts.what]       逾時訊息裡的描述，出錯時才看得懂在等什麼
   * @param {number} [opts.timeoutMs]
   */
  waitFor(match, { what = '訊息', timeoutMs = config.timeoutMs } = {}) {
    const already = this.received.find(match);
    if (already) {
      return Promise.resolve(already);
    }
    return new Promise((resolve, reject) => {
      const waiter = { match, settle: null };
      const timer = setTimeout(() => {
        this.#waiters.delete(waiter);
        reject(
          new Error(
            `[${this.name}] 等待「${what}」逾時（${timeoutMs}ms）。` +
              `這條連線期間共收到 ${this.received.length} 則：` +
              JSON.stringify(this.received.map((m) => m.destination)),
          ),
        );
      }, timeoutMs);
      waiter.settle = (message) => {
        clearTimeout(timer);
        resolve(message);
      };
      this.#waiters.add(waiter);
    });
  }

  /**
   * 斷言在 windowMs 內**沒有**收到符合 match 的訊息。
   *
   * 用來測「私人推播不會外洩給別的玩家」。這種斷言天生只能是
   * 「等一段時間都沒發生」，所以窗口要夠長到訊息真的會到、
   * 又不能長到讓整份測試變慢——這裡用的是「對照組已經收到了」
   * 之後再等，等於拿正向訊息當同步點，比空等一個固定秒數可靠。
   */
  async expectNone(match, { what = '訊息', windowMs = 1500 } = {}) {
    await new Promise((resolve) => setTimeout(resolve, windowMs));
    const leaked = this.received.find(match);
    if (leaked) {
      throw new Error(
        `[${this.name}] 不該收到「${what}」，但收到了：${leaked.destination} ${leaked.body}`,
      );
    }
  }

  async close() {
    await this.client.deactivate();
  }
}

/** token 為 null 代表**完全不送** Authorization header（測「沒帶憑證」）。 */
function newClient(token) {
  return new Client({
    brokerURL: config.wsTarget,
    // 兩邊實作都是讀 STOMP CONNECT 帧的 Authorization header，
    // **不是** HTTP header——瀏覽器的 WebSocket API 根本不能設 HTTP header，
    // 這是 STOMP over WebSocket 一律把鑑權放進 CONNECT 帧的原因。
    connectHeaders: token == null ? {} : { Authorization: `Bearer ${token}` },
    // 0 = 關掉自動重連。不關的話，「爛 token 應該被拒絕」那幾個案例
    // 會變成無限重連迴圈，測試不會失敗只會卡住。
    reconnectDelay: 0,
    heartbeatIncoming: 10000,
    heartbeatOutgoing: 10000,
    debug: config.debug ? (msg) => console.error(`[stomp] ${msg}`) : () => {},
  });
}

/**
 * 連上並訂閱指定的 destination，回傳已就緒的 Session。
 *
 * 訂閱是在 onConnect 裡做的：STOMP 規格要求 SUBSCRIBE 必須在 CONNECTED
 * 之後才送，提早送會被當成協議錯誤。
 */
export function connect({ playerId, token = mintToken({ playerId }), subscriptions = [], name }) {
  return new Promise((resolve, reject) => {
    const client = newClient(token);
    const session = new Session(name ?? `player-${playerId}`, client);

    const timer = setTimeout(() => {
      void client.deactivate();
      reject(new Error(`[${session.name}] CONNECT 逾時（${config.timeoutMs}ms）：${config.wsTarget}`));
    }, config.timeoutMs);

    client.onConnect = (frame) => {
      clearTimeout(timer);
      session.connectedFrame = frame;
      for (const destination of subscriptions) {
        client.subscribe(destination, (message) => session._deliver(destination, message));
      }
      resolve(session);
    };
    client.onStompError = (frame) => {
      clearTimeout(timer);
      void client.deactivate();
      reject(new Error(`[${session.name}] 握手被拒：${frame.headers.message} / ${frame.body}`));
    };
    client.onWebSocketError = (event) => {
      clearTimeout(timer);
      reject(new Error(`[${session.name}] WebSocket 錯誤：${event?.message ?? event}`));
    };

    client.activate();
  });
}

/**
 * 用一張**應該被拒絕**的 token 連線，回傳伺服器的反應。
 *
 * 斷言的是兩件事而不是一件：收到 ERROR 帧、而且連線被關掉。
 * 只檢查 ERROR 是不夠的——規格明訂送出 ERROR 之後伺服器 MUST 關閉連線，
 * 若只回 ERROR 卻留著連線，等於留著一條沒有身分的連線在那裡。
 *
 * @returns {Promise<{errorFrame: object|null, closed: boolean, connected: boolean}>}
 */
export function connectExpectingRejection({ token }) {
  return new Promise((resolve, reject) => {
    const client = newClient(token);
    const result = { errorFrame: null, closed: false, connected: false };

    const timer = setTimeout(() => {
      void client.deactivate();
      // 逾時本身就是有意義的結果：既沒被拒也沒連上，這在契約上是失敗。
      resolve(result);
    }, config.timeoutMs);

    const finish = () => {
      clearTimeout(timer);
      void client.deactivate();
      resolve(result);
    };

    client.onStompError = (frame) => {
      result.errorFrame = { headers: frame.headers, body: frame.body };
      // 不在這裡 finish：還要等連線真的被關掉，那才是完整的契約。
    };
    client.onWebSocketClose = () => {
      result.closed = true;
      finish();
    };
    client.onConnect = () => {
      result.connected = true;
      finish();
    };
    client.onWebSocketError = (event) => {
      clearTimeout(timer);
      reject(new Error(`WebSocket 錯誤：${event?.message ?? event}`));
    };

    client.activate();
  });
}

// ── Kafka ─────────────────────────────────────────────────────────────

/**
 * 濾掉 kafkajs 的 TimeoutNegativeWarning。
 *
 * 成因是它自己的 bug（requestQueue/index.js 的 scheduleCheckPendingRequests）：
 * throttledUntil 的初始值是 -1，待處理佇列是空的時候那個「夾成正數」的分支
 * 不會執行，於是 -1 - Date.now() 這個很大的負數直接被丟給 setTimeout。
 * Node 會夾成 1ms 並印一行警告——行為完全正常，但那行字夾在測試輸出裡
 * 會讓人以為是我們的問題。
 *
 * ⚠️ 只濾掉這一種，其餘警告原封不動轉給原本的處理器。
 * 用 --no-warnings 或無條件 return 才是真的危險：之後任何一個
 * DeprecationWarning、ExperimentalWarning 都會跟著消失。
 */
function silenceKafkaJsTimeoutWarning() {
  const original = process.listeners('warning');
  process.removeAllListeners('warning');
  process.on('warning', (warning) => {
    if (warning.name === 'TimeoutNegativeWarning') {
      return;
    }
    for (const listener of original) {
      listener(warning);
    }
  });
}
silenceKafkaJsTimeoutWarning();

/**
 * 建一個 producer。扮演的是 game-service / rank-service 那一端。
 *
 * logLevel.NOTHING：kafkajs 預設會把一堆 INFO 印進 stderr，混在測試輸出
 * 裡很難看。真的要查連線問題時設 CONTRACT_DEBUG=true。
 */
export async function createProducer() {
  const kafka = new Kafka({
    clientId: `notify-contract-${runId}`,
    brokers: [config.kafkaBroker],
    logLevel: config.debug ? logLevel.INFO : logLevel.NOTHING,
    retry: { retries: 5 },
  });
  const producer = kafka.producer({ allowAutoTopicCreation: true });
  await producer.connect();
  return producer;
}

/**
 * 發一則事件。
 *
 * key 用玩家 ID：真實 producer 這樣做是為了讓同一個玩家的事件落在同一個
 * partition、保住順序。廣播事件沒有這個需求，key 給 null。
 */
export async function send(producer, topic, key, event) {
  await producer.send({
    topic,
    messages: [{ key: key == null ? null : String(key), value: JSON.stringify(event) }],
  });
}
