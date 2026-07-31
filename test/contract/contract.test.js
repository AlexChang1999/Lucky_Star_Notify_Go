/**
 * 跨語言黑箱契約測試。
 *
 * 同一份測試，對 Java 版（8087）與 Go 版（8088）都必須通過。
 * 兩邊都綠 = 兩個實作對外行為等價，而且是**有證據**的等價，不是宣稱的。
 *
 *   CONTRACT_TARGET=go   npm test        # 本 repo 的 deploy/docker-compose.yml
 *   CONTRACT_TARGET=java npm test        # 團隊 repo 的 notification-service
 *
 * 詳細跑法見同目錄的 README.md。
 *
 * # 測試邊界（為什麼只斷言這些）
 *
 * 斷言的都是**前端真的依賴**的東西：destination、body 的欄位與型別、
 * MESSAGE 帧的 subscription/message-id、以及鑑權失敗的反應。
 * 刻意**不**斷言的：CONNECTED 的 heart-beat 值（兩邊的心跳能力本來就
 * 可以不同，規格就是為此才要協商）、session id 格式、ERROR 帧的文字內容
 * （Java 是 Spring 產生的英文、Go 是自己寫的）。把那些寫進斷言等於把
 * 「不是契約的東西」當成契約，會讓測試在無害的差異上變紅。
 */

import test, { before, after } from 'node:test';
import assert from 'node:assert/strict';

import {
  config,
  runId,
  PLAYER_A,
  PLAYER_B,
  DEST_PRIVATE,
  DEST_BROADCAST,
  DEST_RANK,
  TOPIC_NOTIFICATION_PUSH,
  TOPIC_GAME_RESULT,
  TOPIC_RANK_UPDATE,
  mintToken,
  connect,
  connectExpectingRejection,
  createProducer,
  send,
} from './harness.js';

/** marker 讓每則事件都認得出是「這次執行的這個案例」發的。 */
const marker = (label) => `contract-${runId}-${label}`;

let producer;
/** alice 訂三個 destination；bob 只訂私人頻道，用來驗證私人推播不外洩。 */
let alice;
let bob;

before(async () => {
  console.error(
    `契約測試目標：${config.target}　WS=${config.wsTarget}　Kafka=${config.kafkaBroker}　runId=${runId}`,
  );
  producer = await createProducer();
  // 先連線訂閱、後發事件。兩邊都是 best-effort 推播、沒有補送，
  // 順序反過來就會偶發地收不到——而那種失敗看起來像實作的 bug。
  alice = await connect({
    playerId: PLAYER_A,
    subscriptions: [DEST_PRIVATE, DEST_BROADCAST, DEST_RANK],
    name: `alice(${PLAYER_A})`,
  });
  bob = await connect({
    playerId: PLAYER_B,
    subscriptions: [DEST_PRIVATE],
    name: `bob(${PLAYER_B})`,
  });
});

after(async () => {
  await Promise.allSettled([alice?.close(), bob?.close(), producer?.disconnect()]);
});

// ── 握手 ──────────────────────────────────────────────────────────────

test('CONNECT 成功後回 STOMP 1.2 的 CONNECTED', () => {
  assert.equal(alice.connectedFrame.headers.version, '1.2');
});

// ── game.result → 私人推播 ────────────────────────────────────────────

test('game.result 推給下注玩家，且 body 是重組過的通知（不是原事件）', async () => {
  const roundId = marker('game');
  await send(producer, TOPIC_GAME_RESULT, PLAYER_A, {
    roundId,
    playerId: PLAYER_A,
    gameType: 'slot',
    bet: 250,
    payout: 500,
    win: true,
    settledAt: '2026-07-31T04:05:06Z',
  });

  const message = await alice.waitFor((m) => m.json?.roundId === roundId, {
    what: 'game.result 的私人推播',
  });

  assert.equal(message.destination, DEST_PRIVATE);
  assert.deepEqual(message.json, {
    type: 'GAME_RESULT',
    roundId,
    gameType: 'slot',
    bet: 250,
    payout: 500,
    win: true,
    settledAt: '2026-07-31T04:05:06Z',
  });

  // ⚠️ 這兩條是整份測試最容易被實作違反的地方，所以獨立寫出來。
  // playerId 是**路由用的**，不是內容——原樣轉推是最直覺的寫法，
  // 但那不是 Java 版的行為。
  assert.ok(!('playerId' in message.json), 'game.result 的推播不該含 playerId');
  // type 必須在第一個：前端用 payload?.type === 'GAME_RESULT' 分流，
  // 順序錯不會壞掉，但它反映的是「這個物件是不是照契約組出來的」。
  assert.deepEqual(Object.keys(message.json), [
    'type',
    'roundId',
    'gameType',
    'bet',
    'payout',
    'win',
    'settledAt',
  ]);
});

test('game.result 缺 playerId 時被略過，不會廣播給任何人', async () => {
  const roundId = marker('game-noplayer');
  await send(producer, TOPIC_GAME_RESULT, null, {
    roundId,
    gameType: 'fishing',
    bet: 100,
    payout: 0,
    win: false,
    settledAt: '2026-07-31T04:05:07Z',
  });

  // 用一則「一定會到」的事件當同步點：它到了，代表前面那則若會被推
  // 也早該到了。單純睡固定秒數的話，機器一慢就變成假綠。
  const canary = marker('game-canary');
  await send(producer, TOPIC_GAME_RESULT, PLAYER_A, {
    roundId: canary,
    playerId: PLAYER_A,
    gameType: 'fishing',
    bet: 100,
    payout: 0,
    win: false,
    settledAt: '2026-07-31T04:05:08Z',
  });
  await alice.waitFor((m) => m.json?.roundId === canary, { what: '對照用的 game.result' });

  assert.ok(
    !alice.received.some((m) => m.json?.roundId === roundId),
    '缺 playerId 的 game.result 不該被推出去',
  );
});

test('私人推播不會外洩給其他玩家', async () => {
  const roundId = marker('isolation');
  await send(producer, TOPIC_GAME_RESULT, PLAYER_A, {
    roundId,
    playerId: PLAYER_A,
    gameType: 'baccarat',
    bet: 999,
    payout: 0,
    win: false,
    settledAt: '2026-07-31T04:05:09Z',
  });

  await alice.waitFor((m) => m.json?.roundId === roundId, { what: '給 alice 的私人推播' });
  await bob.expectNone((m) => m.json?.roundId === roundId, {
    what: '本來要給 alice 的私人推播',
  });
});

// ── notification.push → 私人 / 廣播 ───────────────────────────────────

test('notification.push 帶 targetPlayerId → 只進該玩家的私人頻道，原事件回吐', async () => {
  const title = marker('push-private');
  const event = {
    targetPlayerId: PLAYER_A,
    type: 'REWARD',
    title,
    message: '你領到了 100 金幣',
    payload: { coins: 100 },
  };
  await send(producer, TOPIC_NOTIFICATION_PUSH, PLAYER_A, event);

  const message = await alice.waitFor((m) => m.json?.title === title, {
    what: 'notification.push 的私人推播',
  });

  assert.equal(message.destination, DEST_PRIVATE);
  // 與 game.result 不同，這個 topic 是**原事件回吐**——三個 topic 的形狀
  // 不一致本身就是契約的一部分，統一它們會與 Java 版不等價。
  assert.deepEqual(message.json, event);
});

test('notification.push 不帶 targetPlayerId → 廣播到 /topic/notifications，null 欄位照樣寫出來', async () => {
  const title = marker('push-broadcast');
  const event = {
    targetPlayerId: null,
    type: 'ANNOUNCEMENT',
    title,
    message: '伺服器將於 03:00 進行例行維護',
    payload: null,
  };
  await send(producer, TOPIC_NOTIFICATION_PUSH, null, event);

  const message = await alice.waitFor((m) => m.json?.title === title, {
    what: 'notification.push 的廣播',
  });

  assert.equal(message.destination, DEST_BROADCAST);
  assert.deepEqual(message.json, event);
  // null 必須是「欄位存在且為 null」，不是「欄位不見了」。
  // 前端寫的是 payload?.coins，欄位消失與值為 null 對它一樣；
  // 但對「兩邊輸出等價」不一樣，而等價正是這份測試要證明的事。
  assert.ok('payload' in message.json, 'payload 為 null 時欄位仍要在');
  assert.ok('targetPlayerId' in message.json, 'targetPlayerId 為 null 時欄位仍要在');
});

// ── rank.update → 廣播 ────────────────────────────────────────────────

test('rank.update 廣播到 /topic/rank，原事件回吐', async () => {
  const type = marker('rank');
  const event = {
    type,
    entries: [
      { playerId: PLAYER_A, nickname: '阿明', score: 12345, rank: 1 },
      { playerId: PLAYER_B, nickname: '小美', score: 6789, rank: 2 },
    ],
    // 用一個超過 2^31 的值：Java 那邊是 long，若哪一邊當成 int 處理
    // 會溢位。JS 的 Number 到 2^53 都是精確的，所以斷言本身是可靠的。
    updatedAt: 1753920000000,
  };
  await send(producer, TOPIC_RANK_UPDATE, null, event);

  const message = await alice.waitFor(
    (m) => m.destination === DEST_RANK && m.json?.type === type,
    { what: 'rank.update 的廣播' },
  );

  assert.deepEqual(message.json, event);
  assert.equal(typeof message.json.updatedAt, 'number');
});

// ── STOMP 帧層面的契約 ────────────────────────────────────────────────

test('MESSAGE 帧一定帶 subscription 與 message-id', async () => {
  const type = marker('headers');
  await send(producer, TOPIC_RANK_UPDATE, null, { type, entries: [], updatedAt: 1 });

  const message = await alice.waitFor(
    (m) => m.destination === DEST_RANK && m.json?.type === type,
    { what: '用來檢查 header 的 rank.update' },
  );

  // ⚠️ @stomp/stompjs 是用 subscription 的值比對自己的訂閱表，對不上就
  // **靜默丟棄**。漏掉這個 header 的服務，症狀是「client 什麼都收不到，
  // 而且兩邊都沒有錯誤訊息」。這條斷言是整份測試裡最便宜也最值得的一條。
  assert.ok(message.headers.subscription, 'MESSAGE 必須帶 subscription header');
  assert.ok(message.headers['message-id'], 'MESSAGE 必須帶 message-id header');
  assert.equal(message.headers.destination, DEST_RANK);
});

test('JSON 不做 HTML 轉義，非 ASCII 也不轉義（逐位元組等價）', async () => {
  const title = marker('escaping');
  const message = '限時活動 A&B <b>加倍</b>「開跑」';
  await send(producer, TOPIC_NOTIFICATION_PUSH, null, {
    targetPlayerId: null,
    type: 'ANNOUNCEMENT',
    title,
    message,
    payload: null,
  });

  const received = await alice.waitFor((m) => m.json?.title === title, {
    what: '含特殊字元的廣播',
  });

  // 這裡刻意看 **body 原文**而不是 JSON.parse 的結果：兩種寫法
  // parse 之後完全相同，位元組卻不同。Go 的 encoding/json 預設會把
  // < > & 轉成 < 這種形式，Jackson 不會——不處理的話兩邊的
  // 位元組永遠對不上，而任何「解析後比對」的測試都抓不到。
  assert.ok(
    received.body.includes('A&B <b>加倍</b>'),
    `body 應保留原字元，實際收到：${received.body}`,
  );
  assert.ok(!received.body.includes('\\u003c'), 'body 不該把 < 轉義成 \\u003c');
  assert.ok(!received.body.includes('\\u0026'), 'body 不該把 & 轉義成 \\u0026');
  assert.ok(received.body.includes('「開跑」'), '非 ASCII 字元應原樣輸出，不轉成 \\uXXXX');
});

// ── 鑑權 ──────────────────────────────────────────────────────────────

for (const [name, makeToken] of [
  ['簽章不對', () => mintToken({ playerId: PLAYER_A, secret: 'a-totally-different-secret-at-least-32b' })],
  ['已過期', () => mintToken({ playerId: PLAYER_A, ttlSeconds: -60 })],
  ['alg 是 none（演算法混淆攻擊）', () => mintToken({ playerId: PLAYER_A, alg: 'none' })],
  ['根本不是 JWT', () => 'not-a-jwt-at-all'],
  ['完全沒帶 Authorization', () => null],
]) {
  test(`CONNECT 帶著${name}的憑證會被拒絕並斷線`, async () => {
    const result = await connectExpectingRejection({ token: makeToken() });

    assert.equal(result.connected, false, '不該握手成功');
    assert.ok(result.errorFrame, '應該收到 STOMP ERROR 帧');
    // 規格明訂伺服器送出 ERROR 之後 MUST 關閉連線。只回 ERROR 卻留著連線，
    // 等於留著一條沒有身分的連線佔資源。
    assert.equal(result.closed, true, 'ERROR 之後連線必須被關閉');
  });
}
