import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import http from 'node:http';
import net from 'node:net';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import r from 'rethinkdb';
import { createApp } from '../src/app.js';
import { loadConfig } from '../src/config.js';
import { TABLES, connect, ensureSchema } from '../src/db.js';

const PROJECT_DIR = fileURLToPath(new URL('..', import.meta.url));

// The suite owns one whole database: it is dropped and recreated before the tests and
// dropped after them. The name must end in "_test" and differ from the application
// database, so the default "bidding" database can never be touched.
export function testDatabaseName(env = process.env) {
  const name = env.RETHINKDB_TEST_DB || 'bidding_test';
  if (!/^[A-Za-z0-9_]+_test$/.test(name)) {
    throw new Error(`Refusing to use "${name}" as the test database: RETHINKDB_TEST_DB must end with "_test".`);
  }
  if (name === loadConfig(env).db.name) {
    throw new Error(`Refusing to use "${name}" as the test database: it is the application database.`);
  }
  return name;
}

export const TEST_DB = testDatabaseName();
export const dbConfig = loadConfig({ ...process.env, RETHINKDB_DB: TEST_DB }).db;
export const auctions = r.db(TEST_DB).table(TABLES.auctions);
export const history = r.db(TEST_DB).table(TABLES.bids);

export const TOP_BID_FIELDS = ['amount', 'bid_id', 'placed_at', 'seq', 'user_id'];
export const HISTORY_FIELDS = ['amount', 'auction_id', 'id', 'placed_at', 'seq', 'user_id'];
export const RESPONSE_BID_FIELDS = ['id', 'auction_id', 'user_id', 'amount', 'seq', 'placed_at'];
export const GENERIC_500 = { error: 'INTERNAL_ERROR', message: 'An unexpected error occurred.' };

const connections = new Set();

export async function openConnection() {
  const conn = await connect(dbConfig);
  connections.add(conn);
  return conn;
}

export async function closeConnections() {
  await Promise.all([...connections].map((conn) => conn.close()));
}

export const openConnectionCount = () => [...connections].filter((conn) => conn.isOpen()).length;

export async function resetTestDatabase(conn) {
  await dropTestDatabase(conn);
  await ensureSchema(conn, TEST_DB);
}

export async function dropTestDatabase(conn) {
  assert.equal(testDatabaseName(), TEST_DB, 'the test database name changed during the run');
  if (await r.dbList().contains(TEST_DB).run(conn)) {
    await r.dbDrop(TEST_DB).run(conn);
  }
}

// Independent app instances, each with its own RethinkDB connection, so concurrent
// requests reach the database the way they would from several servers.
export async function startApps(count) {
  const stats = { inFlight: 0, maxInFlight: 0 };
  const instances = [];
  for (let i = 0; i < count; i += 1) {
    const conn = await openConnection();
    const app = createApp({ conn, dbName: TEST_DB });
    const server = http.createServer((req, res) => {
      stats.inFlight += 1;
      stats.maxInFlight = Math.max(stats.maxInFlight, stats.inFlight);
      res.once('close', () => {
        stats.inFlight -= 1;
      });
      app(req, res);
    });
    server.listen(0, '127.0.0.1');
    await once(server, 'listening');
    instances.push({ conn, server, url: `http://127.0.0.1:${server.address().port}` });
  }
  return { instances, stats, url: instances[0].url };
}

export async function stopApps({ instances }) {
  for (const { conn, server } of instances) {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    await conn.close();
  }
}

export async function request(baseUrl, { method = 'POST', path = '/bid', json, body, headers = {} } = {}) {
  const init = { method, headers: { ...headers } };
  if (json !== undefined) {
    init.body = JSON.stringify(json);
    init.headers['content-type'] = 'application/json';
  } else if (body !== undefined) {
    init.body = body;
  }
  const response = await fetch(`${baseUrl}${path}`, init);
  const text = await response.text();
  let parsed = null;
  try {
    parsed = JSON.parse(text);
  } catch {}
  return { status: response.status, body: parsed, text, headers: response.headers };
}

export const postBid = (baseUrl, auctionId, userId, amount) =>
  request(baseUrl, { json: { auction_id: auctionId, user_id: userId, amount } });

let idCounter = 0;
export function uniqueId(label) {
  idCounter += 1;
  return `${label}-${idCounter}`;
}

export async function createAuction(conn, { id = uniqueId('auction'), startingPrice = 1000, endsAt = r.now().add(3600) } = {}) {
  await auctions
    .insert({ id, title: `Test ${id}`, starting_price: startingPrice, ends_at: endsAt, created_at: r.now(), bid_count: 0, top_bid: null })
    .run(conn);
  return id;
}

export const getAuction = (conn, auctionId) => auctions.get(auctionId).run(conn);

export const getHistory = (conn, auctionId) =>
  history
    .between([auctionId, r.minval], [auctionId, r.maxval], { index: 'auction_seq' })
    .orderBy({ index: 'auction_seq' })
    .coerceTo('array')
    .run(conn);

// A top_bid, history row or API bid in the API's shape, with placed_at as an ISO string.
export function asApiBid(auctionId, bid) {
  const placedAt = bid.placed_at instanceof Date ? bid.placed_at.toISOString() : bid.placed_at;
  return { id: bid.id ?? bid.bid_id, auction_id: auctionId, user_id: bid.user_id, amount: bid.amount, seq: bid.seq, placed_at: placedAt };
}

// Invariants every auction must satisfy at rest. With complete, history must also hold
// exactly one row for each accepted bid (seq 1..bid_count).
export async function assertAuctionConsistent(conn, auctionId, { complete = true } = {}) {
  const auction = await getAuction(conn, auctionId);
  const rows = await getHistory(conn, auctionId);
  const top = auction.top_bid;
  if (top === null) {
    assert.equal(auction.bid_count, 0, 'no top bid means no accepted bids');
  } else {
    assert.deepEqual(Object.keys(top).sort(), TOP_BID_FIELDS, 'top_bid has exactly the five approved fields');
    assert.equal(top.seq, auction.bid_count, 'the top bid is the latest accepted bid');
    assert.equal(top.bid_id, `${auctionId}:${top.amount}`);
    assert.ok(top.placed_at < auction.ends_at, 'the top bid was placed before ends_at');
  }
  rows.forEach((row, i) => {
    assert.deepEqual(Object.keys(row).sort(), HISTORY_FIELDS, 'history rows have exactly six fields');
    assert.equal(row.id, `${auctionId}:${row.amount}`);
    assert.ok(Number.isInteger(row.seq) && row.seq >= 1 && row.seq <= auction.bid_count, `seq ${row.seq} is within 1..bid_count`);
    assert.ok(row.placed_at < auction.ends_at, 'history bids were placed before ends_at');
    if (i === 0) {
      assert.ok(row.amount >= auction.starting_price, 'the first bid is at least the starting price');
    } else {
      assert.ok(row.seq > rows[i - 1].seq, 'seq values are unique');
      assert.ok(row.amount > rows[i - 1].amount, 'amounts strictly increase with seq');
    }
  });
  if (top !== null && rows.at(-1)?.seq === top.seq) {
    assert.deepEqual(asApiBid(auctionId, rows.at(-1)), asApiBid(auctionId, top), 'the latest history row is the top bid');
  }
  if (complete) {
    assert.deepEqual(rows.map((row) => row.seq), range(1, auction.bid_count), 'history holds every accepted bid');
  }
  return { auction, rows };
}

// Renames the history table while work() runs so the app's history writes really fail,
// then restores it.
export async function withHistoryTableOffline(conn, work) {
  const db = r.db(TEST_DB);
  await db.table(TABLES.bids).config().update({ name: 'bids_offline' }).run(conn);
  await db.table('bids_offline').wait({ timeout: 30 }).run(conn);
  try {
    return await work();
  } finally {
    await db.table('bids_offline').config().update({ name: TABLES.bids }).run(conn);
    await db.table(TABLES.bids).wait({ timeout: 30 }).run(conn);
  }
}

const IPC_SIGINT_PRELOAD = `data:text/javascript,${encodeURIComponent(
  "process.on('message', (m) => { if (m === 'SIGINT') { process.disconnect(); process.emit('SIGINT'); } }); process.channel?.unref();",
)}`;

// Runs src/server.js in a child process. With sigintOverIpc, child.send('SIGINT') makes the
// server emit SIGINT to itself, because Windows cannot deliver POSIX signals to another process.
export function spawnServer(env, { sigintOverIpc = false } = {}) {
  const args = sigintOverIpc ? ['--import', IPC_SIGINT_PRELOAD, 'src/server.js'] : ['src/server.js'];
  const child = spawn(process.execPath, args, {
    cwd: PROJECT_DIR,
    env: { ...process.env, ...env },
    stdio: sigintOverIpc ? ['ignore', 'pipe', 'pipe', 'ipc'] : ['ignore', 'pipe', 'pipe'],
  });
  let output = '';
  child.stdout.on('data', (chunk) => {
    output += chunk;
  });
  child.stderr.on('data', (chunk) => {
    output += chunk;
  });
  return {
    child,
    exited: once(child, 'exit').then(([code]) => code),
    output: () => output,
    stop: () => {
      if (child.exitCode === null && child.signalCode === null) child.kill();
    },
  };
}

export async function freePort() {
  const server = net.createServer().listen(0, '127.0.0.1');
  await once(server, 'listening');
  const { port } = server.address();
  await new Promise((resolve) => server.close(resolve));
  return port;
}

export function probeConnection(port) {
  return new Promise((resolve) => {
    const socket = net.connect(port, '127.0.0.1');
    socket.once('connect', () => {
      socket.destroy();
      resolve('connected');
    });
    socket.once('error', (error) => resolve(error.code));
  });
}

// A TCP proxy in front of RethinkDB whose connections can be cut to simulate an outage.
export async function startTcpProxy({ host, port }) {
  const sockets = new Set();
  const server = net.createServer((client) => {
    const upstream = net.connect(port, host);
    for (const socket of [client, upstream]) {
      sockets.add(socket);
      socket.on('error', () => {});
      socket.on('close', () => sockets.delete(socket));
    }
    client.pipe(upstream).pipe(client);
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const cut = () => sockets.forEach((socket) => socket.destroy());
  return {
    port: server.address().port,
    cut,
    close: () => {
      cut();
      return new Promise((resolve) => server.close(resolve));
    },
  };
}

export async function waitUntil(check, what, timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  while (!(await check())) {
    if (Date.now() > deadline) {
      throw new Error(`Timed out waiting for ${what}`);
    }
    await sleep(20);
  }
}

export const within = (promise, ms) => Promise.race([promise, sleep(ms, 'timeout', { ref: false })]);

export const range = (from, count) => Array.from({ length: count }, (_, i) => from + i);

export function shuffle(values) {
  const copy = [...values];
  for (let i = copy.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [copy[i], copy[j]] = [copy[j], copy[i]];
  }
  return copy;
}
