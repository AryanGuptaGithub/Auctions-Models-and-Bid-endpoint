import assert from 'node:assert/strict';
import { once } from 'node:events';
import net from 'node:net';
import { after, before, describe, it } from 'node:test';
import { setTimeout as sleep } from 'node:timers/promises';
import r from 'rethinkdb';
import { applyBid, placeBid } from '../src/placeBid.js';
import {
  GENERIC_500,
  RESPONSE_BID_FIELDS,
  TEST_DB,
  TOP_BID_FIELDS,
  asApiBid,
  assertAuctionConsistent,
  auctions,
  closeConnections,
  createAuction,
  dbConfig,
  dropTestDatabase,
  freePort,
  getAuction,
  getHistory,
  history,
  openConnection,
  openConnectionCount,
  postBid,
  probeConnection,
  range,
  request,
  resetTestDatabase,
  shuffle,
  spawnServer,
  startApps,
  startTcpProxy,
  stopApps,
  uniqueId,
  waitUntil,
  withHistoryTableOffline,
  within,
} from './helpers.js';

const APP_INSTANCES = 4;
const ROUNDS = 10;
const RACE_ROUNDS = 100;
const KNOWN_RACE =
  'known race, unresolved: an outbid bid whose history row has not been written yet exists nowhere in the database';

let admin;
let apps;
let url;

before(async () => {
  admin = await openConnection();
  await resetTestDatabase(admin);
  apps = await startApps(APP_INSTANCES);
  url = apps.url;
});

after(async () => {
  if (apps) await stopApps(apps);
  if (admin) await dropTestDatabase(admin);
  await closeConnections();
  assert.equal(openConnectionCount(), 0, 'every RethinkDB connection opened by the tests is closed');
  await sleep(200);
  const sockets = process.getActiveResourcesInfo().filter((resource) => resource.startsWith('TCP'));
  assert.deepEqual(sockets, [], 'no TCP sockets or servers are left open');
});

const bid = (auctionId, userId, amount, baseUrl = url) => postBid(baseUrl, auctionId, userId, amount);

// Sends every request before awaiting any, spread over all app instances.
function burst(requests) {
  apps.stats.maxInFlight = 0;
  return Promise.all(
    requests.map(({ auctionId, userId, amount }, i) => bid(auctionId, userId, amount, apps.instances[i % apps.instances.length].url)),
  );
}

function assertAccepted(response) {
  assert.equal(response.status, 201, response.text);
  assert.equal(response.body.status, 'accepted');
  assert.deepEqual(Object.keys(response.body.bid), RESPONSE_BID_FIELDS);
  return response.body.bid;
}

function assertRejected(response, error, expected = {}) {
  assert.equal(response.status, 409, response.text);
  assert.equal(response.body.error, error);
  for (const [key, value] of Object.entries(expected)) {
    assert.deepEqual(response.body[key], value, key);
  }
}

function assertValidationError(response, messagePattern) {
  assert.equal(response.status, 400, response.text);
  assert.equal(response.body.error, 'VALIDATION_ERROR');
  assert.match(response.body.message, messagePattern);
}

// Deliberately wrong (control only): decides in JavaScript, then writes unconditionally.
async function naiveBid(conn, auctionId, userId, amount) {
  const auction = await auctions.get(auctionId).run(conn);
  const top = auction.top_bid;
  if (top ? amount <= top.amount : amount < auction.starting_price) {
    return null;
  }
  const seq = auction.bid_count + 1;
  await auctions
    .get(auctionId)
    .update({ bid_count: seq, top_bid: { bid_id: `${auctionId}:${amount}`, user_id: userId, amount, seq, placed_at: r.now() } })
    .run(conn);
  return { amount, seq };
}

describe('A. validation and HTTP behaviour', () => {
  const json = { 'content-type': 'application/json' };

  it('rejects a request with no body and no JSON content type', async () => {
    assertValidationError(await request(url), /^Request body must be a JSON object/);
  });

  it('rejects a JSON content type with an empty body, naming every field', async () => {
    assertValidationError(await request(url, { body: '', headers: json }), /auction_id.*user_id.*amount/);
  });

  it('rejects malformed JSON', async () => {
    for (const body of ['{"auction_id":', '{auction_id: "x"}', '{"amount": NaN}', "{'amount': 1}"]) {
      const response = await request(url, { body, headers: json });
      assert.equal(response.status, 400, body);
      assert.deepEqual(response.body, { error: 'VALIDATION_ERROR', message: 'Request body is not valid JSON.' }, body);
    }
  });

  it('rejects JSON bodies that are not objects', async () => {
    for (const body of ['[]', '[1, 2]', '"text"', '42', 'null', 'true']) {
      const response = await request(url, { body, headers: json });
      assert.equal(response.status, 400, body);
      assert.equal(response.body.error, 'VALIDATION_ERROR', body);
    }
  });

  it('rejects a body larger than 4kb', async () => {
    const response = await request(url, { json: { auction_id: 'a', user_id: 'u'.repeat(5000), amount: 1000 } });
    assert.equal(response.status, 400);
    assert.deepEqual(response.body, { error: 'VALIDATION_ERROR', message: 'Request body must not exceed 4kb.' });
  });

  it('rejects a missing amount and amounts that are not positive safe integers', async () => {
    const auctionId = await createAuction(admin);
    const amounts = [undefined, null, true, '5100', '', [], {}, 0, -0, -1, 0.5, 1.5, 5100.25, 2 ** 53, 2 ** 53 + 2, 1e21];
    for (const amount of amounts) {
      const response = await request(url, { json: { auction_id: auctionId, user_id: 'user-1', amount } });
      assertValidationError(response, /^amount must be a positive safe integer/);
    }
    assert.equal((await getAuction(admin, auctionId)).bid_count, 0);
  });

  it('rejects a missing auction_id and invalid ones', async () => {
    const auctionIds = [undefined, null, 42, true, [], {}, '', ' ', 'bad id!', 'a'.repeat(65), 'demo:auction', 'auction_1', 'auction-ü', 'trailing-newline\n'];
    for (const auctionId of auctionIds) {
      const response = await request(url, { json: { auction_id: auctionId, user_id: 'user-1', amount: 1000 } });
      assertValidationError(response, /^auction_id must be/);
    }
  });

  it('rejects a missing user_id and invalid ones', async () => {
    const auctionId = await createAuction(admin);
    for (const userId of [undefined, null, 42, false, [], {}, '', 'u'.repeat(129)]) {
      const response = await request(url, { json: { auction_id: auctionId, user_id: userId, amount: 1000 } });
      assertValidationError(response, /^user_id must be/);
    }
    assert.equal((await getAuction(admin, auctionId)).bid_count, 0);
  });

  it('accepts the longest identifiers and the largest safe amount', async () => {
    const auctionId = await createAuction(admin, { id: 'a'.repeat(64) });
    const accepted = assertAccepted(await bid(auctionId, '€'.repeat(128), Number.MAX_SAFE_INTEGER));
    assert.equal(accepted.id, `${auctionId}:${Number.MAX_SAFE_INTEGER}`);
    await assertAuctionConsistent(admin, auctionId);
  });

  it('returns 404 for an unknown auction', async () => {
    const response = await bid('no-such-auction', 'user-1', 1000);
    assert.equal(response.status, 404);
    assert.deepEqual(response.body, { error: 'AUCTION_NOT_FOUND', message: 'Auction "no-such-auction" does not exist.' });
  });

  it('answers other routes and methods with the Express defaults, without stack traces', async () => {
    for (const [method, path] of [['GET', '/bid'], ['PUT', '/bid'], ['PATCH', '/bid'], ['DELETE', '/bid'], ['POST', '/bids'], ['GET', '/']]) {
      const response = await request(url, { method, path });
      assert.equal(response.status, 404, `${method} ${path}`);
      assert.match(response.headers.get('content-type'), /^text\/html/);
      assert.ok(response.text.includes(`Cannot ${method} ${path}`), response.text);
      assert.doesNotMatch(response.text, /\bat \S+ \(|node_modules|Error:/);
    }
    const options = await request(url, { method: 'OPTIONS', path: '/bid' });
    assert.equal(options.status, 200);
    assert.equal(options.headers.get('allow'), 'POST');
  });

  it('returns a generic 500 when the database connection is gone', async (t) => {
    const consoleError = t.mock.method(console, 'error', () => {});
    const auctionId = await createAuction(admin);
    const broken = await startApps(1);
    try {
      await broken.instances[0].conn.close();
      const response = await bid(auctionId, 'user-1', 1000, broken.url);
      assert.equal(response.status, 500);
      assert.equal(response.text, JSON.stringify(GENERIC_500));
    } finally {
      await stopApps(broken);
    }
    assert.ok(consoleError.mock.calls.some((call) => String(call.arguments[1]).includes('Connection is closed')), 'the cause is logged on the server');
    assert.equal((await getAuction(admin, auctionId)).bid_count, 0);
  });

  it('returns a generic 500 when the database returns an unexpected result', async (t) => {
    const consoleError = t.mock.method(console, 'error', () => {});
    const auctionId = uniqueId('no-deadline');
    await auctions.insert({ id: auctionId, title: 'missing ends_at', starting_price: 1000, created_at: r.now(), bid_count: 0, top_bid: null }).run(admin);
    const response = await bid(auctionId, 'user-1', 1000);
    assert.equal(response.status, 500);
    assert.equal(response.text, JSON.stringify(GENERIC_500));
    assert.ok(consoleError.mock.calls.some((call) => /Unexpected auction update result/.test(String(call.arguments[1]))));
  });
});

describe('B. bid rules', () => {
  it('accepts a first bid equal to the starting price', async () => {
    const auctionId = await createAuction(admin, { startingPrice: 1000 });
    const accepted = assertAccepted(await bid(auctionId, 'user-1', 1000));
    assert.deepEqual(
      { ...accepted, placed_at: typeof accepted.placed_at },
      { id: `${auctionId}:1000`, auction_id: auctionId, user_id: 'user-1', amount: 1000, seq: 1, placed_at: 'string' },
    );
    assert.equal(new Date(accepted.placed_at).toISOString(), accepted.placed_at);
    const { auction, rows } = await assertAuctionConsistent(admin, auctionId);
    assert.equal(auction.bid_count, 1);
    assert.deepEqual(asApiBid(auctionId, auction.top_bid), accepted);
    assert.deepEqual(rows.map((row) => asApiBid(auctionId, row)), [accepted]);
  });

  it('rejects a first bid below the starting price and changes nothing', async () => {
    const auctionId = await createAuction(admin, { startingPrice: 1000 });
    const unchanged = await getAuction(admin, auctionId);
    const response = await bid(auctionId, 'user-1', 999);
    assert.equal(response.status, 409);
    assert.deepEqual(response.body, {
      error: 'BID_TOO_LOW',
      message: 'The first bid must be at least the starting price of 1000.',
      current_top_amount: null,
      minimum_amount: 1000,
    });
    assert.deepEqual(await getAuction(admin, auctionId), unchanged);
    assert.deepEqual(await getHistory(admin, auctionId), []);
  });

  it('accepts a higher bid and rejects equal and lower bids without changing the auction', async () => {
    const auctionId = await createAuction(admin, { startingPrice: 1000 });
    assertAccepted(await bid(auctionId, 'user-1', 1000));
    const unchanged = await getAuction(admin, auctionId);
    for (const [userId, amount] of [['user-2', 1000], ['user-2', 999], ['user-3', 1]]) {
      const response = await bid(auctionId, userId, amount);
      assert.equal(response.status, 409);
      assert.deepEqual(response.body, {
        error: 'BID_TOO_LOW',
        message: 'Bid must be higher than the current top bid of 1000.',
        current_top_amount: 1000,
        minimum_amount: 1001,
      });
    }
    assert.deepEqual(await getAuction(admin, auctionId), unchanged);
    assert.equal(assertAccepted(await bid(auctionId, 'user-2', 1001)).seq, 2);
    await assertAuctionConsistent(admin, auctionId);
  });

  it('lets the top bidder raise their own bid', async () => {
    const auctionId = await createAuction(admin);
    assertAccepted(await bid(auctionId, 'user-1', 1000));
    const raised = assertAccepted(await bid(auctionId, 'user-1', 1500));
    assert.equal(raised.seq, 2);
    const { auction, rows } = await assertAuctionConsistent(admin, auctionId);
    assert.deepEqual(asApiBid(auctionId, auction.top_bid), raised);
    assert.deepEqual(rows.map((row) => [row.user_id, row.amount]), [['user-1', 1000], ['user-1', 1500]]);
  });

  it('replaces top_bid completely, keeping exactly the five approved fields', async () => {
    const auctionId = await createAuction(admin);
    for (const [userId, amount] of [['user-1', 1000], ['user-2', 1200], ['user-1', 5000]]) {
      const accepted = assertAccepted(await bid(auctionId, userId, amount));
      const { top_bid: top } = await getAuction(admin, auctionId);
      assert.deepEqual(Object.keys(top).sort(), TOP_BID_FIELDS);
      assert.deepEqual(asApiBid(auctionId, top), accepted);
    }
  });

  it('records every accepted bid in history with its deterministic id and sequence', async () => {
    const auctionId = await createAuction(admin);
    const accepted = [];
    for (const [userId, amount] of [['user-1', 1000], ['user-2', 1000], ['user-2', 1100], ['user-3', 1050], ['user-3', 1300]]) {
      const response = await bid(auctionId, userId, amount);
      if (response.status === 201) accepted.push(response.body.bid);
    }
    assert.deepEqual(accepted.map((b) => [b.id, b.seq]), [[`${auctionId}:1000`, 1], [`${auctionId}:1100`, 2], [`${auctionId}:1300`, 3]]);
    const { rows } = await assertAuctionConsistent(admin, auctionId);
    assert.deepEqual(rows.map((row) => asApiBid(auctionId, row)), accepted);
  });
});

describe('C. deadline boundary', () => {
  it('pinned clock: accepted at ends_at - 1 ms, closed at exactly ends_at and at ends_at + 1 ms', async () => {
    const endsAt = new Date(Math.floor(Date.now() / 1000) * 1000 + 3_600_000);
    const auctionId = await createAuction(admin, { endsAt });
    const at = (offsetMs, userId, amount) =>
      placeBid(admin, TEST_DB, { auction_id: auctionId, user_id: userId, amount }, { now: new Date(endsAt.getTime() + offsetMs) });

    const accepted = await at(-1, 'user-1', 1000);
    assert.equal(accepted.type, 'accepted');
    assert.equal(accepted.bid.placed_at.getTime(), endsAt.getTime() - 1, 'placed_at is the evaluation time');
    assert.deepEqual(await at(0, 'user-2', 2000), { type: 'closed', endsAt });
    assert.deepEqual(await at(1, 'user-2', 3000), { type: 'closed', endsAt });
    assert.deepEqual(await at(1, 'user-2', 1), { type: 'closed', endsAt }, 'the deadline is checked before the amount');
    assert.deepEqual(await at(0, 'user-1', 1000), { type: 'duplicate', bid: accepted.bid }, 'a retry after the deadline still gets the original');
    const { auction } = await assertAuctionConsistent(admin, auctionId);
    assert.equal(auction.bid_count, 1);
  });

  it('decides by evaluation time, not commit time', async () => {
    // ends_at passed 10 s ago; the update is evaluated as of 1 ms before it and written now.
    const endsAt = new Date(Math.floor(Date.now() / 1000) * 1000 - 10_000);
    const auctionId = await createAuction(admin, { endsAt });
    const late = await placeBid(admin, TEST_DB, { auction_id: auctionId, user_id: 'user-1', amount: 1000 }, { now: new Date(endsAt.getTime() - 1) });
    assert.equal(late.type, 'accepted');
    assert.ok((await r.now().run(admin)) > endsAt, 'the write happened after ends_at by the database clock');
    assertRejected(await bid(auctionId, 'user-2', 2000), 'AUCTION_CLOSED', { ends_at: endsAt.toISOString() });
    await assertAuctionConsistent(admin, auctionId);
  });

  it('closes a real short-lived auction at ends_at by the database clock', async () => {
    const auctionId = await createAuction(admin, { endsAt: r.now().add(1) });
    const { ends_at: endsAt } = await getAuction(admin, auctionId);
    const outcomes = [];
    let lastAccepted;
    for (let amount = 1000; ; amount += 1) {
      const response = await bid(auctionId, `user-${amount % 3}`, amount);
      outcomes.push(response.status === 201 ? 'accepted' : response.body.error);
      if (response.status === 201) lastAccepted = response.body.bid;
      if ((await r.now().run(admin)).getTime() > endsAt.getTime() + 300) break;
      await sleep(20);
    }
    const firstClosed = outcomes.indexOf('AUCTION_CLOSED');
    assert.ok(firstClosed > 0, `bids were accepted before the deadline and rejected after it: ${outcomes.join(' ')}`);
    assert.ok(outcomes.slice(0, firstClosed).every((outcome) => outcome === 'accepted'));
    assert.ok(outcomes.slice(firstClosed).every((outcome) => outcome === 'AUCTION_CLOSED'), 'once closed, always closed');
    const { auction } = await assertAuctionConsistent(admin, auctionId);
    assert.equal(auction.bid_count, firstClosed);
    const marginMs = endsAt.getTime() - auction.top_bid.placed_at.getTime();
    assert.ok(marginMs > 0 && marginMs < 500, `the last accepted bid was evaluated ${marginMs} ms before ends_at`);
    const retry = await bid(auctionId, lastAccepted.user_id, lastAccepted.amount);
    assert.deepEqual(retry.body, { status: 'duplicate', bid: lastAccepted });
  });
});

describe('D. concurrency: real HTTP requests through several app instances and connections', () => {
  it('control: a naive read-then-write implementation loses updates under the same load', async (t) => {
    let anomaly = null;
    let rounds = 0;
    while (!anomaly && rounds < ROUNDS) {
      rounds += 1;
      const auctionId = await createAuction(admin);
      const outcomes = await Promise.all(
        shuffle(range(1000, 30)).map((amount, i) => naiveBid(apps.instances[i % apps.instances.length].conn, auctionId, `user-${i}`, amount)),
      );
      const accepted = outcomes.filter(Boolean);
      const auction = await getAuction(admin, auctionId);
      if (auction.bid_count !== accepted.length) {
        anomaly = `${accepted.length} bids were told they won but bid_count is ${auction.bid_count}`;
      } else if (new Set(accepted.map((a) => a.seq)).size !== accepted.length) {
        anomaly = 'two winning bids share a seq';
      } else if (auction.top_bid.amount !== 1029) {
        anomaly = `the top bid is ${auction.top_bid.amount}, not the highest amount 1029`;
      }
    }
    t.diagnostic(`naive implementation, round ${rounds}: ${anomaly ?? 'no anomaly'}`);
    assert.ok(anomaly, `no anomaly in ${rounds} rounds: this load does not interleave reads and writes`);
  });

  it('distinct amounts at once: unique consecutive seqs, and bid_count, top_bid and history agree with the 201s', async (t) => {
    let requests = 0;
    let accepted = 0;
    let maxInFlight = 0;
    for (let round = 0; round < ROUNDS; round += 1) {
      const auctionId = await createAuction(admin, { startingPrice: 1000 });
      const sent = shuffle(range(1000, 30)).map((amount, i) => ({ auctionId, userId: `user-${i % 6}`, amount }));
      const responses = await burst(sent);
      maxInFlight = Math.max(maxInFlight, apps.stats.maxInFlight);
      const wins = [];
      for (const response of responses) {
        if (response.status === 201) {
          wins.push(response.body.bid);
        } else {
          assertRejected(response, 'BID_TOO_LOW');
        }
      }
      wins.sort((a, b) => a.seq - b.seq);
      assert.deepEqual(wins.map((w) => w.seq), range(1, wins.length), 'seqs are unique and consecutive');
      for (let i = 1; i < wins.length; i += 1) {
        assert.ok(wins[i].amount > wins[i - 1].amount, 'amounts strictly increase with seq');
      }
      assert.equal(wins.at(-1).amount, 1029, 'the highest amount always wins');
      const { auction, rows } = await assertAuctionConsistent(admin, auctionId);
      assert.equal(auction.bid_count, wins.length);
      assert.deepEqual(asApiBid(auctionId, auction.top_bid), wins.at(-1));
      assert.deepEqual(rows.map((row) => asApiBid(auctionId, row)), wins);
      requests += sent.length;
      accepted += wins.length;
    }
    t.diagnostic(`${ROUNDS} rounds, ${requests} requests, ${accepted} accepted, up to ${maxInFlight} requests in flight at once`);
  });

  it('identical requests from one user at once: one 201, every other request 200 with the same bid', async (t) => {
    let maxInFlight = 0;
    for (let round = 0; round < ROUNDS; round += 1) {
      const auctionId = await createAuction(admin);
      const responses = await burst(range(0, 20).map(() => ({ auctionId, userId: 'user-1', amount: 1000 })));
      maxInFlight = Math.max(maxInFlight, apps.stats.maxInFlight);
      const statuses = responses.map((res) => res.status);
      const created = responses.filter((res) => res.status === 201);
      assert.equal(created.length, 1, `statuses: ${statuses}`);
      assert.equal(responses.filter((res) => res.status === 200).length, 19, `statuses: ${statuses}`);
      for (const res of responses.filter((res) => res.status === 200)) {
        assert.deepEqual(res.body, { status: 'duplicate', bid: created[0].body.bid });
      }
      const { auction, rows } = await assertAuctionConsistent(admin, auctionId);
      assert.equal(auction.bid_count, 1);
      assert.deepEqual(rows.map((row) => asApiBid(auctionId, row)), [created[0].body.bid]);
    }
    t.diagnostic(`${ROUNDS} rounds of 20 identical requests, up to ${maxInFlight} in flight at once`);
  });

  it('one amount from many users at once: accepted once, and no other user is told it is their duplicate', async () => {
    for (let round = 0; round < ROUNDS; round += 1) {
      const auctionId = await createAuction(admin);
      const responses = await burst(range(0, 20).map((i) => ({ auctionId, userId: `user-${i}`, amount: 1000 })));
      const created = responses.filter((res) => res.status === 201);
      assert.equal(created.length, 1, `statuses: ${responses.map((res) => res.status)}`);
      for (const res of responses.filter((res) => res.status !== 201)) {
        assertRejected(res, 'BID_TOO_LOW', { current_top_amount: 1000, minimum_amount: 1001 });
      }
      const { auction, rows } = await assertAuctionConsistent(admin, auctionId);
      assert.equal(auction.bid_count, 1);
      assert.equal(auction.top_bid.user_id, created[0].body.bid.user_id);
      assert.equal(rows.length, 1);
    }
  });

  it('one amount from many users, each sending it twice: only the winner gets a 200', async () => {
    for (let round = 0; round < ROUNDS; round += 1) {
      const auctionId = await createAuction(admin);
      const sent = shuffle(range(0, 20).map((i) => ({ auctionId, userId: `user-${i % 10}`, amount: 1000 })));
      const responses = await burst(sent);
      const created = responses.filter((res) => res.status === 201);
      assert.equal(created.length, 1, `statuses: ${responses.map((res) => res.status)}`);
      const winner = created[0].body.bid.user_id;
      responses.forEach((res, i) => {
        if (res.status === 201) return;
        if (sent[i].userId === winner) {
          assert.deepEqual(res.body, { status: 'duplicate', bid: created[0].body.bid });
        } else {
          assertRejected(res, 'BID_TOO_LOW', { current_top_amount: 1000 });
        }
      });
      await assertAuctionConsistent(admin, auctionId);
    }
  });

  it('valid, equal, lower and contested bids mixed: every rule and the strict increase still hold', async () => {
    for (let round = 0; round < ROUNDS; round += 1) {
      const auctionId = await createAuction(admin, { startingPrice: 1000 });
      const opening = assertAccepted(await bid(auctionId, 'opener', 1000));
      const sent = shuffle([
        ...range(1001, 15).map((amount, i) => ({ userId: `raiser-${i % 5}`, amount })),
        ...range(1001, 15).map((amount, i) => ({ userId: `rival-${i % 5}`, amount })),
        ...range(0, 5).map((i) => ({ userId: `equal-${i}`, amount: 1000 })),
        ...range(0, 10).map((i) => ({ userId: `lower-${i}`, amount: 900 + i * 10 })),
      ]).map((b) => ({ auctionId, ...b }));
      const responses = await burst(sent);
      const wins = [opening];
      responses.forEach((res, i) => {
        if (sent[i].amount <= 1000) {
          assertRejected(res, 'BID_TOO_LOW');
        } else if (res.status === 201) {
          wins.push(res.body.bid);
        } else {
          assertRejected(res, 'BID_TOO_LOW');
        }
      });
      wins.sort((a, b) => a.seq - b.seq);
      assert.equal(new Set(wins.map((w) => w.amount)).size, wins.length, 'each amount is accepted at most once');
      assert.deepEqual(wins.map((w) => w.seq), range(1, wins.length));
      assert.equal(wins.at(-1).amount, 1015);
      const { auction, rows } = await assertAuctionConsistent(admin, auctionId);
      assert.equal(auction.bid_count, wins.length);
      assert.deepEqual(rows.map((row) => asApiBid(auctionId, row)), wins);
    }
  });
});

describe('E. duplicate recovery and history', () => {
  it('returns the original bid when the client retries after missing the response', async () => {
    const auctionId = await createAuction(admin);
    const original = assertAccepted(await bid(auctionId, 'user-1', 1000));
    const retry = await bid(auctionId, 'user-1', 1000);
    assert.equal(retry.status, 200);
    assert.deepEqual(retry.body, { status: 'duplicate', bid: original });
    const { auction, rows } = await assertAuctionConsistent(admin, auctionId);
    assert.equal(auction.bid_count, 1);
    assert.equal(rows.length, 1);
  });

  it('returns the original bid when it is retried after a later bid outbid it', async () => {
    const auctionId = await createAuction(admin);
    const original = assertAccepted(await bid(auctionId, 'user-1', 1000));
    assertAccepted(await bid(auctionId, 'user-2', 1100));
    const retry = await bid(auctionId, 'user-1', 1000);
    assert.deepEqual(retry.body, { status: 'duplicate', bid: original });
    assert.equal((await getAuction(admin, auctionId)).bid_count, 2);
  });

  it('never lets another user claim an accepted bid', async () => {
    const auctionId = await createAuction(admin);
    assertAccepted(await bid(auctionId, 'user-1', 1000));
    assertRejected(await bid(auctionId, 'user-2', 1000), 'BID_TOO_LOW', { current_top_amount: 1000 });
    assertAccepted(await bid(auctionId, 'user-3', 1100));
    assertRejected(await bid(auctionId, 'user-2', 1000), 'BID_TOO_LOW', { current_top_amount: 1100 });
    assertRejected(await bid(auctionId, 'user-1', 1100), 'BID_TOO_LOW', { current_top_amount: 1100 });
    await assertAuctionConsistent(admin, auctionId);
  });

  it('never overwrites an existing history row (insert-if-absent)', async () => {
    const auctionId = await createAuction(admin);
    const existing = {
      id: `${auctionId}:1000`,
      auction_id: auctionId,
      user_id: 'someone-else',
      amount: 1000,
      seq: 99,
      placed_at: new Date('2020-01-01T00:00:00.000Z'),
      marker: 'pre-existing row',
    };
    await history.insert(existing).run(admin);
    assertAccepted(await bid(auctionId, 'user-1', 1000));
    assertAccepted(await bid(auctionId, 'user-2', 1100));
    assert.deepEqual(await history.get(existing.id).run(admin), existing);
  });

  it('keeps the bid accepted and the top bid correct when the history write fails', async (t) => {
    const consoleError = t.mock.method(console, 'error', () => {});
    const auctionId = await createAuction(admin);
    const accepted = assertAccepted(await withHistoryTableOffline(admin, () => bid(auctionId, 'user-1', 1000)));
    assert.ok(
      consoleError.mock.calls.some((call) => String(call.arguments[0]).startsWith(`Bid history not recorded for ${auctionId}:1000:`)),
      'the failed history write is logged',
    );
    const { auction, rows } = await assertAuctionConsistent(admin, auctionId, { complete: false });
    assert.equal(auction.bid_count, 1);
    assert.deepEqual(asApiBid(auctionId, auction.top_bid), accepted);
    assert.deepEqual(rows, []);
    const retry = await bid(auctionId, 'user-1', 1000);
    assert.deepEqual(retry.body, { status: 'duplicate', bid: accepted });
    await assertAuctionConsistent(admin, auctionId);
  });

  it('repairs history after a process failure between the auction update and the history write', async () => {
    const auctionId = await createAuction(admin);
    const lost = await applyBid(admin, TEST_DB, { auctionId, userId: 'user-1', amount: 1000 });
    assert.equal(lost.type, 'accepted');
    assert.deepEqual(await getHistory(admin, auctionId), []);
    const retry = await bid(auctionId, 'user-1', 1000);
    assert.deepEqual(retry.body, { status: 'duplicate', bid: asApiBid(auctionId, lost.bid) });
    await assertAuctionConsistent(admin, auctionId);

    await applyBid(admin, TEST_DB, { auctionId, userId: 'user-2', amount: 1100 });
    assertAccepted(await bid(auctionId, 'user-3', 1200));
    const { rows } = await assertAuctionConsistent(admin, auctionId);
    assert.deepEqual(rows.map((row) => row.amount), [1000, 1100, 1200]);
  });

  it('documented limitation: two consecutive failures lose the earlier history row for good', async () => {
    const auctionId = await createAuction(admin);
    assertAccepted(await bid(auctionId, 'user-1', 1000));
    await applyBid(admin, TEST_DB, { auctionId, userId: 'user-2', amount: 1100 });
    await applyBid(admin, TEST_DB, { auctionId, userId: 'user-3', amount: 1200 });
    assertAccepted(await bid(auctionId, 'user-4', 1300));
    const { auction, rows } = await assertAuctionConsistent(admin, auctionId, { complete: false });
    assert.equal(auction.bid_count, 4);
    assert.equal(auction.top_bid.amount, 1300);
    assert.deepEqual(rows.map((row) => [row.seq, row.amount]), [[1, 1000], [3, 1200], [4, 1300]], 'seq 2 is a visible gap');
    assertRejected(await bid(auctionId, 'user-2', 1100), 'BID_TOO_LOW', { current_top_amount: 1300 });
  });

  it('a retry is recognised once the pending history writes of the original and the newer bid land', async () => {
    const auctionId = await createAuction(admin);
    const original = await applyBid(admin, TEST_DB, { auctionId, userId: 'user-1', amount: 1000 });
    const newer = await applyBid(admin, TEST_DB, { auctionId, userId: 'user-2', amount: 1100 });
    await history.insert([original.bid, newer.bid]).run(admin);
    const retry = await bid(auctionId, 'user-1', 1000);
    assert.deepEqual(retry.body, { status: 'duplicate', bid: asApiBid(auctionId, original.bid) });
    await assertAuctionConsistent(admin, auctionId);
  });

  it('race: a retry overlapping the original history write and a newer bid gets the original bid', { todo: KNOWN_RACE }, async () => {
    // The committed state inside the window: both auction updates landed, neither history write has.
    const auctionId = await createAuction(admin);
    await applyBid(admin, TEST_DB, { auctionId, userId: 'user-1', amount: 1000 });
    await applyBid(admin, TEST_DB, { auctionId, userId: 'user-2', amount: 1100 });
    const retry = await bid(auctionId, 'user-1', 1000);
    assert.equal(retry.status, 200, `the retry of an accepted bid got ${retry.status} ${retry.text}`);
  });

  it('race under real load: an accepted bid and its concurrent retry never get 201 and 409', { todo: KNOWN_RACE }, async (t) => {
    let misses = 0;
    for (let round = 0; round < RACE_ROUNDS; round += 1) {
      const auctionId = await createAuction(admin);
      const [first, , retry] = await burst([
        { auctionId, userId: 'user-1', amount: 1000 },
        { auctionId, userId: 'user-2', amount: 1100 },
        { auctionId, userId: 'user-1', amount: 1000 },
      ]);
      if ([first.status, retry.status].sort().join() === '201,409') misses += 1;
    }
    t.diagnostic(`${misses} of ${RACE_ROUNDS} rounds answered the retry of an accepted bid with 409`);
    assert.equal(misses, 0);
  });
});

describe('F. process and resource behaviour', () => {
  const serverEnv = () => ({ RETHINKDB_HOST: dbConfig.host, RETHINKDB_PORT: String(dbConfig.port), RETHINKDB_DB: TEST_DB });

  it('the server exits with code 1 when RethinkDB is unreachable', async () => {
    const server = spawnServer({ ...serverEnv(), RETHINKDB_PORT: String(await freePort()), PORT: String(await freePort()) });
    try {
      assert.equal(await within(server.exited, 15_000), 1);
      assert.match(server.output(), /Server failed to start: ReqlDriverError: Could not connect/);
    } finally {
      server.stop();
    }
  });

  it('the server exits with code 1 when its HTTP port is taken, after closing its RethinkDB connection', async () => {
    const blocker = net.createServer().listen(0, '127.0.0.1');
    await once(blocker, 'listening');
    const server = spawnServer({ ...serverEnv(), PORT: String(blocker.address().port) });
    try {
      assert.equal(await within(server.exited, 15_000), 1, 'an open RethinkDB connection would keep the process alive');
      assert.match(server.output(), /Server failed to start: Error: listen EADDRINUSE/);
    } finally {
      server.stop();
      await new Promise((resolve) => blocker.close(resolve));
    }
  });

  it('on SIGINT it finishes in-flight requests, closes its connections and exits 0 (SIGINT emitted in-process via IPC, not a real Ctrl+C)', async () => {
    const auctionId = await createAuction(admin);
    const port = await freePort();
    const server = spawnServer({ ...serverEnv(), PORT: String(port) }, { sigintOverIpc: true });
    try {
      await waitUntil(() => server.output().includes('listening on'), 'the server to start');
      const body = JSON.stringify({ auction_id: auctionId, user_id: 'user-1', amount: 1000 });
      const socket = net.connect(port, '127.0.0.1');
      await once(socket, 'connect');
      socket.setEncoding('utf8');
      let response = '';
      socket.on('data', (chunk) => {
        response += chunk;
      });
      const socketClosed = once(socket, 'close');
      socket.write(
        `POST /bid HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\n` +
          `Content-Length: ${Buffer.byteLength(body)}\r\nConnection: close\r\n\r\n${body.slice(0, 10)}`,
      );
      await sleep(200);
      server.child.send('SIGINT');
      await waitUntil(() => server.output().includes('Shutting down (SIGINT)'), 'the shutdown to start');
      assert.equal(await within(probeConnection(port), 10_000), 'ECONNREFUSED', 'no new connections are accepted');
      assert.equal(response, '', 'the in-flight request is still waiting for the rest of its body');
      socket.write(body.slice(10));
      await within(socketClosed, 10_000);
      assert.match(response, /^HTTP\/1\.1 201 Created/);
      assert.equal(await within(server.exited, 15_000), 0);
      assert.match(server.output(), /HTTP server and RethinkDB connection closed\./);
      assert.equal((await getAuction(admin, auctionId)).bid_count, 1);
    } finally {
      server.stop();
    }
  });

  it('the server shuts down with exit code 1 when its RethinkDB connection is lost', async () => {
    const proxy = await startTcpProxy({ host: dbConfig.host, port: dbConfig.port });
    const port = await freePort();
    const server = spawnServer({ ...serverEnv(), RETHINKDB_HOST: '127.0.0.1', RETHINKDB_PORT: String(proxy.port), PORT: String(port) });
    try {
      await waitUntil(() => server.output().includes('listening on'), 'the server to start');
      const auctionId = await createAuction(admin);
      assertAccepted(await bid(auctionId, 'user-1', 1000, `http://127.0.0.1:${port}`));
      proxy.cut();
      assert.equal(await within(server.exited, 15_000), 1);
      assert.match(server.output(), /RethinkDB connection closed unexpectedly/);
    } finally {
      server.stop();
      await proxy.close();
    }
  });
});
