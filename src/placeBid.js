import r from 'rethinkdb';
import { TABLES } from './db.js';

const AUCTION_CLOSED = 'AUCTION_CLOSED';
const BID_TOO_LOW = 'BID_TOO_LOW';
const AUCTION_ID_PATTERN = /^[A-Za-z0-9-]{1,64}$/;
const MAX_USER_ID_LENGTH = 128;

const auctions = (dbName) => r.db(dbName).table(TABLES.auctions);
const bids = (dbName) => r.db(dbName).table(TABLES.bids);
const bidIdFor = (auctionId, amount) => `${auctionId}:${amount}`;

export async function placeBid(conn, dbName, body, { now } = {}) {
  const validation = validateBidRequest(body);
  if (validation.error) {
    return { type: 'invalid', message: validation.error };
  }
  const request = validation.value;

  const decision = await applyBid(conn, dbName, request, now);
  if (decision.type === 'accepted') {
    await recordHistory(conn, dbName, [decision.previousTopBid, decision.bid].filter(Boolean));
    return { type: 'accepted', bid: decision.bid };
  }
  if (decision.type === 'not_found') {
    return { type: 'not_found', auctionId: request.auctionId };
  }
  return resolveRejection(conn, dbName, request, decision.type);
}

function validateBidRequest(body) {
  if (body === null || typeof body !== 'object' || Array.isArray(body)) {
    return {
      error: 'Request body must be a JSON object with auction_id, user_id and amount (Content-Type: application/json).',
    };
  }
  const { auction_id: auctionId, user_id: userId, amount } = body;
  const problems = [];
  if (typeof auctionId !== 'string' || !AUCTION_ID_PATTERN.test(auctionId)) {
    problems.push('auction_id must be a string of 1-64 letters, digits or hyphens');
  }
  if (typeof userId !== 'string' || userId.length < 1 || userId.length > MAX_USER_ID_LENGTH) {
    problems.push(`user_id must be a string of 1-${MAX_USER_ID_LENGTH} characters`);
  }
  if (!Number.isSafeInteger(amount) || amount <= 0) {
    problems.push('amount must be a positive safe integer in minor currency units');
  }
  if (problems.length > 0) {
    return { error: `${problems.join('; ')}.` };
  }
  return { value: { auctionId, userId, amount } };
}

// `now` exists only so tests can pin the clock; without it the database clock decides.
export async function applyBid(conn, dbName, { auctionId, userId, amount }, now) {
  const evaluatedAt = now === undefined ? r.now() : r.expr(now);
  const result = await auctions(dbName)
    .get(auctionId)
    .update(
      (auction) =>
        r.branch(
          evaluatedAt.ge(auction('ends_at')),
          r.error(AUCTION_CLOSED),
          r.branch(
            auction('top_bid').eq(null),
            r.expr(amount).lt(auction('starting_price')),
            r.expr(amount).le(auction('top_bid')('amount')),
          ),
          r.error(BID_TOO_LOW),
          {
            bid_count: auction('bid_count').add(1),
            // update deep-merges objects and r.literal is rejected inside r.branch, so all
            // five fields are always written to replace the previous top bid completely.
            top_bid: {
              bid_id: bidIdFor(auctionId, amount),
              user_id: userId,
              amount,
              seq: auction('bid_count').add(1),
              placed_at: evaluatedAt,
            },
          },
        ),
      { returnChanges: true },
    )
    .run(conn);
  return classifyUpdate(auctionId, result);
}

function classifyUpdate(auctionId, result) {
  const { changes, ...summary } = result;
  const change = changes?.[0];
  if (summary.replaced === 1 && summary.errors === 0 && change?.new_val?.top_bid) {
    const previous = change.old_val?.top_bid;
    return {
      type: 'accepted',
      bid: fromTopBid(auctionId, change.new_val.top_bid),
      previousTopBid: previous ? fromTopBid(auctionId, previous) : null,
    };
  }
  if (summary.skipped === 1 && summary.errors === 0) {
    return { type: 'not_found' };
  }
  if (summary.errors === 1 && summary.first_error === AUCTION_CLOSED) {
    return { type: 'closed' };
  }
  if (summary.errors === 1 && summary.first_error === BID_TOO_LOW) {
    return { type: 'too_low' };
  }
  throw new Error(`Unexpected auction update result: ${JSON.stringify(summary)}`);
}

// A rejected request may retry a bid that was already accepted (the client missed the
// response). Accepted bids never change, so the original is looked up by its ID.
async function resolveRejection(conn, dbName, { auctionId, userId, amount }, reason) {
  const bidId = bidIdFor(auctionId, amount);
  const { auction, storedBid } = await r
    .expr({ auction: auctions(dbName).get(auctionId), storedBid: bids(dbName).get(bidId) })
    .run(conn);
  if (auction === null) {
    return { type: 'not_found', auctionId };
  }

  const topBid = auction.top_bid;
  const original = topBid?.bid_id === bidId ? fromTopBid(auctionId, topBid) : storedBid && bidRecord(storedBid);
  if (original?.user_id === userId) {
    if (storedBid === null) {
      await recordHistory(conn, dbName, [original]);
    }
    return { type: 'duplicate', bid: original };
  }
  if (reason === 'closed') {
    return { type: 'closed', endsAt: auction.ends_at };
  }
  return {
    type: 'too_low',
    currentTopAmount: topBid ? topBid.amount : null,
    minimumAmount: topBid ? topBid.amount + 1 : auction.starting_price,
  };
}

// The auction document already holds the accepted bid, so a failed history write is
// logged instead of failing the request. A retry of the same request, or the next
// accepted bid, writes the missing row later.
async function recordHistory(conn, dbName, records) {
  try {
    const result = await bids(dbName)
      .insert(records, { conflict: (_id, storedBid, _newBid) => storedBid })
      .run(conn);
    if (result.errors > 0) {
      throw new Error(result.first_error);
    }
  } catch (error) {
    console.error(`Bid history not recorded for ${records.map((record) => record.id).join(', ')}: ${error.message}`);
  }
}

function fromTopBid(auctionId, topBid) {
  return bidRecord({ ...topBid, id: topBid.bid_id, auction_id: auctionId });
}

function bidRecord({ id, auction_id, user_id, amount, seq, placed_at }) {
  return { id, auction_id, user_id, amount, seq, placed_at };
}
