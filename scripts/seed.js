// Seeds one example auction for local development. Run `npm run db:setup` first.
// Safe to rerun: if the auction already exists it is left exactly as it is,
// including its deadline, bid_count and top_bid. Seeds no users and no bids.
import r from 'rethinkdb';
import { loadConfig } from '../src/config.js';
import { TABLES, withConnection } from '../src/db.js';

// Stable ID so the README and manual requests can refer to this auction.
const DEMO_AUCTION_ID = 'demo-auction';
const OPEN_FOR_SECONDS = 30 * 24 * 60 * 60; // closes 30 days after it is first seeded

function demoAuction() {
  return {
    id: DEMO_AUCTION_ID,
    title: 'Demo auction: vintage film camera',
    starting_price: 5000, // minor units, e.g. 50.00 in a currency with cents
    // Both times come from the RethinkDB clock, the clock that checks bid deadlines.
    ends_at: r.now().add(OPEN_FOR_SECONDS),
    created_at: r.now(),
    bid_count: 0,
    top_bid: null,
  };
}

// Inserts the demo auction only if its ID is free. On a primary-key conflict the
// conflict function returns the stored document, so nothing is overwritten.
async function insertIfAbsent(conn, dbName) {
  const result = await r
    .db(dbName)
    .table(TABLES.auctions)
    .insert(demoAuction(), { conflict: (_id, storedDoc, _newDoc) => storedDoc })
    .run(conn);
  if (result.errors > 0) {
    throw new Error(`Inserting the demo auction failed: ${result.first_error}`);
  }
  if (result.inserted === 1) return 'created';
  if (result.unchanged === 1) return 'already exists, left unchanged';
  throw new Error(`Unexpected insert result: ${JSON.stringify(result)}`);
}

function printAuction(outcome, auction) {
  console.log(`  ${auction.id}: ${outcome}`);
  console.log(`    title           ${auction.title}`);
  console.log(`    starting_price  ${auction.starting_price} (minor units)`);
  console.log(`    created_at      ${auction.created_at.toISOString()}`);
  console.log(`    ends_at         ${auction.ends_at.toISOString()} (${auction.open ? 'open' : 'closed'})`);
  console.log(`    bid_count       ${auction.bid_count}`);
  console.log(`    top_bid         ${auction.top_bid ? JSON.stringify(auction.top_bid) : 'none'}`);
  if (!auction.open) {
    console.log('  The demo auction has closed and seeding never reopens it. Use another RETHINKDB_DB to start fresh.');
  }
}

async function main() {
  const { db } = loadConfig();
  console.log(`seed: RethinkDB ${db.host}:${db.port}, database "${db.name}"`);

  await withConnection(db, async (conn) => {
    const outcome = await insertIfAbsent(conn, db.name);
    // open is computed by the database clock and only displayed, never stored.
    const auction = await r
      .db(db.name)
      .table(TABLES.auctions)
      .get(DEMO_AUCTION_ID)
      .merge((stored) => ({ open: r.now().lt(stored('ends_at')) }))
      .run(conn);
    printAuction(outcome, auction);
  });
}

try {
  await main();
} catch (error) {
  console.error(`seed failed: ${error.name}: ${error.message}`);
  process.exitCode = 1;
}
