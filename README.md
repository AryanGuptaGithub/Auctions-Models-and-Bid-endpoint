# Auction Bidding API

Assignment ID: v#hdf38%44

A Node.js service with a single endpoint, `POST /bid`, that places bids on auctions stored in RethinkDB. A bid is accepted only if it is strictly higher than the current top bid and the auction has not closed.

Built with Node.js (ES modules), Express 5, RethinkDB 2.4 and the official `rethinkdb` driver. Tests use the built-in `node:test` runner.

## Requirements

- Node.js 24 or later (developed with 24.14.1) and npm
- Docker Desktop, for RethinkDB

## Setup (PowerShell)

Start RethinkDB 2.4.4 in Docker. The ports are published on 127.0.0.1 only, because RethinkDB's `admin` account has no password by default.

```powershell
docker run -d --name bidding-rethinkdb -p 127.0.0.1:28015:28015 -p 127.0.0.1:8080:8080 rethinkdb:2.4.4-bookworm-slim
```

If the container already exists, start it with `docker start bidding-rethinkdb`. The RethinkDB admin UI is at http://127.0.0.1:8080.

Then install dependencies, create the schema, seed an example auction and start the server:

```powershell
npm install
npm run db:setup
npm run seed
npm start
```

- `db:setup` creates the `bidding` database, the `auctions` and `bids` tables and the `bids.auction_seq` index. It only creates what is missing and never drops data, so it is safe to rerun.
- `seed` creates the auction `demo-auction` with a starting price of 5000, closing 30 days after it is first seeded. Rerunning it leaves an existing auction exactly as it is, so it will not reopen a closed auction; set `RETHINKDB_DB` to another name to start fresh.
- `start` serves the API on http://127.0.0.1:3000. On Ctrl+C (SIGINT) or SIGTERM it stops accepting connections, finishes in-flight requests and closes the database connection.

### Configuration

| Variable | Default | Purpose |
|---|---|---|
| `HTTP_HOST`, `PORT` | `127.0.0.1`, `3000` | HTTP listener |
| `RETHINKDB_HOST`, `RETHINKDB_PORT` | `localhost`, `28015` | RethinkDB server |
| `RETHINKDB_DB` | `bidding` | Application database |
| `RETHINKDB_USER`, `RETHINKDB_PASSWORD` | `admin`, empty | RethinkDB credentials |
| `RETHINKDB_TEST_DB` | `bidding_test` | Test database; must end in `_test` |

## API

`POST /bid` with a JSON body:

```json
{ "auction_id": "demo-auction", "user_id": "user-123", "amount": 5100 }
```

- `auction_id`: 1–64 letters, digits or hyphens
- `user_id`: a string of 1–128 characters
- `amount`: a positive safe integer in minor currency units (5100 means 51.00)

| Status | Body | When |
|---|---|---|
| 201 | `{"status":"accepted","bid":{...}}` | The bid is accepted and is the new top bid |
| 200 | `{"status":"duplicate","bid":{...}}` | A retry: this user already placed this bid, and the original is returned |
| 400 | `{"error":"VALIDATION_ERROR","message":"..."}` | Missing, malformed or invalid body (limit 4 KB) |
| 404 | `{"error":"AUCTION_NOT_FOUND","message":"..."}` | Unknown auction |
| 409 | `{"error":"BID_TOO_LOW","message":"...","current_top_amount":5100,"minimum_amount":5101}` | Not above the current top bid, or below the starting price (`current_top_amount` is `null` before the first bid) |
| 409 | `{"error":"AUCTION_CLOSED","message":"...","ends_at":"..."}` | The auction has closed |
| 500 | `{"error":"INTERNAL_ERROR","message":"An unexpected error occurred."}` | Unexpected failure; details are only logged on the server |

A bid in a response looks like this, where `id` is `<auction_id>:<amount>` and `seq` is the bid's position among the auction's accepted bids:

```json
{ "id": "demo-auction:5100", "auction_id": "demo-auction", "user_id": "user-123", "amount": 5100, "seq": 1, "placed_at": "2026-10-28T10:17:13.684Z" }
```

### Example requests

PowerShell:

```powershell
$bid = @{ auction_id = 'demo-auction'; user_id = 'user-123'; amount = 5100 } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:3000/bid -ContentType 'application/json' -Body $bid   # 201, accepted
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:3000/bid -ContentType 'application/json' -Body $bid   # 200, duplicate

# Invoke-RestMethod throws on 4xx and 5xx responses; the response body is in ErrorDetails.
$rival = @{ auction_id = 'demo-auction'; user_id = 'user-456'; amount = 5100 } | ConvertTo-Json
try { Invoke-RestMethod -Method Post -Uri http://127.0.0.1:3000/bid -ContentType 'application/json' -Body $rival }
catch { $_.ErrorDetails.Message }   # 409, BID_TOO_LOW
```

curl (bash):

```bash
curl -i -X POST http://127.0.0.1:3000/bid -H 'Content-Type: application/json' \
  -d '{"auction_id":"demo-auction","user_id":"user-456","amount":5200}'
```

## Design

### Data

- `auctions`: `id`, `title`, `starting_price`, `ends_at`, `created_at`, `bid_count`, and `top_bid` (`null` or `{ bid_id, user_id, amount, seq, placed_at }`). `top_bid` is the authoritative current top bid. There is no status field: an auction is open while the database clock is before `ends_at`.
- `bids`: a history of accepted bids with `id` (`<auction_id>:<amount>`), `auction_id`, `user_id`, `amount`, `seq` and `placed_at`, indexed on `[auction_id, seq]`.

### Accepting a bid

Each bid is decided by a single RethinkDB `update` on the auction document:

1. If `now >= ends_at`, it is rejected as `AUCTION_CLOSED`.
2. If there is no top bid and the amount is below `starting_price`, or there is a top bid and the amount is not strictly higher than it, it is rejected as `BID_TOO_LOW`.
3. Otherwise `bid_count` is incremented and `top_bid` is replaced by the new bid, with `seq` set to the new `bid_count`.

RethinkDB applies a deterministic update to a single document atomically, so concurrent bids on the same auction take effect one at a time and each is checked against the result of the previous one. The top bid is never read into the application and then written back separately. Accepted bids therefore get consecutive `seq` values, their amounts strictly increase with `seq`, and `top_bid` is always the highest accepted bid. Rejected bids change nothing. A user may raise their own bid; it only has to be higher than the current top bid.

### Deadline

`now` is `r.now()`: the RethinkDB server's clock, fixed when the server receives the query. It is used both for the deadline check and as the bid's `placed_at`. The cutoff is the time the query is evaluated, not the time the write is committed: a bid received just before `ends_at` that waits behind other writes to the same auction can still be accepted and saved shortly after `ends_at`, while any bid received at or after `ends_at` is rejected. `seq`, not `placed_at`, gives the order of acceptance.

### Duplicates and retries

Accepted amounts strictly increase, so an amount can be accepted at most once per auction, and `<auction_id>:<amount>` identifies an accepted bid. When a bid is rejected, the service looks that ID up in the current top bid and in the history table. If the same user owns it, the request is a retry and gets 200 with the original bid; if another user owns it, the response is 409. Simultaneous identical requests from one user produce one 201, and the rest get 200.

### Bid history

After a bid is accepted, it and the previous top bid are inserted into `bids` with an insert-if-absent conflict rule, so an existing row is never overwritten. RethinkDB has no transactions across documents or tables, so this is a second, separate write. If it fails, the bid stays accepted (the response is still 201) and the error is logged; the missing row is written later by a retry of the same request or by the next accepted bid.

## Tests

```powershell
npm test
```

The integration tests need RethinkDB running. They use the database `bidding_test` (or `RETHINKDB_TEST_DB`), which is dropped and recreated at the start and dropped again at the end. The name must end in `_test` and must differ from the application database, so `bidding` is never touched. The suite sends real concurrent requests through several app instances and also covers validation, bid rules, the deadline boundary, duplicates, history failures, and server startup and shutdown. A run takes about 30 seconds.

Current result: 40 passing, 0 failing, 2 todo. The two todo tests assert the correct behavior for the retry race described below. One fails on every run; the other fails only when the race occurs during that run. `node --test` lists todo tests separately and does not fail the run because of them.

## Known limitations

- **Retry race.** A retry that arrives after its bid was accepted and then outbid, but before either history write has finished, gets 409 instead of 200, because at that moment the bid exists neither as the top bid nor in history. In a test that sends a bid, a higher bid and the retry together, this happened in 0–2 of 100 rounds per run. Retrying once the writes have finished returns 200. A fix that keeps accepted bids on the auction document until their history row exists was designed but not implemented.
- **Lost history row.** If the process fails between the auction update and the history insert, and the process handling the next accepted bid also fails before its insert, the earlier bid's history row is lost permanently. It shows up as a gap in `seq`; `top_bid` and `bid_count` stay correct, but a retry of the lost bid gets 409.
- The amounts in a 409 response come from a read made after the decision and may already be out of date.
- Each server process uses one RethinkDB connection and does not reconnect. If the connection drops, the server shuts down with exit code 1.
- Deadlines depend on the clock of the single RethinkDB server; a cluster would need synchronized clocks. Only a single node was tested.
- There is no authentication; `user_id` is taken from the request body.

## Project layout

```
src/config.js     configuration from environment variables
src/db.js         connections and schema setup
src/placeBid.js   validation, the atomic bid update, duplicate lookup and history writes
src/app.js        Express app and HTTP responses
src/server.js     startup and graceful shutdown
scripts/          db:setup and seed
test/             integration tests
```
