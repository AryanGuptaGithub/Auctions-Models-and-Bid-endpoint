import express from 'express';
import { placeBid } from './placeBid.js';

const JSON_BODY_LIMIT = '4kb';

export function createApp({ conn, dbName }) {
  const app = express();
  app.disable('x-powered-by');

  app.post('/bid', express.json({ limit: JSON_BODY_LIMIT }), async (req, res) => {
    const outcome = await placeBid(conn, dbName, req.body);
    const { status, body } = toResponse(outcome);
    res.status(status).json(body);
  });

  app.use(handleError);
  return app;
}

function toResponse(outcome) {
  switch (outcome.type) {
    case 'accepted':
      return { status: 201, body: { status: 'accepted', bid: outcome.bid } };
    case 'duplicate':
      return { status: 200, body: { status: 'duplicate', bid: outcome.bid } };
    case 'invalid':
      return { status: 400, body: { error: 'VALIDATION_ERROR', message: outcome.message } };
    case 'not_found':
      return {
        status: 404,
        body: { error: 'AUCTION_NOT_FOUND', message: `Auction "${outcome.auctionId}" does not exist.` },
      };
    case 'too_low':
      return {
        status: 409,
        body: {
          error: 'BID_TOO_LOW',
          message:
            outcome.currentTopAmount === null
              ? `The first bid must be at least the starting price of ${outcome.minimumAmount}.`
              : `Bid must be higher than the current top bid of ${outcome.currentTopAmount}.`,
          current_top_amount: outcome.currentTopAmount,
          minimum_amount: outcome.minimumAmount,
        },
      };
    case 'closed':
      return {
        status: 409,
        body: {
          error: 'AUCTION_CLOSED',
          message: `The auction closed at ${outcome.endsAt.toISOString()}.`,
          ends_at: outcome.endsAt,
        },
      };
    default:
      throw new Error(`Unknown bid outcome: ${outcome.type}`);
  }
}

// Express 5 forwards both body-parser errors and rejected route handlers here.
function handleError(error, req, res, next) {
  if (res.headersSent) {
    next(error);
    return;
  }
  const bodyProblem = describeBodyError(error);
  if (bodyProblem) {
    res.status(400).json({ error: 'VALIDATION_ERROR', message: bodyProblem });
    return;
  }
  console.error(`${req.method} ${req.originalUrl} failed:`, error);
  res.status(500).json({ error: 'INTERNAL_ERROR', message: 'An unexpected error occurred.' });
}

function describeBodyError(error) {
  if (error.type === 'entity.parse.failed') {
    return 'Request body is not valid JSON.';
  }
  if (error.type === 'entity.too.large') {
    return `Request body must not exceed ${JSON_BODY_LIMIT}.`;
  }
  if (typeof error.type === 'string' && error.expose === true && error.status >= 400 && error.status < 500) {
    return 'Request body could not be read.';
  }
  return null;
}
