import http from 'node:http';
import { createApp } from './app.js';
import { loadConfig } from './config.js';
import { connect } from './db.js';

const SHUTDOWN_TIMEOUT_MS = 10_000;

function listen(server, { host, port }) {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, host, () => {
      server.off('error', reject);
      resolve();
    });
  });
}

// In-flight requests finish before the database connection closes; any still open
// after SHUTDOWN_TIMEOUT_MS are cut off.
async function closeGracefully(server, conn) {
  const forceClose = setTimeout(() => {
    console.error('Shutdown timed out; closing the remaining HTTP connections.');
    server.closeAllConnections();
  }, SHUTDOWN_TIMEOUT_MS);
  await new Promise((resolve) => server.close(resolve));
  clearTimeout(forceClose);
  await conn.close();
}

async function main() {
  const config = loadConfig();
  const conn = await connect(config.db);
  const server = http.createServer(createApp({ conn, dbName: config.db.name }));
  try {
    await listen(server, config.http);
  } catch (error) {
    await conn.close();
    throw error;
  }
  console.log(
    `Auction bidding API listening on http://${config.http.host}:${config.http.port} ` +
      `(RethinkDB ${config.db.host}:${config.db.port}, database "${config.db.name}")`,
  );

  let stopping = null;
  const stop = (reason, exitCode) => {
    stopping ??= (async () => {
      console.log(`Shutting down (${reason})...`);
      process.exitCode = exitCode;
      await closeGracefully(server, conn);
      console.log('HTTP server and RethinkDB connection closed.');
    })().catch((error) => {
      console.error(`Shutdown failed: ${error.message}`);
      process.exitCode = 1;
    });
    return stopping;
  };

  process.once('SIGINT', () => stop('SIGINT', 0));
  process.once('SIGTERM', () => stop('SIGTERM', 0));
  // The connection is not re-established; requests would only fail until a restart.
  conn.once('close', () => {
    if (!stopping) {
      console.error('RethinkDB connection closed unexpectedly. Restart the server once RethinkDB is reachable.');
      stop('lost RethinkDB connection', 1);
    }
  });
}

try {
  await main();
} catch (error) {
  console.error(`Server failed to start: ${error.name}: ${error.message}`);
  process.exitCode = 1;
}
