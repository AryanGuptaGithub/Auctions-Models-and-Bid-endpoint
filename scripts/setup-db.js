// Creates the database, tables and indexes if they are missing.
// Safe to rerun: existing databases, tables, indexes and documents are never
// dropped or modified.
import { loadConfig } from '../src/config.js';
import { ensureSchema, withConnection } from '../src/db.js';

async function main() {
  const { db } = loadConfig();
  console.log(`db:setup: RethinkDB ${db.host}:${db.port}, database "${db.name}"`);

  const steps = await withConnection(db, (conn) => ensureSchema(conn, db.name));
  for (const step of steps) {
    console.log(`  ${step.action.padEnd(8)} ${step.kind.padEnd(9)} ${step.name}`);
    if (step.definition) {
      console.log(`  ${''.padEnd(18)} ${step.definition}`);
    }
  }
  console.log('Schema is ready.');
}

try {
  await main();
} catch (error) {
  console.error(`db:setup failed: ${error.name}: ${error.message}`);
  process.exitCode = 1;
}
