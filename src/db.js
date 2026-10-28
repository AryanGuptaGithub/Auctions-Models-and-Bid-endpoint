import r from 'rethinkdb';



export const TABLES = Object.freeze({ auctions: 'auctions', bids: 'bids' });

const PRIMARY_KEY = 'id';
const TABLE_READY_TIMEOUT_SECONDS = 30;


const INDEXES = {
  [TABLES.auctions]: {},
  [TABLES.bids]: {
    auction_seq: (bid) => [bid('auction_id'), bid('seq')],
  },
};


export async function connect({ host, port, user, password }) {
  const conn = await r.connect({ host, port, user, password });
 
  conn.on('error', (error) => {
    console.error(`RethinkDB connection error: ${error.message}`);
  });
  return conn;
}

// Runs work(conn) on a new connection and always closes the connection afterwards.
export async function withConnection(dbConfig, work) {
  const conn = await connect(dbConfig);
  try {
    return await work(conn);
  } finally {
    await conn.close();
  }
}


export async function ensureSchema(conn, dbName) {
  const steps = [await ensureDatabase(conn, dbName)];
  for (const [table, indexes] of Object.entries(INDEXES)) {
    steps.push(await ensureTable(conn, dbName, table));
    for (const [index, definition] of Object.entries(indexes)) {
      steps.push(await ensureIndex(conn, dbName, table, index, definition));
    }
  }
  return steps;
}

async function ensureDatabase(conn, dbName) {
  const exists = await r.dbList().contains(dbName).run(conn);
  if (!exists) {
    await r.dbCreate(dbName).run(conn);
  }
  return { kind: 'database', name: dbName, action: exists ? 'exists' : 'created' };
}

async function ensureTable(conn, dbName, table) {
  const exists = await r.db(dbName).tableList().contains(table).run(conn);
  if (exists) {
    const primaryKey = await r.db(dbName).table(table).info()('primary_key').run(conn);
    if (primaryKey !== PRIMARY_KEY) {
      throw new Error(
        `Table ${dbName}.${table} already exists with primary key "${primaryKey}" instead of "${PRIMARY_KEY}". ` +
          'Setup never alters existing tables; fix or remove it manually.',
      );
    }
  } else {
    await r.db(dbName).tableCreate(table, { primaryKey: PRIMARY_KEY }).run(conn);
  }
  await r.db(dbName).table(table).wait({ timeout: TABLE_READY_TIMEOUT_SECONDS }).run(conn);
  return { kind: 'table', name: `${dbName}.${table}`, action: exists ? 'exists' : 'created' };
}

async function ensureIndex(conn, dbName, table, index, definition) {
  const tableRef = r.db(dbName).table(table);
  const exists = await tableRef.indexList().contains(index).run(conn);
  if (!exists) {
    await tableRef.indexCreate(index, definition).run(conn);
  }
  const [status] = await tableRef.indexWait(index).run(conn);
  if (status.multi || status.geo) {
    throw new Error(
      `Index ${dbName}.${table}.${index} already exists as a ${status.multi ? 'multi' : 'geo'} index, ` +
        'which this application does not expect. Setup never drops indexes; fix or remove it manually.',
    );
  }
  return { kind: 'index', name: `${dbName}.${table}.${index}`, action: exists ? 'exists' : 'created', definition: status.query };
}
