

const DEFAULTS = {
  HTTP_HOST: '127.0.0.1',
  PORT: '3000',
  RETHINKDB_HOST: 'localhost',
  RETHINKDB_PORT: '28015',
  RETHINKDB_DB: 'bidding',
  RETHINKDB_USER: 'admin',
  RETHINKDB_PASSWORD: '',
};


export function loadConfig(env = process.env) {
  const read = (key) => (env[key] === undefined || env[key] === '' ? DEFAULTS[key] : env[key]);
  return Object.freeze({
    http: Object.freeze({
      host: read('HTTP_HOST'),
      port: parsePort('PORT', read('PORT')),
    }),
    db: Object.freeze({
      host: read('RETHINKDB_HOST'),
      port: parsePort('RETHINKDB_PORT', read('RETHINKDB_PORT')),
      name: parseDbName('RETHINKDB_DB', read('RETHINKDB_DB')),
      user: read('RETHINKDB_USER'),
      password: read('RETHINKDB_PASSWORD'),
    }),
  });
}

function parsePort(key, value) {
  const port = Number(value);
  if (!/^\d+$/.test(value) || port < 1 || port > 65535) {
    throw new Error(`${key} must be an integer from 1 to 65535 (got "${value}")`);
  }
  return port;
}


function parseDbName(key, value) {
  if (!/^[A-Za-z0-9_]+$/.test(value)) {
    throw new Error(`${key} may only contain letters, digits and underscores (got "${value}")`);
  }
  return value;
}
