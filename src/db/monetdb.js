const monetdb = require('monetdb');

function getConn() {
  const host = String(process.env.MONET_HOST || 'localhost').trim();
  const port = parseInt(String(process.env.MONET_PORT || '6543').trim(), 10);
  const dbname = String(process.env.MONET_DB || 'airline_mt').trim();
  const username = String(process.env.MONET_USER || 'monetdb').trim();
  const password = String(process.env.MONET_PASSWORD || 'monetdb').trim();

  if (!dbname) throw new Error('MONET_DB vacío: define MONET_DB=airline_mt en .env');

  return new monetdb.Connection({
    host,
    port,
    dbname,           // nombre de la BD
    database: dbname, // compat
    username,
    password,
    language: 'sql',  // importante
    // opcionales:
    // autocommit: true,
    // timezone: 'UTC'
  });
}

async function query(sql) {
  const conn = getConn();
  await conn.connect();
  // El cliente expone 'execute', NO 'query'
  const res = await conn.execute(sql);
  await conn.close();

  // Algunas versiones devuelven { data: [...], columns: [...] }
  return res && res.data ? res.data : res;
}

module.exports = { query };
