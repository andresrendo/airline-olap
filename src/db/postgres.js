const { Pool } = require('pg');

const pool = new Pool({
  host: process.env.PG_HOST || 'localhost',
  port: Number(process.env.PG_PORT || 55432),
  database: process.env.PG_DB || 'airline_pg',
  user: process.env.PG_USER || 'admin',
  password: String(process.env.PG_PASSWORD ?? '').trim()
});

module.exports = {
  query: (text, params) => pool.query(text, params),
  pool
};
