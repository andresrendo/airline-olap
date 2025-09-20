const { Pool } = require('pg');

const pool = new Pool({
  host: process.env.PG_HOST || 'localhost',
  port: Number(process.env.PG_PORT || 55432),
  database: process.env.PG_DB || 'airline_pg',
  user: process.env.PG_USER || 'admin',
  password: process.env.PG_PASSWORD || ''
});

module.exports = {
  query: (text, params) => pool.query(text, params),
  pool
};
