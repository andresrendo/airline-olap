require('dotenv').config();
const express = require('express');
const cors = require('cors');

const pg = require('./db/postgres');
const monet = require('./db/monetdb');

const { revenueByCountrySQL } = require('./queries/revenueByCountry');
const { topRoutesSQL } = require('./queries/topRoutes');

const app = express();
app.use(cors());
app.use(express.json());

app.get('/api/health', (req, res) => {
  res.json({ ok: true, service: 'airline-olap', time: new Date().toISOString() });
});

app.get('/api/health/pg', async (req, res) => {
  try {
    const r = await pg.query('SELECT 1 AS ok');
    res.json({ ok: true, result: r.rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.get('/api/health/monet', async (req, res) => {
  try {
    const r = await monet.query('SELECT 1 AS ok;');
    res.json({ ok: true, result: r });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.get('/api/olap/revenue-by-country', async (req, res) => {
  const year = req.query.year || new Date().getFullYear();
  const db = (req.query.db || 'both').toLowerCase();
  const sql = revenueByCountrySQL(year);

  try {
    if (db === 'pg') {
      const r = await pg.query(sql);
      return res.json({ db: 'pg', rows: r.rows });
    }
    if (db === 'monet') {
      const r = await monet.query(sql);
      return res.json({ db: 'monet', rows: r });
    }
    const [rpg, rmon] = await Promise.all([pg.query(sql), monet.query(sql)]);
    res.json({ db: 'both', pg: rpg.rows, monet: rmon });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.get('/api/olap/top-routes', async (req, res) => {
  const year = req.query.year || new Date().getFullYear();
  const limit = req.query.limit || 10;
  const db = (req.query.db || 'both').toLowerCase();
  const sql = topRoutesSQL(year, limit);

  try {
    if (db === 'pg') {
      const r = await pg.query(sql);
      return res.json({ db: 'pg', rows: r.rows });
    }
    if (db === 'monet') {
      const r = await monet.query(sql);
      return res.json({ db: 'monet', rows: r });
    }
    const [rpg, rmon] = await Promise.all([pg.query(sql), monet.query(sql)]);
    res.json({ db: 'both', pg: rpg.rows, monet: rmon });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

const port = Number(process.env.PORT || 3000);
app.listen(port, () => {
  console.log(`OLAP API listening on http://localhost:${port}`);
});
