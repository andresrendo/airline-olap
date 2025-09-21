require('dotenv').config();

const { monitorDb } = require('./controllers/monitorController');

const express = require('express');
const cors = require('cors');

const pg = require('./db/postgres');
const monet = require('./db/monetdb');
const { getTopRoutes } = require('./controllers/topRoutesController');
const { getRevenueByCountry } = require('./controllers/revenueController');

const app = express();
app.use(cors());
app.use(express.json());

// Monitoring endpoint
app.get('/api/monitor/:db', monitorDb);

// Health endpoints
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

// OLAP endpoints
app.get('/api/olap/revenue-by-country', getRevenueByCountry);

app.get('/api/olap/top-routes', getTopRoutes);


const port = Number(process.env.PORT || 3000);
app.listen(port, () => {
  console.log(`OLAP API listening on http://localhost:${port}`);
});
