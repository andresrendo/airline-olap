// src/controllers/monitorController.js
const pg = require('../db/postgres');
const monet = require('../db/monetdb');
const { measureQueryPerformance } = require('../monitoring/resourceMonitor');

// Simple test query for both DBs
const TEST_QUERY = 'SELECT 1';

async function monitorDb(req, res) {
  const db = (req.params.db || '').toLowerCase();
  let queryFn;
  if (db === 'pg') {
    queryFn = () => pg.query(TEST_QUERY);
  } else if (db === 'monet') {
    queryFn = () => monet.query(TEST_QUERY);
  } else {
    return res.status(400).json({ ok: false, error: 'db param must be "pg" or "monet"' });
  }

  const perf = await measureQueryPerformance(queryFn);
  res.json({ db, ...perf });
}

module.exports = { monitorDb };
