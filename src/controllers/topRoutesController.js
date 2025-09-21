// src/controllers/topRoutesController.js
const pg = require('../db/postgres');
const monet = require('../db/monetdb');
const { topRoutesSQL } = require('../queries/topRoutes');

async function getTopRoutes(req, res) {
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
}

module.exports = { getTopRoutes };
