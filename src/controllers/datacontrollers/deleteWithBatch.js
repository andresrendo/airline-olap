const pg = require('../../db/postgres');
const { newMonetConn } = require('./monetConn');

const CHUNK_SIZE = 1000; // ids por DELETE IN (...)

function makeId(n) { return `FL${n}`; }

async function deleteWithBatch(req, res) {
  try {
    const countParam = req.query.count ? Math.max(1, parseInt(String(req.query.count), 10)) : null;

    // obtener max generado en Postgres (mismo criterio que antes)
    const pgMaxRes = await pg.query(
      `SELECT MAX( (regexp_replace(flight_id, '^FL',''))::int ) AS maxnum 
       FROM airline_dw.dim_flight WHERE flight_id ~ '^FL[0-9]+$'`
    );
    let currentMax = null;
    if (pgMaxRes && pgMaxRes.rows && pgMaxRes.rows[0] && pgMaxRes.rows[0].maxnum != null) {
      currentMax = Number(pgMaxRes.rows[0].maxnum);
    }
    if (!currentMax || currentMax < 10000) {
      return res.json({ ok: true, deleted: [], note: 'No generated flights found.' });
    }

    const totalGenerated = currentMax - 10000 + 1;
    const deleteCount = countParam ? Math.min(countParam, totalGenerated) : totalGenerated;
    const firstToDelete = currentMax - deleteCount + 1;

    const nums = [];
    for (let n = currentMax; n >= firstToDelete; n--) nums.push(n);
    // convert to ids ascending for response
    const deletedIdsAsc = nums.slice().reverse().map(makeId);

    // open MonetDB
    const mconn = newMonetConn(); await mconn.connect();
    const mexec = typeof mconn.query === 'function' ? mconn.query.bind(mconn) : mconn.execute.bind(mconn);

    // delete in chunks to avoid massive SQL
    for (let i = 0; i < nums.length; i += CHUNK_SIZE) {
      const chunkNums = nums.slice(i, i + CHUNK_SIZE);
      const chunkIds = chunkNums.map(n => makeId(n));
      // Postgres: parameterized IN (...)
      const placeholders = chunkIds.map((_, idx) => `$${idx + 1}`).join(',');
      const params = chunkIds;
      try {
        await pg.query('BEGIN');
        await pg.query(
          `DELETE FROM airline_dw.fact_flight_metrics WHERE flight_id IN (${placeholders})`,
          params
        );
        await pg.query(
          `DELETE FROM airline_dw.dim_flight WHERE flight_id IN (${placeholders})`,
          params
        );
        await pg.query('COMMIT');
      } catch (e) {
        try { await pg.query('ROLLBACK'); } catch (_) {}
        console.error('pg chunk delete error', e && e.message);
        // continue to try deleting remaining chunks
      }

      // MonetDB: build safe quoted list and execute (no trailing ;)
      const safeList = chunkIds.map(id => `'${id.replace(/'/g, "''")}'`).join(',');
      try {
        await mexec(`DELETE FROM airline_dw.fact_flight_metrics WHERE flight_id IN (${safeList})`);
        await mexec(`DELETE FROM airline_dw.dim_flight WHERE flight_id IN (${safeList})`);
      } catch (e) {
        console.error('monet chunk delete error', e && e.message);
      }
    }

    try{ await mconn.close(); } catch(_) {}

    return res.json({ ok: true, deleted: deletedIdsAsc });
  } catch (err) {
    console.error('deleteWithBatch error', err && err.message);
    return res.status(500).json({ ok: false, error: String(err && err.message) });
  }
}

module.exports = { deleteWithBatch };