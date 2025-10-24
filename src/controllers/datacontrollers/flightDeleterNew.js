const pg = require('../../db/postgres');
const { newMonetConn } = require('./monetConn');

async function deleteGeneratedFlights(req, res){
  try {
    const countParam = req.query.count ? Math.max(1, parseInt(String(req.query.count),10)) : null;
    // find current max generated id in Postgres as simple approach
    const pgMaxRes = await pg.query(`SELECT MAX( (regexp_replace(flight_id, '^FL',''))::int ) AS maxnum FROM airline_dw.dim_flight WHERE flight_id ~ '^FL[0-9]+$'`);
    let currentMax = null;
    if (pgMaxRes && pgMaxRes.rows && pgMaxRes.rows[0] && pgMaxRes.rows[0].maxnum != null) currentMax = Number(pgMaxRes.rows[0].maxnum);
    if (!currentMax || currentMax < 10000) return res.json({ ok: true, deleted: [], note: 'No generated flights found.' });

    const totalGenerated = currentMax - 10000 + 1;
    const deleteCount = countParam ? Math.min(countParam, totalGenerated) : totalGenerated;
    const firstToDelete = currentMax - deleteCount + 1;

    const deleted = [];
    const mconn = newMonetConn(); await mconn.connect();
    const mexec = typeof mconn.query === 'function' ? mconn.query.bind(mconn) : mconn.execute.bind(mconn);

    for (let n = currentMax; n >= firstToDelete; n--){
      const fid = `FL${n}`;
      try { await pg.query('DELETE FROM airline_dw.fact_flight_metrics WHERE flight_id = $1', [fid]); } catch(_) {}
      try { await mexec(`DELETE FROM airline_dw.fact_flight_metrics WHERE flight_id = '${fid}'`); } catch(_) {}
      try { await pg.query('DELETE FROM airline_dw.dim_flight WHERE flight_id = $1', [fid]); } catch(_) {}
      try { await mexec(`DELETE FROM airline_dw.dim_flight WHERE flight_id = '${fid}'`); } catch(_) {}
      deleted.push(fid);
    }

    try{ await mconn.close(); }catch(_){}
    return res.json({ ok: true, deleted: deleted.reverse() });
  } catch (err){
    console.error('deleteGeneratedFlightsNew error', err && err.message);
    return res.status(500).json({ ok: false, error: String(err && err.message) });
  }
}

module.exports = { deleteGeneratedFlights };