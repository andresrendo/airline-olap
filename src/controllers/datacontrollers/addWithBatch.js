const pg = require('../../db/postgres');
const { newMonetConn } = require('./monetConn');
const {
  padId, randomDuration, pickPilot, pickDelayStatus, pickSeatClass, randomPrice, sqlLiteral
} = require('./flightHelpers');
const { fetchReferenceIds } = require('./refsFecther');

const BATCH_SIZE = Number(process.env.INSERT_BATCH_SIZE) || 500;

function buildDimValues(rows) {
  return rows.map(r =>
    `(${sqlLiteral(r.flight_id)}, ${sqlLiteral(r.flight_duration_min)}, ${sqlLiteral(r.departure_airport_id)}, ${sqlLiteral(r.arrival_airport_id)}, ${sqlLiteral(r.pilot_name)}, ${sqlLiteral(r.aircraft_id)}, ${sqlLiteral(r.delay_status)})`
  ).join(',');
}
function buildFactValues(rows) {
  return rows.map(r =>
    `(${sqlLiteral(r.passenger_id)}, ${sqlLiteral(r.flight_id)}, ${sqlLiteral(r.airport_id)}, ${sqlLiteral(r.date_id)}, ${sqlLiteral(r.seat_class)}, ${sqlLiteral(r.ticket_price_usd)})`
  ).join(',');
}

async function pgInsertBatches(pgClient, dimRows, factRows) {
  if ((!dimRows || dimRows.length === 0) && (!factRows || factRows.length === 0)) return;
  try {
    await pgClient.query('BEGIN');
    if (dimRows && dimRows.length) {
      const vals = buildDimValues(dimRows);
      const sql = `INSERT INTO airline_dw.dim_flight (flight_id, flight_duration_min, departure_airport_id, arrival_airport_id, pilot_name, aircraft_id, delay_status) VALUES ${vals} ON CONFLICT (flight_id) DO NOTHING;`;
      await pgClient.query(sql);
    }
    if (factRows && factRows.length) {
      const vals = buildFactValues(factRows);
      const sql = `INSERT INTO airline_dw.fact_flight_metrics (passenger_id, flight_id, airport_id, date_id, seat_class, ticket_price_usd) VALUES ${vals};`;
      await pgClient.query(sql);
    }
    await pgClient.query('COMMIT');
  } catch (e) {
    try { await pgClient.query('ROLLBACK'); } catch (_) {}
    throw e;
  }
}

// --- REPLACED: monetInsertBatches para evitar ';' problem en MonetDB ---
async function monetInsertBatches(mexec, dimRows, factRows) {
  if ((!dimRows || dimRows.length === 0) && (!factRows || factRows.length === 0)) return;
  // helper que quita ; finales (MonetDB client da error con SCOLON en ciertas sentencias)
  const exec = async (sql) => {
    const safe = String(sql).replace(/;+\s*$/, '');
    return await mexec(safe);
  };

  try {
    // usar BEGIN TRANSACTION sin ; (o solo 'BEGIN' sin ;)
    await exec('BEGIN TRANSACTION');
    if (dimRows && dimRows.length) {
      const vals = buildDimValues(dimRows);
      const sql = `INSERT INTO airline_dw.dim_flight (flight_id, flight_duration_min, departure_airport_id, arrival_airport_id, pilot_name, aircraft_id, delay_status) VALUES ${vals}`;
      await exec(sql);
    }
    if (factRows && factRows.length) {
      const vals = buildFactValues(factRows);
      const sql = `INSERT INTO airline_dw.fact_flight_metrics (passenger_id, flight_id, airport_id, date_id, seat_class, ticket_price_usd) VALUES ${vals}`;
      await exec(sql);
    }
    await exec('COMMIT');
  } catch (e) {
    try { await exec('ROLLBACK'); } catch (_) {}
    throw e;
  }
}

async function generateFlightsBatch(req, res) {
  try {
    const count = Math.max(1, parseInt(String(req.query.count || '1'), 10));
    const refs = await fetchReferenceIds();
    if (!refs || !refs.airports.length || !refs.aircraft.length || !refs.passengers.length || !refs.dates.length) {
      return res.status(400).json({ ok: false, error: 'Dimensiones insuficientes en las BDs para generar vuelos.' });
    }

    const inserted = [];
    const mconn = newMonetConn(); await mconn.connect();
    const mexec = typeof mconn.query === 'function' ? mconn.query.bind(mconn) : mconn.execute.bind(mconn);

    const pgMaxRes = await pg.query(`SELECT MAX( (regexp_replace(flight_id, '^FL',''))::int ) AS maxnum FROM airline_dw.dim_flight WHERE flight_id ~ '^FL[0-9]+$'`);
    let startNum = 10000;
    if (pgMaxRes && pgMaxRes.rows && pgMaxRes.rows[0] && pgMaxRes.rows[0].maxnum != null) {
      startNum = Math.max(startNum, Number(pgMaxRes.rows[0].maxnum) + 1);
    }

    const dimBatch = [];
    const factBatch = [];

    for (let i = 0; i < count; i++) {
      const n = startNum + i;
      const fid = padId(n);
      const duration = randomDuration();
      const pilot = pickPilot();
      const delayStatus = pickDelayStatus();
      let dep = refs.airports[Math.floor(Math.random()*refs.airports.length)];
      let arr = refs.airports[Math.floor(Math.random()*refs.airports.length)];
      if (dep === arr) arr = refs.airports.find(a=>a!==dep) || arr;
      const ac = refs.aircraft[Math.floor(Math.random()*refs.aircraft.length)];

      const passengerId = refs.passengers[Math.floor(Math.random()*refs.passengers.length)];
      const dateId = refs.dates[Math.floor(Math.random()*refs.dates.length)];
      const ticketPrice = randomPrice();
      const seatClass = pickSeatClass();

      dimBatch.push({
        flight_id: fid, flight_duration_min: duration, departure_airport_id: dep,
        arrival_airport_id: arr, pilot_name: pilot, aircraft_id: ac, delay_status: delayStatus
      });
      factBatch.push({
        passenger_id: passengerId, flight_id: fid, airport_id: dep, date_id: dateId, seat_class: seatClass, ticket_price_usd: ticketPrice
      });

      if (dimBatch.length >= BATCH_SIZE) {
        try {
          // primero MonetDB, luego Postgres (mantener orden conocido)
          await monetInsertBatches(mexec, dimBatch, factBatch);
          await pgInsertBatches(pg, dimBatch, factBatch);
          dimBatch.forEach(r => inserted.push(r.flight_id));
        } catch (e) {
          console.error('batch insertion error', e && e.message);
          // decidimos continuar; no hacemos rollback entre DBs (puedes mejorar aquí)
        } finally {
          dimBatch.length = 0;
          factBatch.length = 0;
        }
      }
    }

    if (dimBatch.length > 0) {
      try {
        await monetInsertBatches(mexec, dimBatch, factBatch);
        await pgInsertBatches(pg, dimBatch, factBatch);
        dimBatch.forEach(r => inserted.push(r.flight_id));
      } catch (e) {
        console.error('leftover batch error', e && e.message);
      }
    }

    try { await mconn.close(); } catch (_) {}
    return res.json({ ok: true, inserted });
  } catch (err) {
    console.error('generateFlightsBatch error', err && err.message);
    return res.status(500).json({ ok: false, error: String(err && err.message) });
  }
}

module.exports = { generateFlightsBatch };