const pg = require('../../db/postgres');
const { newMonetConn } = require('./monetConn');
const { padId, randomDuration, pickPilot, pickDelayStatus, pickSeatClass, randomPrice, sqlLiteral } = require('./flightHelpers');
const { fetchReferenceIds } = require('./refsFecther');

async function generateFlights(req, res){
  try {
    const count = Math.max(1, parseInt(String(req.query.count || '1'), 10));
    const refs = await fetchReferenceIds();
    if (!refs || !refs.airports.length || !refs.aircraft.length || !refs.passengers.length || !refs.dates.length) {
      return res.status(400).json({ ok: false, error: 'Dimensiones insuficientes en las BDs para generar vuelos.' });
    }

    const inserted = [];
    const mconn = newMonetConn(); await mconn.connect();
    const mexec = typeof mconn.query === 'function' ? mconn.query.bind(mconn) : mconn.execute.bind(mconn);

    // find a safe start number by checking current max in both DBs (simple approach: check postgres)
    const pgMaxRes = await pg.query(`SELECT MAX( (regexp_replace(flight_id, '^FL',''))::int ) AS maxnum FROM airline_dw.dim_flight WHERE flight_id ~ '^FL[0-9]+$'`);
    let startNum = 10000;
    if (pgMaxRes && pgMaxRes.rows && pgMaxRes.rows[0] && pgMaxRes.rows[0].maxnum != null){
      startNum = Math.max(startNum, Number(pgMaxRes.rows[0].maxnum) + 1);
    }

    for (let i = 0; i < count; i++){
      const n = startNum + i;
      const fid = padId(n);
      const duration = randomDuration();
      const pilot = pickPilot();
      const delayStatus = pickDelayStatus();
      let dep = refs.airports[Math.floor(Math.random()*refs.airports.length)];
      let arr = refs.airports[Math.floor(Math.random()*refs.airports.length)];
      if (dep === arr) arr = refs.airports.find(a=>a!==dep) || arr;
      const ac = refs.aircraft[Math.floor(Math.random()*refs.aircraft.length)];

      // insert dim flight MonetDB then Postgres (compensate on failure)
      try {
        await mexec(`INSERT INTO airline_dw.dim_flight (flight_id, flight_duration_min, departure_airport_id, arrival_airport_id, pilot_name, aircraft_id, delay_status)
          VALUES (${sqlLiteral(fid)}, ${sqlLiteral(duration)}, ${sqlLiteral(dep)}, ${sqlLiteral(arr)}, ${sqlLiteral(pilot)}, ${sqlLiteral(ac)}, ${sqlLiteral(delayStatus)})`);
      } catch(e){
        // skip on monet fail
        continue;
      }

      try {
        await pg.query(
          `INSERT INTO airline_dw.dim_flight (flight_id, flight_duration_min, departure_airport_id, arrival_airport_id, pilot_name, aircraft_id, delay_status)
           VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (flight_id) DO NOTHING`,
          [fid, duration, dep||null, arr||null, pilot, ac||null, delayStatus]
        );
      } catch(e){
        // rollback monet insert if pg failed
        try { await mexec(`DELETE FROM airline_dw.dim_flight WHERE flight_id = ${sqlLiteral(fid)}`); } catch(_){}
        continue;
      }

      // insert fact: minimal example
      const passengerId = refs.passengers[Math.floor(Math.random()*refs.passengers.length)];
      const dateId = refs.dates[Math.floor(Math.random()*refs.dates.length)];
      const ticketPrice = randomPrice();
      const seatClass = pickSeatClass();

      try {
        await mexec(`INSERT INTO airline_dw.fact_flight_metrics (passenger_id, flight_id, airport_id, date_id, seat_class, ticket_price_usd)
          VALUES (${sqlLiteral(passengerId)}, ${sqlLiteral(fid)}, ${sqlLiteral(dep)}, ${sqlLiteral(dateId)}, ${sqlLiteral(seatClass)}, ${sqlLiteral(ticketPrice)})`);
      } catch(e){
        // cleanup dims if fact fails
        try { await pg.query('DELETE FROM airline_dw.dim_flight WHERE flight_id=$1', [fid]); } catch(_){}
        try { await mexec(`DELETE FROM airline_dw.dim_flight WHERE flight_id = ${sqlLiteral(fid)}`); } catch(_){}
        continue;
      }

      try {
        await pg.query(`INSERT INTO airline_dw.fact_flight_metrics (passenger_id, flight_id, airport_id, date_id, seat_class, ticket_price_usd)
          VALUES ($1,$2,$3,$4,$5,$6)`, [passengerId, fid, dep, dateId, seatClass, ticketPrice]);
      } catch(e){
        // cleanup monet inserts if pg fact fails
        try { await mexec(`DELETE FROM airline_dw.fact_flight_metrics WHERE flight_id = ${sqlLiteral(fid)}`); } catch(_){}
        try { await mexec(`DELETE FROM airline_dw.dim_flight WHERE flight_id = ${sqlLiteral(fid)}`); } catch(_){}
        try { await pg.query('DELETE FROM airline_dw.dim_flight WHERE flight_id=$1', [fid]); } catch(_){}
        continue;
      }

      inserted.push(fid);
    }

    try{ await mconn.close(); }catch(_){}
    return res.json({ ok: true, inserted });
  } catch (err){
    console.error('generateFlightsNew error', err && err.message);
    return res.status(500).json({ ok: false, error: String(err && err.message) });
  }
}

module.exports = { generateFlights };