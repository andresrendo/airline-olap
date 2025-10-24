const pg = require('../../db/postgres');
const { newMonetConn } = require('./monetConn');
const { parseMonetRowsForSingleColumn } = require('./flightHelpers');

async function fetchReferenceIds(){
  const pgSets = { airports: new Set(), aircraft: new Set(), passengers: new Set(), dates: new Set() };

  try {
    const r = await pg.query('SELECT airport_id FROM airline_dw.dim_airport');
    if (r && r.rows) r.rows.forEach(x => x.airport_id && pgSets.airports.add(x.airport_id));
  } catch(e){}

  try {
    const r = await pg.query('SELECT aircraft_id FROM airline_dw.dim_aircraft');
    if (r && r.rows) r.rows.forEach(x => x.aircraft_id && pgSets.aircraft.add(x.aircraft_id));
  } catch(e){}

  try {
    const r = await pg.query('SELECT passenger_id FROM airline_dw.dim_passenger');
    if (r && r.rows) r.rows.forEach(x => x.passenger_id && pgSets.passengers.add(x.passenger_id));
  } catch(e){}

  try {
    const r = await pg.query('SELECT date_id FROM airline_dw.dim_date');
    if (r && r.rows) r.rows.forEach(x => (x.date_id||x.date_id===0) && pgSets.dates.add(String(x.date_id)));
  } catch(e){}

  // MonetDB
  let conn;
  try {
    conn = newMonetConn(); await conn.connect();
    const exec = typeof conn.query === 'function' ? conn.query.bind(conn) : conn.execute.bind(conn);

    const ra = await exec('SELECT airport_id FROM airline_dw.dim_airport');
    parseMonetRowsForSingleColumn(ra).forEach(v => v && pgSets.airports.add(v)); // union behavior - we will intersect later

    const rc = await exec('SELECT aircraft_id FROM airline_dw.dim_aircraft');
    parseMonetRowsForSingleColumn(rc).forEach(v => v && pgSets.aircraft.add(v));

    const rp = await exec('SELECT passenger_id FROM airline_dw.dim_passenger');
    parseMonetRowsForSingleColumn(rp).forEach(v => v && pgSets.passengers.add(v));

    const rd = await exec('SELECT date_id FROM airline_dw.dim_date');
    parseMonetRowsForSingleColumn(rd).forEach(v => (v||v===0) && pgSets.dates.add(String(v)));
  } catch(e){
    // ignore - partial data still useful
  } finally {
    if (conn) try { await conn.close(); } catch (e) {}
  }

  const toArr = s => Array.from(s);
  return {
    airports: toArr(pgSets.airports),
    aircraft: toArr(pgSets.aircraft),
    passengers: toArr(pgSets.passengers),
    dates: toArr(pgSets.dates).map(x=>Number(x)).filter(n=>Number.isFinite(n))
  };
}

module.exports = { fetchReferenceIds };