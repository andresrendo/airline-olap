const pg = require('../db/postgres');
const monetdbDriver = require('monetdb'); // usar driver directamente para sesiones
const monet = require('../db/monetdb'); // mantiene la función query para health checks, etc.

const BASE_NUM = 10000;
const BASE_PREFIX = 'FL';

const PILOTS = [
  'Luis Garcia','Anna Rodriguez','Maria Khan','Luis Lee','Luis Khan','Ali Rodriguez',
  'Maria Khan','Luis Garcia','John Smith','Anna Smith','Maria Smith','Luis Smith',
  'Chen Khan','Luis Rodriguez','Luis Rodriguez','Maria Lee'
];

const SEAT_CLASSES = ['Economy', 'Premium Economy', 'Business', 'First'];

function padId(num) {
  return `${BASE_PREFIX}${num}`;
}

function safeMonetParseMax(res) {
  if (!res) return null;
  // normalizar .data
  if (res && typeof res === 'object' && Array.isArray(res.data)) res = res.data;
  if (Array.isArray(res) && res.length > 0) {
    const row = res[0];
    if (row && typeof row === 'object' && ('maxnum' in row)) {
      const v = row.maxnum;
      return v === null ? null : Number(v);
    }
    if (Array.isArray(row) && row.length > 0) {
      const v = row[0];
      return v === null ? null : Number(v);
    }
    if (typeof row === 'number') return row;
  }
  return null;
}

async function getMaxGeneratedNumber() {
  let pgMax = null;
  try {
    const q = `
      SELECT MAX( (regexp_replace(flight_id, '^FL',''))::int ) AS maxnum
      FROM airline_dw.dim_flight
      WHERE flight_id ~ '^FL[0-9]+$'
        AND (regexp_replace(flight_id,'^FL',''))::int >= $1
    `;
    const r = await pg.query(q, [BASE_NUM]);
    if (r && r.rows && r.rows[0] && r.rows[0].maxnum != null) {
      pgMax = Number(r.rows[0].maxnum);
    }
  } catch (err) {
    pgMax = null;
  }

  let monetMax = null;
  try {
    const q = `SELECT MAX(CAST(SUBSTRING(flight_id FROM 3) AS INT)) AS maxnum FROM airline_dw.dim_flight WHERE flight_id LIKE 'FL%'`;
    const r = await monet.query(q);
    monetMax = safeMonetParseMax(r);
    if (monetMax != null && monetMax < BASE_NUM) monetMax = null;
  } catch (err) {
    try {
      const r2 = await monet.query(`SELECT flight_id FROM airline_dw.dim_flight WHERE flight_id LIKE 'FL%'`);
      const rows = (r2 && typeof r2 === 'object' && Array.isArray(r2.data)) ? r2.data : r2;
      if (Array.isArray(rows)) {
        let vals = [];
        for (const row of rows) {
          let fid = null;
          if (Array.isArray(row)) fid = row[0];
          else if (row && typeof row === 'object' && 'flight_id' in row) fid = row.flight_id;
          else if (typeof row === 'string') fid = row;
          if (typeof fid === 'string' && fid.startsWith(BASE_PREFIX)) {
            const num = Number(fid.slice(2));
            if (!Number.isNaN(num) && num >= BASE_NUM) vals.push(num);
          }
        }
        if (vals.length) monetMax = Math.max(...vals);
      }
    } catch (err2) {
      monetMax = null;
    }
  }

  const candidates = [pgMax, monetMax].filter(v => Number.isFinite(v));
  if (!candidates.length) return null;
  return Math.max(...candidates);
}

function randomDuration() {
  return Math.floor(Math.random() * (720 - 60 + 1)) + 60;
}

function sqlLiteral(v) {
  if (v == null) return 'NULL';
  if (typeof v === 'number') return String(v);
  return `'${String(v).replace(/'/g, "''")}'`;
}

function parseMonetRowsForSingleColumn(res) {
  // monet.query / monetdb returns may be { data: [...] } or [...] or [{col:val}, ...]
  const out = [];
  if (!res) return out;
  if (res && typeof res === 'object' && Array.isArray(res.data)) res = res.data;
  if (Array.isArray(res)) {
    for (const row of res) {
      if (Array.isArray(row)) {
        out.push(row[0]);
      } else if (row && typeof row === 'object') {
        const keys = Object.keys(row);
        if (keys.length > 0) out.push(row[keys[0]]);
      } else {
        out.push(row);
      }
    }
  }
  return out.filter(x => x != null);
}

function pickRandom(arr) {
  if (!arr || arr.length === 0) return null;
  return arr[Math.floor(Math.random() * arr.length)];
}

function pickPilot() {
  return pickRandom(PILOTS);
}

function pickDelayStatus() {
  // 30% Delayed, 70% On Time (ajustable)
  return Math.random() < 0.3 ? 'Delayed' : 'On Time';
}

function pickSeatClass() {
  return pickRandom(SEAT_CLASSES);
}

function randomPrice(min = 50, max = 1500) {
  const v = Math.random() * (max - min) + min;
  return Math.round(v * 100) / 100;
}

function newMonetConn() {
  const host = String(process.env.MONET_HOST || 'localhost').trim();
  const port = parseInt(String(process.env.MONET_PORT || '6543').trim(), 10);
  const database = String(process.env.MONET_DB || 'airline_mt').trim();
  const username = String(process.env.MONET_USER || 'monetdb').trim();
  const password = String(process.env.MONET_PASSWORD || 'monetdb').trim();

  return new monetdbDriver.Connection({
    host,
    port,
    dbname: database,
    database, // algunos clientes aceptan 'database'
    username,
    password,
    language: 'sql',
    autoCommit: true
  });
}

async function fetchReferenceIds() {
  // Fetch ids from Postgres and MonetDB, prefer intersection for consistency.
  const pgAirports = new Set();
  const pgAircraft = new Set();
  const pgPassengers = new Set();
  const pgDates = new Set();

  const monetAirports = new Set();
  const monetAircraft = new Set();
  const monetPassengers = new Set();
  const monetDates = new Set();

  try {
    const r1 = await pg.query('SELECT airport_id FROM airline_dw.dim_airport');
    if (r1 && r1.rows) r1.rows.forEach(r => r.airport_id && pgAirports.add(r.airport_id));
  } catch (e) {}
  try {
    const r2 = await pg.query('SELECT aircraft_id FROM airline_dw.dim_aircraft');
    if (r2 && r2.rows) r2.rows.forEach(r => r.aircraft_id && pgAircraft.add(r.aircraft_id));
  } catch (e) {}
  try {
    const r3 = await pg.query('SELECT passenger_id FROM airline_dw.dim_passenger');
    if (r3 && r3.rows) r3.rows.forEach(r => r.passenger_id && pgPassengers.add(r.passenger_id));
  } catch (e) {}
  try {
    const r4 = await pg.query('SELECT date_id FROM airline_dw.dim_date');
    if (r4 && r4.rows) r4.rows.forEach(r => (r.date_id || r.date_id === 0) && pgDates.add(String(r.date_id)));
  } catch (e) {}

  let conn;
  try {
    conn = newMonetConn();
    await conn.connect();
    const exec = typeof conn.query === 'function' ? conn.query.bind(conn) : conn.execute.bind(conn);

    const ra = await exec('SELECT airport_id FROM airline_dw.dim_airport');
    parseMonetRowsForSingleColumn(ra).forEach(v => v && monetAirports.add(v));

    const rc = await exec('SELECT aircraft_id FROM airline_dw.dim_aircraft');
    parseMonetRowsForSingleColumn(rc).forEach(v => v && monetAircraft.add(v));

    const rp = await exec('SELECT passenger_id FROM airline_dw.dim_passenger');
    parseMonetRowsForSingleColumn(rp).forEach(v => v && monetPassengers.add(v));

    const rd = await exec('SELECT date_id FROM airline_dw.dim_date');
    parseMonetRowsForSingleColumn(rd).forEach(v => (v || v === 0) && monetDates.add(String(v)));
  } catch (e) {
    // ignore
  } finally {
    if (conn) try { await conn.close(); } catch (e) {}
  }

  const intersect = (aSet, bSet) => {
    const out = [];
    for (const v of aSet) if (bSet.has(v)) out.push(v);
    return out;
  };

  // compute intersections
  const airportsIntersect = intersect(pgAirports, monetAirports);
  const aircraftIntersect = intersect(pgAircraft, monetAircraft);
  const passengersIntersect = intersect(pgPassengers, monetPassengers);
  const datesIntersect = intersect(pgDates, monetDates);

  // union fallbacks (kept for diagnostic / optional behavior)
  const airportsUnion = Array.from(new Set([...pgAirports, ...monetAirports]));
  const aircraftUnion = Array.from(new Set([...pgAircraft, ...monetAircraft]));
  const passengersUnion = Array.from(new Set([...pgPassengers, ...monetPassengers]));
  const datesUnion = Array.from(new Set([...pgDates, ...monetDates]));

  // convert dates to numbers
  const toNum = arr => arr.map(d => Number(d)).filter(n => Number.isFinite(n));

  return {
    airportsIntersect,
    aircraftIntersect,
    passengersIntersect,
    datesIntersect: toNum(datesIntersect),
    airportsUnion,
    aircraftUnion,
    passengersUnion,
    datesUnion: toNum(datesUnion),
    usedIntersectionAll: airportsIntersect.length > 0 && aircraftIntersect.length > 0 && passengersIntersect.length > 0 && datesIntersect.length > 0
  };
}

async function generateFlights(req, res) {
  try {
    const count = Math.max(1, parseInt(String(req.query.count || '1'), 10));
    const currentMax = await getMaxGeneratedNumber();
    const startNum = (Number.isFinite(currentMax) ? currentMax + 1 : BASE_NUM);
    const endNum = startNum + count - 1;

    const refs = await fetchReferenceIds();

    // Strict guarantee: require a common (intersection) id set for passenger, date, airport and aircraft.
    if (!refs.usedIntersectionAll) {
      return res.status(400).json({
        ok: false,
        error: 'No hay IDs comunes en ambas BDs para dim_passenger/dim_date/dim_airport/dim_aircraft. Pobla las dimensiones en ambas BDs o permitir fallback.'
      });
    }

    // use the intersect arrays as sources
    const refsForUse = {
      airports: refs.airportsIntersect || [],
      aircraft: refs.aircraftIntersect || [],
      passengers: refs.passengersIntersect || [],
      dates: refs.datesIntersect || []
    };

    // debug quick info
    console.log('generateFlights: refs counts -> airports:', refsForUse.airports.length,
      'aircraft:', refsForUse.aircraft.length,
      'passengers:', refsForUse.passengers.length,
      'dates:', refsForUse.dates.length);

    const inserted = [];

    // open one MonetDB connection for the whole operation
    const mconn = newMonetConn();
    await mconn.connect();
    const mexec = typeof mconn.query === 'function' ? mconn.query.bind(mconn) : mconn.execute.bind(mconn);

    try {
      for (let n = startNum; n <= endNum; n++) {
        const fid = padId(n);
        const duration = randomDuration();
        const pilot = pickPilot();
        const delayStatus = pickDelayStatus();
        let dep = pickRandom(refsForUse.airports);
        let arr = pickRandom(refsForUse.airports);
        if (dep && arr && dep === arr) {
          const alt = (refsForUse.airports || []).find(a => a !== dep);
          if (alt) arr = alt;
        }
        const ac = pickRandom(refsForUse.aircraft);

        // DIM FLIGHT: insert en MonetDB primero, luego Postgres. Si PG falla, borrar en MonetDB.
        let monetDimInserted = false;
        try {
          const sqlMon = `INSERT INTO airline_dw.dim_flight
            (flight_id, flight_duration_min, departure_airport_id, arrival_airport_id, pilot_name, aircraft_id, delay_status)
            VALUES (${sqlLiteral(fid)}, ${sqlLiteral(duration)}, ${sqlLiteral(dep)}, ${sqlLiteral(arr)}, ${sqlLiteral(pilot)}, ${sqlLiteral(ac)}, ${sqlLiteral(delayStatus)})`;
          await mexec(sqlMon);

          // verify presence
          const chk = await mexec(`SELECT flight_id FROM airline_dw.dim_flight WHERE flight_id = ${sqlLiteral(fid)}`);
          if (parseMonetRowsForSingleColumn(chk).some(v => String(v) === fid)) {
            monetDimInserted = true;
          } else {
            // retry once
            await mexec(sqlMon);
            const chk2 = await mexec(`SELECT flight_id FROM airline_dw.dim_flight WHERE flight_id = ${sqlLiteral(fid)}`);
            monetDimInserted = parseMonetRowsForSingleColumn(chk2).some(v => String(v) === fid);
            if (!monetDimInserted) console.error('MonetDB dim_flight insert verification FAILED for', fid, { chk, chk2 });
          }
        } catch (err) {
          monetDimInserted = false;
          console.error('MonetDB dim_flight insert error for', fid, err && err.stack ? err.stack : err);
        }

        if (!monetDimInserted) {
          // MonetDB failed, skip creating in Postgres
          continue;
        }

        // Now insert in Postgres; if PG fails, remove MonetDB row to keep symmetry
        let pgDimInserted = false;
        try {
          const pgRes = await pg.query(
            `INSERT INTO airline_dw.dim_flight
              (flight_id, flight_duration_min, departure_airport_id, arrival_airport_id, pilot_name, aircraft_id, delay_status)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (flight_id) DO NOTHING
             RETURNING flight_id`,
            [fid, duration, dep || null, arr || null, pilot, ac || null, delayStatus]
          );
          pgDimInserted = !!(pgRes && pgRes.rowCount && pgRes.rowCount > 0) || (!!pgRes && pgRes.rowCount === 0); // treat existing row as present
        } catch (err) {
          pgDimInserted = false;
          console.error('Postgres dim_flight insert error for', fid, err && err.message);
        }

        if (!pgDimInserted) {
          // remove monet row as PG failed to create/acknowledge the dim
          try {
            await mexec(`DELETE FROM airline_dw.dim_flight WHERE flight_id = ${sqlLiteral(fid)}`);
            console.warn('Removed MonetDB dim_flight after Postgres insert failure for', fid);
          } catch (e) {
            console.error('Failed to remove MonetDB dim_flight after Postgres failure for', fid, e && e.stack ? e.stack : e);
          }
          continue;
        }

        // if dims were already present in both DBs (no INSERT happened due to conflict),
        // consider them present and continue creating facts. We treat presence as success.
        const dimsPresentInBoth = (pgDimInserted || (!pgDimInserted)) && (monetDimInserted || (!monetDimInserted));
        // Now create fact rows referencing the dims
        const passengerId = pickRandom(refsForUse.passengers) || null;
        const dateId = pickRandom(refsForUse.dates) || null;
        const seatClass = pickSeatClass();
        const ticketPrice = randomPrice(50, 1500);
        const tax = Math.round(ticketPrice * (Math.random() * 0.15) * 100) / 100; // up to 15%
        const baggage = Math.round(Math.random() * 100 * 100) / 100;
        const discount = Math.round(Math.random() * 50 * 100) / 100;
        // choose fact airport (prefer departure)
        const factAirport = dep || arr || pickRandom(refsForUse.airports) || null;

        // If required FK refs are missing, skip this flight (and remove dims we inserted)
        if (!passengerId || !dateId || !factAirport) {
          // cleanup dims
          try { await pg.query(`DELETE FROM airline_dw.dim_flight WHERE flight_id = $1`, [fid]); } catch (e) {}
          try { await mexec(`DELETE FROM airline_dw.dim_flight WHERE flight_id = ${sqlLiteral(fid)}`); } catch (e) {}
          continue;
        }

        // INSERT FACT: MonetDB primero, luego Postgres (compensar si PG falla)
        // MonetDB fact insert first
        let monetFactInserted = false;
        try {
          const sqlF = `INSERT INTO airline_dw.fact_flight_metrics
            (passenger_id, flight_id, airport_id, date_id, seat_class, ticket_price_usd, tax_usd, baggage_fee_usd, discount_usd)
            VALUES (${sqlLiteral(passengerId)}, ${sqlLiteral(fid)}, ${sqlLiteral(factAirport)}, ${sqlLiteral(dateId)}, ${sqlLiteral(seatClass)}, ${sqlLiteral(ticketPrice)}, ${sqlLiteral(tax)}, ${sqlLiteral(baggage)}, ${sqlLiteral(discount)})`;
          await mexec(sqlF);
          const chkF = await mexec(`SELECT id FROM airline_dw.fact_flight_metrics WHERE flight_id = ${sqlLiteral(fid)} AND passenger_id = ${sqlLiteral(passengerId)} AND date_id = ${sqlLiteral(dateId)} LIMIT 1`);
          monetFactInserted = parseMonetRowsForSingleColumn(chkF).length > 0;
          if (!monetFactInserted) {
            // retry once
            await mexec(sqlF);
            const chkF2 = await mexec(`SELECT id FROM airline_dw.fact_flight_metrics WHERE flight_id = ${sqlLiteral(fid)} AND passenger_id = ${sqlLiteral(passengerId)} AND date_id = ${sqlLiteral(dateId)} LIMIT 1`);
            monetFactInserted = parseMonetRowsForSingleColumn(chkF2).length > 0;
            if (!monetFactInserted) console.error('MonetDB fact insert verification FAILED for', fid, { passengerId, dateId, sqlF, chkF2 });
          }
        } catch (err) {
          monetFactInserted = false;
          console.error('MonetDB fact insert error for', fid, err && err.stack ? err.stack : err);
        }

        if (!monetFactInserted) {
          // remove dims created earlier in both DBs to keep symmetry
          try { await pg.query(`DELETE FROM airline_dw.dim_flight WHERE flight_id = $1`, [fid]); } catch (e) {}
          try { await mexec(`DELETE FROM airline_dw.dim_flight WHERE flight_id = ${sqlLiteral(fid)}`); } catch (e) {}
          continue;
        }

        // Then insert in Postgres; if PG fails, delete monet fact and monet dim
        let pgFactInserted = false;
        try {
          const pgF = await pg.query(
            `INSERT INTO airline_dw.fact_flight_metrics
              (passenger_id, flight_id, airport_id, date_id, seat_class, ticket_price_usd, tax_usd, baggage_fee_usd, discount_usd)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id`,
            [passengerId, fid, factAirport, dateId, seatClass, ticketPrice, tax, baggage, discount]
          );
          pgFactInserted = !!(pgF && pgF.rowCount && pgF.rowCount > 0);
        } catch (err) {
          pgFactInserted = false;
          console.error('Postgres fact insert error for', fid, err && err.message);
        }

        if (!pgFactInserted) {
          // remove monet fact and monet dim
          try { await mexec(`DELETE FROM airline_dw.fact_flight_metrics WHERE flight_id = ${sqlLiteral(fid)} AND passenger_id = ${sqlLiteral(passengerId)} AND date_id = ${sqlLiteral(dateId)}`); } catch (e) {}
          try { await mexec(`DELETE FROM airline_dw.dim_flight WHERE flight_id = ${sqlLiteral(fid)}`); } catch (e) {}
          // ensure PG dim removed as well (in case PG partially created)
          try { await pg.query(`DELETE FROM airline_dw.dim_flight WHERE flight_id = $1`, [fid]); } catch (e) {}
          continue;
        }

        // success on both DBs
        inserted.push(fid);
      }
    } finally {
      try { await mconn.close(); } catch (e) {}
    }

    return res.json({ ok: true, inserted });
  } catch (err) {
    console.error('generateFlights error', err && err.message);
    return res.status(500).json({ ok: false, error: String(err && err.message) });
  }
}

async function deleteGeneratedFlights(req, res) {
  try {
    const countParam = req.query.count ? Math.max(1, parseInt(String(req.query.count), 10)) : null;
    const currentMax = await getMaxGeneratedNumber();

    if (!Number.isFinite(currentMax) || currentMax < BASE_NUM) {
      return res.status(200).json({ ok: true, deleted: [], note: 'No generated flights (>= FL' + BASE_NUM + ') to delete.' });
    }

    const MAX_DELTA = 1_000_000;
    const maxAllowed = BASE_NUM + MAX_DELTA;
    if (currentMax > maxAllowed) {
      console.error('deleteGeneratedFlights aborted: detected suspicious max generated id:', currentMax);
      return res.status(400).json({ ok: false, error: 'Suspicious max generated flight id detected, aborting delete.' });
    }

    const maxNum = Math.floor(currentMax);
    const totalGenerated = maxNum - BASE_NUM + 1;
    const deleteCount = countParam ? Math.min(countParam, totalGenerated) : totalGenerated;
    const firstToDelete = Math.max(BASE_NUM, maxNum - deleteCount + 1);

    const deleted = [];

    const mconn = newMonetConn();
    await mconn.connect();
    const mexec = typeof mconn.query === 'function' ? mconn.query.bind(mconn) : mconn.execute.bind(mconn);

    try {
      for (let n = maxNum; n >= firstToDelete; n--) {
        if (n < BASE_NUM) break;
        const fid = padId(n);

        // delete fact rows first (both DBs), then dim_flight
        try {
          await pg.query(`DELETE FROM airline_dw.fact_flight_metrics WHERE flight_id = $1`, [fid]);
        } catch (err) {
          console.error('Postgres delete fact error', fid, err && err.message);
        }
        try {
          await mexec(`DELETE FROM airline_dw.fact_flight_metrics WHERE flight_id = ${sqlLiteral(fid)}`);
        } catch (err) {
          // ignore
        }

        // delete dim_flight
        try {
          await pg.query(`DELETE FROM airline_dw.dim_flight WHERE flight_id = $1`, [fid]);
        } catch (err) {
          console.error('Postgres delete dim error', fid, err && err.message);
        }
        try {
          await mexec(`DELETE FROM airline_dw.dim_flight WHERE flight_id = ${sqlLiteral(fid)}`);
        } catch (err) {
          // ignore
        }

        deleted.push(fid);
      }
    } finally {
      try { await mconn.close(); } catch (e) {}
    }

    return res.json({ ok: true, deleted: deleted.reverse() });
  } catch (err) {
    console.error('deleteGeneratedFlights error', err && err.message);
    return res.status(500).json({ ok: false, error: String(err && err.message) });
  }
}

module.exports = {
  generateFlights,
  deleteGeneratedFlights
};