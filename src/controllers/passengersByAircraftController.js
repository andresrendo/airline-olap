const pg = require('../db/postgres');
const monet = require('../db/monetdb');
const { passengersByAircraftSQL } = require('../queries/passengersByAircraft');
const { measureQueryPerformance } = require('../monitoring/resourceMonitor');

function buildMetrics(perf) {
  return {
    durationMs: perf.durationMs,
    memory: {
      before: perf.memBefore,
      after: perf.memAfter,
      delta: {
        used: perf.memAfter.used - perf.memBefore.used,
        usage: perf.memAfter.usage - perf.memBefore.usage
      }
    },
    cpu: {
      before: perf.cpuBefore,
      after: perf.cpuAfter,
      delta: {
        user: perf.cpuAfter.user - perf.cpuBefore.user,
        sys: perf.cpuAfter.sys - perf.cpuBefore.sys,
        idle: perf.cpuAfter.idle - perf.cpuBefore.idle
      }
    }
  };
}


async function getPassengersByAircraft(req, res) {
  const year = req.query.year || new Date().getFullYear();
  const db = (req.query.db || 'both').toLowerCase();

  // Genera el SQL correcto para cada motor
  const sqlPg = passengersByAircraftSQL(year, 'pg');
  const sqlMonet = passengersByAircraftSQL(year, 'monet');

  try {
    if (db === 'pg') {
      const perf = await measureQueryPerformance(() => pg.query(sqlPg));
      return res.json({
        db: 'pg',
        metrics: buildMetrics(perf),
        query: { sql: sqlPg, params: { year } },
        result: perf.result && perf.result.rows ? perf.result.rows : perf.result,
        error: perf.error
      });
    }
    if (db === 'monet') {
      const perf = await measureQueryPerformance(() => monet.query(sqlMonet));
      return res.json({
        db: 'monet',
        metrics: buildMetrics(perf),
        query: { sql: sqlMonet, params: { year } },
        result: perf.result,
        error: perf.error
      });
    }
    // Ambos motores
    const [perfPg, perfMonet] = await Promise.all([
      measureQueryPerformance(() => pg.query(sqlPg)),
      measureQueryPerformance(() => monet.query(sqlMonet))
    ]);
    res.json({
      db: 'both',
      pg: {
        metrics: buildMetrics(perfPg),
        query: { sql: sqlPg, params: { year } },
        result: perfPg.result && perfPg.result.rows ? perfPg.result.rows : perfPg.result,
        error: perfPg.error
      },
      monet: {
        metrics: buildMetrics(perfMonet),
        query: { sql: sqlMonet, params: { year } },
        result: perfMonet.result,
        error: perfMonet.error
      }
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
}


module.exports = { getPassengersByAircraft };