// src/controllers/topRoutesController.js

const pg = require('../db/postgres');
const monet = require('../db/monetdb');
const { topRoutesSQL } = require('../queries/topRoutes');
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

async function getTopRoutes(req, res) {
  const year = req.query.year || 2023;
  const limit = req.query.limit || 10;
  const db = (req.query.db || 'both').toLowerCase();
  const sql = topRoutesSQL(year, limit);

  try {
    if (db === 'pg') {
      const perf = await measureQueryPerformance(() => pg.query(sql));
      return res.json({
        db: 'pg',
        metrics: buildMetrics(perf),
        query: { sql, params: { year, limit } },
        result: perf.result && perf.result.rows ? perf.result.rows : perf.result,
        error: perf.error
      });
    }
    if (db === 'monet') {
      const perf = await measureQueryPerformance(() => monet.query(sql));
      return res.json({
        db: 'monet',
        metrics: buildMetrics(perf),
        query: { sql, params: { year, limit } },
        result: perf.result,
        error: perf.error
      });
    }
    const [perfPg, perfMonet] = await Promise.all([
      measureQueryPerformance(() => pg.query(sql)),
      measureQueryPerformance(() => monet.query(sql))
    ]);
    res.json({
      db: 'both',
      pg: {
        metrics: buildMetrics(perfPg),
        query: { sql, params: { year, limit } },
        result: perfPg.result && perfPg.result.rows ? perfPg.result.rows : perfPg.result,
        error: perfPg.error
      },
      monet: {
        metrics: buildMetrics(perfMonet),
        query: { sql, params: { year, limit } },
        result: perfMonet.result,
        error: perfMonet.error
      }
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
}

module.exports = { getTopRoutes };
