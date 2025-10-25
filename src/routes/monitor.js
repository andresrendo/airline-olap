const express = require('express');
const router = express.Router();
const monitorController = require('../controllers/monitorController');
const monitor = require('../monitoring/resourceMonitor'); // <--- agregar

// /api/monitor/db-sizes
router.get('/db-sizes', async (req, res) => {
  try {
    const intervalMs = Number(req.query.intervalMs) || 0;

    const monetContainer = 'runmonet-monetdb-1';
    const monetPath = '/var/monetdb5/dbfarm/airline_mt';
    const pgContainer = 'runmonet-postgres-1';
    const pgUser = process.env.PG_USER || 'admin'; // usar mismo usuario que src/db/postgres.js

    // snapshots start
    const monetStart = await monitor.getMonetDbfarmBytes(monetContainer, monetPath);
    const pgStart = await monitor.getPostgresDatabaseSizes(pgContainer, pgUser);

    // si el start de Postgres devolvió error, devolver 500 con detalle
    if (pgStart && pgStart.error) {
      console.error('pgStart error', pgStart);
      return res.status(500).json({ ok: false, error: pgStart.error || 'postgres snapshot error', details: pgStart.stderr || null });
    }
    if (monetStart && monetStart.error) {
      console.error('monetStart error', monetStart);
      return res.status(500).json({ ok: false, error: monetStart.error });
    }

    if (intervalMs > 0) await new Promise(r => setTimeout(r, intervalMs));

    // snapshots end
    const monetEnd = await monitor.getMonetDbfarmBytes(monetContainer, monetPath);
    const pgEnd = await monitor.getPostgresDatabaseSizes(pgContainer, pgUser);

    if (pgEnd && pgEnd.error) {
      console.error('pgEnd error', pgEnd);
      return res.status(500).json({ ok: false, error: pgEnd.error || 'postgres snapshot error', details: pgEnd.stderr || null });
    }
    if (monetEnd && monetEnd.error) {
      console.error('monetEnd error', monetEnd);
      return res.status(500).json({ ok: false, error: monetEnd.error });
    }

    // construir respuesta (asegurar arrays)
    const monet = {
      container: monetEnd.container,
      path: monetEnd.path,
      bytes: monetEnd.bytes || 0,
      deltaBytes: (monetEnd.bytes || 0) - (monetStart.bytes || 0)
    };

    const s = Array.isArray(pgStart) ? pgStart : [];
    const e = Array.isArray(pgEnd) ? pgEnd : [];
    const endMap = new Map(e.map(item => [item.datname, Number(item.bytes || 0)]));
    const postgres = s.map(item => {
      const startB = Number(item.bytes || 0);
      const endB = endMap.get(item.datname) || 0;
      return { datname: item.datname, bytes: endB, deltaBytes: endB - startB };
    });

    return res.json({ ok: true, monet, postgres, intervalMs });
  } catch (err) {
    console.error('monitor/db-sizes error', err);
    return res.status(500).json({ ok: false, error: String(err) });
  }
});

// añade al menos esta ruta (GET /api/monitor?db=pg)
router.get('/', monitorController.monitorPerf);

// si ya tienes /db-sizes u otros, mantenlos
// router.get('/db-sizes', ...);

module.exports = router;