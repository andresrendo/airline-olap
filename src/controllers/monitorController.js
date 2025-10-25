// src/controllers/monitorController.js
const pg = require('../db/postgres');
const monet = require('../db/monetdb');
const monitor = require('../monitoring/resourceMonitor'); // <--- agregar

// Simple test query for both DBs
const TEST_QUERY = 'SELECT 1';

function toNumber(v){ return Number(v || 0); }

async function takePgSnapshot(container, user) {
  const res = await monitor.getPostgresDatabaseSizes(container, user);
  // errores explícitos desde el monitor
  if (!res) return [];
  if (res && res.error) throw new Error(res.error);
  // ya es un array esperado
  if (Array.isArray(res)) return res;
  // caso: un solo objeto { datname, bytes }
  if (typeof res === 'object' && res.datname) return [ { datname: res.datname, bytes: Number(res.bytes || 0) } ];
  // caso: string JSON
  if (typeof res === 'string') {
    try {
      const parsed = JSON.parse(res);
      if (Array.isArray(parsed)) return parsed;
    } catch (e) { /* ignore */ }
  }
  // inesperado: log y devolver array vacío para evitar TypeError
  console.warn('takePgSnapshot: unexpected result from getPostgresDatabaseSizes', res);
  return [];
}

async function takeMonetSnapshot(container, path) {
  const res = await monitor.getMonetDbfarmBytes(container, path);
  if (!res) throw new Error('no result from getMonetDbfarmBytes');
  if (res && res.error) throw new Error(res.error);
  // bytes puede venir como número o string; normalizar
  if (typeof res.bytes === 'string' && !isNaN(Number(res.bytes))) {
    res.bytes = Number(res.bytes);
  }
  if (typeof res.bytes === 'number') return res;
  // inesperado: log y lanzar para visibilidad (Monet debe devolver bytes)
  console.warn('takeMonetSnapshot: unexpected result from getMonetDbfarmBytes', res);
  throw new Error('unexpected monet result');
}

exports.monitorDb = async (req, res) => {
  try {
    const param = (req.params.db || '').toLowerCase(); // 'pg' | 'monet' | other -> both
    const intervalMs = Number(req.query.intervalMs) || 0;

    const monetContainer = 'runmonet-monetdb-1';
    const monetPath = '/var/monetdb5/dbfarm/airline_mt';
    const pgContainer = 'runmonet-postgres-1';
    const pgUser = process.env.PG_USER || 'postgres'; // usar PG_USER desde .env (ej. "admin")

    // helpers
    const pgStart = (param === 'monet') ? null : await takePgSnapshot(pgContainer, pgUser);
    const monetStart = (param === 'pg') ? null : await takeMonetSnapshot(monetContainer, monetPath);

    if (intervalMs > 0) await new Promise(r => setTimeout(r, intervalMs));

    const pgEnd = (param === 'monet') ? null : await takePgSnapshot(pgContainer, pgUser);
    const monetEnd = (param === 'pg') ? null : await takeMonetSnapshot(monetContainer, monetPath);

    // build response
    const result = { intervalMs };

    if (monetEnd) {
      const mStartBytes = toNumber(monetStart && monetStart.bytes);
      const mEndBytes = toNumber(monetEnd.bytes);
      result.monet = {
        container: monetEnd.container,
        path: monetEnd.path,
        bytes: mEndBytes,
        deltaBytes: mEndBytes - mStartBytes
      };
    }

    if (pgEnd) {
      // ensure arrays
      const s = Array.isArray(pgStart) ? pgStart : [];
      const e = Array.isArray(pgEnd) ? pgEnd : [];
      const endMap = new Map((e || []).map(item => [item.datname, toNumber(item.bytes)]));
      result.postgres = (s || []).map(item => {
        const startB = toNumber(item.bytes);
        const endB = endMap.get(item.datname) || 0;
        return { datname: item.datname, bytes: endB, deltaBytes: endB - startB };
      });
    }

    return res.json({ ok: true, ...result });
  } catch (err) {
    console.error('monitorDb error', err);
    return res.status(500).json({ ok: false, error: String(err) });
  }
};

exports.monitorPerf = async (req, res) => {
  try {
    const dbParam = (req.query.db || '').toLowerCase(); // 'pg' | 'monet' | ''
    const monetContainer = 'runmonet-monetdb-1';
    const monetPath = '/var/monetdb5/dbfarm/airline_mt';
    const pgContainer = 'runmonet-postgres-1';
    const pgUser = process.env.PG_USER || 'admin';

    // measure Postgres by timing the postgres-size snapshot
    const measurePg = async () => await monitor.measureQueryPerformance(() => monitor.getPostgresDatabaseSizes(pgContainer, pgUser));
    // measure MonetDB by timing the du call
    const measureMonet = async () => await monitor.measureQueryPerformance(() => monitor.getMonetDbfarmBytes(monetContainer, monetPath));

    if (dbParam === 'pg') {
      const pgPerf = await measurePg();
      return res.json(pgPerf);
    }
    if (dbParam === 'monet') {
      const monetPerf = await measureMonet();
      return res.json(monetPerf);
    }
    // no db param: return both
    const [pgPerf, monetPerf] = await Promise.all([measurePg(), measureMonet()]);
    return res.json({ ok: true, pg: pgPerf, monet: monetPerf });
  } catch (err) {
    console.error('monitorPerf error', err);
    return res.status(500).json({ ok: false, error: String(err) });
  }
};
