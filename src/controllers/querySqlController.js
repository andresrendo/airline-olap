const fs = require('fs');
const path = require('path');

const queriesDir = path.join(__dirname, '..', 'queries');
const mapping = {};

// cargar archivos de queries dinámicamente (silencioso si falla)
fs.readdirSync(queriesDir).forEach(file => {
  if (!file.endsWith('.js')) return;
  const key = path.basename(file, '.js');
  try {
    mapping[key] = require(path.join(queriesDir, file));
  } catch (e) {
    console.error('querySqlController: failed to require', file, e && e.message);
  }
});

/**
 * Endpoint: GET /api/olap/query-sql?name=<queryName>&year=2023&limit=10
 * Devuelve: { ok: true, name, pg: '<sql>', monet: '<sql>' }
 */
async function getQuerySql(req, res) {
  try {
    const name = String(req.query.name || '').trim();
    if (!name) return res.status(400).json({ ok: false, error: 'name query param required' });

    const mod = mapping[name];
    if (!mod) return res.status(404).json({ ok: false, error: `query module not found: ${name}` });

    // helper para convertir cualquier resultado en string SQL
    const normalize = (val) => {
      if (val == null) return '';
      if (typeof val === 'string') return val;
      if (typeof val === 'object') {
        try { return JSON.stringify(val, null, 2); } catch (e) { return String(val); }
      }
      try { return String(val); } catch (e) { return JSON.stringify(val, null, 2); }
    };

    // attempt to find explicit exports for pg/monet
    let pgSql = null;
    let monetSql = null;

    // if module exports pg or monet properties use them
    if (mod && typeof mod === 'object') {
      if (mod.pg) pgSql = normalize(mod.pg);
      if (mod.monet) monetSql = normalize(mod.monet);
      // support common named exports: <name>SQL, e.g. topRoutesSQL or delayedAverageSQL
      const namedFn = mod[`${name}SQL`] || Object.values(mod).find(v => typeof v === 'function');
      if (!pgSql && namedFn && typeof namedFn === 'function') {
        // prepare possible params from querystring
        const year = req.query.year !== undefined ? Number.parseInt(req.query.year, 10) : undefined;
        const limit = req.query.limit !== undefined ? Number.parseInt(req.query.limit, 10) : undefined;
        // call function (support sync or async). Pass only defined numeric args.
        try {
          const args = [];
          if (typeof year === 'number' && !Number.isNaN(year)) args.push(year);
          if (typeof limit === 'number' && !Number.isNaN(limit)) args.push(limit);
          const result = await namedFn(...args);
          pgSql = normalize(result);
          monetSql = monetSql || pgSql;
        } catch (e) {
          console.error(`querySqlController: calling function for ${name} failed`, e && e.message);
        }
      }
    }

    // if module itself is a function (module.exports = function ...)
    if (!pgSql && typeof mod === 'function') {
      const year = req.query.year !== undefined ? Number.parseInt(req.query.year, 10) : undefined;
      const limit = req.query.limit !== undefined ? Number.parseInt(req.query.limit, 10) : undefined;
      try {
        const args = [];
        if (typeof year === 'number' && !Number.isNaN(year)) args.push(year);
        if (typeof limit === 'number' && !Number.isNaN(limit)) args.push(limit);
        const result = await mod(...args);
        pgSql = normalize(result);
        monetSql = monetSql || pgSql;
      } catch (e) {
        console.error(`querySqlController: calling module function for ${name} failed`, e && e.message);
      }
    }

    // fallback: if still empty, try stringify module exports (useful for debugging)
    if (!pgSql) {
      try { pgSql = normalize(mod); } catch (e) { pgSql = ''; }
    }
    if (!monetSql) monetSql = pgSql;

    return res.json({ ok: true, name, pg: pgSql, monet: monetSql });
  } catch (err) {
    console.error('getQuerySql error', err && err.message);
    return res.status(500).json({ ok: false, error: String(err && err.message) });
  }
}

module.exports = { getQuerySql };