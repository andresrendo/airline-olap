// src/monitoring/resourceMonitor.js
const os = require('os');
const { exec, execFile } = require('child_process');
const pgPool = require('../db/postgres'); // debe exportar un Pool (node-postgres)
// Obtiene stats de Docker para los contenedores especificados

// --- estado previo (en memoria del proceso) para calcular deltas ---
let prevMonetBytes = null;
let prevPgSizes = {}; // map datname -> bytes

// función auxiliar que ejecuta docker y devuelve estructura uniforme
function dockerExecArgs(args = [], timeout = 7000) {
  return new Promise((resolve) => {
    const start = Date.now();
    execFile('docker', args, { timeout }, (err, stdout, stderr) => {
      const dur = Date.now() - start;
      const out = String(stdout || '');
      const errStr = String(stderr || '');
      // log para detectar overhead de docker exec
      console.log(`[DOCKER EXEC] cmd="docker ${args.join(' ')}" durationMs=${dur} stdout_len=${out.length} stderr_len=${errStr.length}`);
      if (err) {
        return resolve({
          error: true,
          code: err.code || null,
          message: err.message || String(err),
          stdout: out,
          stderr: errStr
        });
      }
      resolve({ error: false, stdout: out, stderr: errStr });
    });
  });
}

function getDockerStats(containers = ['runmonet-postgres-1', 'runmonet-monetdb-1']) {
  return new Promise((resolve) => {
    const cmd = `docker stats --no-stream --format "{{.Name}}:{{.CPUPerc}}:{{.MemUsage}}:{{.BlockIO}}:{{.NetIO}}" ${containers.join(' ')}`;
    // Timeout de 3 segundos para evitar bloqueos
    exec(cmd, { timeout: 3000 }, (err, stdout, stderr) => {
      if (err) {
        const errObj = { error: 'No se pudo obtener datos de Docker. Verifica permisos y que Docker esté corriendo.' };
        return resolve(errObj);
      }
      const out = String(stdout || '').trim();
      if (!out) {
        return resolve({});
      }
      const lines = out.split('\n');
      const stats = {};
      lines.forEach(line => {
        // formato esperado: Name:CPUPerc:MemUsage:BlockIO:NetIO
        const parts = line.split(':');
        if (parts.length >= 5) {
          const netio = parts.pop();
          const blockio = parts.pop();
          const mem = parts.pop();
          const cpu = parts.pop();
          const name = parts.join(':');
          stats[name] = { cpu: cpu.trim(), mem: mem.trim(), blockio: blockio.trim(), netio: netio.trim() };
        } else {
          const [name, cpu = '-', mem = '-'] = parts;
          stats[name || 'unknown'] = { cpu: (cpu || '-').trim(), mem: (mem || '-').trim(), blockio: '-', netio: '-' };
        }
      });
      resolve(Object.assign({}, stats));
    });
  });
}

function getCpuUsage() {
  // Returns average CPU usage percentage since last call
  const cpus = os.cpus();
  let user = 0, nice = 0, sys = 0, idle = 0, irq = 0;
  for (let cpu of cpus) {
    user += cpu.times.user;
    nice += cpu.times.nice;
    sys += cpu.times.sys;
    idle += cpu.times.idle;
    irq += cpu.times.irq;
  }
  const total = user + nice + sys + idle + irq;
  return {
    user: user / total,
    sys: sys / total,
    idle: idle / total,
    total
  };
}

function getMemoryUsage() {
  return {
    total: os.totalmem(),
    free: os.freemem(),
    used: os.totalmem() - os.freemem(),
    usage: (os.totalmem() - os.freemem()) / os.totalmem()
  };
}

async function measureQueryPerformance(queryFn) {
  const memBefore = getMemoryUsage();
  const cpuBefore = getCpuUsage();
  const start = process.hrtime.bigint();
  let result, error;
  try {
    result = await queryFn();
  } catch (e) {
    error = e;
  }
  const end = process.hrtime.bigint();
  const memAfter = getMemoryUsage();
  const cpuAfter = getCpuUsage();
  const durationMs = Number(end - start) / 1e6;
  return {
    durationMs,
    memBefore,
    memAfter,
    cpuBefore,
    cpuAfter,
    result,
    error: error ? String(error) : null
  };
}

/**
 * Devuelve los bytes exactos ocupados por un path (ej. dbfarm) dentro del contenedor.
 */
function getMonetDbfarmBytes(containerName = 'runmonet-monetdb-1', dataPath = '/var/monetdb5/dbfarm/airline_mt') {
  return new Promise(async (resolve) => {
    try {
      const args = ['exec', containerName, 'bash', '-lc', `du -sb ${dataPath} 2>/dev/null || echo '0\t${dataPath}'`];
      const res = await dockerExecArgs(args, 5000);
      if (res.error) return resolve({ error: res.message || res.code || 'error', stderr: res.stderr || '' });
      const m = String(res.stdout || '').trim().match(/^(\d+)\s+/);
      const bytes = m ? Number(m[1]) : 0;
      const deltaBytes = prevMonetBytes === null ? 0 : (bytes - prevMonetBytes);

      console.log(`[MONET SIZE] prev=${prevMonetBytes} current=${bytes} delta=${deltaBytes}`);

      prevMonetBytes = bytes;
      const value = { container: containerName, path: dataPath, bytes, deltaBytes };
      resolve(Object.assign({}, value));
    } catch (e) {
      resolve({ error: String(e) });
    }
  });
}

/**
 * Obtiene tamaños (en bytes) de todas las bases de datos de PostgreSQL.
 */
async function getPostgresDatabaseSizes() {
  try {
    const sql = `SELECT datname, pg_database_size(datname) AS bytes FROM pg_database;`;
    const res = await pgPool.query(sql);
    const out = res.rows.map(r => {
      const bytes = typeof r.bytes === 'string' ? Number(r.bytes) : (r.bytes || 0);
      const prev = prevPgSizes[r.datname];
      const deltaBytes = (typeof prev === 'number') ? (bytes - prev) : 0;
      prevPgSizes[r.datname] = bytes;
      console.log(`[PG SIZE] db=${r.datname} prev=${prev} current=${bytes} delta=${deltaBytes}`);
      return { datname: r.datname, bytes, deltaBytes };
    });
    return out;
  } catch (e) {
    return { error: String(e) };
  }
}

/**
 * Obtiene tamaños en bytes de tablas de usuario en la BD actual.
 */
async function getPostgresTableSizes() {
  try {
    const sql = `SELECT relname, pg_total_relation_size(relid) AS bytes FROM pg_catalog.pg_stat_user_tables;`;
    const res = await pgPool.query(sql);
    const out = res.rows.map(r => {
      const bytes = typeof r.bytes === 'string' ? Number(r.bytes) : (r.bytes || 0);
      return { relname: r.relname, bytes };
    });
    return out;
  } catch (e) {
    return { error: String(e) };
  }
}

// --- FIN nuevas funciones ---

module.exports = { getCpuUsage, getMemoryUsage, measureQueryPerformance, getDockerStats, getMonetDbfarmBytes, getPostgresDatabaseSizes, getPostgresTableSizes };
