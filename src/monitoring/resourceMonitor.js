// src/monitoring/resourceMonitor.js
const os = require('os');
const { exec, execFile } = require('child_process');
// Obtiene stats de Docker para los contenedores especificados

// helper: ejecuta 'docker' con args (no pasa por shell -> evita problemas de quoting en Windows)
function dockerExecArgs(args = [], timeout = 7000) {
  return new Promise((resolve) => {
    execFile('docker', args, { timeout }, (err, stdout, stderr) => {
      if (err) {
        return resolve({
          error: true,
          code: err.code || null,
          message: err.message || String(err),
          stdout: String(stdout || ''),
          stderr: String(stderr || '')
        });
      }
      resolve({ error: false, stdout: String(stdout || ''), stderr: String(stderr || '') });
    });
  });
}

function getDockerStats(containers = ['runmonet-postgres-1', 'runmonet-monetdb-1']) {
  return new Promise((resolve) => {
    // añadir BlockIO y NetIO al formato
    const cmd = `docker stats --no-stream --format "{{.Name}}:{{.CPUPerc}}:{{.MemUsage}}:{{.BlockIO}}:{{.NetIO}}" ${containers.join(' ')}`;
    // Timeout de 3 segundos para evitar bloqueos
    exec(cmd, { timeout: 3000 }, (err, stdout, stderr) => {
      if (err) {
        resolve({ error: 'No se pudo obtener datos de Docker. Verifica permisos y que Docker esté corriendo.' });
        return;
      }
      const out = String(stdout || '').trim();
      if (!out) {
        resolve({});
        return;
      }
      const lines = out.split('\n');
      const stats = {};
      lines.forEach(line => {
        // formato esperado: Name:CPUPerc:MemUsage:BlockIO:NetIO
        const parts = line.split(':');
        // nombre puede contener ":" raramente; tomar primeros 5 campos desde la derecha
        // para mayor robustez, recomponer:
        if (parts.length >= 5) {
          const netio = parts.pop();
          const blockio = parts.pop();
          const mem = parts.pop();
          const cpu = parts.pop();
          const name = parts.join(':');
          stats[name] = { cpu: cpu.trim(), mem: mem.trim(), blockio: blockio.trim(), netio: netio.trim() };
        } else {
          // fallback sencillo
          const [name, cpu = '-', mem = '-'] = parts;
          stats[name || 'unknown'] = { cpu: (cpu || '-').trim(), mem: (mem || '-').trim(), blockio: '-', netio: '-' };
        }
      });
      resolve(stats);
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

// --- NUEVAS FUNCIONES: tamaños precisos para MonetDB y PostgreSQL ---

/**
 * Devuelve los bytes exactos ocupados por un path (ej. dbfarm) dentro del contenedor.
 */
function getMonetDbfarmBytes(containerName = 'runmonet-monetdb-1', dbPath = '/var/monetdb5/dbfarm/airline_mt') {
  return new Promise(async (resolve) => {
    try {
      // pasamos el comando a 'bash -lc' como un único arg (execFile evita que Windows rompa las comillas)
      const args = ['exec', containerName, 'bash', '-lc', `du -sb ${dbPath} 2>/dev/null || echo '0\t${dbPath}'`];
      const res = await dockerExecArgs(args, 5000);
      if (res.error) return resolve({ error: res.error, stderr: res.stderr });
      const out = (res.stdout || '').trim();
      const m = out.match(/^(\d+)\s+/);
      const bytes = m ? Number(m[1]) : 0;
      resolve({ container: containerName, path: dbPath, bytes });
    } catch (e) {
      resolve({ error: String(e) });
    }
  });
}

/**
 * Obtiene tamaños (en bytes) de todas las bases de datos de PostgreSQL dentro del contenedor.
 */
function getPostgresDatabaseSizes(containerName = 'runmonet-postgres-1', pgUser = process.env.PG_USER || 'postgres', pgDb = process.env.PG_DB || 'postgres') {
  return new Promise(async (resolve) => {
    try {
      const sql = `SELECT datname || '|' || pg_database_size(datname) FROM pg_database;`;

      // Usa docker exec con args simples (sin -e). Tu container permite psql -U admin -d airline_pg sin PGPASSWORD.
      const args = ['exec', containerName, 'psql', '-U', pgUser, '-d', pgDb, '-At', '-c', sql];
      const res = await dockerExecArgs(args, 8000);

      if (res.error) {
        // devolver detalle para que el caller lo pueda mostrar (stderr, code, message)
        return resolve({ error: res.message || 'psql returned error', stderr: res.stderr || '', code: res.code || null });
      }

      const lines = String(res.stdout || '').trim().split('\n').filter(Boolean);
      const out = lines.map(l => {
        const [datname, bytesStr] = l.split('|');
        return { datname, bytes: Number(bytesStr || 0) };
      });
      resolve(out);
    } catch (e) {
      resolve({ error: String(e) });
    }
  });
}

/**
 * Obtiene tamaños en bytes de tablas de usuario en la BD actual (postgres) dentro del contenedor.
 */
function getPostgresTableSizes(containerName = 'runmonet-postgres-1', pgUser = 'postgres') {
  return new Promise(async (resolve) => {
    try {
      const sql = `SELECT relname || '|' || pg_total_relation_size(relid) FROM pg_catalog.pg_stat_user_tables;`;
      const args = ['exec', containerName, 'psql', '-U', pgUser, '-At', '-c', sql];
      const res = await dockerExecArgs(args, 8000);
      if (res.error) return resolve({ error: res.error, stderr: res.stderr });
      const lines = String(res.stdout || '').trim().split('\n').filter(Boolean);
      const out = lines.map(l => {
        const [relname, bytesStr] = l.split('|');
        return { relname, bytes: Number(bytesStr || 0) };
      });
      resolve(out);
    } catch (e) {
      resolve({ error: String(e) });
    }
  });
}

// --- FIN nuevas funciones ---

module.exports = {
  getCpuUsage,
  getMemoryUsage,
  measureQueryPerformance,
  getDockerStats,
  getMonetDbfarmBytes,
  getPostgresDatabaseSizes,
  getPostgresTableSizes
};
