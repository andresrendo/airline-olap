// src/monitoring/resourceMonitor.js
const os = require('os');
const { exec } = require('child_process');
// Obtiene stats de Docker para los contenedores especificados
function getDockerStats(containers = ['runmonet-postgres-1', 'runmonet-monetdb-1']) {
  return new Promise((resolve) => {
    const cmd = `docker stats --no-stream --format "{{.Name}}:{{.CPUPerc}}:{{.MemUsage}}" ${containers.join(' ')}`;
    // Timeout de 3 segundos para evitar bloqueos
    exec(cmd, { timeout: 3000 }, (err, stdout, stderr) => {
      if (err) {
        resolve({ error: 'No se pudo obtener datos de Docker. Verifica permisos y que Docker esté corriendo.' });
        return;
      }
      const lines = stdout.trim().split('\n');
      const stats = {};
      lines.forEach(line => {
        const [name, cpu, mem] = line.split(':');
        stats[name] = { cpu, mem };
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

module.exports = { getCpuUsage, getMemoryUsage, measureQueryPerformance, getDockerStats };
