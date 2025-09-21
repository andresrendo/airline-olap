// src/monitoring/resourceMonitor.js
const os = require('os');

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

module.exports = { getCpuUsage, getMemoryUsage, measureQueryPerformance };
