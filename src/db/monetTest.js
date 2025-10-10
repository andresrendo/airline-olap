// monetTest.js
// Ejecuta: node monetTest.js
// Asegúrate (PowerShell):
// $env:MONET_HOST="localhost"
// $env:MONET_PORT="6543"
// $env:MONET_DB="airline_mt"
// $env:MONET_USER="monetdb"
// $env:MONET_PASSWORD="monetdb"

const monetdb = require('monetdb');

/** ========= CONFIG ========= **/
const CFG = {
  host: String(process.env.MONET_HOST || 'localhost').trim(),
  port: parseInt(String(process.env.MONET_PORT || '6543').trim(), 10),
  database: String(process.env.MONET_DB || 'airline_mt').trim(),
  username: String(process.env.MONET_USER || 'monetdb').trim(),
  password: String(process.env.MONET_PASSWORD || 'monetdb').trim(),
  autoCommit: true, // <- nombre correcto para el driver
};

/** ========= CONEXIÓN ========= **/
function newConnection() {
  return new monetdb.Connection({
    host: CFG.host,
    port: CFG.port,
    database: CFG.database,
    username: CFG.username,
    password: CFG.password,
    language: 'sql',
    autoCommit: CFG.autoCommit,
  });
}

/** ========= HELPERS ========= **/
function sqlLiteral(v) {
  if (v == null) return 'NULL';
  if (typeof v === 'number') return String(v);
  return `'${String(v).replace(/'/g, "''")}'`;
}

/** ========= MAIN TEST ========= **/
async function main() {
  console.log('== MonetDB Node single-session test ==');
  console.log('Config:', CFG);

  const conn = newConnection();
  await conn.connect();

  const exec = typeof conn.query === 'function' ? conn.query.bind(conn) : conn.execute.bind(conn);
  const run = async (sql) => {
    const res = await exec(sql);
    return res && res.data ? res.data : res;
  };

  try {
    // 0) Ping
    console.log('Ping SELECT 1:', await run('SELECT 1;'));

    // 1) Diagnóstico extendido (compara con DBeaver)
    const diag = await run(`
      SELECT name, value
      FROM sys.env()
      WHERE name IN ('gdk_dbname','gdk_dbfarm','mapi_port','monet_pid','current_user','current_schema')
      ORDER BY name;
    `);
    console.log('Diag (sys.env):', diag);

    // 2) Marcador para confirmar misma instancia entre Node y DBeaver
    await run(`CREATE SCHEMA IF NOT EXISTS dbg;`);
    await run(`CREATE TABLE IF NOT EXISTS dbg.marker (txt VARCHAR(200));`);
    await run(`INSERT INTO dbg.marker VALUES ('hello-' || current_timestamp);`);
    const mark = await run(`SELECT * FROM dbg.marker ORDER BY txt DESC LIMIT 1;`);
    console.log('Marker (último):', mark);

    // 3) Verifica existencia de la tabla
    const table = await run(`
      SELECT t.name AS table_name, s.name AS schema_name
      FROM sys.tables t
      JOIN sys.schemas s ON s.id = t.schema_id
      WHERE s.name='airline_dw' AND t.name='dim_flight';
    `);
    console.log('Tabla airline_dw.dim_flight:', table);
    if (!table || table.length === 0) {
      throw new Error(`No existe airline_dw.dim_flight en la base '${CFG.database}'.`);
    }

    // 4) INSERT con nuevo ID
    const TEST_ID = process.env.TEST_FLIGHT_ID || 'FLGET999';
    const insertSQL = `
      INSERT INTO airline_dw.dim_flight
        (flight_id, flight_duration_min, departure_airport_id, arrival_airport_id, pilot_name, aircraft_id, delay_status)
      VALUES
        (${sqlLiteral(TEST_ID)}, 120, 'AMS', 'CDG', 'A.Lee', 'D-PQS3', 'On Time');
    `;
    console.log('\nSQL ejecutado:\n', insertSQL);
    await run(insertSQL);
    console.log('INSERT ejecutado correctamente (autoCommit activo).');

    // 5) Verificación en esta misma sesión
    const checkAll = await run(`
      SELECT * FROM airline_dw.dim_flight WHERE flight_id = ${sqlLiteral(TEST_ID)};
    `);
    console.log('\nVerificación SELECT * (misma sesión):', checkAll);

    // 6) Muestra 3 filas
    console.log('\nPrimeras 3 filas:', await run(`SELECT * FROM airline_dw.dim_flight LIMIT 3;`));

    console.log('\n✅ Listo. Verifica en DBeaver:');
    console.log(`   SELECT * FROM dbg.marker ORDER BY txt DESC LIMIT 5;`);
    console.log(`   SELECT * FROM airline_dw.dim_flight WHERE flight_id = '${TEST_ID}';`);
    console.log('   (Si no ves el marker/registro, DBeaver apunta a otro puerto/instancia)');
  } catch (err) {
    console.error('\n❌ ERROR MonetDB:', {
      name: err?.name,
      code: err?.code,
      message: err?.message,
      stack: err?.stack,
    });
  } finally {
    await conn.close();
    console.log('Conexión cerrada.');
  }
}

if (require.main === module) main();
