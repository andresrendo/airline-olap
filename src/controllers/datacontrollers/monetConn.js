const monetdbDriver = require('monetdb');

function newMonetConn(){
  const host = String(process.env.MONET_HOST || 'localhost').trim();
  const port = parseInt(String(process.env.MONET_PORT || '6543').trim(),10);
  const database = String(process.env.MONET_DB || 'airline_mt').trim();
  const username = String(process.env.MONET_USER || 'monetdb').trim();
  const password = String(process.env.MONET_PASSWORD || 'monetdb').trim();

  return new monetdbDriver.Connection({
    host, port,
    dbname: database,
    database,
    username,
    password,
    language: 'sql',
    autoCommit: true
  });
}

module.exports = { newMonetConn };