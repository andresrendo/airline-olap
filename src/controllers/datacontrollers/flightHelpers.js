// utilidades para generación de vuelos
const BASE_PREFIX = 'FL';
const PILOTS = [
  'Luis Garcia','Anna Rodriguez','Maria Khan','Luis Lee','Luis Khan','Ali Rodriguez',
  'Maria Khan','Luis Garcia','John Smith','Anna Smith','Maria Smith','Luis Smith',
  'Chen Khan','Luis Rodriguez','Luis Rodriguez','Maria Lee'
];
const SEAT_CLASSES = ['Economy', 'Premium Economy', 'Business', 'First'];

function padId(num) { return `${BASE_PREFIX}${num}`; }
function randomDuration() { return Math.floor(Math.random() * (720 - 60 + 1)) + 60; }
function pickRandom(arr){ if(!arr||arr.length===0) return null; return arr[Math.floor(Math.random()*arr.length)]; }
function pickPilot(){ return pickRandom(PILOTS); }
function pickDelayStatus(){ return Math.random() < 0.3 ? 'Delayed' : 'On Time'; }
function pickSeatClass(){ return pickRandom(SEAT_CLASSES); }
function randomPrice(min=50,max=1500){ const v = Math.random()*(max-min)+min; return Math.round(v*100)/100; }

function sqlLiteral(v){
  if (v == null) return 'NULL';
  if (typeof v === 'number') return String(v);
  return `'${String(v).replace(/'/g,"''")}'`;
}

function parseMonetRowsForSingleColumn(res){
  const out = [];
  if (!res) return out;
  if (typeof res === 'object' && Array.isArray(res.data)) res = res.data;
  if (Array.isArray(res)){
    for (const row of res){
      if (Array.isArray(row)) out.push(row[0]);
      else if (row && typeof row === 'object'){ const k = Object.keys(row)[0]; out.push(row[k]); }
      else out.push(row);
    }
  }
  return out.filter(x=>x!=null);
}

function safeMonetParseMax(res){
  if (!res) return null;
  if (typeof res === 'object' && Array.isArray(res.data)) res = res.data;
  if (Array.isArray(res) && res.length>0){
    const row = res[0];
    if (row && typeof row === 'object' && ('maxnum' in row)) return row.maxnum === null ? null : Number(row.maxnum);
    if (Array.isArray(row) && row.length>0) return row[0]===null?null:Number(row[0]);
    if (typeof row === 'number') return row;
  }
  return null;
}

module.exports = {
  padId, randomDuration, pickPilot, pickDelayStatus, pickSeatClass, randomPrice,
  sqlLiteral, parseMonetRowsForSingleColumn, safeMonetParseMax
};