require('dotenv').config();

const { monitorDb } = require('./controllers/monitorController');
const { getDockerStats } = require('./monitoring/resourceMonitor');

const express = require('express');
const cors = require('cors');

const pg = require('./db/postgres');
const monet = require('./db/monetdb');
const { getTopRoutes } = require('./controllers/topRoutesController');
const { getRevenueByCountry } = require('./controllers/revenueController');
const { getTicketsByWeekday } = require('./controllers/ticketsByDayController');
const { getLeastTravelledNationality } = require('./controllers/leastTravelledController');
const { getRevenueBySeatClassAndMonth } = require('./controllers/revenueBySeatController');
const { getRevenueAccumulated } = require('./controllers/revenueAccumController');
const { getDelayedAverage } = require('./controllers/delayedAverageController');
const { getPassengersByAircraft } = require('./controllers/passengersByAircraftController');



const app = express();
app.use(cors());
app.use(express.json());

//Docker stats endpoint
app.get('/api/monitor/docker-stats', async (req, res) => {
  try {
    const stats = await getDockerStats();
    res.json({ ok: true, stats });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

// Monitoring endpoint
app.get('/api/monitor/:db', monitorDb);

app.get('/api/olap/tickets-by-weekday', getTicketsByWeekday);

app.get('/api/olap/least-travelled-nationality', getLeastTravelledNationality);

app.get('/api/olap/revenue-by-seatclass-month', getRevenueBySeatClassAndMonth);

app.get('/api/olap/revenue-accumulated', getRevenueAccumulated);

app.get('/api/olap/delayed-average', getDelayedAverage);
app.get('/api/olap/passengers-by-aircraft', getPassengersByAircraft);




// Health endpoints
app.get('/api/health', (req, res) => {
  res.json({ ok: true, service: 'airline-olap', time: new Date().toISOString() });
});

app.get('/api/health/pg', async (req, res) => {
  try {
    const r = await pg.query('SELECT 1 AS ok');
    res.json({ ok: true, result: r.rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.get('/api/health/monet', async (req, res) => {
  try {
    const r = await monet.query('SELECT 1 AS ok;');
    res.json({ ok: true, result: r });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

// OLAP endpoints
app.get('/api/olap/revenue-by-country', getRevenueByCountry);

app.get('/api/olap/top-routes', getTopRoutes);


const port = Number(process.env.PORT || 3000);
app.listen(port, () => {
  console.log(`OLAP API listening on http://localhost:${port}`);
});
