require('dotenv').config();

const { monitorDb } = require('./controllers/monitorController');
const { getDockerStats } = require('./monitoring/resourceMonitor');

const express = require('express');
const app = express();
const monitorRouter = require('./routes/monitor');

app.use(express.json());
const cors = require('cors');
app.use(cors());

app.use('/api/monitor', monitorRouter);

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
const { getAdjustedProfitRoutes } = require('./controllers/adjustedProfitRoutesController');
const { getFrequentPassengerRevenue } = require('./controllers/frequentPassengerRevenueController');
const { generateFlights, deleteGeneratedFlights } = require('./controllers/flightDataController');
const {
  generateFlights: generateFlightsNew,
  deleteGeneratedFlights: deleteGeneratedFlightsNew
} = require('./controllers/datacontrollers/flightControllerNew');
// row-by-row (usa flightGeneratorNew para generar y flightDeleterNew para borrar)
const { generateFlights: generateFlightsRow } =
  require('./controllers/datacontrollers/flightGeneratorNew');
const { deleteGeneratedFlights: deleteFlightsRow } =
  require('./controllers/datacontrollers/flightDeleterNew');
const { generateFlightsBatch } = require('./controllers/datacontrollers/addWithBatch');
const { deleteWithBatch } = require('./controllers/datacontrollers/deleteWithBatch');
const { getQuerySql } = require('./controllers/querySqlController');

// helper seguro para registrar rutas y comprobar tipos
function registerPost(path, handler, name) {
  if (typeof handler !== 'function') {
    console.error(`Route handler for ${path} (${name}) is not a function. typeof=${typeof handler}`);
    throw new TypeError(`Handler for ${path} (${name}) must be a function`);
  }
  app.post(path, handler);
}

// reemplaza las llamadas directas a app.post(...) por registerPost(...)
registerPost('/api/olap/add-flights', generateFlights, 'generateFlights');
registerPost('/api/olap/remove-flights', deleteGeneratedFlights, 'deleteGeneratedFlights');

registerPost('/api/olap/add-flights-new', generateFlightsNew, 'generateFlightsNew');
registerPost('/api/olap/remove-flights-new', deleteGeneratedFlightsNew, 'deleteGeneratedFlightsNew');

registerPost('/api/olap/add-flights-row', generateFlightsRow, 'generateFlightsRow');
registerPost('/api/olap/remove-flights-row', deleteFlightsRow, 'deleteFlightsRow');

registerPost('/api/olap/add-flights-batch', generateFlightsBatch, 'generateFlightsBatch');
registerPost('/api/olap/remove-flights-batch', deleteWithBatch, 'deleteWithBatch');

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
app.get('/api/olap/adjusted-profit-routes', getAdjustedProfitRoutes);
app.get('/api/olap/frequent-passenger-revenue', getFrequentPassengerRevenue);
app.get('/api/olap/query-sql', getQuerySql);


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
