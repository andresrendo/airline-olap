function passengersByAircraftSQL(year, db = 'pg') {
  const y = Number.parseInt(year, 10);
  if (!Number.isFinite(y)) throw new Error('Invalid year');
  const yearField = db === 'monet' ? '"year"' : 'year';
  const tablePrefix = db === 'monet' ? 'airline_dw.' : ''; // Ajusta si MonetDB usa esquema
  return `
    SELECT
      da.model AS aircraft_model,
      COUNT(DISTINCT ffm.passenger_id) AS total_passengers
    FROM ${tablePrefix}fact_flight_metrics ffm
    JOIN ${tablePrefix}dim_flight df ON ffm.flight_id = df.flight_id
    JOIN ${tablePrefix}dim_aircraft da ON df.aircraft_id = da.aircraft_id
    JOIN ${tablePrefix}dim_date dd ON ffm.date_id = dd.date_id
    WHERE dd.${yearField} = ${y}
    GROUP BY da.model
    ORDER BY total_passengers DESC
    LIMIT 20
  `;
}

module.exports = { passengersByAircraftSQL };