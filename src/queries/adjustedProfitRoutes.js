function adjustedProfitRoutesSQL(year = 2023, limit = 20, db = 'pg') {
  const y = Number.parseInt(year, 10);
  const lim = Number.parseInt(limit, 10) || 20;
  const yearField = db === 'monet' ? '"year"' : 'year';
  const schema = db === 'monet' ? 'airline_dw.' : ''; // Ajusta si MonetDB usa esquema

  return `
    SELECT
      dep_airport.airport_name AS departure_airport,
      arr_airport.airport_name AS arrival_airport,
      COUNT(DISTINCT fm.flight_id) AS total_flights,
      SUM(
        fm.ticket_price_usd
        + COALESCE(fm.tax_usd, 0)
        + COALESCE(fm.baggage_fee_usd, 0)
        - COALESCE(fm.discount_usd, 0)
      ) AS total_revenue,
      AVG(
        CASE
          WHEN df.delay_status = 'Delayed' THEN 15
          WHEN df.delay_status = 'On time' THEN 0
          ELSE 0
        END
      ) AS avg_delay_min,
      AVG(ac.seat_capacity) AS avg_seat_capacity,
      COUNT(fm.passenger_id) / NULLIF(COUNT(DISTINCT fm.flight_id) * AVG(ac.seat_capacity), 0) AS avg_occupancy_rate,
      (
        SUM(
          fm.ticket_price_usd
          + COALESCE(fm.tax_usd, 0)
          + COALESCE(fm.baggage_fee_usd, 0)
          - COALESCE(fm.discount_usd, 0)
        )
        * (COUNT(fm.passenger_id) / NULLIF(COUNT(DISTINCT fm.flight_id) * AVG(ac.seat_capacity), 0))
        * CASE
            WHEN AVG(
              CASE
                WHEN df.delay_status = 'Delayed' THEN 15
                WHEN df.delay_status = 'On time' THEN 0
                ELSE 0
              END
            ) > 15 THEN 0.85
            ELSE 1
          END
      ) AS adjusted_profit
    FROM ${schema}fact_flight_metrics fm
    JOIN ${schema}dim_flight df ON fm.flight_id = df.flight_id
    JOIN ${schema}dim_aircraft ac ON df.aircraft_id = ac.aircraft_id
    JOIN ${schema}dim_airport dep_airport ON df.departure_airport_id = dep_airport.airport_id
    JOIN ${schema}dim_airport arr_airport ON df.arrival_airport_id = arr_airport.airport_id
    JOIN ${schema}dim_date d ON fm.date_id = d.date_id
    WHERE d.${yearField} = ${y}
    GROUP BY dep_airport.airport_name, arr_airport.airport_name
    ORDER BY adjusted_profit DESC
    LIMIT ${lim};
  `;
}

module.exports = { adjustedProfitRoutesSQL };