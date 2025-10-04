function frequentPassengerRevenueSQL(year = 2023, limit = 20, db = 'pg') {
  const y = Number.parseInt(year, 10);
  const lim = Number.parseInt(limit, 10) || 20;
  const yearField = db === 'monet' ? '"year"' : 'year';
  const monthField = db === 'monet' ? '"month"' : 'month';
  const schema = db === 'monet' ? 'airline_dw.' : '';

  return `
    SELECT
      dep_airport.airport_name AS departure_airport,
      arr_airport.airport_name AS arrival_airport,
      d.${monthField},
      SUM(
        fm.ticket_price_usd
        + COALESCE(fm.tax_usd, 0)
        + COALESCE(fm.baggage_fee_usd, 0)
        - COALESCE(fm.discount_usd, 0)
      ) AS total_revenue,
      SUM(
        CASE
          WHEN freq.passenger_id IS NOT NULL
          THEN fm.ticket_price_usd
            + COALESCE(fm.tax_usd, 0)
            + COALESCE(fm.baggage_fee_usd, 0)
            - COALESCE(fm.discount_usd, 0)
          ELSE 0
        END
      ) AS frequent_revenue,
      ROUND(
        100.0 * SUM(
          CASE
            WHEN freq.passenger_id IS NOT NULL
            THEN fm.ticket_price_usd
              + COALESCE(fm.tax_usd, 0)
              + COALESCE(fm.baggage_fee_usd, 0)
              - COALESCE(fm.discount_usd, 0)
            ELSE 0
          END
        ) / NULLIF(SUM(
          fm.ticket_price_usd
          + COALESCE(fm.tax_usd, 0)
          + COALESCE(fm.baggage_fee_usd, 0)
          - COALESCE(fm.discount_usd, 0)
        ), 0), 2
      ) AS frequent_revenue_pct
    FROM ${schema}fact_flight_metrics fm
    JOIN ${schema}dim_flight df ON fm.flight_id = df.flight_id
    JOIN ${schema}dim_airport dep_airport ON df.departure_airport_id = dep_airport.airport_id
    JOIN ${schema}dim_airport arr_airport ON df.arrival_airport_id = arr_airport.airport_id
    JOIN ${schema}dim_date d ON fm.date_id = d.date_id
    LEFT JOIN (
      SELECT
        fm.passenger_id
      FROM ${schema}fact_flight_metrics fm
      JOIN ${schema}dim_date d ON fm.date_id = d.date_id
      WHERE d.${yearField} = ${y}
      GROUP BY fm.passenger_id
      HAVING COUNT(DISTINCT fm.flight_id) > 5
    ) freq ON fm.passenger_id = freq.passenger_id
    WHERE d.${yearField} = ${y}
    GROUP BY dep_airport.airport_name, arr_airport.airport_name, d.${monthField}
    ORDER BY frequent_revenue_pct DESC, total_revenue DESC
    LIMIT ${lim};
  `;
}

module.exports = { frequentPassengerRevenueSQL };