function revenueByCountrySQL(year) {
  const y = Number.parseInt(year, 10);
  if (!Number.isFinite(y)) throw new Error('Invalid year');
  return `
    SELECT
      a.country_name,
      ROUND(SUM(
        f.ticket_price_usd
        + COALESCE(f.tax_usd, 0)
        + COALESCE(f.baggage_fee_usd, 0)
        - COALESCE(f.discount_usd, 0)
      ), 2) AS revenue
    FROM airline_dw.fact_flight_metrics f
    JOIN airline_dw.dim_flight fl ON f.flight_id = fl.flight_id
    JOIN airline_dw.dim_airport a ON fl.departure_airport_id = a.airport_id
    JOIN airline_dw.dim_date d ON f.date_id = d.date_id
    WHERE d."year" = ${y}
    GROUP BY a.country_name
    ORDER BY revenue DESC
    LIMIT 20
  `;
}

module.exports = { revenueByCountrySQL };