function topRoutesSQL(year, limit = 10) {
  const y = Number.parseInt(year, 10);
  const lim = Number.parseInt(limit, 10) || 10;
  if (!Number.isFinite(y)) throw new Error('year inválido');

  return `
    SELECT
      dep.airport_name AS origin_airport,
      arr.airport_name AS destination_airport,
      COUNT(*) AS tickets,
      ROUND(SUM(
        fm.ticket_price_usd
        + COALESCE(fm.tax_usd, 0)
        + COALESCE(fm.baggage_fee_usd, 0)
        - COALESCE(fm.discount_usd, 0)
      ), 2) AS revenue
    FROM airline_dw.fact_flight_metrics fm
    JOIN airline_dw.dim_flight fl ON fm.flight_id = fl.flight_id
    JOIN airline_dw.dim_airport dep ON fl.departure_airport_id = dep.airport_id
    JOIN airline_dw.dim_airport arr ON fl.arrival_airport_id = arr.airport_id
    JOIN airline_dw.dim_date d ON fm.date_id = d.date_id
    WHERE d."year" = ${y}
    GROUP BY dep.airport_name, arr.airport_name
    ORDER BY revenue DESC
    LIMIT ${lim}
  `;
}

module.exports = { topRoutesSQL };
