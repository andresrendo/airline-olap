function revenueBySeatClassAndMonthSQL(year = 2023) {
  return `
    SELECT
      f.seat_class AS clase_asiento,
      d.month_name AS mes,
      ROUND(SUM(
        f.ticket_price_usd
        + COALESCE(f.tax_usd, 0)
        + COALESCE(f.baggage_fee_usd, 0)
        - COALESCE(f.discount_usd, 0)
      ), 2) AS revenue
    FROM airline_dw.fact_flight_metrics f
    JOIN airline_dw.dim_date d ON f.date_id = d.date_id
    WHERE d."year" = ${year}
    GROUP BY f.seat_class, d.month_name
    ORDER BY f.seat_class, d.month_name;
  `;
}

module.exports = { revenueBySeatClassAndMonthSQL };