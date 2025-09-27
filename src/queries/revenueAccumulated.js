function revenueAccumulatedSQL(year = 2023) {
  return `
    WITH revenue_por_clase_mes AS (
      SELECT
        f.seat_class AS clase_asiento,
        d.month_name AS mes,
        d."month" AS mes_num,
        ROUND(SUM(
          f.ticket_price_usd
          + COALESCE(f.tax_usd, 0)
          + COALESCE(f.baggage_fee_usd, 0)
          - COALESCE(f.discount_usd, 0)
        ), 2) AS revenue_mensual
      FROM airline_dw.fact_flight_metrics f
      JOIN airline_dw.dim_date d ON f.date_id = d.date_id
      WHERE d."year" = ${year}
      GROUP BY f.seat_class, d.month_name, d."month"
    )
    SELECT
      clase_asiento,
      mes,
      revenue_mensual,
      SUM(revenue_mensual) OVER (PARTITION BY clase_asiento ORDER BY mes_num) AS revenue_acumulado,
      RANK() OVER (PARTITION BY mes ORDER BY revenue_mensual DESC) AS ranking_clase
    FROM revenue_por_clase_mes
    ORDER BY mes_num, ranking_clase;
  `;
}

module.exports = { revenueAccumulatedSQL };