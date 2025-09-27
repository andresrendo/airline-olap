function delayedAverageSQL(year = 2023) {
  return `
    WITH retraso_por_aeropuerto_mes AS (
      SELECT
        fl.arrival_airport_id,
        a.airport_name AS aeropuerto_llegada,
        d.month_name AS mes,
        d."month" AS mes_num,
        AVG(CASE WHEN fl.delay_status = 'Delayed' THEN fl.flight_duration_min ELSE NULL END) AS retraso_promedio
      FROM airline_dw.fact_flight_metrics f
      JOIN airline_dw.dim_flight fl ON f.flight_id = fl.flight_id
      JOIN airline_dw.dim_airport a ON fl.arrival_airport_id = a.airport_id
      JOIN airline_dw.dim_date d ON f.date_id = d.date_id
      WHERE d."year" = ${year}
      GROUP BY fl.arrival_airport_id, a.airport_name, d.month_name, d."month"
    )
    SELECT
      aeropuerto_llegada,
      mes,
      retraso_promedio,
      RANK() OVER (PARTITION BY mes ORDER BY retraso_promedio DESC) AS ranking_retraso
    FROM retraso_por_aeropuerto_mes
    ORDER BY mes_num, ranking_retraso;
  `;
}

module.exports = { delayedAverageSQL };