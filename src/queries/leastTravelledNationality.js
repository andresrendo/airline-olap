function leastTravelledNationalitySQL(year = 2023, limit = 1) {
  return `
    SELECT
      p.nationality,
      COUNT(*) AS total_viajes
    FROM airline_dw.fact_flight_metrics f
    JOIN airline_dw.dim_passenger p ON f.passenger_id = p.passenger_id
    JOIN airline_dw.dim_date d ON f.date_id = d.date_id
    WHERE d."year" = ${year}
    GROUP BY p.nationality
    ORDER BY total_viajes ASC
    LIMIT ${limit}
  `;
}

module.exports = { leastTravelledNationalitySQL };