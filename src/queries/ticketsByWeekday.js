function ticketsByWeekdaySQL(year = 2023, limit = 10) {
  return `
    SELECT
      d.weekday,
      COUNT(*) AS tickets
    FROM airline_dw.fact_flight_metrics f
    JOIN airline_dw.dim_date d ON f.date_id = d.date_id
    WHERE d."year" = ${year}
    GROUP BY d.weekday
    ORDER BY tickets DESC
    LIMIT ${limit}
  `;
}

module.exports = { ticketsByWeekdaySQL };