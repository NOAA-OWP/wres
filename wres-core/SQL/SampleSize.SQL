SELECT FR.lead_time, COUNT(*)
FROM ForecastResult FR
INNER JOIN Forecast F
	ON FR.forecast_id = F.forecast_id
GROUP BY FR.lead_time
