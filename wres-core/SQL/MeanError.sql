
WITH forecasts AS
(
	SELECT forecast_id, forecast_date
	FROM Forecast F
	INNER JOIN ObservationLocation OL
		ON OL.observationlocation_id = F.observationlocation_id
	INNER JOIN Variable V
		ON V.variable_id = F.variable_id
	WHERE F.source = '/home/ctubbs/workspace/wres/wres-core/resources/BLKO2_GEFSMAP.ASC'
		AND V.variable_name = 'precipitation'
		AND OL.lid = 'BLKO2'
		AND F.forecast_date > '1800-01-01'
		AND F.forecast_date < '2500-01-01'
)
SELECT FR.lead_time, AVG(O.measurement - FR.measurement)
FROM forecasts F
INNER JOIN ForecastResult FR
	ON F.forecast_id = FR.forecast_id
INNER JOIN ObservationResult O
	ON O.valid_date = (F.forecast_date + (INTERVAL '1 hour' * FR.lead_time))
		AND O.observation_id = (
			SELECT observation_id
			FROM Observation O
			INNER JOIN ObservationLocation OL
				ON OL.observationlocation_id = O.observationlocation_id
			INNER JOIN Variable V
				ON V.variable_id = O.variable_id
			WHERE V.variable_name = 'precipitation'
				AND OL.lid = 'BLKO2'
		)
WHERE FR.measurement > '-infinity'
	AND FR.measurement < 'infinity'
GROUP BY FR.lead_time
ORDER BY FR.lead_time;