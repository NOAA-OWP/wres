WITH forecast_measurements AS (
	SELECT DISTINCT forecast_id, lead_time, array_agg(FR.measurement) OVER (PARTITION BY forecast_id, lead_time) AS measurements
	FROM ForecastResult FR
	ORDER BY lead_time
)
SELECT (F.forecast_date + (INTERVAL '1 hour' * FM.lead_time)) AS forecast_date, FM.lead_time, O.measurement, FM.measurements
FROM Forecast F
INNER JOIN forecast_measurements FM
	ON FM.forecast_id = F.forecast_id
INNER JOIN ObservationResult O
	ON O.valid_date = (F.forecast_date + (INTERVAL '1 hour' * FM.lead_time))
INNER JOIN Observation OBS
	ON O.observation_id = OBS.observation_id
		AND OBS.variable_id = F.variable_id
		AND OBS.observationlocation_id = F.observationlocation_id
INNER JOIN ObservationLocation OL
	ON OL.observationlocation_id = OBS.observationlocation_id
INNER JOIN Variable V
	ON V.variable_id = OBS.variable_id
WHERE F.source = '/home/ctubbs/workspace/wres/wres-core/resources/BLKO2_GEFSMAP.ASC'
	AND V.variable_name = 'precipitation'
	AND OL.lid = 'BLKO2'
ORDER BY F.forecast_date, lead_time
