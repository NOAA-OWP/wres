PREPARE pair (text, smallint, text) AS
	SELECT lead_time, O.measurement AS observation, FR.measurements AS forecasts, OBS.measurementunit_id AS observation_unit, FCST.measurementunit_id AS forecast_unit
	FROM Forecast FCST
	INNER JOIN Observation OBS
		ON OBS.variable_id = FCST.variable_id
			AND OBS.observationlocation_id = FCST.observationlocation_id
	INNER JOIN ForecastResult FR
		ON FR.forecast_id = FCST.forecast_id
	INNER JOIN ObservationResult O
		ON O.observation_id = OBS.observation_id
			AND O.valid_date + (INTERVAL '1 hour' * 6) = FCST.forecast_date + (INTERVAL '1 hour' * FR.lead_time)
	INNER JOIN ObservationLocation L
		ON L.observationlocation_id = FCST.observationlocation_id
			AND L.observationlocation_id = OBS.observationlocation_id
	WHERE FCST.source = $1
		AND FCST.variable_id = $2
		AND L.lid = $3;

EXECUTE pair ('/home/ctubbs/workspace/wres/wres-core/resources/BLKO2_GEFSMAP.ASC', 2, 'BLKO2');