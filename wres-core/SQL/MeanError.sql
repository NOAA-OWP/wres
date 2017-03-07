
-- The valid date on the observation is incremented by 6 hours due to it being recorded in CST instead of UTC

WITH forecasts AS												-- Gather all necessary forecasts
(
	SELECT forecast_id, forecast_date
	FROM Forecast F
	INNER JOIN ObservationLocation OL
		ON OL.observationlocation_id = F.observationlocation_id
	INNER JOIN Variable V
		ON V.variable_id = F.variable_id
	WHERE F.source = '/home/ctubbs/workspace/wres/wres-core/resources/BLKO2_GEFSMAP.ASC'			-- From a specific source
		AND V.variable_name = 'precipitation'								-- For a specific variable
		AND OL.lid = 'BLKO2'										-- For a specific location
		AND F.forecast_date > '1800-01-01'								-- After a specific date
	 	AND F.forecast_date < '2500-01-01'								-- Before a specific date
)
SELECT FR.lead_time, AVG((FR.measurement * 25.4) - (O.measurement * 25.4))					-- Convert values from in to mm, subtract the forecast and observation, average the differences
FROM forecasts F
INNER JOIN ForecastResult FR
	ON F.forecast_id = FR.forecast_id
INNER JOIN ObservationResult O
	ON (O.valid_date + INTERVAL '1 hour' * 6) = (F.forecast_date + (INTERVAL '1 hour' * FR.lead_time))	-- The CST adjustment = the lead time adjustment
		AND O.observation_id = (									-- Results belong to Observations for that location and variable
			SELECT observation_id
			FROM Observation O
			INNER JOIN ObservationLocation OL
				ON OL.observationlocation_id = O.observationlocation_id
			INNER JOIN Variable V
				ON V.variable_id = O.variable_id
			WHERE V.variable_name = 'precipitation'
				AND OL.lid = 'BLKO2'
		)
WHERE FR.measurement > '-infinity'										-- The forecast result is greater than the minimum
	AND FR.measurement < 'infinity'										-- The forecast result is less than the maximum
GROUP BY FR.lead_time												-- Group all values by lead time (all 6's together, all 12's, etc)
ORDER BY FR.lead_time;
