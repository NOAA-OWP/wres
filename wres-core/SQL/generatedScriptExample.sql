SELECT * FROM (WITH sourceTwo AS      -- The CTE that produces the array for the second source
(
   SELECT F.forecast_date + INTERVAL '1 hour' * lead AS sourceTwoDate,       -- The date to match the first source's
         array_agg(FV.forecasted_value * UC.factor) AS measurements      -- Array consisting of each ensemble member corresponding to a lead time from a forecast
     FROM wres.Forecast F        -- Start by selecting from the available forecasts
     INNER JOIN wres.ForecastEnsemble FE     -- Retrieve all of the ensembles for the forecasts from above
         ON F.forecast_id = FE.forecast_id       -- Match on the generated identifier for the forecast
     INNER JOIN wres.ForecastValue FV        -- Retrieve the values for all of the retrieved ensembles
         ON FV.forecastensemble_id = FE.forecastensemble_id      -- Match on the generated identifier for the ensemble matched with the forecast
     INNER JOIN wres.UnitConversion UC       -- Retrieve the conversion factor to convert the value with the ensemble's unit to the desired unit
         ON UC.from_unit = FE.measurementunit_id     -- The conversion factor will be obtained by matching the unit from the ensemble with the factor's unit to convert from
     WHERE lead = 18        -- Select the values attached to the lead time specification passed into the thread
         AND FE.variableposition_id = 1     -- Select the ensembles for variable values at a specific location
         AND UC.to_unit = 3     -- Determine to unit conversion based on the specification's indication of the desired unit to convert to
     GROUP BY F.forecast_date, FV.lead       -- Combine results based on the date of their initial forecast, then on their lead time
)
SELECT O.observed_value * UC.factor AS sourceOneValue,       -- Select a single observed value converted to the requested unit of measurement
     ST.measurements     -- Select the values returned from the previous query
FROM wres.Observation O      -- Start by searching through the observed values
INNER JOIN sourceTwo ST      -- Match the observed values with the previous query
     ON O.observation_time = ST.sourceTwoDate      -- match the values based on their dates
INNER JOIN wres.UnitConversion UC        -- Determine the conversion factor
     ON UC.from_unit = O.measurementunit_id      -- Match on the unit to convert from
WHERE UC.to_unit = 3     -- Find the correct conversion factor based on the unit of measurement to convert to
     AND O.variableposition_id = 2     -- Select only observations from a variable at a specific locations
ORDER BY O.observation_time      -- Order results based on date (earliest to latest)
) AS pairs;
