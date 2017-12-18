-- View: wres.forecasts

-- DROP VIEW wres.forecasts;

CREATE OR REPLACE VIEW wres.forecasts AS 
SELECT  VBF.feature_id,
	VBF.variable_id,
	TS.initialization_date AS basis_time,
	TS.initialization_date + INTERVAL '1 HOUR' * FV.lead AS valid_time,
	FV.lead,
	FV.forecasted_value,
	TS.ensemble_id,
	TS.measurementunit_id,
	TS.timeseries_id
FROM wres.TimeSeries TS
INNER JOIN wres.ForecastValue FV
	ON FV.timeseries_id = TS.timeseries_id
INNER JOIN wres.VariableByFeature VBF
	ON VBF.variableposition_id = TS.variableposition_id;