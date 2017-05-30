SELECT 	--V.variable_name, 
	--FT.lid, 
	--E.ensemble_name, 
	--E.ensemblemember_id, 
	--F.forecast_date + (INTERVAL '1 hour' * FV.lead) AS "valid date", 
	FV.lead, 
	AVG((forecasted_value * UC.factor) - (observed_value * OUC.factor)) AS error--,
	--O.observed_value * OUC.factor AS "observed value", 
	--FV.forecasted_value,
	--FV.forecasted_value * UC.factor AS "forecasted value"
FROM wres.Forecast F
INNER JOIN wres.ForecastEnsemble FE
	ON FE.forecast_id = F.forecast_id
INNER JOIN wres.VariablePosition VP
	ON VP.variableposition_id = FE.variableposition_id
/*INNER JOIN wres.Ensemble E
	ON E.ensemble_id = FE.ensemble_id*/
INNER JOIN wres.FeaturePosition FP
	ON FP.variableposition_id = VP.variableposition_id
INNER JOIN wres.Feature FT
	ON FT.feature_id = FP.feature_id
INNER JOIN wres.Variable V
	ON V.variable_id = VP.variable_id
INNER JOIN wres.ForecastValue FV
	ON FE.forecastensemble_id = FV.forecastensemble_id
INNER JOIN wres.Observation O
	ON O.observation_time = F.forecast_date + (INTERVAL '1 hour' * FV.lead)
INNER JOIN wres.VariablePosition OVP
	ON OVP.variableposition_id = O.variableposition_id
INNER JOIN wres.FeaturePosition OFP
	ON OFP.variableposition_id = OVP.variableposition_id
INNER JOIN wres.Feature OFT
	ON OFT.feature_id = OFP.feature_id
INNER JOIN wres.Variable OV
	ON OV.variable_id = OVP.variable_id
INNER JOIN wres.UnitConversion UC
	ON UC.from_unit = FE.measurementunit_id
INNER JOIN wres.UnitConversion OUC
	ON OUC.from_unit = O.measurementunit_id
WHERE /*E.ensemblemember_id = 1961
	AND*/ V.variable_id = 112					-- SQIN
	AND F.forecast_id = 1
	AND OV.variable_id = 113				-- QINE
	AND OFT.feature_id = 945080209
	AND FT.feature_id = 945080208
	AND UC.to_unit = FE.measurementunit_id
	AND OUC.to_unit = FE.measurementunit_id
GROUP BY FV.lead
ORDER BY FV.lead
--LIMIT 400;

