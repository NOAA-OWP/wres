CREATE VIEW wres.VariableByFeature AS
SELECT VP.variableposition_id, F.feature_id, variable_name, comid, lid, gage_id, rfc, st, st_code, huc, feature_name, latitude, longitude
FROM wres.VariablePosition VP
INNER JOIN wres.Variable V
	ON V.variable_id = VP.variable_id
INNER JOIN wres.Feature F
	ON F.feature_id = VP.x_position;