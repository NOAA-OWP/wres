DROP FUNCTION IF EXISTS wres.get_variableposition_id(IN f_id integer, IN v_id integer);

CREATE OR REPLACE FUNCTION wres.get_variableposition_id(IN f_id integer, IN v_id integer) 
RETURNS integer AS $BODY$
DECLARE
	vp_id integer;
BEGIN
	LOCK TABLE wres.featureposition, wres.variableposition;

	IF NOT EXISTS (
		SELECT 1
		FROM wres.featureposition FP
		INNER JOIN wres.variableposition VP
			ON FP.variableposition_id = VP.variableposition_id
		WHERE FP.feature_id = f_id
			AND VP.variable_id = v_id
	) THEN
		WITH vp_insert AS
		(
			INSERT INTO wres.variableposition (variable_id, x_position)
			SELECT v_id,
				COALESCE((
					SELECT MAX(VP.x_position) + 1
					FROM wres.variableposition VP
					WHERE VP.variable_id = v_id
				), 0)
			RETURNING variableposition_id
		)
		INSERT INTO wres.featureposition (variableposition_id, feature_id)
		SELECT VI.variableposition_id, f_id
		FROM vp_insert VI;
	END IF;

	SELECT VP.variableposition_id INTO vp_id
	FROM wres.VariablePosition VP
	INNER JOIN wres.FeaturePosition FP
		ON VP.variableposition_id = FP.variableposition_id
	WHERE FP.feature_id = f_id
		AND VP.variable_id = v_id
	LIMIT 1;

	RETURN vp_id;
END
$BODY$ LANGUAGE plpgsql VOLATILE NOT LEAKPROOF;
