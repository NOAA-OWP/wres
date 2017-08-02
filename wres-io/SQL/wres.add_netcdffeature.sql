-- Function: wres.add_netcdffeature(integer, integer)

-- DROP FUNCTION wres.add_netcdffeature(integer, integer);

CREATE OR REPLACE FUNCTION wres.add_netcdffeature(
    new_position_id integer,
    new_comid integer)
  RETURNS void AS
$BODY$
BEGIN
	INSERT INTO wres.Feature (comid)
	SELECT new_comid
	WHERE NOT EXISTS (
		SELECT 1
		FROM wres.Feature
		WHERE comid = new_comid
	);

	INSERT INTO wres.NetCDFFeature (feature_id, position_id)
	SELECT feature_id, new_position_id
	FROM wres.Feature F
	WHERE F.comid = new_comid
		AND NOT EXISTS (
			SELECT 1
			FROM wres.NetCDFFeature NF
			WHERE NF.feature_id = F.feature_id
				AND NF.position_id = new_position_id
		);
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION wres.add_netcdffeature(integer, integer)
  OWNER TO wres;
