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

	INSERT INTO NetCDFComid (comid, position_id)
	SELECT new_comid, new_position_id
	WHERE NOT EXISTS (
		SELECT 1
		FROM NetCDFComid
		WHERE comid = new_comid
			AND position_id = new_position_id
	);
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION wres.add_netcdffeature(integer, integer)
  OWNER TO wres;
