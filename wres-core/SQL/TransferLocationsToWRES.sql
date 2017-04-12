INSERT INTO wres.Feature (comid, lid, gage_id, rfc, st, st_code, feature_name)
SELECT 	comid, 
	lid, 
	gage_id, 
	rfc, 
	st, 
	nws_st, 
	nws_name	
FROM ObservationLocation OL1
WHERE comid > 0
	AND NOT EXISTS (
		SELECT 1
		FROM wres.Feature F
		WHERE F.comid = OL1.comid
			AND F.lid = OL1.lid
			AND F.gage_id = OL1.gage_id
	);