INSERT INTO ObservationLocation 
(
	comid, 
	lid, 
	gage_id, 
	huc, 
	rfc, 
	fcst, 
	hsa,
	typ,
	fip,
	st,
	nws_st,
	atv,
	ref,
	hcdn,
	da,
	nws_lat,
	nws_lon,
	usgs_lat,
	usgs_lon,
	cac,
	coord,
	alt,
	alt_acy,
	datum,
	goes_id,
	nws_name,
	usgs_name,
	in_model_one,
	in_model_one_one
)
SELECT CAST(trim(comid) AS INTEGER) AS comid,
	trim(lid) AS lid,
	trim(gage_id) AS gage_id,
	trim(huc) AS huc,
	trim(rfc) AS rfc,
	trim(fcst) AS fcst,
	trim(hsa) AS hsa,
	trim(typ) AS typ,
	trim(fip) AS fip,
	trim(st) AS st,
	trim(nws_st) AS nws_st,
	CASE
		WHEN TRIM(atv) = '' THEN NULL
		ELSE trim(atv)::boolean
	END AS atv,
	CASE
		WHEN trim(ref) = '' THEN NULL
		ELSE trim(ref)::boolean
	END AS ref,
	trim(hcdn) AS hcdn,
	CASE
		WHEN TRIM(da) = '' THEN NULL
		ELSE CAST(trim(da) AS REAL)
	END AS da,
	CASE
		WHEN TRIM(nws_lat) = '' THEN NULL
		ELSE trim(nws_lat)::real
	END AS nws_lat,
	CASE
		WHEN TRIM(nws_lon) = '' THEN NULL
		ELSE trim(nws_lon)::real
	END AS nws_lon,
	CASE
		WHEN TRIM(usgs_lat) = '' THEN NULL
		ELSE trim(usgs_lat)::REAL
	END AS usgs_lat,
	CASE
		WHEN TRIM(usgs_lon) = '' THEN NULL
		ELSE trim(usgs_lon)::REAL
	END AS usgs_lon,
	trim(cac) AS cac,
	trim(coord) AS coord,
	CASE
		WHEN TRIM(alt) = '' THEN NULL
		ELSE trim(alt)::real
	END AS alt,
	CASE
		WHEN TRIM(alt_acy) = '' THEN NULL
		ELSE trim(alt_acy)::REAL
	END AS alt_acy,
	trim(datum) AS datum,
	CASE
		WHEN TRIM(goes_id) = 'Y' THEN NULL
		WHEN TRIM(goes_id) = 'N' THEN NULL
		WHEN TRIM(goes_id) = '' THEN NULL
		ELSE trim(goes_id)
	END as goes_id,
	trim(nws_name) AS nws_name,
	trim(usgs_name) AS usgs_name,
	trim(n1_0)::boolean AS n1_0,
	trim(n1_1)::boolean AS n1_1
FROM nwm_location;