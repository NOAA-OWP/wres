TRUNCATE ObservationLocation RESTART IDENTITY CASCADE;

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
		WHEN TRIM(da) = '' OR NOT textregexeq(da, '^[[:digit:]]+(\.[[:digit:]]+)?$') THEN NULL
		ELSE CAST(trim(da) AS REAL)
	END AS da,
	CASE
		WHEN TRIM(nws_lat) = '' OR NOT textregexeq(TRIM(nws_lat), '^-?[[:digit:]]+(\.[[:digit:]]+)?$') THEN NULL
		WHEN TRIM(coord) = 'NAD27' AND TRIM(nws_lon) != '' AND textregexeq(TRIM(nws_lon), '^-?[[:digit:]]+(\.[[:digit:]]+)?$')
			THEN ST_Y(ST_Transform(ST_SETSRID(ST_MAKEPOINT(TRIM(nws_lon)::real, TRIM(nws_lat)::real), 4267), 4269))
		ELSE trim(nws_lat)::real
	END AS nws_lat,
	CASE
		WHEN TRIM(nws_lon) = '' OR NOT textregexeq(TRIM(nws_lon), '^-?[[:digit:]]+(\.[[:digit:]]+)?$') THEN NULL
		WHEN TRIM(coord) = 'NAD27' AND TRIM(nws_lat) != '' AND textregexeq(TRIM(nws_lat), '^-?[[:digit:]]+(\.[[:digit:]]+)?$')
			THEN ST_X(ST_Transform(ST_SETSRID(ST_MAKEPOINT(TRIM(nws_lon)::real, TRIM(nws_lat)::real), 4267), 4269))
		ELSE trim(nws_lon)::real
	END AS nws_lon,
	CASE
		WHEN TRIM(usgs_lat) = '' OR NOT textregexeq(TRIM(usgs_lat), '^-?[[:digit:]]+(\.[[:digit:]]+)?$') THEN NULL
		WHEN TRIM(coord) = 'NAD27' AND TRIM(usgs_lon) != '' AND textregexeq(TRIM(usgs_lon), '^-?[[:digit:]]+(\.[[:digit:]]+)?$')
			THEN ST_Y(ST_Transform(ST_SETSRID(ST_MAKEPOINT(TRIM(usgs_lon)::real, TRIM(usgs_lat)::real), 4267), 4269))
		ELSE trim(usgs_lat)::REAL
	END AS usgs_lat,
	CASE
		WHEN TRIM(usgs_lon) = '' OR NOT textregexeq(TRIM(usgs_lon), '^-?[[:digit:]]+(\.[[:digit:]]+)?$') THEN NULL
		WHEN TRIM(coord) = 'NAD27' AND TRIM(usgs_lat) != '' AND textregexeq(TRIM(usgs_lat), '^-?[[:digit:]]+(\.[[:digit:]]+)?$')
			THEN ST_X(ST_Transform(ST_SETSRID(ST_MAKEPOINT(TRIM(usgs_lon)::real, TRIM(usgs_lat)::real), 4267), 4269))
		ELSE trim(usgs_lon)::REAL
	END AS usgs_lon,
	trim(cac) AS cac,
	CASE
		WHEN TRIM(coord) = '' THEN NULL
		ELSE 'NAD83'
	END AS coord,
	CASE
		WHEN TRIM(alt) = '' OR NOT textregexeq(TRIM(alt), '^[[:digit:]]+(\.[[:digit:]]+)?$') THEN NULL
		ELSE trim(alt)::real
	END AS alt,
	CASE
		WHEN TRIM(alt_acy) = '' OR NOT textregexeq(TRIM(alt_acy), '^[[:digit:]]+(\.[[:digit:]]+)?$') THEN NULL
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
	CASE
		WHEN trim(n1_0) = '' THEN null
		ELSE trim(n1_0)::boolean
	END AS n1_0,
	CASE
		WHEN trim(n1_1) = '' THEN NULL
		ELSE trim(n1_1)::boolean 
	END AS n1_1
FROM nwm_location;