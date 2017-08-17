-- Local Point: 87.5692 LON, 33.2098 LAT (Tuscaloosa, AL)

WITH closest AS
(
	SELECT ST_Distance(ST_MakePoint(87.5692, 33.2098), ST_MakePoint(nws_lon, nws_lat)) as distance, observationlocation_id, lid, rfc
	FROM ObservationLocation
	ORDER BY distance
	LIMIT 1
)
SELECT distance, observationlocation_id, concat('The closest station is '''::text, lid, ''' in '''::text, rfc, ''''::text)
FROM closest;