DELETE FROM "Coordinte";

INSERT INTO "Coordinate" (geographic_coordinate)
WITH coordinates AS
(
	SELECT DISTINCT nws_lat AS lat, nws_lon AS lon
	FROM ObservationLocation
	WHERE NOT nws_lat IS null AND NOT nws_lon IS null

	UNION

	SELECT DISTINCT usgs_lat AS lat, usgs_lon AS lon
	FROM ObservationLocation
	WHERE NOT usgs_lat IS NULL AND NOT usgs_lon IS NULL
)
SELECT point(lon, lat)
FROM coordinates;

SELECT *
FROM "Coordinate";