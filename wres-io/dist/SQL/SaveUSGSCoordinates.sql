DELETE FROM USGSLocationCoordinate;

INSERT INTO USGSLocationCoordinate(coordinate_id, observationlocation_id)
SELECT C.coordinate_id, L.observationlocation_id
FROM ObservationLocation L
INNER JOIN Coordinate C
	ON ST_EQUALS(geographic_coordinate::geometry, st_makepoint(usgs_lon, usgs_lat))
WHERE NOT usgs_lat IS null AND NOT usgs_lon IS null;