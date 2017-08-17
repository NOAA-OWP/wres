DELETE FROM NWSLocationCoordinate;

INSERT INTO NWSLocationCoordinate(coordinate_id, observationlocation_id)
SELECT C.coordinate_id, L.observationlocation_id
FROM ObservationLocation L
INNER JOIN Coordinate C
	ON ST_EQUALS(geographic_coordinate::geometry, st_makepoint(nws_lon, nws_lat))
WHERE NOT nws_lat IS null AND NOT nws_lon IS null;