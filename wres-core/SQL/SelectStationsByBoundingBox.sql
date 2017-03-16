SELECT L.*
FROM "Coordinate" C
INNER JOIN "NWSLocationCoordinate" NLC
	ON NLC.coordinate_id = C.coordinate_id
INNER JOIN ObservationLocation L
	ON L.observationlocation_id = NLC.observationlocation_id
WHERE C.geographic_coordinate::geometry && ST_MakeEnvelope(80.5, 54.0, 97.0, 32.0);

-- BB: Top: 54, Bottom: 32, Left: 80.5, right: 97 