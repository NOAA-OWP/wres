-- Table: public.USGSLocationCoordinate

-- DROP TABLE public.USGSLocationCoordinate;

CREATE TABLE public.USGSLocationCoordinate
(
	coordinate_id INT,
	observationlocation_id INT,
	CONSTRAINT usgslocationcoordinate_coordinate_fk FOREIGN KEY (coordinate_id)
		REFERENCES Coordinate (coordinate_id) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE CASCADE,
	CONSTRAINT usgslocationcoordinate_observationlocation_fk FOREIGN KEY (observationlocation_id)
		REFERENCES ObservationLocation (observationlocation_id) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE CASCADE
)
WITH (
  OIDS=FALSE
);

CREATE INDEX usgslocationcoordinate_coordinate_idx
ON USGSLocationCoordinate(coordinate_id);