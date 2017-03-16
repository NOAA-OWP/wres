-- Table: public."NWSLocationCoordinate"

-- DROP TABLE public."NWSLocationCoordinate";

CREATE TABLE public.NWSLocationCoordinate
(
  coordinate_id integer,
  observationlocation_id integer,
  CONSTRAINT nwslocationcoordinate_coordinate_fk FOREIGN KEY (coordinate_id)
      REFERENCES public.Coordinate (coordinate_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE,
  CONSTRAINT nwslocationcoordinate_observationlocation_fk FOREIGN KEY (observationlocation_id)
      REFERENCES public.observationlocation (observationlocation_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE
)
WITH (
  OIDS=FALSE
);

CREATE INDEX nwslocationcoordinate_coordinate_idx
  ON public.NWSLocationCoordinate
  USING btree
  (coordinate_id);

