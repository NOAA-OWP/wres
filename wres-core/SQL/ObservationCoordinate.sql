-- Table: public."ObservationCoordinate"

-- DROP TABLE public."ObservationCoordinate";

CREATE TABLE public."ObservationCoordinate"
(
  coordinate_id integer,
  observation_id integer,
  CONSTRAINT observationcoordinate_coordinate_fk FOREIGN KEY (coordinate_id)
      REFERENCES public."Coordinate" (coordinate_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE,
  CONSTRAINT observationcoordinate_observation_fk FOREIGN KEY (observation_id)
      REFERENCES public.observation (observation_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE
)
WITH (
  OIDS=FALSE
);

CREATE INDEX observationcoordinate_coordinate_idx
  ON public."ObservationCoordinate"
  USING btree
  (coordinate_id);

