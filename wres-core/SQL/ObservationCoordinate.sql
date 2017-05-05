-- Table: public.observationcoordinate

DROP TABLE IF EXISTS public.observationcoordinate CASCADE;

CREATE TABLE IF NOT EXISTS public.observationcoordinate
(
  coordinate_id integer,
  observation_id integer,
  CONSTRAINT observationcoordinate_coordinate_fk FOREIGN KEY (coordinate_id)
      REFERENCES public.coordinate (coordinate_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE
      DEFERRABLE INITIALLY DEFERRED,
  CONSTRAINT observationcoordinate_observation_fk FOREIGN KEY (observation_id)
      REFERENCES public.observation (observation_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE
      DEFERRABLE INITIALLY DEFERRED
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.observationcoordinate
  OWNER TO wres;

-- Index: public.observationcoordinate_coordinate_idx

DROP INDEX IF EXISTS public.observationcoordinate_coordinate_idx;

CREATE INDEX IF NOT EXISTS observationcoordinate_coordinate_idx
  ON public.observationcoordinate
  USING btree
  (coordinate_id);

