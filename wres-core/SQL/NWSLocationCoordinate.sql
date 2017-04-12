-- Table: public.nwslocationcoordinate

DROP TABLE IF EXISTS public.nwslocationcoordinate CASCADE;

CREATE TABLE IF NOT EXISTS public.nwslocationcoordinate
(
  coordinate_id integer,
  observationlocation_id integer,
  CONSTRAINT nwslocationcoordinate_coordinate_fk FOREIGN KEY (coordinate_id)
      REFERENCES public.coordinate (coordinate_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE
      DEFERRABLE INITIALLY DEFERRED,
  CONSTRAINT nwslocationcoordinate_observationlocation_fk FOREIGN KEY (observationlocation_id)
      REFERENCES public.observationlocation (observationlocation_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE
      DEFERRABLE INITIALLY DEFERRED
)
WITH (
  OIDS=FALSE
);

-- Index: public.nwslocationcoordinate_coordinate_idx

DROP INDEX IF EXISTS public.nwslocationcoordinate_coordinate_idx;

CREATE INDEX IF NOT EXISTS nwslocationcoordinate_coordinate_idx
  ON public.nwslocationcoordinate
  USING btree
  (coordinate_id);

