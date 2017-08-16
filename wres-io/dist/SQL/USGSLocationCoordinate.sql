-- Table: public.usgslocationcoordinate

DROP TABLE IF EXISTS public.usgslocationcoordinate CASCADE;

CREATE TABLE IF NOT EXISTS public.usgslocationcoordinate
(
  coordinate_id integer,
  observationlocation_id integer,
  CONSTRAINT usgslocationcoordinate_coordinate_fk FOREIGN KEY (coordinate_id)
      REFERENCES public.coordinate (coordinate_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE
      DEFERRABLE INITIALLY DEFERRED,
  CONSTRAINT usgslocationcoordinate_observationlocation_fk FOREIGN KEY (observationlocation_id)
      REFERENCES public.observationlocation (observationlocation_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE
      DEFERRABLE INITIALLY DEFERRED
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.usgslocationcoordinate
  OWNER TO wres;

-- Index: public.usgslocationcoordinate_coordinate_idx

DROP INDEX IF EXISTS public.usgslocationcoordinate_coordinate_idx;

CREATE INDEX IF NOT EXISTS usgslocationcoordinate_coordinate_idx
  ON public.usgslocationcoordinate
  USING btree
  (coordinate_id);

