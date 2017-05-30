-- Table: public.unitconversion

DROP TABLE IF EXISTS public.unitconversion CASCADE;

CREATE TABLE IF NOT EXISTS public.unitconversion
(
  from_unit smallint,
  to_unit smallint,
  factor smallint,
  CONSTRAINT from_measurementunit_fk FOREIGN KEY (from_unit)
      REFERENCES public.measurementunit (measurementunit_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE
      DEFERRABLE INITIALLY DEFERRED,
  CONSTRAINT to_measurementunit_fk FOREIGN KEY (to_unit)
      REFERENCES public.measurementunit (measurementunit_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE
      DEFERRABLE INITIALLY DEFERRED
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.unitconversion
  OWNER TO wres;

-- Index: public.unitconversion_measurementunit_idx

DROP INDEX IF EXISTS public.unitconversion_measurementunit_idx;

CREATE INDEX IF NOT EXISTS unitconversion_measurementunit_idx
  ON public.unitconversion
  USING btree
  (from_unit, to_unit);

