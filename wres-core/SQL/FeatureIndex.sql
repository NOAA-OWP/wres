-- Table: public.featureindex

DROP TABLE IF EXISTS public.featureindex CASCADE;

CREATE TABLE IF NOT EXISTS public.featureindex
(
  netcdfsource_id integer,
  variable_position integer DEFAULT 0,
  observationlocation_id integer NOT NULL,
  CONSTRAINT featureindex_netcdfsource_fk FOREIGN KEY (netcdfsource_id)
      REFERENCES public.netcdfsource (netcdfsource_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE
      DEFERRABLE INITIALLY DEFERRED,
  CONSTRAINT featureindex_observationlocation_fk FOREIGN KEY (observationlocation_id)
      REFERENCES public.observationlocation (observationlocation_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE
      DEFERRABLE INITIALLY DEFERRED
)
WITH (
  OIDS=FALSE
);

-- Index: public.featureindex_netcdfsource_idx

DROP INDEX IF EXISTS public.featureindex_netcdfsource_idx;

CREATE INDEX IF NOT EXISTS featureindex_netcdfsource_idx
  ON public.featureindex
  USING btree
  (netcdfsource_id);

-- Index: public.featureindex_observationlocation_idx

DROP INDEX IF EXISTS public.featureindex_observationlocation_idx;

CREATE INDEX IF NOT EXISTS featureindex_observationlocation_idx
  ON public.featureindex
  USING btree
  (observationlocation_id);

