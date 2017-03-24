-- Table: public.featureindex

-- DROP TABLE public.featureindex;

CREATE TABLE public.featureindex
(
  netcdfsource_id INTEGER,
  variable_position INTEGER DEFAULT 0,
  observationlocation_id INTEGER NOT NULL,
  CONSTRAINT featureindex_netcdfsource_fk FOREIGN KEY (netcdfsource_id)
      REFERENCES public.netcdfsource (netcdfsource_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE,
  CONSTRAINT featureindex_observationlocation_fk FOREIGN KEY (observationlocation_id)
      REFERENCES public.observationlocation (observationlocation_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE
)
WITH (
  OIDS=FALSE
);

-- Index: public.featureindex_netcdfsource_idx

-- DROP INDEX public.featureindex_netcdfsource_idx;

CREATE INDEX featureindex_netcdfsource_idx
  ON public.featureindex
  USING btree
  (netcdfsource_id);

-- Index: public.featureindex_observationlocation_idx

-- DROP INDEX public.featureindex_observationlocation_idx;

CREATE INDEX featureindex_observationlocation_idx
  ON public.featureindex
  USING btree
  (observationlocation_id);

