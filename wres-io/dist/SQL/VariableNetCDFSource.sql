-- Table: public.variablenetcdfsource

DROP TABLE IF EXISTS public.variablenetcdfsource CASCADE;

CREATE TABLE IF NOT EXISTS public.variablenetcdfsource
(
  variable_id integer NOT NULL,
  netcdfsource_id integer NOT NULL,
  CONSTRAINT variablenetcdfsource_netcdfsource_fk FOREIGN KEY (netcdfsource_id)
      REFERENCES public.netcdfsource (netcdfsource_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE
      DEFERRABLE INITIALLY DEFERRED,
  CONSTRAINT variablenetcdfsource_variable_fk FOREIGN KEY (variable_id)
      REFERENCES public.variable (variable_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE
      DEFERRABLE INITIALLY DEFERRED
)
WITH (
  OIDS=FALSE
);

-- Index: public.variablenetcdfsouce_netcdfsource_idx

DROP INDEX IF EXISTS public.variablenetcdfsouce_netcdfsource_idx;

CREATE INDEX IF NOT EXISTS variablenetcdfsouce_netcdfsource_idx
  ON public.variablenetcdfsource
  USING btree
  (netcdfsource_id);

-- Index: public.variablenetcdfsouce_variable_idx

DROP INDEX IF EXISTS public.variablenetcdfsouce_variable_idx;

CREATE INDEX IF NOT EXISTS variablenetcdfsouce_variable_idx
  ON public.variablenetcdfsource
  USING btree
  (variable_id);

