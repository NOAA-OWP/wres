-- Table: public.observation

DROP TABLE IF EXISTS public.observation CASCADE;

CREATE TABLE IF NOT EXISTS public.observation
(
  observation_id SERIAL,
  source text NOT NULL,
  variable_id integer,
  measurementunit_id integer,
  CONSTRAINT observation_pk PRIMARY KEY (observation_id),
  CONSTRAINT observation_variable_fk FOREIGN KEY (variable_id)
      REFERENCES public.variable (variable_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
      DEFERRABLE INITIALLY DEFERRED
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.observation
  OWNER TO wres;

-- Index: public.observation_variable_idx

DROP INDEX IF EXISTS public.observation_variable_idx;

CREATE INDEX IF NOT EXISTS observation_variable_idx
  ON public.observation
  USING btree
  (variable_id);

