-- Table: public.observationresult

DROP TABLE IF EXISTS public.observationresult CASCADE;

CREATE TABLE IF NOT EXISTS public.observationresult
(
  observation_id integer,
  valid_date timestamp without time zone NOT NULL,
  measurement double precision NOT NULL,
  observationlocation_id integer NOT NULL
)
WITH (
  OIDS=FALSE
);

-- Index: public.observationresult_location_idx

DROP INDEX IF EXISTS public.observationresult_location_idx;

CREATE INDEX IF NOT EXISTS observationresult_location_idx
  ON public.observationresult
  USING btree
  (observationlocation_id);

-- Index: public.observationresult_observation_idx

DROP INDEX IF EXISTS public.observationresult_observation_idx;

CREATE INDEX IF NOT EXISTS observationresult_observation_idx
  ON public.observationresult
  USING btree
  (observation_id);

-- Index: public.observationresult_valid_date_idx

DROP INDEX IF EXISTS public.observationresult_valid_date_idx;

CREATE INDEX IF NOT EXISTS observationresult_valid_date_idx
  ON public.observationresult
  USING btree
  (valid_date);

