-- Table: public.forecastresult

DROP TABLE IF EXISTS public.forecastresult CASCADE;

CREATE TABLE IF NOT EXISTS public.forecastresult
(
  forecast_id integer NOT NULL,
  lead_time smallint NOT NULL,
  measurements real[] NOT NULL,
  observationlocation_id integer NOT NULL
)
WITH (
  OIDS=FALSE
);

-- Index: public.forecastresult_forecast_idx

DROP INDEX IF EXISTS public.forecastresult_forecast_idx;

CREATE INDEX IF NOT EXISTS forecastresult_forecast_idx
  ON public.forecastresult
  USING btree
  (forecast_id);

-- Index: public.forecastresult_lead_idx

DROP INDEX IF EXISTS public.forecastresult_lead_idx;

CREATE INDEX IF NOT EXISTS forecastresult_lead_idx
  ON public.forecastresult
  USING btree
  (lead_time);

-- Index: public.forecastresult_location_idx

DROP INDEX IF EXISTS public.forecastresult_location_idx;

CREATE INDEX IF NOT EXISTS forecastresult_location_idx
  ON public.forecastresult
  USING btree
  (observationlocation_id);

