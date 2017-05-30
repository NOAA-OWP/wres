-- Table: public.forecast

DROP TABLE IF EXISTS public.forecast CASCADE;

CREATE TABLE IF NOT EXISTS public.forecast
(
  forecast_id SERIAL,
  forecast_date timestamp without time zone NOT NULL,
  source text NOT NULL,
  measurementunit_id smallint NOT NULL,
  variable_id smallint NOT NULL,
  CONSTRAINT forecast_pk PRIMARY KEY (forecast_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.forecast
  OWNER TO wres;

-- Index: public.forecast_forecast_date_idx

DROP INDEX IF EXISTS public.forecast_forecast_date_idx;

CREATE INDEX IF NOT EXISTS forecast_forecast_date_idx
  ON public.forecast
  USING btree
  (forecast_date);

-- Index: public.forecast_source_idx

DROP INDEX IF EXISTS public.forecast_source_idx;

CREATE INDEX IF NOT EXISTS forecast_source_idx
  ON public.forecast
  USING btree
  (source COLLATE pg_catalog."default");

