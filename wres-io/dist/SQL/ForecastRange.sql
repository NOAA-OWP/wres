-- Table: public.forecastrange

DROP TABLE IF EXISTS public.forecastrange CASCADE;

CREATE TABLE IF NOT EXISTS public.forecastrange
(
  forecastrange_id SERIAL,
  range_name text NOT NULL,
  timestep smallint NOT NULL,
  added_date timestamp without time zone DEFAULT now(),
  CONSTRAINT forecastrange_pk PRIMARY KEY (forecastrange_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.forecastrange
  OWNER TO wres;

INSERT INTO public.ForecastRange (range_name, timestep)
VALUES	('short',1),
	('medium',6),
	('long',24);
