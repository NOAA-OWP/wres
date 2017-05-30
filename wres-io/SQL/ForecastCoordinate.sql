-- Table: public.forecastcoordinate

DROP TABLE IF EXISTS public.forecastcoordinate CASCADE;

CREATE TABLE IF NOT EXISTS public.forecastcoordinate
(
  coordinate_id integer,
  forecast_id integer,
  CONSTRAINT forecastcoordinate_coordinate_fk FOREIGN KEY (coordinate_id)
      REFERENCES public.coordinate (coordinate_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE
      DEFERRABLE INITIALLY DEFERRED,
  CONSTRAINT forecastcoordinate_forecast_fk FOREIGN KEY (forecast_id)
      REFERENCES public.forecast (forecast_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE
      DEFERRABLE INITIALLY DEFERRED
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.forecastcoordinate
  OWNER TO wres;

-- Index: public.forecastcoordinate_coordinate_idx

DROP INDEX IF EXISTS public.forecastcoordinate_coordinate_idx;

CREATE INDEX IF NOT EXISTS forecastcoordinate_coordinate_idx
  ON public.forecastcoordinate
  USING btree
  (coordinate_id);

