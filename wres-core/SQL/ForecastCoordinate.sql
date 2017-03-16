-- Table: public.ForecastCoordinate

-- DROP TABLE public.ForecastCoordinate;

CREATE TABLE public.ForecastCoordinate
(
  coordinate_id integer,
  forecast_id integer,
  CONSTRAINT forecastcoordinate_coordinate_fk FOREIGN KEY (coordinate_id)
      REFERENCES public.Coordinate (coordinate_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE,
  CONSTRAINT forecastcoordinate_forecast_fk FOREIGN KEY (forecast_id)
      REFERENCES public.forecast (forecast_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE
)
WITH (
  OIDS=FALSE
);

CREATE INDEX forecastcoordinate_coordinate_idx
  ON public.ForecastCoordinate
  USING btree
  (coordinate_id);

