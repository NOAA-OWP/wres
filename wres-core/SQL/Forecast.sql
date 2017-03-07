-- Table: Forecast

-- DROP TABLE Forecast;

CREATE TABLE Forecast
(
  forecast_id serial,
  forecast_date TIMESTAMP NOT NULL,
  source text NOT NULL,
  measurementunit_id smallint NOT NULL,
  forecastrange_id smallint NOT NULL,
  observationlocation_id integer NOT NULL,
  variable_id SMALLINT NOT NULL,
  projection_id smallint,
  CONSTRAINT forecast_pk PRIMARY KEY (forecast_id),
  CONSTRAINT forecast_forecastrange_fk FOREIGN KEY (forecastrange_id)
	REFERENCES ForecastRange(forecastrange_id) MATCH SIMPLE
	ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT forecast_measurementunit_fk FOREIGN KEY (measurementunit_id)
	REFERENCES MeasurementUnit (measurementunit_id) MATCH SIMPLE
	ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT forecast_projection_fk FOREIGN KEY (projection_id)
	REFERENCES Projection (projection_id) MATCH SIMPLE
	ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT forecast_observationlocation_fk FOREIGN KEY (observationlocation_id)
	REFERENCES ObservationLocation (observationlocation_id) MATCH SIMPLE
	ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT forecast_variable_fk FOREIGN KEY (variable_id)
	REFERENCES Variable (variable_id) MATCH SIMPLE
	ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);

CREATE INDEX forecast_forecast_date_idx
  ON public.forecast
  USING btree
  (forecast_date);

CREATE INDEX forecast_source_idx
  ON public.forecast 
  USING btree (source);
