-- Table: wres.Forecast

CREATE SCHEMA IF NOT EXISTS wres;

DROP TABLE IF EXISTS wres.Forecast;

CREATE TABLE IF NOT EXISTS wres.Forecast
(
  forecast_id serial,
  forecast_date TIMESTAMP NOT NULL,
  forecasttype_id SMALLINT REFERENCES wres.ForecastType (forecasttype_id),
  CONSTRAINT forecast_pk PRIMARY KEY (forecast_id)
)
WITH (
  OIDS=FALSE
);

CREATE INDEX IF NOT EXISTS forecast_forecast_date_idx
  ON wres.forecast
  USING btree
  (forecast_date);
