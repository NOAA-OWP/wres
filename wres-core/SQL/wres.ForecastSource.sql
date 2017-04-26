-- Table: wres.ForecastSource

CREATE SCHEMA IF NOT EXISTS wres;

DROP TABLE IF EXISTS wres.ForecastSource;

CREATE TABLE IF NOT EXISTS wres.ForecastSource
(
  forecast_id INT NOT NULL,
  source_id INT NOT NULL
)
WITH (
  OIDS=FALSE
);

CREATE INDEX IF NOT EXISTS forecastsource_forecast_idx
  ON wres.forecastsource
  USING btree
  (forecast_id);

CREATE INDEX IF NOT EXISTS forecastsource_source_idx
  ON wres.forecastsource
  USING btree
  (source_id);
