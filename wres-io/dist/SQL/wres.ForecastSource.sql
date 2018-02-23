-- Table: wres.ForecastSource
-- This should eventually become somethine like 'wres.TimeSeriesSource'

CREATE SCHEMA IF NOT EXISTS wres AUTHORIZATION wres;

DROP TABLE IF EXISTS wres.ForecastSource CASCADE;

CREATE TABLE IF NOT EXISTS wres.ForecastSource
(
  forecast_id INT NOT NULL,
  source_id INT NOT NULL
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wres.forecastsource
  OWNER TO wres;

CREATE INDEX IF NOT EXISTS forecastsource_forecast_idx
  ON wres.forecastsource
  USING btree
  (forecast_id);

CREATE INDEX IF NOT EXISTS forecastsource_source_idx
  ON wres.forecastsource
  USING btree
  (source_id);
