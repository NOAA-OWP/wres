-- Table: wres.Forecast

CREATE SCHEMA IF NOT EXISTS wres AUTHORIZATION wres;

DROP TABLE IF EXISTS wres.Forecast CASCADE;

CREATE TABLE IF NOT EXISTS wres.Forecast
(
  forecast_id serial,
  forecast_date TIMESTAMP NOT NULL,
  scenario_id SMALLINT,
  CONSTRAINT forecast_pk PRIMARY KEY (forecast_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wres.forecast
  OWNER TO wres;

CREATE INDEX IF NOT EXISTS forecast_forecast_date_idx
  ON wres.forecast
  USING btree
  (forecast_date);

CREATE INDEX IF NOT EXISTS forecast_scenario_idx
  ON wres.Forecast
  USING btree
  (scenario_id);
