-- Table: wres.ForecastValue

CREATE SCHEMA IF NOT EXISTS wres AUTHORIZATION wres;

DROP TABLE IF EXISTS wres.ForecastValue;

CREATE TABLE IF NOT EXISTS wres.ForecastValue
(
  forecastensemble_id INT,
  lead INT,
  forecasted_value FLOAT
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wres.forecastvalue
  OWNER TO wres;

CREATE INDEX IF NOT EXISTS forecastvalue_forecastensemble_idx
  ON wres.ForecastValue
  (forecastensemble_id);

CREATE INDEX IF NOT EXISTS forecastvalue_lead_idx
  ON wres.ForecastValue
  (lead);

