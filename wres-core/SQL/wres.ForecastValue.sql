-- Table: wres.ForecastValue

CREATE SCHEMA IF NOT EXISTS wres AUTHORIZATION wres;

DROP TABLE IF EXISTS wres.ForecastValue;

CREATE TABLE IF NOT EXISTS wres.ForecastValue
(
  forecastensemble_id INT,
  lead SMALLINT,
  forecasted_value FLOAT,
  CONSTRAINT forecastvalue_forecastensemble_fk FOREIGN KEY (forecastensemble_id)
	REFERENCES wres.ForecastEnsemble (forecastensemble_id)
	ON DELETE CASCADE
	DEFERRABLE INITIALLY DEFERRED
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wres.forecastvalue
  OWNER TO wres;

-- Index: wres.forecastensemble_source_idx

-- DROP INDEX IF EXISTS wres.forecastensemble_source_idx;

CREATE INDEX IF NOT EXISTS forecastvalue_forecastensemble_idx
  ON wres.ForecastValue
  (forecastensemble_id);

CREATE INDEX IF NOT EXISTS forecastvalue_lead_idx
  ON wres.ForecastValue
  (lead);

