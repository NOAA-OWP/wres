-- Table: wres.ForecastEnsemble
-- This should probably be renamed to something like 'wres.TimeSeries'

CREATE SCHEMA IF NOT EXISTS wres AUTHORIZATION wres;

DROP TABLE IF EXISTS wres.ForecastEnsemble;

CREATE TABLE IF NOT EXISTS wres.ForecastEnsemble
(
  forecastensemble_id SERIAL,
  forecast_id INT,
  variableposition_id INT,
  ensemble_id INT,
  measurementunit_id INT,
  initialization_date timestamp without time zone,
  CONSTRAINT forecastensemble_pk PRIMARY KEY (forecastensemble_id)/*,

	FKs have been removed until dynamic removal and reinstatement has been implemented
  
  CONSTRAINT forecastensemble_forecast_fk FOREIGN KEY (forecast_id)
	REFERENCES wres.Forecast (forecast_id)
	ON DELETE CASCADE
	DEFERRABLE INITIALLY DEFERRED,
  CONSTRAINT forecastensemble_variableposition_id FOREIGN KEY (variableposition_id)
	REFERENCES wres.VariablePosition (variableposition_id)
	ON DELETE CASCADE
	DEFERRABLE INITIALLY DEFERRED,
  CONSTRAINT forecastensemble_ensemble_fk FOREIGN KEY (ensemble_id)
	REFERENCES wres.ensemble (ensemble_id)
	ON DELETE CASCADE
	DEFERRABLE INITIALLY DEFERRED,
  CONSTRAINT forecastensemble_measurementunit_fk FOREIGN KEY (measurementunit_id)
	REFERENCES wres.measurementunit (measurementunit_id)
	ON DELETE CASCADE
	DEFERRABLE INITIALLY DEFERRED*/
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wres.forecastensemble
  OWNER TO wres;

-- Index: wres.forecastensemble_source_idx

DROP INDEX IF EXISTS wres.forecastensemble_source_idx;

CREATE INDEX IF NOT EXISTS forecastensemble_variableposition_idx
  ON wres.ForecastEnsemble
  (variableposition_id);

-- Index: wres.forecastensemble_idx

DROP INDEX IF EXISTS wres.forecastensemble_idx;

CREATE INDEX IF NOT EXISTS forecastensemble_idx
  ON wres.forecastensemble
  USING btree
  (forecastensemble_id);
ALTER TABLE wres.forecastensemble CLUSTER ON forecastensemble_idx;

DROP INDEX IF EXISTS wres.forecastensemble_initialization_idx;

CREATE INDEX IF NOT EXISTS forecastensemble_initialization_idx
  ON wres.Forecastensemble (variableposition_id, ensemble_id, initialization_date, measurementunit_id);

