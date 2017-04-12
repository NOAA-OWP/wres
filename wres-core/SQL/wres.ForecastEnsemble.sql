-- Table: wres.ForecastEnsemble

CREATE SCHEMA IF NOT EXISTS wres;

DROP TABLE IF EXISTS wres.ForecastEnsemble;

CREATE TABLE IF NOT EXISTS wres.ForecastEnsemble
(
  forecastensemble_id SERIAL,
  forecast_id INT,
  variableposition_id INT,
  ensemble_id INT,
  measurementunit_id INT,
  CONSTRAINT forecastensemble_pk PRIMARY KEY (forecastensemble_id),
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
	DEFERRABLE INITIALLY DEFERRED
)
WITH (
  OIDS=FALSE
);

-- Index: wres.forecastensemble_source_idx

DROP INDEX IF EXISTS wres.forecastensemble_source_idx;

CREATE INDEX IF NOT EXISTS forecastensemble_variableposition_idx
  ON wres.ForecastEnsemble
  (variableposition_id);

