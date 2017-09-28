-- Table: wres.TimeSeries

CREATE SCHEMA IF NOT EXISTS wres AUTHORIZATION wres;

DROP TABLE IF EXISTS wres.TimeSeries;

CREATE TABLE IF NOT EXISTS wres.TimeSeries
(
  timeseries_id SERIAL,
  variableposition_id INT,
  ensemble_id INT,
  measurementunit_id INT,
  initialization_date timestamp without time zone,
  CONSTRAINT timeseries_pk PRIMARY KEY (timeseries_id)/*,

	FKs have been removed until dynamic removal and reinstatement has been implemented
  CONSTRAINT timeseries_variableposition_id FOREIGN KEY (variableposition_id)
	REFERENCES wres.VariablePosition (variableposition_id)
	ON DELETE CASCADE
	DEFERRABLE INITIALLY DEFERRED,
  CONSTRAINT timeseries_ensemble_fk FOREIGN KEY (ensemble_id)
	REFERENCES wres.ensemble (ensemble_id)
	ON DELETE CASCADE
	DEFERRABLE INITIALLY DEFERRED,
  CONSTRAINT timeseries_measurementunit_fk FOREIGN KEY (measurementunit_id)
	REFERENCES wres.measurementunit (measurementunit_id)
	ON DELETE CASCADE
	DEFERRABLE INITIALLY DEFERRED*/
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wres.timeseries
  OWNER TO wres;
ALTER TABLE wres.timeseries CLUSTER ON timeseries_pk;

CREATE INDEX IF NOT EXISTS timeseries_variableposition_idx
  ON wres.TimeSeries
  (variableposition_id);

DROP INDEX IF EXISTS wres.timeseries_initialization_idx;

CREATE INDEX IF NOT EXISTS timeseries_initialization_idx
  ON wres.TimeSeries
  (
    variableposition_id, 
    ensemble_id,
    initialization_date,
    measurementunit_id
  );

