-- Table: wres.Observation

CREATE SCHEMA IF NOT EXISTS wres;

DROP TABLE IF EXISTS wres.Observation;

CREATE TABLE IF NOT EXISTS wres.Observation
(
  variableposition_id INT,
  observation_time timestamp,
  observed_value FLOAT,
  measurementunit_id INT,
  CONSTRAINT observation_featurevariable_fk FOREIGN KEY (variableposition_id)
	REFERENCES wres.variableposition (variableposition_id) MATCH SIMPLE
	ON DELETE CASCADE
	DEFERRABLE INITIALLY DEFERRED,
  CONSTRAINT observation_measurementunit_fk FOREIGN KEY (measurementunit_id)
	REFERENCES wres.MeasurementUnit (measurementunit_id) MATCH SIMPLE
	ON DELETE CASCADE
	DEFERRABLE INITIALLY DEFERRED
)
WITH (
  OIDS=FALSE
);

DROP INDEX IF EXISTS observation_variableposition_idx;
  
CREATE INDEX IF NOT EXISTS observation_variableposition_idx
  ON wres.observation
  USING btree
  (variableposition_id);

DROP INDEX IF EXISTS observation_time_idx;

CREATE INDEX IF NOT EXISTS observation_time_idx
  ON wres.observation
  (observation_time);
