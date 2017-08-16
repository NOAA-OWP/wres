-- Table: wres.featureposition

CREATE SCHEMA IF NOT EXISTS wres AUTHORIZATION wres;

DROP TABLE IF EXISTS wres.featureposition;

CREATE TABLE IF NOT EXISTS wres.featureposition
(
  variableposition_id INT NOT NULL,
  feature_id integer NOT NULL,
  CONSTRAINT featureposition_variableposition_fk FOREIGN KEY (variableposition_id)
	REFERENCES wres.VariablePosition (variableposition_id)
	ON DELETE CASCADE
	DEFERRABLE INITIALLY DEFERRED,
  CONSTRAINT featureposition_feature_fk FOREIGN KEY (feature_id)
	REFERENCES wres.Feature (feature_id)
	ON DELETE CASCADE
	DEFERRABLE INITIALLY DEFERRED
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wres.featureposition
  OWNER TO wres;

CREATE INDEX IF NOT EXISTS featureposition_feature_idx ON wres.featureposition (feature_id);
CREATE INDEX IF NOT EXISTS featureposition_position_idx ON wres.featureposition (variableposition_id);

