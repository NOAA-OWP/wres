-- Table: wres.ObservationSource

CREATE SCHEMA IF NOT EXISTS wres;

DROP TABLE IF EXISTS wres.ObservationSource;

CREATE TABLE IF NOT EXISTS wres.ObservationSource
(
  observation_id INT NOT NULL,
  source_id INT NOT NULL
)
WITH (
  OIDS=FALSE
);

CREATE INDEX IF NOT EXISTS observationsource_observation_idx
  ON wres.ObservationSource
  USING btree
  (observation_id);

CREATE INDEX IF NOT EXISTS observationsource_source_idx
  ON wres.ObservationSource
  USING btree
  (source_id);
