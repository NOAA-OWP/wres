-- Table: wres.Ensemble

CREATE SCHEMA IF NOT EXISTS wres AUTHORIZATION wres;

DROP TABLE IF EXISTS wres.Ensemble;

CREATE TABLE IF NOT EXISTS wres.Ensemble
(
  ensemble_id SERIAL,
  ensemble_name TEXT,
  qualifier_id TEXT,
  ensemblemember_id INT,
  CONSTRAINT ensemble_pk PRIMARY KEY (ensemble_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wres.ensemble
  OWNER TO wres;

CREATE INDEX IF NOT EXISTS ensemble_name_idx
  ON wres.Ensemble (ensemble_name);

INSERT INTO wres.Ensemble (ensemble_name)
SELECT 'default'
WHERE NOT EXISTS (
	SELECT 1
	FROM wres.Ensemble
	WHERE ensemble_name = 'default'
);