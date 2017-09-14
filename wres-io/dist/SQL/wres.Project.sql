-- Table: wres.Project

CREATE SCHEMA IF NOT EXISTS wres AUTHORIZATION wres;

DROP TABLE IF EXISTS wres.Project CASCADE;

CREATE TABLE IF NOT EXISTS wres.Project
(
  project_id smallserial NOT NULL,
  project_name text NOT NULL,
  left_type text NOT NULL,
  right_type text NOT NULL,
  baseline_type text
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wres.Project
  OWNER TO wres;