-- Table: wres.ProjectSource

CREATE SCHEMA IF NOT EXISTS wres AUTHORIZATION wres;

DROP TABLE IF EXISTS wres.ProjectSource;

CREATE TABLE IF NOT EXISTS wres.ProjectSource
(
    project_id SMALLINT NOT NULL,
    source_id INTEGER NOT NULL,
    member operating_member NOT NULL,
    active_time TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    inactive_time TIMESTAMP WITHOUT TIME ZONE
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wres.ProjectSource
  OWNER TO wres;