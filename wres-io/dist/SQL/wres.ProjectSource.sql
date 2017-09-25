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

-- Index: wres.projectsource_project_idx

-- DROP INDEX wres.projectsource_project_idx;

CREATE INDEX projectsource_project_idx
  ON wres.projectsource
  USING btree
  (project_id);
ALTER TABLE wres.projectsource CLUSTER ON projectsource_project_idx;

-- Index: wres.projectsource_source_idx

-- DROP INDEX wres.projectsource_source_idx;

CREATE INDEX projectsource_source_idx
  ON wres.projectsource
  USING btree
  (source_id);