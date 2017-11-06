-- Table: wres.scenario

CREATE SCHEMA IF NOT EXISTS wres AUTHORIZATION wres;

DROP TABLE IF EXISTS wres.scenario CASCADE;

CREATE TABLE IF NOT EXISTS wres.scenario
(
  scenario_id smallserial NOT NULL,
  scenario_name text NOT NULL,
  scenario_type text
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wres.scenario
  OWNER TO wres;