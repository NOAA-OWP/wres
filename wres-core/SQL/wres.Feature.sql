-- Table: wres.Feature

CREATE SCHEMA IF NOT EXISTS wres AUTHORIZATION wres;

DROP TABLE IF EXISTS wres.Feature CASCADE;

CREATE TABLE IF NOT EXISTS wres.Feature
(
  feature_id SERIAL PRIMARY KEY,
  comid INT,
  lid text,
  gage_id text,
  rfc text,
  st text,
  st_code text,
  huc text,
  feature_name text
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wres.feature
  OWNER TO wres;

DROP INDEX IF EXISTS wres.feature_comid_idx;

CREATE INDEX IF NOT EXISTS feature_comid_idx
  ON wres.Feature
  USING btree
  (comid);

DROP INDEX IF EXISTS wres.feature_lid_idx;

CREATE INDEX IF NOT EXISTS feature_lid_idx
  ON wres.Feature
  USING btree
  (lid);
