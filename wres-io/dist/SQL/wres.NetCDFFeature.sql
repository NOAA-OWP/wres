-- Table: wres.netcdffeature

CREATE SCHEMA IF NOT EXISTS wres AUTHORIZATION wres;

DROP INDEX IF EXISTS netcdffeature_idx;
DROP TABLE IF EXISTS wres.netcdffeature;

CREATE TABLE IF NOT EXISTS wres.netcdffeature
(
  feature_id integer,
  position_id integer
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wres.netcdffeature
  OWNER TO wres;

-- Index: wres.netcdffeature_idx

-- DROP INDEX wres.netcdffeature_idx;

CREATE INDEX netcdffeature_idx
  ON wres.netcdffeature
  USING btree
  (feature_id, position_id);
ALTER TABLE wres.netcdffeature CLUSTER ON netcdffeature_idx;