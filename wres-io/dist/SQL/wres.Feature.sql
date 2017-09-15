-- Table: wres.feature

CREATE SCHEMA IF NOT EXISTS wres;

DROP TABLE IF EXISTS wres.feature;

CREATE TABLE IF NOT EXISTS wres.feature
(
  feature_id serial NOT NULL,
  comid integer,
  lid text,
  gage_id text,
  rfc text,
  st text,
  st_code text,
  huc text,
  feature_name text,
  latitude real,
  longitude real,
  nwm_index int,
  parent_feature_id int,
  CONSTRAINT feature_pkey PRIMARY KEY (feature_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wres.feature
  OWNER TO wres;

-- Index: wres.feature_comid_idx

DROP INDEX IF EXISTS wres.feature_comid_idx;

CREATE INDEX IF NOT EXISTS feature_comid_idx
  ON wres.feature
  USING btree
  (comid);

-- Index: wres.feature_coordinates_idx

DROP INDEX IF EXISTS wres.feature_coordinates_idx;

CREATE INDEX IF NOT EXISTS feature_coordinates_idx
  ON wres.feature
  USING btree
  (latitude, longitude);

-- Index: wres.feature_lid_idx

DROP INDEX IF EXISTS wres.feature_lid_idx;

CREATE INDEX IF NOT EXISTS feature_lid_idx
  ON wres.feature
  USING btree
  (lid COLLATE pg_catalog."default");

DROP INDEX IF EXISTS wres.parent_feature_idx;

CREATE INDEX IF NOT EXISTS parent_feature_idx
  ON wres.Feature
  USING btree
  (parent_feature_id);

