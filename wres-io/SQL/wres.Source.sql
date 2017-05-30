-- Table: wres.Source

CREATE SCHEMA IF NOT EXISTS wres AUTHORIZATION wres;

DROP TABLE IF EXISTS wres.Source;

CREATE TABLE IF NOT EXISTS wres.Source
(
  source_id serial,
  path text NOT NULL,
  output_time timestamp NOT NULL,
  is_point_data boolean default true,
  CONSTRAINT source_pk PRIMARY KEY (source_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wres.source
  OWNER TO wres;

CREATE INDEX IF NOT EXISTS source_output_time_idx
  ON wres.Source
  USING btree
  (output_time);

CREATE INDEX IF NOT EXISTS source_path_idx
  ON wres.Source 
  USING btree (path);
