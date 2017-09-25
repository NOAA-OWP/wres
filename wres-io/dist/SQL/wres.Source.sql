-- Table: wres.Source

CREATE SCHEMA IF NOT EXISTS wres AUTHORIZATION wres;

DROP TABLE IF EXISTS wres.Source;

CREATE TABLE IF NOT EXISTS wres.Source
(
  source_id serial,
  path text NOT NULL,
  output_time timestamp NOT NULL,
  is_point_data boolean default true,
  lead smallint,
  hash TEXT,
  CONSTRAINT source_pk PRIMARY KEY (source_id)
)
WITH (
 OIDS=FALSE
);
ALTER TABLE wres.Source 
  OWNER TO wres;

DROP INDEX IF EXISTS wres.source_idx;
CREATE INDEX IF NOT EXISTS source_idx
  ON wres.Source (source_id);

DROP INDEX IF EXISTS wres.source_hash_idx;

CREATE INDEX IF NOT EXISTS source_hash_idx
  ON wres.Source
  (hash);
ALTER TABLE wres.Source CLUSTER ON source_hash_idx;

DROP INDEX IF EXISTS wres.source_output_time_idx;

CREATE INDEX IF NOT EXISTS source_output_time_idx
  ON wres.Source
  (output_time);

DROP INDEX IF EXISTS wres.source_path_idx;

CREATE INDEX IF NOT EXISTS source_path_idx
  ON wres.Source
  USING btree
  (path);
