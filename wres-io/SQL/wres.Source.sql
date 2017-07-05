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
  CONSTRAINT source_pk PRIMARY KEY (source_id)
)
-- only needed for postgres <= 8.0, after that, OIDS are not created, by default.
-- WITH (
--  OIDS=FALSE
--)
;
GRANT SELECT, INSERT, UPDATE, DELETE ON wres.Source TO wres;

CREATE INDEX IF NOT EXISTS source_output_time_idx
  ON wres.Source
-- default for postgres is btree, h2 doesn't have this option
--  USING btree
  (output_time);

-- h2 (in-memory?) can't do an index on text. TODO: restore this index if needed
--CREATE INDEX IF NOT EXISTS source_path_idx
--  ON wres.Source
--  USING btree
--  (path);
