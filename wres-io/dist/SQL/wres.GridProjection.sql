-- Table: wres.gridprojection

-- DROP TABLE wres.gridprojection;

CREATE TABLE wres.gridprojection
(
  gridprojection_id serial NOT NULL,
  srtext text,
  proj4 text,
  projection_mapping text,
  x_resolution double precision,
  y_resolution double precision,
  x_unit text,
  y_unit text,
  x_type text,
  y_type text,
  x_size integer,
  y_size integer,
  load_complete boolean DEFAULT false,
  CONSTRAINT gridprojection_pkey PRIMARY KEY (gridprojection_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wres.gridprojection
  OWNER TO wres;

-- Index: wres.gridprojection_srtext_proj4_idx

-- DROP INDEX wres.gridprojection_srtext_proj4_idx;

CREATE INDEX gridprojection_srtext_proj4_idx
  ON wres.gridprojection
  USING btree
  (srtext COLLATE pg_catalog."default", proj4 COLLATE pg_catalog."default");