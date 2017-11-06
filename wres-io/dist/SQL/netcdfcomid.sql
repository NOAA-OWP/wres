-- Table: netcdfcomid

DROP INDEX IF EXISTS netcdfcomid_idx;
DROP TABLE IF EXISTS netcdfcomid;

CREATE TABLE IF NOT EXISTS netcdfcomid
(
  comid integer,
  position_id integer
)
WITH (
  OIDS=FALSE
);
ALTER TABLE netcdfcomid
  OWNER TO wres;

-- Index: wres.netcdffeature_idx

-- DROP INDEX wres.netcdffeature_idx;

CREATE INDEX IF NOT EXISTS netcdfcomid_idx
  ON netcdfcomid
  USING btree
  (comid);
ALTER TABLE netcdfcomid CLUSTER ON netcdfcomid_idx;

