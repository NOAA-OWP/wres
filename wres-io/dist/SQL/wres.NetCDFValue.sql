-- Table: wres.NetCDFValue

CREATE SCHEMA IF NOT EXISTS wres;

-- DROP TABLE IF EXISTS wres.NetCDFValue;

CREATE TABLE IF NOT EXISTS wres.NetCDFValue
(
	source_id int NOT NULL,
	variable_id smallint NOT NULL,
	x_position int NOT NULL,
	y_position smallint,
	variable_value DOUBLE PRECISION NOT NULL--,
	--CONSTRAINT netcdfvalue_pk PRIMARY KEY(source_id, variable_id, x_position, y_position)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wres.NetCDFValue
  OWNER TO wres;

-- Index: wres.netcdfvalue_variable_idx

-- DROP INDEX IF EXISTS wres.netcdfvalue_variable_idx;

CREATE INDEX IF NOT EXISTS netcdfvalue_variable_idx
  ON wres.NetCDFValue
  USING btree
  (variable_id);

-- Index: wres.netcdfvalue_source_idx

-- DROP INDEX IF EXISTS wres.netcdfvalue_source_idx;

CREATE INDEX IF NOT EXISTS netcdfvalue_source_idx
  ON wres.NetCDFValue
  USING btree
  (source_id);

-- Index: wres.netcdfvalue_position_idx

-- DROP INDEX IF EXISTS wres.netcdfvalue_position_idx;

CREATE INDEX IF NOT EXISTS netcdfvalue_position_idx
  ON wres.NetCDFValue
  USING btree
  (x_position, y_position);
