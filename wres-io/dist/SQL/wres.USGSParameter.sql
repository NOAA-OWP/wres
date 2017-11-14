-- Table: wres.USGSParameter

-- DROP TABLE IF EXISTS wres.USGSParameter;

CREATE TABLE IF NOT EXISTS wres.USGSParameter
(
  name TEXT,
  description TEXT,
  parameter_code TEXT,
  measurement_unit TEXT,
  measurementunit_id TEXT,
  aggregation TEXT,
  CONSTRAINT usgsparameter_code UNIQUE (parameter_code),
  CONSTRAINT usgsparameter_name_agg_measurement UNIQUE (name, aggregation, measurement_unit)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wres.USGSParameter
  OWNER TO wres;

CREATE INDEX IF NOT EXISTS usgsparameter_name_agg_measurement_idx
  ON wres.USGSParameter (name, aggregation, measurement_unit);

