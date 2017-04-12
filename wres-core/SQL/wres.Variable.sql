-- Table: wres.Variable

CREATE SCHEMA IF NOT EXISTS wres;

DROP TABLE IF EXISTS wres.Variable;

CREATE TABLE IF NOT EXISTS wres.Variable
(
	variable_id SERIAL,
	variable_name text UNIQUE NOT NULL,
	variable_type text,
	description text,
	measurementunit_id INT,
	CONSTRAINT variable_pk PRIMARY KEY (variable_id),
	CONSTRAINT variable_measurementunit_fk FOREIGN KEY (measurementunit_id)
		REFERENCES wres.MeasurementUnit (measurementunit_id) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE CASCADE
)
WITH (
  OIDS=FALSE
);
CREATE INDEX IF NOT EXISTS variable_variable_name_idx ON wres.Variable(variable_name);
