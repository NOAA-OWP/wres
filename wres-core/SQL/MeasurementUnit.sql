-- Table: MeasurementUnit

-- DROP TABLE MeasurementUnit;

CREATE TABLE MeasurementUnit
(
  measurementunit_id serial,
  unit_name text NOT NULL,
  added_date TIMESTAMP DEFAULT now(),
  CONSTRAINT measurementunit_pk PRIMARY KEY (measurementunit_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE MeasurementUnit
  OWNER TO "christopher.tubbs";

 INSERT INTO MeasurementUnit (unit_name)
 VALUES ('NONE');