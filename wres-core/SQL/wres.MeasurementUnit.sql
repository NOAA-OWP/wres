-- Table: wres.MeasurementUnit

CREATE SCHEMA IF NOT EXISTS wres;

DROP TABLE IF EXISTS wres.MeasurementUnit CASCADE;

CREATE TABLE IF NOT EXISTS wres.MeasurementUnit
(
  measurementunit_id serial,
  unit_name text NOT NULL,
  CONSTRAINT measurementunit_pk PRIMARY KEY (measurementunit_id)
)
WITH (
  OIDS=FALSE
);

 INSERT INTO wres.MeasurementUnit (unit_name)
 VALUES	('NONE'),
	('CMS'),
	('CFS'),
	('FT'),
	('F'),
	('C'),
	('IN'),
	('M'),
	('MS'),
	('HR'),
	('H'),
	('S'),
	('MM'),
	('CM');
