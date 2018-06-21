-- Table: wres.MeasurementUnit

CREATE SCHEMA IF NOT EXISTS wres AUTHORIZATION wres;

CREATE TABLE IF NOT EXISTS wres.MeasurementUnit
(
  measurementunit_id serial,
  unit_name text NOT NULL,
  CONSTRAINT measurementunit_pk PRIMARY KEY (measurementunit_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wres.MeasurementUnit
    OWNER TO wres;

ALTER TABLE wres.MeasurementUnit DROP CONSTRAINT IF EXISTS measurementunit_name_uidx;
ALTER TABLE wres.MeasurementUnit ADD CONSTRAINT measurementunit_name_uidx UNIQUE (unit_name);

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
	('CM'),
	('m3 s-1'),
	('kg m{-2}'),
	('%'),
	('ft/sec'),
	('gal/min'),
	('mgd'),
	('m/sec'),
	('ft3/day'),
	('ac-ft'),
	('mph'),
	('l/sec'),
	('ft3/s'),
	('m3/s'),
	('mm/s'),
	('mm s^-1'),
	('mm s{-1}'),
	('mm s-1'),
	('mm h^-1'),
	('mm/h'),
	('mm h-1'),
	('mm h{-1}'),
	('kg/m^2'),
	('kg/m^2h'),
	('kg/m^2s'),
	('kg/m^2/s'),
	('kg/m^2/h'),
	('Pa'),
	('W/m^2'),
	('W m{-2}'),
	('W m-2'),
	('m s-1'),
	('m/s'),
	('m s{-1}'),
	('kg kg-1'),
	('kg kg{-1}'),
	('kg m-2'),
	('kg m{-2}'),
	('fraction'),
	('K')
ON CONFLICT DO NOTHING;