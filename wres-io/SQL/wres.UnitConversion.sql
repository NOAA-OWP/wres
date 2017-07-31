-- Table: wres.UnitConversion

CREATE SCHEMA IF NOT EXISTS wres AUTHORIZATION wres;

DROP TABLE IF EXISTS wres.UnitConversion CASCADE;

CREATE TABLE IF NOT EXISTS wres.UnitConversion
(
	from_unit SMALLINT,
	to_unit SMALLINT,
	factor DOUBLE PRECISION,
	initial_offset DOUBLE PRECISION DEFAULT 0,
	final_offset DOUBLE PRECISION DEFAULT 0,
	CONSTRAINT from_measurementunit_fk FOREIGN KEY (from_unit)
		REFERENCES wres.MeasurementUnit(measurementunit_id) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE CASCADE,
	CONSTRAINT to_measurementunit_fk FOREIGN KEY (to_unit)
		REFERENCES wres.MeasurementUnit(measurementunit_id) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE CASCADE
);
ALTER TABLE wres.unitconversion
  OWNER TO wres;

CREATE INDEX IF NOT EXISTS unitconversion_measurementunit_idx
	ON wres.UnitConversion
	USING btree
	(from_unit, to_unit);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT measurementunit_id, measurementunit_id, 1
FROM wres.MeasurementUnit M
WHERE NOT EXISTS (
	SELECT 1
	FROM wres.UnitConversion UC
	WHERE UC.from_unit = M.measurementunit_id
		AND UC.from_unit = UC.to_unit
);

-- VOLUME
INSERT INTO wres.UnitConversion (from_unit, to_unit, factor)
SELECT  F.measurementunit_id,
	T.measurementunit_id,
	35.3146662127
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'CFS'
WHERE F.unit_name = 'CMS'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT 	F.measurementunit_id,
	T.measurementunit_id,
	1/35.3146662127
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'CMS'
WHERE F.unit_name = 'CFS'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

-- DISTANCE
INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	3.28084
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'FT'
WHERE F.unit_name = 'M'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	0.00328084
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'FT'
WHERE F.unit_name = 'MM'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	0.08333333333333
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'FT'
WHERE F.unit_name = 'IN'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	12
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'IN'
WHERE F.unit_name = 'FT'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	39.3701
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'IN'
WHERE F.unit_name = 'M'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	0.0328084
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'FT'
WHERE F.unit_name = 'CM'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	0.0393701
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'IN'
WHERE F.unit_name = 'MM'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	0.393701
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'IN'
WHERE F.unit_name = 'CM'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	0.3048
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'M'
WHERE F.unit_name = 'FT'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	0.0254
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'M'
WHERE F.unit_name = 'IN'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	0.001
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'M'
WHERE F.unit_name = 'MM'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	0.01
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'MM'
WHERE F.unit_name = 'CM'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

-- TIME
INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	1.6667e-5
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'M'
WHERE F.unit_name = 'MS'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	60
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'M'
WHERE F.unit_name = 'HR'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	60
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'M'
WHERE F.unit_name = 'H'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	0.0166667
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'M'
WHERE F.unit_name = 'S'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	60000
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'MS'
WHERE F.unit_name = 'M'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	3.6e6
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'MS'
WHERE F.unit_name = 'HR'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	3.6e6
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'MS'
WHERE F.unit_name = 'H'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	1000
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'MS'
WHERE F.unit_name = 'S'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	0.0166667
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'HR'
WHERE F.unit_name = 'M'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	2.7778e-7
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'HR'
WHERE F.unit_name = 'MS'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	1
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'HR'
WHERE F.unit_name = 'H'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	0.000277778
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'HR'
WHERE F.unit_name = 'S'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	2.7778e-7
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'H'
WHERE F.unit_name = 'MS'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	1
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'H'
WHERE F.unit_name = 'HR'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	0.0166667
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'H'
WHERE F.unit_name = 'M'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	0.000277778
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'H'
WHERE F.unit_name = 'S'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	0.001
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'S'
WHERE F.unit_name = 'MS'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	3600
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'S'
WHERE F.unit_name = 'HR'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	3600
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'S'
WHERE F.unit_name = 'H'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	60
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'S'
WHERE F.unit_name = 'M'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);


-- Temperature
INSERT INTO wres.UnitConversion(from_unit, to_unit, factor, initial_offset)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	(5.0 / 9.0),
	-32.0
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'C'
WHERE F.unit_name = 'F'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);

INSERT INTO wres.UnitConversion(from_unit, to_unit, factor, final_offset)
SELECT	F.measurementunit_id,
	T.measurementunit_id,
	(9.0 / 5.0),
	32.0
FROM wres.MeasurementUnit F
INNER JOIN wres.MeasurementUnit T
	ON T.unit_name = 'F'
WHERE F.unit_name = 'C'
	AND NOT EXISTS (
		SELECT 1
		FROM wres.UnitConversion
		WHERE from_unit = F.measurementunit_id
			AND to_unit = T.measurementunit_id
	);