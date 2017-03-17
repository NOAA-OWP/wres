CREATE TABLE UnitConversion
(
	from_unit SMALLINT,
	to_unit SMALLINT,
	factor SMALLINT,
	CONSTRAINT from_measurementunit_fk FOREIGN KEY (from_unit)
		REFERENCES MeasurementUnit(measurementunit_id) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE CASCADE,
	CONSTRAINT to_measurementunit_fk FOREIGN KEY (to_unit)
		REFERENCES MeasurementUnit(measurementunit_id) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE CASCADE
);

CREATE INDEX unitconversion_measurementunit_idx
	ON UnitConversion
	USING btree
	(from_unit, to_unit);

INSERT INTO UnitConversion(from_unit, to_unit, factor)
VALUES (1, 1, 1);
