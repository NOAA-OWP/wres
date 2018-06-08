-- View: wres.Conversions

-- DROP VIEW wres.Conversions;

CREATE OR REPLACE VIEW wres.Conversions AS 
 SELECT F.measurementunit_id AS from_unit_id,
     T.measurementunit_id AS to_unit_id,
     F.unit_name AS from_unit,
     T.unit_name AS to_unit,
     UC.initial_offset,
     UC.factor,
     UC.final_offset,
     CASE
         WHEN UC.initial_offset != 0 AND UC.final_offset != 0 THEN '(x + (' || UC.initial_offset || ')) * ' || trunc(UC.factor::NUMERIC, 4) || ' + ' || UC.final_offset
         WHEN UC.initial_offset != 0 THEN '(x + (' || UC.initial_offset || ')) * ' || trunc(UC.factor::NUMERIC, 4)
         WHEN UC.final_offset != 0 THEN 'x * ' || trunc(UC.factor::NUMERIC, 4) || ' + ' || UC.final_offset
         ELSE 'x * ' || trunc(UC.factor::NUMERIC, 4)
     END AS formula
 FROM wres.UnitConversion UC
 INNER JOIN wres.MeasurementUnit F
     ON F.measurementunit_id = UC.from_unit
 INNER JOIN wres.MeasurementUnit T
     ON T.measurementunit_id = UC.to_unit     
 ORDER BY from_unit, to_unit
 
ALTER TABLE wres.Conversions
  OWNER TO wres;
