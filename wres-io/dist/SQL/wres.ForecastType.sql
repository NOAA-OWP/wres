-- Table: wres.ForecastType

CREATE SCHEMA IF NOT EXISTS wres AUTHORIZATION wres;

DROP TABLE IF EXISTS wres.ForecastType CASCADE;

CREATE TABLE IF NOT EXISTS wres.ForecastType
(
  forecasttype_id smallserial,
  type_name text NOT NULL,
  timestep SMALLINT NOT NULL,
  step_count SMALLINT NOT NULL,
  CONSTRAINT forecasttype_pk PRIMARY KEY (forecasttype_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wres.forecasttype
  OWNER TO wres;

 INSERT INTO wres.ForecastType (type_name, timestep, step_count)
 SELECT type_name, timestep, step_count
 FROM (
	VALUES 	('short', 1, 18),
		('medium', 3, 80),
		('long', 6, 120),
		('analysis', 1, 15),
		('variable', 1, 1)
) AS FT (type_name, timestep, step_count)
WHERE NOT EXISTS (
	SELECT 1
	FROM wres.ForecastType F
	WHERE F.type_name = FT.type_name
);
