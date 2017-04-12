-- Table: wres.ForecastType

CREATE SCHEMA IF NOT EXISTS wres;

DROP TABLE IF EXISTS wres.ForecastType;

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

 INSERT INTO wres.ForecastType (type_name, timestep, step_count)
 VALUES ('short', 1, 18),
	('medium', 3, 80),
	('long', 6, 120);
