-- Table: ForecastRange

-- DROP TABLE ForecastRange;

CREATE TABLE ForecastRange
(
  forecastrange_id serial,
  range_name text NOT NULL,
  timestep SMALLINT NOT NULL,
  added_date TIMESTAMP DEFAULT now(),
  CONSTRAINT forecastrange_pk PRIMARY KEY (forecastrange_id)
)
WITH (
  OIDS=FALSE
);

 INSERT INTO ForecastRange (range_name, timestep)
 VALUES ('short', 1),
	('medium', 6),
	('long', 24);
