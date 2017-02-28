-- Table: Observation

-- DROP TABLE Observation;

CREATE TABLE Observation
(
  observation_id SERIAL,
  source text NOT NULL,
  observationlocation_id INT,
  variable_id INT,
  measurementunit_id INT,
  projection_id INT,
  CONSTRAINT observation_pk PRIMARY KEY (observation_id),
  CONSTRAINT observation_observationlocation_fk FOREIGN KEY (observationlocation_id)
	REFERENCES ObservationLocation (observationlocation_id) MATCH SIMPLE
	ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT observation_variable_fk FOREIGN KEY (variable_id)
	REFERENCES Variable (variable_id) MATCH SIMPLE
	ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT observation_projection_fk FOREIGN KEY (projection_id)
	REFERENCES Projection (projection_id) MATCH SIMPLE
	ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);
ALTER TABLE Observation
  OWNER TO "christopher.tubbs";