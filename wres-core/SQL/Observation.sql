-- Table: Observation

--DROP TABLE Observation;

CREATE TABLE Observation
(
  observation_id SERIAL,
  source text NOT NULL,
  variable_id INT,
  measurementunit_id INT,
  CONSTRAINT observation_pk PRIMARY KEY (observation_id),
  CONSTRAINT observation_variable_fk FOREIGN KEY (variable_id)
	REFERENCES Variable (variable_id) MATCH SIMPLE
	ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);
  
CREATE INDEX observation_variable_idx
  ON observation
  USING btree
  (variable_id);
