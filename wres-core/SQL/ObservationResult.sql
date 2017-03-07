-- Table: ObservationResult

-- DELETE FROM ObservationResult;
-- DELETE FROM Observation;
-- DROP TABLE ObservationResult;

CREATE TABLE ObservationResult
(
  observation_id INT,
  valid_date TIMESTAMP NOT NULL,
  measurement DOUBLE PRECISION NOT NULL,
  revision SMALLINT DEFAULT 1,
  approval_date TIMESTAMP,
  CONSTRAINT observationresult_observation_fk FOREIGN KEY (observation_id)
	REFERENCES Observation (observation_id) MATCH SIMPLE
	ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);
CREATE INDEX observationresult_observation_idx ON ObservationResult(observation_id);
CREATE INDEX observationresult_valid_date_idx ON ObservationResult(valid_date);
