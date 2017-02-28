-- Table: Variable

DROP TABLE Variable;

CREATE TABLE Variable
(
	variable_id SERIAL,
	variable_name text NOT NULL,
	variable_type text,
	description text,
	measurementunit_id INT,
	added_date timestamp DEFAULT now(),
	CONSTRAINT variable_pk PRIMARY KEY (variable_id),
	CONSTRAINT variable_measurementunit_fk FOREIGN KEY (measurementunit_id)
		REFERENCES MeasurementUnit (measurementunit_id) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);
ALTER TABLE Variable
  OWNER TO "christopher.tubbs";

 INSERT INTO Variable (variable_name, variable_type, description, measurementunit_id)
 VALUES ('precipitation', 'double', 'precipitation', 1);