-- Table: wres.variableposition

CREATE SCHEMA IF NOT EXISTS wres AUTHORIZATION wres;

DROP TABLE IF EXISTS wres.variableposition CASCADE;

CREATE TABLE IF NOT EXISTS wres.variableposition
(
  variableposition_id serial NOT NULL,
  variable_id integer,
  x_position integer,
  y_position integer,
  CONSTRAINT variableposition_pk PRIMARY KEY (variableposition_id),
  CONSTRAINT variableposition_variable_fk FOREIGN KEY (variable_id)
      REFERENCES wres.variable (variable_id)
      ON DELETE CASCADE
      DEFERRABLE INITIALLY DEFERRED
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wres.variableposition
  OWNER TO wres;

-- Index: wres.variableposition_variable_idx

-- DROP INDEX IF EXISTS wres.variableposition_variable_idx;

CREATE INDEX IF NOT EXISTS variableposition_variable_idx
  ON wres.variableposition
  USING btree
  (variable_id);

-- Index: wres.variableposition_x_idx

-- DROP INDEX IF EXISTS wres.variableposition_x_idx;

CREATE INDEX IF NOT EXISTS variableposition_x_idx
  ON wres.variableposition
  USING btree
  (x_position);

-- Index: wres.variableposition_y_idx

-- DROP INDEX IF EXISTS wres.variableposition_y_idx;

CREATE INDEX IF NOT EXISTS variableposition_y_idx
  ON wres.variableposition
  USING btree
  (y_position);

