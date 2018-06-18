-- Table: wres.NetCDFCoordinate

DROP TABLE IF EXISTS wres.NetCDFCoordinate CASCADE;

CREATE TABLE IF NOT EXISTS wres.NetCDFCoordinate
(
  gridprojection_id integer NOT NULL,
  x_position smallint NOT NULL,
  y_position smallint NOT NULL,
  geographic_coordinate point NOT NULL
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wres.NetCDFCoordinate
  OWNER TO wres;

-- Index: wres.NetCDFCoordinate_coordinate_idx

DROP INDEX IF EXISTS wres.NetCDFCoordinate_coordinate_idx;

CREATE INDEX IF NOT EXISTS NetCDFCoordinate_coordinate_idx
  ON wres.NetCDFCoordinate
  USING gist
  (geographic_coordinate);

