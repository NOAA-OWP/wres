-- Table: wres.NetCDFCoordinate

DROP TABLE IF EXISTS wres.NetCDFCoordinate CASCADE;

CREATE TABLE IF NOT EXISTS wres.NetCDFCoordinate
(
  x_position smallint NOT NULL,
  y_position smallint NOT NULL,
  geographic_coordinate point NOT NULL,
  resolution smallint NOT NULL DEFAULT 0 -- The degree of accuracy for the coordinate. If the resolution is greater than 0, the coordinate is valid for all points within the resolution from the coordinates. A resolution of 0 indicates an absolute location.
)
WITH (
  OIDS=FALSE
);
ALTER TABLE wres.NetCDFCoordinate
  OWNER TO wres;

COMMENT ON COLUMN wres.NetCDFCoordinate.resolution IS 'The degree of accuracy for the coordinate. If the resolution is greater than 0, the coordinate is valid for all points within the resolution from the coordinates. A resolution of 0 indicates an absolute location.';


-- Index: wres.NetCDFCoordinate_coordinate_idx

DROP INDEX IF EXISTS wres.NetCDFCoordinate_coordinate_idx;

CREATE INDEX IF NOT EXISTS NetCDFCoordinate_coordinate_idx
  ON wres.NetCDFCoordinate
  USING gist
  (geographic_coordinate);

