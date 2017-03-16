-- Table: public.Coordinate

-- DROP TABLE public.Coordinate;

CREATE TABLE public.coordinate
(
  coordinate_id SERIAL,
  geographic_coordinate point NOT NULL,
  resolution integer NOT NULL DEFAULT 0, -- The degree of accuracy for the coordinate. If the resolution is greater than 0, the coordinate is valid for all points within the resolution from the coordinates. A resolution of 0 indicates an absolute location.
  CONSTRAINT coordinate_pk PRIMARY KEY (coordinate_id)
)
WITH (
  OIDS=FALSE
);

CREATE INDEX coordinate_resolution_idx
ON coordinate
USING GIST (geographic_coordinate);

COMMENT ON COLUMN public.coordinate.resolution IS 'The degree of accuracy for the coordinate. If the resolution is greater than 0, the coordinate is valid for all points within the resolution from the coordinates. A resolution of 0 indicates an absolute location.';

