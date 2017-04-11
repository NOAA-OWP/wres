-- Table: public.coordinate

DROP TABLE IF EXISTS public.coordinate CASCADE;

CREATE TABLE IF NOT EXISTS public.coordinate
(
  coordinate_id SERIAL,
  geographic_coordinate point NOT NULL,
  resolution integer NOT NULL DEFAULT 0, -- The degree of accuracy for the coordinate. If the resolution is greater than 0, the coordinate is valid for all points within the resolution from the coordinates. A resolution of 0 indicates an absolute location.
  CONSTRAINT coordinate_pk PRIMARY KEY (coordinate_id)
)
WITH (
  OIDS=FALSE
);

COMMENT ON COLUMN public.coordinate.resolution IS 'The degree of accuracy for the coordinate. If the resolution is greater than 0, the coordinate is valid for all points within the resolution from the coordinates. A resolution of 0 indicates an absolute location.';


-- Index: public.coordinate_resolution_idx

DROP INDEX IF EXISTS public.coordinate_resolution_idx;

CREATE INDEX IF NOT EXISTS coordinate_resolution_idx
  ON public.coordinate
  USING gist
  (geographic_coordinate);

