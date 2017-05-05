-- Table: public.projection

DROP TABLE IF EXISTS public.projection CASCADE;

CREATE TABLE IF NOT EXISTS public.projection
(
  projection_id SERIAL,
  transform_name text,
  grid_mapping_name text,
  coordinate_axes text[],
  esri_pe_string text,
  standard_parallel smallint[],
  longitude_of_central_median double precision,
  lattitude_of_projection_origin double precision,
  false_easting double precision DEFAULT 0.0,
  false_northing double precision DEFAULT 0.0,
  earth_radius double precision DEFAULT 6370000.0,
  proj4 text,
  CONSTRAINT projection_pk PRIMARY KEY (projection_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.projection
  OWNER TO wres;

INSERT INTO public.Projection (transform_name, false_easting, false_northing, earth_radius)
SELECT 'UNDEFINED', 0, 0, 6370000
WHERE NOT EXISTS (
	SELECT 1
	FROM public.Projection
	WHERE transform_name = 'UNDEFINED'
		AND false_easting = 0
		AND false_northing = 0
		AND earth_radius = 6370000
);