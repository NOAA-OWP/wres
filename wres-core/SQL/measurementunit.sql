-- Table: public.measurementunit

DROP TABLE IF EXISTS public.measurementunit CASCADE;

CREATE TABLE IF NOT EXISTS public.measurementunit
(
  measurementunit_id integer NOT NULL DEFAULT nextval('measurementunit_measurementunit_id_seq'::regclass),
  unit_name text NOT NULL,
  added_date timestamp without time zone DEFAULT now(),
  CONSTRAINT measurementunit_pk PRIMARY KEY (measurementunit_id)
)
WITH (
  OIDS=FALSE
);

INSERT INTO (unit_name)
SELECT 'NONE'
WHERE NOT EXISTS (
	SELECT 1
	FROM public.measurementunit
	WHERE unit_name = 'NONE'
);