-- Table: public.measurementunit

DROP TABLE IF EXISTS public.measurementunit;

CREATE TABLE IF NOT EXISTS public.measurementunit
(
  measurementunit_id SERIAL,
  unit_name text NOT NULL,
  added_date timestamp without time zone DEFAULT now(),
  CONSTRAINT measurementunit_pk PRIMARY KEY (measurementunit_id)
)
WITH (
  OIDS=FALSE
);
