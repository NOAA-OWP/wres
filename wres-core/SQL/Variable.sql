-- Table: public.variable

DROP TABLE IF EXISTS public.variable CASCADE;

CREATE TABLE IF NOT EXISTS public.variable
(
  variable_id SERIAL,
  variable_name text NOT NULL,
  variable_type text,
  description text,
  measurementunit_id integer,
  added_date timestamp without time zone DEFAULT now(),
  CONSTRAINT variable_pk PRIMARY KEY (variable_id),
  CONSTRAINT variable_measurementunit_fk FOREIGN KEY (measurementunit_id)
      REFERENCES public.measurementunit (measurementunit_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
      DEFERRABLE INITIALLY DEFERRED,
  CONSTRAINT variable_variable_name_key UNIQUE (variable_name)
)
WITH (
  OIDS=FALSE
);

-- Index: public.variable_variable_name_idx

DROP INDEX IF EXISTS public.variable_variable_name_idx;

CREATE INDEX IF NOT EXISTS variable_variable_name_idx
  ON public.variable
  USING btree
  (variable_name COLLATE pg_catalog."default");

