-- Table: public.indexqueue

-- DROP TABLE public.indexqueue;

CREATE TABLE IF NOT EXISTS public.indexqueue
(
  indexqueue_id SERIAL,
  table_name text NOT NULL,
  index_name text NOT NULL,
  column_definition text NOT NULL,
  method text DEFAULT 'btree'::text
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.indexqueue
  OWNER TO wres;
