-- Table: public.netcdfsource

DROP TABLE IF EXISTS public.netcdfsource CASCADE;

CREATE TABLE IF NOT EXISTS public.netcdfsource
(
  netcdfsource_id integer NOT NULL DEFAULT nextval('netcdfsource_netcdfsource_id_seq'::regclass),
  valid_date timestamp without time zone NOT NULL,
  file_path text NOT NULL,
  is_forecast boolean DEFAULT true,
  ingest_date timestamp without time zone DEFAULT now(),
  lead_time integer,
  CONSTRAINT netcdfsource_pkey PRIMARY KEY (netcdfsource_id)
)
WITH (
  OIDS=FALSE
);

-- Index: public.netcdfsource_valid_date_idx

DROP INDEX IF EXISTS public.netcdfsource_valid_date_idx;

CREATE INDEX IF NOT EXISTS netcdfsource_valid_date_idx
  ON public.netcdfsource
  USING btree
  (valid_date);

