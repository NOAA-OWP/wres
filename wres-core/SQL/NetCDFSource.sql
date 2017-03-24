-- Table: public.netcdfsource

-- DROP TABLE public.netcdfsource;

CREATE TABLE public.netcdfsource
(
	netcdfsource_id SERIAL PRIMARY KEY,
	valid_date timestamp without time zone NOT NULL,
	file_path TEXT NOT NULL,
	is_forecast BOOLEAN DEFAULT TRUE,
	ingest_date timestamp without time zone DEFAULT now(),
	lead_time int
)
WITH (
  OIDS=FALSE
);

-- Index: public.netcdfsource_valid_date_idx

-- DROP INDEX public.netcdfsource_valid_date_idx;

CREATE INDEX netcdfsource_valid_date_idx
  ON public.netcdfsource
  USING btree
  (valid_date);

