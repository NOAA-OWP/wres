-- Table: public.executionlog

CREATE TABLE IF NOT EXISTS public.executionlog
(
  log_id serial,
  arguments text NOT NULL,
  system_settings TEXT NOT NULL,
  project TEXT,
  username text,
  address text,
  start_time timestamp without time zone,
  run_time interval,
  failed boolean DEFAULT false,
  error TEXT
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.executionlog
  OWNER TO wres;

ALTER TABLE public.executionlog
  ADD COLUMN IF NOT EXISTS input_code TYPE text;
