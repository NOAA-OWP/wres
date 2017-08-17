-- Table: public.executionlog

DROP TABLE public.executionlog;

CREATE TABLE IF NOT EXISTS public.executionlog
(
  log_id serial,
  arguments text NOT NULL,
  system_settings xml NOT NULL,
  project xml,
  username text,
  address text,
  start_time timestamp without time zone,
  run_time interval,
  failed boolean DEFAULT false
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.executionlog
  OWNER TO wres;
