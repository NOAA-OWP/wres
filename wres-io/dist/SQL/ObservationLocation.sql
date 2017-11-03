-- Table: public.observationlocation

DROP TABLE IF EXISTS public.observationlocation CASCADE;

CREATE TABLE IF NOT EXISTS public.observationlocation
(
  observationlocation_id SERIAL,
  comid integer NOT NULL,
  lid text NOT NULL,
  gage_id text NOT NULL,
  huc text DEFAULT ''::text,
  rfc text DEFAULT ''::text,
  fcst text DEFAULT ''::text,
  hsa text DEFAULT ''::text,
  typ text DEFAULT ''::text,
  fip text DEFAULT ''::text,
  st text NOT NULL,
  nws_st text NOT NULL,
  atv boolean DEFAULT true,
  ref boolean DEFAULT false,
  hcdn text DEFAULT ''::text,
  da real DEFAULT 0.0,
  nws_lat real DEFAULT 0.0,
  nws_lon real DEFAULT 0.0,
  usgs_lat real DEFAULT 0.0,
  usgs_lon real DEFAULT 0.0,
  cac text DEFAULT ''::text,
  coord text DEFAULT ''::text,
  alt real DEFAULT 0.0,
  alt_acy real DEFAULT 0.0,
  datum text DEFAULT ''::text,
  goes_id text DEFAULT '0'::text,
  in_model_one boolean DEFAULT true,
  in_model_one_one boolean DEFAULT true,
  nws_name text DEFAULT ''::text,
  usgs_name text DEFAULT ''::text,
  CONSTRAINT observationlocation_pk PRIMARY KEY (observationlocation_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.observationlocation
  OWNER TO wres;

-- Index: public.observationlocation_comid_idx

DROP INDEX IF EXISTS public.observationlocation_comid_idx;

CREATE INDEX IF NOT EXISTS observationlocation_comid_idx
  ON public.observationlocation
  USING btree
  (comid);

-- Index: public.observationlocation_latlon_idx

DROP INDEX IF EXISTS public.observationlocation_latlon_idx;

CREATE INDEX IF NOT EXISTS observationlocation_latlon_idx
  ON public.observationlocation
  USING btree
  (nws_lat, nws_lon);

