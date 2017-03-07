-- Table: ObservationLocation

-- DROP TABLE ObservationLocation;

CREATE TABLE ObservationLocation
(
	observationlocation_id SERIAL,
	comid INTEGER NOT NULL,
	lid text NOT NULL,
	gage_id text NOT NULL ,
	huc text DEFAULT '',
	rfc text DEFAULT '',
	fcst text DEFAULT '',
	hsa text DEFAULT '',
	typ text DEFAULT '',
	fip text DEFAULT '',
	st text NOT NULL,
	nws_st text NOT NULL,
	atv BOOLEAN DEFAULT TRUE,
	ref BOOLEAN DEFAULT FALSE,
	hcdn text DEFAULT '',
	da REAL DEFAULT 0.0,
	nws_lat REAL DEFAULT 0.0,
	nws_lon REAL DEFAULT 0.0,
	usgs_lat REAL DEFAULT 0.0,
	usgs_lon REAL DEFAULT 0.0,
	cac text DEFAULT '',
	coord text DEFAULT '',
	alt REAL DEFAULT 0.0,
	alt_acy REAL DEFAULT 0.0,
	datum text DEFAULT '',
	goes_id text DEFAULT '0',
	in_model_one BOOLEAN DEFAULT TRUE,
	in_model_one_one BOOLEAN DEFAULT TRUE,
	nws_name text DEFAULT '',
	usgs_name text DEFAULT '',
	CONSTRAINT observationlocation_pk PRIMARY KEY (observationlocation_id)
)
WITH (
  OIDS=FALSE
);
