-- Table: ObservationLocation

-- DROP TABLE ObservationLocation;

CREATE TABLE ObservationLocation
(
	observationlocation_id SERIAL,
	comid text DEFAULT '',
	lid text NOT NULL,
	gage_id text DEFAULT '',
	huc text DEFAULT '',
	rfc text DEFAULT '',
	fcst text DEFAULT '',
	hsa text DEFAULT '',
	typ text DEFAULT '',
	fip text DEFAULT '',
	st text DEFAULT '',
	nws_st text DEFAULT '',
	atv text DEFAULT '',
	ref text DEFAULT '',
	hcdn text DEFAULT '',
	da text DEFAULT '',
	nws_lat text DEFAULT '',
	nws_lon text DEFAULT '',
	usgs_lat text DEFAULT '',
	usgs_lon text DEFAULT '',
	cac text DEFAULT '',
	coord text DEFAULT '',
	alt text DEFAULT '',
	alt_acy text DEFAULT '',
	datum text DEFAULT '',
	goes_id text DEFAULT '',
	nws_name text DEFAULT '',
	usgs_name text DEFAULT '',
	comid_v1_1 text DEFAULT '',
	CONSTRAINT observationlocation_pk PRIMARY KEY (observationlocation_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE ObservationLocation
  OWNER TO "christopher.tubbs";