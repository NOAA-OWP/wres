-- Table: Projection

-- DROP TABLE Projection;

CREATE TABLE Projection
(
	projection_id SERIAL,
	transform_name text,
	grid_mapping_name text,
	coordinate_axes text[],
	esri_pe_string text,
	standard_parallel SMALLINT[],
	longitude_of_central_median FLOAT,
	lattitude_of_projection_origin FLOAT,
	false_easting FLOAT DEFAULT 0.0,
	false_northing FLOAT DEFAULT 0.0,
	earth_radius FLOAT DEFAULT 6370000.0,
	proj4 text,
	CONSTRAINT projection_pk PRIMARY KEY (projection_id)
)
WITH (
  OIDS=FALSE
);

INSERT INTO Projection (transform_name)
VALUES ('UNDEFINED');
