INSERT INTO public.spatial_ref_sys (srid, auth_name, auth_srid, srtext, proj4text)
WITH max AS
(
	SELECT MAX(srid) + 1 AS srid
	FROM public.spatial_ref_sys
)
SELECT	max.srid AS srid, 
	'wres' AS auth_name, 
	max.srid AS auth_srid, 
	'PROJCS["Sphere_Lambert_Conformal_Conic",GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6370000.0,0.0]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic"],PARAMETER["false_easting",0.0],PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-97.0],PARAMETER["standard_parallel_1",30.0],PARAMETER["standard_parallel_2",60.0],PARAMETER["latitude_of_origin",40.0000076294],UNIT["Meter",1.0]];-35691800 -29075200 126180232.640845;-100000 10000;-100000 10000;0.001;0.001;0.001;IsHighPrecision' AS srtext, 
	'+proj=lcc +lat_1=30 +lat_2=60 +lat_0=40 +lon_0=-97 +x_0=0 +y_0=0 +a=6370000 +b=6370000 +units=m +no_defs' AS proj4text
FROM max
WHERE NOT EXISTS (
	SELECT 1
	FROM public.spatial_ref_sys
	WHERE auth_name = 'wres'
		AND proj4text = '+proj=lcc +lat_1=30 +lat_2=60 +lat_0=40 +lon_0=-97 +x_0=0 +y_0=0 +a=6370000 +b=6370000 +units=m +no_defs'
		AND srtext = 'PROJCS["Sphere_Lambert_Conformal_Conic",GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6370000.0,0.0]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic"],PARAMETER["false_easting",0.0],PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-97.0],PARAMETER["standard_parallel_1",30.0],PARAMETER["standard_parallel_2",60.0],PARAMETER["latitude_of_origin",40.0000076294],UNIT["Meter",1.0]];-35691800 -29075200 126180232.640845;-100000 10000;-100000 10000;0.001;0.001;0.001;IsHighPrecision'
)
RETURNING *;