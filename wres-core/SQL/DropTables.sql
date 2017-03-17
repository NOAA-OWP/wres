DO $$
BEGIN
	DROP TABLE IF EXISTS ForecastCoordinate;
	DROP TABLE IF EXISTS ObservationCoordinate;
	DROP TABLE IF EXISTS USGSLocationCoordinate;
	DROP TABLE IF EXISTS NWSLocationCoordinate;
	DROP TABLE IF EXISTS ForecastResult;
	DROP TABLE IF EXISTS Forecast;
	DROP TABLE IF EXISTS ObservationResult;
	DROP TABLE IF EXISTS Observation;
	DROP TABLE IF EXISTS ForecastRange;
	DROP TABLE IF EXISTS nwm_location;
	DROP TABLE IF EXISTS Variable;
	DROP TABLE IF EXISTS Projection;
	DROP TABLE IF EXISTS Coordinate;
	DROP TABLE IF EXISTS ObservationLocation;
	DROP TABLE IF EXISTS UnitConversion;
	DROP TABLE IF EXISTS MeasurementUnit;
END  $$;
