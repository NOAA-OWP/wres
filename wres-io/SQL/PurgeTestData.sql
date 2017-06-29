TRUNCATE wres.ForecastSource RESTART IDENTITY CASCADE;
TRUNCATE wres.ForecastValue;
TRUNCATE wres.Observation;
TRUNCATE wres.Source RESTART IDENTITY CASCADE;
TRUNCATE wres.ForecastEnsemble RESTART IDENTITY CASCADE;
TRUNCATE wres.Forecast RESTART IDENTITY CASCADE;
--TRUNCATE wres.NetCDFValue;
--TRUNCATE wres.VariablePosition RESTART IDENTITY CASCADE;
--TRUNCATE wres.Variable RESTART IDENTITY CASCADE;

-- There used to be logic to remove partitions here, but the script to generate the drops should be used instead

VACUUM FULL VERBOSE ANALYZE wres.ForecastSource;
VACUUM FULL VERBOSE ANALYZE wres.Source;
VACUUM FULL VERBOSE ANALYZE wres.ForecastEnsemble;
VACUUM FULL VERBOSE ANALYZE wres.Forecast;
VACUUM FULL VERBOSE ANALYZE wres.ForecastValue;
VACUUM FULL VERBOSE ANALYZE wres.Observation;
--VACUUM FULL VERBOSE ANALYZE wres.NetCDFValue;
--VACUUM FULL VERBOSE ANALYZE wres.Variable;