# Gets an existing project dataset as if it were observations.
# The CSV can then be used instead of whatever data format produced the dataset.
# Should work with WRES 5.17
# Set database stuff directly here.
# Argument 1 is project_id
# Argument 2 is dataset name (e.g. left, right, baseline)

db_hostname=localhost
db_port=5432
db_name=wres
db_username=wres_user

project_id=${1:-1}
member=${2:-"left"}

read -d '' observation_query <<EOF
select F.name as location,
    F.description as location_description,
    F.srid as location_srid,
    F.wkt as location_wkt,
    TS.variable_name as variable_name,
    TS.scale_period as timescale_in_minutes,
    TS.scale_function as timescale_function,
    MU.unit_name as measurement_unit,
    to_char( TS.initialization_date +  INTERVAL '1' MINUTE * TSV.lead, 'YYYY-MM-DD"T"HH24:MI:SS"Z"' ) as value_date,
    TSV.series_value as value
from wres.ProjectSource PS
inner join wres.TimeSeries TS
    on PS.source_id = TS.source_id
inner join wres.Feature F
    on TS.feature_id = F.feature_id
inner join wres.MeasurementUnit MU
    on TS.measurementunit_id = MU.measurementunit_id
inner join wres.Ensemble E
    on TS.ensemble_id = E.ensemble_id
inner join wres.TimeSeriesValue TSV
    on TS.timeseries_id = TSV.timeseries_id
where PS.project_id = ${project_id}
    and PS.member = '${member}'
ORDER BY TS.source_id
EOF

psql -h ${db_hostname} -p ${db_port} -d ${db_name} -U ${db_username} -c "\copy ( ${observation_query} ) TO project_${project_id}_${member}_observations.csv CSV HEADER"
