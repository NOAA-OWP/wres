dump_file_prefix='wresbackup'
database_host='fake.fqdn'
database_name='wres'
database_username='wres_user'
pg_restore_command=$(which pg_restore)
cpus=$(nproc)
max_j=8
min_j=1

while getopts "f:h:d:U:p:" opt
do
    case $opt in
        f)
            dump_file_prefix=$OPTARG
            ;;
        h)
            database_host=$OPTARG
            ;;
        d)
            database_name=$OPTARG
            ;;
        U)
            database_username=$OPTARG
            ;;
        p)
            pg_restore_command=$OPTARG
            ;;
        \?)
            echo "Usage: $0 -f dump_file_prefix -h database_host -d database_name -U database_username [ -p pg_restore_command ]"
            exit 2
            ;;
    esac
done

dump_file=${dump_file_prefix}.pgdump
changelog_dump_file=${dump_file_prefix}_changelog.pgdump
list_of_non_schema_objects_file=${dump_file_prefix}.pgdumplist

computed_j=$(($cpus - 2))
j=$computed_j

if [ $computed_j -gt $max_j ]
then
    j=$max_j
elif [ $computed_j -lt $min_j ]
then
    j=$min_j
fi


list_objects_command="$pg_restore_command -e -l ${dump_file} -f ${list_of_non_schema_objects_file}"
restore_pre_data_only_command="$pg_restore_command -e -j $j --no-owner -h ${database_host} -d ${database_name} -U ${database_username} --section=pre-data"
restore_data_only_table_command="$pg_restore_command -e -j $j --no-owner -h ${database_host} -d ${database_name} -U ${database_username} --data-only --strict-names "
restore_post_data_only_command="$pg_restore_command -e -j $j --no-owner -h ${database_host} -d ${database_name} -U ${database_username} --section=post-data"

# Some basic checks before execution.
dump_file_exists="(does NOT exist!)"

if [ -f $dump_file ]
then
    dump_file_exists="(exists)"
fi

dump_file_readable="(is NOT readable!)"

if [ -r $dump_file ]
then
    dump_file_readable="(readable)"
fi



changelog_dump_file_exists="(does NOT exist!)"

if [ -f $changelog_dump_file ]
then
    changelog_dump_file_exists="(exists)"
fi

changelog_dump_file_readable="(is NOT readable!)"

if [ -r $changelog_dump_file ]
then
    changelog_dump_file_readable="(readable)"
fi

list_of_non_schema_objects_file_exists="(does not yet exist)"

if [ -f $list_of_non_schema_objects_file ]
then
    list_of_non_schema_objects_file_exists="(ALREADY EXISTS!)"
fi

pg_restore_command_exists="(does NOT exist!)"

if [ -f $pg_restore_command ]
then
    pg_restore_command_exists="(exists)"
fi

pg_restore_command_is_executable="(is NOT executable)"

if [ -x $pg_restore_command ]
then
    pg_restore_command_is_executable="(executable)"
fi

database_host_resolves="(does NOT resolve)"

if [ ! -z "$database_host" ]
then
    nslookup ${database_host} >/dev/null 2>&1
    resolved=$?
fi

if [ "$resolved" == "0" ]
then
    database_host_resolves="(resolves)"
fi


echo "Restoring dump_file ${dump_file} ${dump_file_exists} ${dump_file_readable}"
echo "Restoring changelog_dump_file ${changelog_dump_file} ${changelog_dump_file_exists} ${changelog_dump_file_readable}"
echo "Using pg_restore executable ${pg_restore_command} ${pg_restore_command_exists} ${pg_restore_command_is_executable}"
echo "Using database host ${database_host} ${database_host_resolves}"
echo "Using database name ${database_name}"
echo "Using database username ${database_username}"
echo "Creating list_of_non_schema_objects_file ${list_of_non_schema_objects_file} ${list_of_non_schema_objects_file_exists}"
echo "Using restore_pre_data_only_command ${restore_pre_data_only_command}"
echo "Using restore_data_only_table_command ${restore_data_only_table_command}"
echo "Using restore_post_data_only_command ${restore_post_data_only_command}"

echo "Assumption: public.scale_function and public.operating_member exist."
echo "To be sure they exist, use psql to run the following before restoring:"
echo "CREATE TYPE scale_function AS ENUM('UNKNOWN', 'MEAN', 'MINIMUM', 'MAXIMUM', 'TOTAL');"
echo "CREATE TYPE operating_member AS ENUM('left', 'right', 'baseline');"

# Require one keystroke before doing it.
read -n1 -r -p "Please ctrl-c if that is not correct, any key otherwise..." key

date --iso-8601=ns
start_seconds=$(date +%s)

# Regarding the "ex" command below:
# Delete lines with word "schema" using posix-compliant method (SO 5410757).
# The reason is the schema setup occurs above/outside this script, allows role
# that owns the database to create and own the schema and grant limited
# permissions to the role running restore (or liquibase).

# It is nice to see the following commands printed when run.
set -x

$list_objects_command \
&& ex +g/SCHEMA/d -cwq ${list_of_non_schema_objects_file} \
&& $restore_pre_data_only_command $changelog_dump_file \
&& $restore_data_only_table_command -n public -t databasechangelog -t databasechangeloglock $changelog_dump_file \
&& $restore_post_data_only_command $changelog_dump_file \
&& $restore_pre_data_only_command  -L ${list_of_non_schema_objects_file} $dump_file \
&& $restore_data_only_table_command -t measurementunit -t conversions -t usgsparameter -t netcdfcoordinate -t unitconversion -t feature -t ensemble -t gridprojection -t variable -t source -t indexqueue $dump_file \
&& $restore_data_only_table_command -t measurementunit_measurementunit_id_seq -t feature_feature_id_seq -t ensemble_ensemble_id_seq -t gridprojection_gridprojection_id_seq -t variable_variable_id_seq -t source_source_id_seq -t indexqueue_indexqueue_id_seq $dump_file \
&& $restore_data_only_table_command -t variablefeature -t variablebyfeature $dump_file \
&& $restore_data_only_table_command -t timeseries -t variablefeature_variablefeature_id_seq $dump_file \
&& $restore_data_only_table_command -t timeseries_timeseries_id_seq -t timeseriessource -t timeseriesvalue -t timeseriesvalue_lead_0 -t timeseriesvalue_lead_1 -t timeseriesvalue_lead_2 -t timeseriesvalue_lead_3 -t timeseriesvalue_lead_4 -t timeseriesvalue_lead_5 -t timeseriesvalue_lead_6 -t timeseriesvalue_lead_7 -t timeseriesvalue_lead_8 -t timeseriesvalue_lead_9 $dump_file \
&& $restore_data_only_table_command -t timeseriesvalue_lead_10 -t timeseriesvalue_lead_11 -t timeseriesvalue_lead_12 -t timeseriesvalue_lead_13 -t timeseriesvalue_lead_14 -t timeseriesvalue_lead_15 -t timeseriesvalue_lead_16 -t timeseriesvalue_lead_17 -t timeseriesvalue_lead_18 -t timeseriesvalue_lead_19 $dump_file \
&& $restore_data_only_table_command -t timeseriesvalue_lead_20 -t timeseriesvalue_lead_21 -t timeseriesvalue_lead_22 -t timeseriesvalue_lead_23 -t timeseriesvalue_lead_24 -t timeseriesvalue_lead_25 -t timeseriesvalue_lead_26 -t timeseriesvalue_lead_27 -t timeseriesvalue_lead_28 -t timeseriesvalue_lead_29 $dump_file \
&& $restore_data_only_table_command -t timeseriesvalue_lead_30 -t timeseriesvalue_lead_31 -t timeseriesvalue_lead_32 -t timeseriesvalue_lead_33 -t timeseriesvalue_lead_34 -t timeseriesvalue_lead_35 -t timeseriesvalue_lead_36 -t timeseriesvalue_lead_37 -t timeseriesvalue_lead_38 -t timeseriesvalue_lead_39 $dump_file \
&& $restore_data_only_table_command -t timeseriesvalue_lead_40 -t timeseriesvalue_lead_41 -t timeseriesvalue_lead_42 -t timeseriesvalue_lead_43 -t timeseriesvalue_lead_44 -t timeseriesvalue_lead_45 -t timeseriesvalue_lead_46 -t timeseriesvalue_lead_47 -t timeseriesvalue_lead_48 -t timeseriesvalue_lead_49 $dump_file \
&& $restore_data_only_table_command -t timeseriesvalue_lead_50 -t timeseriesvalue_lead_51 -t timeseriesvalue_lead_52 -t timeseriesvalue_lead_53 -t timeseriesvalue_lead_54 -t timeseriesvalue_lead_55 -t timeseriesvalue_lead_56 -t timeseriesvalue_lead_57 -t timeseriesvalue_lead_58 -t timeseriesvalue_lead_59 $dump_file \
&& $restore_data_only_table_command -t timeseriesvalue_lead_60 -t timeseriesvalue_lead_61 -t timeseriesvalue_lead_62 -t timeseriesvalue_lead_63 -t timeseriesvalue_lead_64 -t timeseriesvalue_lead_65 -t timeseriesvalue_lead_66 -t timeseriesvalue_lead_67 -t timeseriesvalue_lead_68 -t timeseriesvalue_lead_69 $dump_file \
&& $restore_data_only_table_command -t timeseriesvalue_lead_70 -t timeseriesvalue_lead_71 -t timeseriesvalue_lead_72 -t timeseriesvalue_lead_73 -t timeseriesvalue_lead_74 -t timeseriesvalue_lead_75 -t timeseriesvalue_lead_76 -t timeseriesvalue_lead_77 -t timeseriesvalue_lead_78 -t timeseriesvalue_lead_79 $dump_file \
&& $restore_data_only_table_command -t timeseriesvalue_lead_80 -t timeseriesvalue_lead_81 -t timeseriesvalue_lead_82 -t timeseriesvalue_lead_83 -t timeseriesvalue_lead_84 -t timeseriesvalue_lead_85 -t timeseriesvalue_lead_86 -t timeseriesvalue_lead_87 -t timeseriesvalue_lead_88 -t timeseriesvalue_lead_89 $dump_file \
&& $restore_data_only_table_command -t timeseriesvalue_lead_90 -t timeseriesvalue_lead_91 -t timeseriesvalue_lead_92 -t timeseriesvalue_lead_93 -t timeseriesvalue_lead_94 -t timeseriesvalue_lead_95 -t timeseriesvalue_lead_96 -t timeseriesvalue_lead_97 -t timeseriesvalue_lead_98 -t timeseriesvalue_lead_99 $dump_file \
&& $restore_data_only_table_command -t timeseriesvalue_lead_100 -t timeseriesvalue_lead_101 -t timeseriesvalue_lead_102 -t timeseriesvalue_lead_103 -t timeseriesvalue_lead_104 -t timeseriesvalue_lead_105 -t timeseriesvalue_lead_106 -t timeseriesvalue_lead_107 -t timeseriesvalue_lead_108 -t timeseriesvalue_lead_109 $dump_file \
&& $restore_data_only_table_command -t timeseriesvalue_lead_110 -t timeseriesvalue_lead_111 -t timeseriesvalue_lead_112 -t timeseriesvalue_lead_113 -t timeseriesvalue_lead_114 -t timeseriesvalue_lead_115 -t timeseriesvalue_lead_116 -t timeseriesvalue_lead_117 -t timeseriesvalue_lead_118 -t timeseriesvalue_lead_119 $dump_file \
&& $restore_data_only_table_command -t timeseriesvalue_lead_120 -t timeseriesvalue_lead_121 -t timeseriesvalue_lead_122 -t timeseriesvalue_lead_123 -t timeseriesvalue_lead_124 -t timeseriesvalue_lead_125 -t timeseriesvalue_lead_126 -t timeseriesvalue_lead_127 -t timeseriesvalue_lead_128 -t timeseriesvalue_lead_129 $dump_file \
&& $restore_data_only_table_command -t timeseriesvalue_lead_130 -t timeseriesvalue_lead_131 -t timeseriesvalue_lead_132 -t timeseriesvalue_lead_133 -t timeseriesvalue_lead_134 -t timeseriesvalue_lead_135 -t timeseriesvalue_lead_136 -t timeseriesvalue_lead_137 -t timeseriesvalue_lead_138 -t timeseriesvalue_lead_139 $dump_file \
&& $restore_data_only_table_command -t timeseriesvalue_lead_140 -t timeseriesvalue_lead_141 -t timeseriesvalue_lead_142 -t timeseriesvalue_lead_143 -t timeseriesvalue_lead_144 -t timeseriesvalue_lead_145 -t timeseriesvalue_lead_146 -t timeseriesvalue_lead_147 -t timeseriesvalue_lead_148 -t timeseriesvalue_lead_149 -t timeseriesvalue_lead_150 -t timeseriesvalue_lead_above_150 $dump_file \
&& $restore_data_only_table_command -t timeseriesvalue_lead_negative_1 -t timeseriesvalue_lead_negative_2 -t timeseriesvalue_lead_negative_3 -t timeseriesvalue_lead_negative_4 -t timeseriesvalue_lead_negative_5 -t timeseriesvalue_lead_negative_6 -t timeseriesvalue_lead_negative_7 -t timeseriesvalue_lead_negative_8 -t timeseriesvalue_lead_negative_9 -t timeseriesvalue_lead_negative_10 -t timeseriesvalue_lead_below_negative_10 $dump_file \
&& $restore_data_only_table_command -t observation -t sourcecompleted -t forecasts -t project $dump_file \
&& $restore_data_only_table_command -t projectsource -t project_project_id_seq -t executionlog -t projectexecutions $dump_file \
&& $restore_data_only_table_command -t executionlog_log_id_seq $dump_file \
&& $restore_post_data_only_command $dump_file \

result=$?
set +x

end_seconds=$(date +%s)
date --iso-8601=ns

echo Restore took around $((end_seconds - start_seconds)) seconds.

if [ "$result" -eq "0" ]
then
    echo "1. Use psql to run \"ALTER SCHEMA wres OWNER TO wres\" on $database_name at $database_host."
    echo "2. Run WRES with migration turned on, e.g. \"-Dwres.attemptToMigrate=true -Dwres.url=$database_host -Dwres.databaseName=$database_name\". (You pick user)"
    echo "3. Use psql to run \"\\dtv wres.*\" on $database_name at $database_host."
    echo "4. Make ownership of the tables consistently the same user."
    echo "5. Runs of WRES with or without migration should now succeed on $database_name at $database_host."
else
    echo "RESTORE FAILED! Examine above and this script to figure out why."
    echo "RESTORE FAILED! This script intentionally does not auto-drop."
    echo "RESTORE FAILED! To start over with restore, use psql to run:"
    echo "drop schema wres cascade; drop table public.databasechangelog; drop table public.databasechangeloglock; create schema wres authorization wres;"
    exit $result
fi
