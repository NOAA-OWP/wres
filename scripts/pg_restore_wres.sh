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
changeloglock_dump_file=${dump_file_prefix}_changeloglock.pgdump

computed_j=$(($cpus - 2))
j=$computed_j

if [ $computed_j -gt $max_j ]
then
    j=$max_j
elif [ $computed_j -lt $min_j ]
then
    j=$min_j
fi

restore_pre_data_only_command="$pg_restore_command -e -j $j --no-owner -h ${database_host} -d ${database_name} -U ${database_username} --section=pre-data"
restore_data_only_table_command="$pg_restore_command -e -j $j --no-owner -h ${database_host} -d ${database_name} -U ${database_username} --data-only -t"
restore_table_command="$pg_restore_command -e -j $j --no-owner -h ${database_host} -d ${database_name} -U ${database_username} -t"
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


changeloglock_dump_file_exists="(does NOT exist!)"

if [ -f $changeloglock_dump_file ]
then
    changeloglock_dump_file_exists="(exists)"
fi

changeloglock_dump_file_readable="(is NOT readable!)"

if [ -r $changeloglock_dump_file ]
then
    changeloglock_dump_file_readable="(readable)"
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
echo "Restoring changeloglock_dump_file ${changeloglock_dump_file} ${changeloglock_dump_file_exists} ${changeloglock_dump_file_readable}"
echo "Using pg_restore executable ${pg_restore_command} ${pg_restore_command_exists} ${pg_restore_command_is_executable}"
echo "Using database host ${database_host} ${database_host_resolves}"
echo "Using database name ${database_name}"
echo "Using database username ${database_username}"
echo "Using restore_pre_data_only_command ${restore_pre_data_only_command}"
echo "Using restore_data_only_table_command ${restore_data_only_table_command}"
echo "Using restore_table_command ${restore_table_command}"
echo "Using restore_post_data_only_command ${restore_post_data_only_command}"

# Require one keystroke before doing it.
read -n1 -r -p "Please ctrl-c if that is not correct, any key otherwise..." key

date --iso-8601=ns
start_seconds=$(date +%s)

# It is nice to see the following commands printed when run.
set -x


$restore_pre_data_only_command $dump_file \
&& $restore_data_only_table_command measurementunit $dump_file \
&& $restore_data_only_table_command usgsparameter $dump_file \
&& $restore_data_only_table_command netcdfcoordinate $dump_file \
&& $restore_data_only_table_command unitconversion $dump_file \
&& $restore_data_only_table_command conversions $dump_file \
&& $restore_data_only_table_command indexqueue $dump_file \
&& $restore_data_only_table_command feature $dump_file \
&& $restore_data_only_table_command ensemble $dump_file \
&& $restore_data_only_table_command gridprojection $dump_file \
&& $restore_data_only_table_command variable $dump_file \
&& $restore_data_only_table_command variablefeature $dump_file \
&& $restore_data_only_table_command source $dump_file \
&& $restore_data_only_table_command timeseries $dump_file \
&& $restore_data_only_table_command timeseriessource $dump_file \
&& $restore_data_only_table_command timeseriesvalue $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_0 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_1 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_2 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_3 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_4 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_5 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_6 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_7 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_8 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_9 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_10 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_11 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_12 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_13 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_14 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_15 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_16 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_17 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_18 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_19 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_20 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_30 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_31 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_32 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_33 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_34 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_35 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_36 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_37 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_38 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_39 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_40 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_41 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_42 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_43 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_44 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_45 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_46 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_47 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_48 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_49 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_50 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_51 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_52 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_53 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_54 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_55 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_56 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_57 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_58 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_59 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_60 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_61 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_62 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_63 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_64 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_65 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_66 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_67 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_68 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_69 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_70 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_71 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_72 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_73 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_74 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_75 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_76 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_77 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_78 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_79 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_80 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_81 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_82 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_83 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_84 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_85 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_86 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_87 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_88 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_89 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_90 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_91 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_92 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_93 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_94 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_95 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_96 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_97 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_98 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_99 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_100 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_101 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_102 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_103 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_104 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_105 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_106 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_107 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_108 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_109 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_110 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_111 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_112 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_113 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_114 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_115 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_116 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_117 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_118 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_119 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_120 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_121 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_122 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_123 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_124 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_125 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_126 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_127 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_128 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_129 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_130 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_131 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_132 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_133 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_134 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_135 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_136 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_137 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_138 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_139 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_140 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_141 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_142 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_143 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_144 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_145 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_146 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_147 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_148 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_149 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_150 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_above_150 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_negative_1 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_negative_2 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_negative_3 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_negative_4 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_negative_5 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_negative_6 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_negative_7 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_negative_8 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_negative_9 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_negative_10 $dump_file \
&& $restore_data_only_table_command timeseriesvalue_lead_below_negative_10 $dump_file \
&& $restore_data_only_table_command observation $dump_file \
&& $restore_data_only_table_command sourcecompleted $dump_file \
&& $restore_data_only_table_command forecasts $dump_file \
&& $restore_data_only_table_command project $dump_file \
&& $restore_data_only_table_command projectsource $dump_file \
&& $restore_data_only_table_command executionlog $dump_file \
&& $restore_data_only_table_command projectexecutions $dump_file \
&& $restore_post_data_only_command $dump_file \
&& $restore_table_command databasechangelog $changelog_dump_file \
&& $restore_table_command databasechangeloglock $changeloglock_dump_file

set +x

end_seconds=$(date +%s)
date --iso-8601=ns

echo Restore took around $((end_seconds - start_seconds)) seconds
