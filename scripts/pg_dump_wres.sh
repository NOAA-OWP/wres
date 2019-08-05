dump_file_prefix='wresbackup'
database_host='fake.fqdn'
database_name='wres'
database_username='wres_user'
pg_dump_command=$(which pg_dump)


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
            pg_dump_command=$OPTARG
            ;;
        \?)
            echo "Usage: $0 -f dump_file_prefix -h database_host -d database_name -U database_username [ -p pg_dump_command ]"
            exit 2
            ;;
    esac
done

dump_file=${dump_file_prefix}.pgdump
changelog_dump_file=${dump_file_prefix}_changelog.pgdump


dump_file_exists="(does not yet exist)"

if [ -f $dump_file ]
then
    dump_file_exists="(ALREADY EXISTS!)"
fi


changelog_dump_file_exists="(does not yet exist)"

if [ -f $changelog_dump_file ]
then
    changelog_dump_file_exists="(ALREADY EXISTS!)"
fi


pg_dump_command_exists="(does NOT exist!)"

if [ -f $pg_dump_command ]
then
    pg_dump_command_exists="(exists)"
fi

pg_dump_command_is_executable="(is NOT executable!)"

if [ -x $pg_dump_command ]
then
    pg_dump_command_is_executable="(executable)"
fi

database_host_resolves="(does NOT resolve!)"

if [ ! -z "$database_host" ]
then
    nslookup ${database_host} >/dev/null 2>&1
    resolved=$?
fi

if [ "$resolved" == "0" ]
then
    database_host_resolves="(resolves)"
fi

echo "Creating dump_file ${dump_file} ${dump_file_exists} ${dump_file_readable}"
echo "Creating changelog_dump_file ${changelog_dump_file} ${changelog_dump_file_exists} ${changelog_dump_file_readable}"
echo "Using pg_dump executable ${pg_dump_command} ${pg_dump_command_exists} ${pg_dump_command_is_executable}"
echo "Using database host ${database_host} ${database_host_resolves}"
echo "Using database name ${database_name}"
echo "Using database username ${database_username}"

# Require one keystroke before doing it.
read -n1 -r -p "Please ctrl-c if that is not correct, any key otherwise..." key

date --iso-8601=ns
start_seconds=$(date +%s)

# It is nice to see the following commands printed when run.
set -x

$pg_dump_command -h ${database_host} -d ${database_name} -Fc -U ${database_username} -n wres > ${dump_file}
$pg_dump_command -h ${database_host} -d ${database_name} -Fc -U ${database_username} -n public --table databasechangelog --table databasechangeloglock > ${changelog_dump_file}

set +x

end_seconds=$(date +%s)
date --iso-8601=ns

echo Dump took around $((end_seconds - start_seconds)) seconds
ls -lah ${dump_file_prefix}*.pgdump
