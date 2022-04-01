# Script to generate liquibase scripts that will create lead partitions and
# their indexes. Based on WRES schema and WRES code as of 2022-03, release 6.0.

dir=$( basename $( pwd ) )

if [[ "$dir" != "scripts" ]]
then
    echo "Script expected to be run from scripts directory or it won't work."
    exit 1
fi

partition_file=../dist/lib/conf/database/wres.TimeSeriesValue_generated_partitions_v6.xml
cat liquibase_partition_header.xml > $partition_file

traces_per_partition=1000000
#traces_per_partition=1000

# Initial migration will create partitions to partition number 1023:
for i in {0..1023}
do
    min=$(( $traces_per_partition * $i ))
    max=$(( $traces_per_partition * $i + traces_per_partition ))
    echo "Min: $min Max: $max";
    sed "s/NAMEHERE/$i/g" liquibase_partition_template.xml \
        | sed "s/HIGH_CHECK_HERE/$max/g" \
        | sed "s/LOW_CHECK_HERE/$min/g" \
        >> $partition_file
done

cat liquibase_partition_footer.xml >> $partition_file
