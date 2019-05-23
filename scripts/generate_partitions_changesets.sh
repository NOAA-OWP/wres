# Script to generate liquibase scripts that will create lead partitions and
# their indexes. Based on WRES schema and WRES code as of 2019-05, release 1.8.

dir=$( basename $( pwd ) )

if [[ "$dir" != "scripts" ]]
then
    echo "Script expected to be run from scripts directory or it won't work."
    exit 1
fi

partition_file=../dist/lib/conf/database/wres.TimeSeriesValue_generated_partitions_v1.xml
index_file=../dist/lib/conf/database/wres.TimeSeriesValue_generated_partitions_v2.xml
cat liquibase_partition_header.xml > $partition_file
cat liquibase_partition_index_header.xml > $index_file

# Positive leads out to partition number 150:
for i in {1..150}
do
    min=$(( 1200 * $i ))
    max=$(( 1200 * $i + 1200 ))
    echo "Min: $min Max: $max";
    sed "s/NAMEHERE/lead_$i/g" liquibase_partition_template.xml \
        | sed "s/HIGH_CHECK_HERE/lead \&lt; $max/g" \
        | sed "s/LOW_CHECK_HERE/lead \&gt;= $min/g" \
        >> $partition_file
    sed "s/NAMEHERE/lead_$i/g" liquibase_partition_index_template.xml \
        >> $index_file
done

# Negative leads out to partition number negative 10:
for i in {1..10}
do
    max=$(( -1200 * $i ))
    min=$(( -1200 * $i - 1200 ))
    echo "Min: $min Max: $max";
    sed "s/NAMEHERE/lead_Negative_$i/g" liquibase_partition_template.xml \
        | sed "s/HIGH_CHECK_HERE/lead \&lt;= $max/g" \
        | sed "s/LOW_CHECK_HERE/lead \&gt; $min/g" \
        >> $partition_file
    sed "s/NAMEHERE/lead_Negative_$i/g" liquibase_partition_index_template.xml \
        >> $index_file
done

# The middle one:
i=0
min=-1200
max=1200
echo "Min: $min Max: $max";
# Note that the middle partition is exclusive > and exclusive <
sed "s/NAMEHERE/lead_$i/g" liquibase_partition_template.xml \
    | sed "s/HIGH_CHECK_HERE/lead \&lt; $max/g" \
    | sed "s/LOW_CHECK_HERE/lead \&gt; $min/g" \
    >> $partition_file
sed "s/NAMEHERE/lead_$i/g" liquibase_partition_index_template.xml \
    >> $index_file

# The catch-all partition below the lowest negative partition
min=-2147483648
max=-13200
sed "s/NAMEHERE/lead_Below_Negative_10/g" liquibase_partition_template.xml \
    | sed "s/HIGH_CHECK_HERE/lead \&lt;= $max/g" \
    | sed "s/LOW_CHECK_HERE/lead \&gt;= $min/g" \
    >> $partition_file
sed "s/NAMEHERE/lead_Below_Negative_10/g" liquibase_partition_index_template.xml \
    >> $index_file

# The catch-all partition above the highest positive partition
min=181200
max=2147483647
# The sed is different here on the high check: note the <= difference.
sed "s/NAMEHERE/lead_Above_150/g" liquibase_partition_template.xml \
    | sed "s/HIGH_CHECK_HERE/lead \&lt;= $max/g" \
    | sed "s/LOW_CHECK_HERE/lead \&gt;= $min/g" \
    >> $partition_file
sed "s/NAMEHERE/lead_Above_150/g" liquibase_partition_index_template.xml \
    >> $index_file

cat liquibase_partition_footer.xml >> $partition_file
cat liquibase_partition_index_footer.xml >> $index_file
