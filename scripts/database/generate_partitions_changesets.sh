# Script to generate liquibase scripts that will create lead partitions. 
# Based on WRES schema and WRES code as of 2026, release 7.4.

dir=$( basename $( pwd ) )

if [[ "$dir" != "database" ]]
then
    echo "Script expected to be run from scripts/database directory or it won't work: $dir"
    exit 1
fi

partition_file=../../wres-io/dist/lib/conf/database/wres.TimeSeriesValue_generated_alter_partitions_v2.xml
cat liquibase_partition_header.xml > $partition_file

# Maximum value of the data type, currently a postgres 8-bit int, aka bigint
data_type_max=9223372036854775807
# Minimum value of the data type, currently a postgres 8-bit int, aka bigint
data_type_min=-9223372036854775808

# Positive lead durations every 12 hours out to 15 days: 30 partitions
group_1_start=0
group_1_increment=60*60*12
# The number of partitions containing positive lead durations
group_1_high_partition_count=30

echo "GROUP 1:" 
for((i=1; i <= group_1_high_partition_count; i++))
do
    min=$(( $group_1_start + ($group_1_increment * ($i-1)) ))
    max=$(( $min + $group_1_increment ))
    name="lead_p${min}s_to_p${max}s"
    echo "$name Min: $min Max: $max";
    sed "s/NAMEHERE/$name/g" liquibase_add_declarative_partition_template.xml \
        | sed "s/LOW_CHECK_HERE/$min/g" \
        | sed "s/HIGH_CHECK_HERE/$max/g" \
        >> $partition_file
done

# Positive lead durations every 24 hours out to 30 days: 15 partitions
group_2_start=15*24*60*60
group_2_increment=60*60*24
# The number of partitions containing positive lead durations
group_2_high_partition_count=15

echo "GROUP 2:" 
for((i=1; i <= group_2_high_partition_count; i++))
do
    min=$(( $group_2_start + ($group_2_increment * ($i-1)) ))
    max=$(( $min + $group_2_increment ))
    name="lead_p${min}s_to_p${max}s"
    echo "$name Min: $min Max: $max";
    sed "s/NAMEHERE/$name/g" liquibase_add_declarative_partition_template.xml \
        | sed "s/LOW_CHECK_HERE/$min/g" \
        | sed "s/HIGH_CHECK_HERE/$max/g" \
        >> $partition_file
done

# Positive lead durations every 120 hours out to 180 days: 30 partitions
group_3_start=30*24*60*60
group_3_increment=60*60*120
# The number of partitions containing positive lead durations
group_3_high_partition_count=30

echo "GROUP 3:" 
for((i=1; i <= group_3_high_partition_count; i++))
do
    min=$(( $group_3_start + ($group_3_increment * ($i-1)) ))
    max=$(( $min + $group_3_increment ))
    name="lead_p${min}s_to_p${max}s"
    echo "$name Min: $min Max: $max";
    sed "s/NAMEHERE/$name/g" liquibase_add_declarative_partition_template.xml \
        | sed "s/LOW_CHECK_HERE/$min/g" \
        | sed "s/HIGH_CHECK_HERE/$max/g" \
        >> $partition_file
done

# Positive lead durations every 192 hours out to 364 days: 23 partitions
group_4_start=180*24*60*60
group_4_increment=60*60*192
# The number of partitions containing positive lead durations
group_4_high_partition_count=23

echo "GROUP 4:" 
for((i=1; i <= group_4_high_partition_count; i++))
do
    min=$(( $group_4_start + ($group_4_increment * ($i-1)) ))
    max=$(( $min + $group_4_increment ))
    name="lead_p${min}s_to_p${max}s"
    echo "$name Min: $min Max: $max";
    sed "s/NAMEHERE/$name/g" liquibase_add_declarative_partition_template.xml \
        | sed "s/LOW_CHECK_HERE/$min/g" \
        | sed "s/HIGH_CHECK_HERE/$max/g" \
        >> $partition_file
done

# The catch-all partition above the highest positive partition
min=$((364*24*60*60))
max=$data_type_max
name="lead_p${min}s_to_max"
echo "HIGHEST:" 
echo "$name Min: $min Max: $max";
# The sed is different here on the high check: note the <= difference.
sed "s/NAMEHERE/$name/g" liquibase_add_declarative_partition_template.xml \
        | sed "s/LOW_CHECK_HERE/$min/g" \
        | sed "s/HIGH_CHECK_HERE/$max/g" \
    >> $partition_file

# Negative lead durations every 365 days out to 40 years: 40 partitions
group_5_start=0
group_5_increment=60*60*24*365
# The number of partitions containing positive lead durations
group_5_high_partition_count=40

echo "GROUP_5:" 
for((i=1; i <= group_5_high_partition_count; i++))
do
    max=$(( $group_5_start - $group_5_increment * ($i-1) ))
    min=$(( $max - $group_5_increment ))
    name="lead_n${min}s_to_n${max}s"
    name=$(tr -d '-' <<<"$name")
    echo "$name Min: $min Max: $max";
    sed "s/NAMEHERE/$name/g" liquibase_add_declarative_partition_template.xml \
        | sed "s/LOW_CHECK_HERE/$min/g" \
        | sed "s/HIGH_CHECK_HERE/$max/g" \
        >> $partition_file
done

# The catch-all partition below the lowest negative partition
min=$data_type_min
max=$((-60*60*24*365*40))
name="lead_min_to_n${max}s"
name=$(tr -d '-' <<<"$name")
echo "LOWEST:" 
echo "$name Min: $min Max: $max";
sed "s/NAMEHERE/$name/g" liquibase_add_declarative_partition_template.xml \
        | sed "s/LOW_CHECK_HERE/$min/g" \
        | sed "s/HIGH_CHECK_HERE/$max/g" \
    >> $partition_file

cat liquibase_partition_footer.xml >> $partition_file
