package wres.system;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helps manage (rotate) database partitions where supported by the dbms.
 *
 * WRES supports postgresql and has experimental support for H2, and likely will
 * work successfully on other database implementations, but partitions are only
 * supported with postgres as of this commit. The queries used for ingest and
 * retrieval of time series data do not directly reference partitions.
 * Partitions are initialized by liquibase migration and then rotated by this
 * class. The caller sets the partition count target with a minimum empty
 * partition count as well. If the partition count is lower than the total
 * existing partitions, existing partitions (low, high, or both) are dropped,
 * and if the partition count is higher than the total existing partitions, only
 * low partitions might be dropped along with high partitions added.
 */
public class DatabasePartitions
{
    private static final Logger LOGGER = LoggerFactory.getLogger( DatabasePartitions.class );
    private final SystemSettings systemSettings;

    /**
     * For a partition table, get the partition number from the name.
     * @param tableName A name like wres.TimeSeriesTraceValue_532
     * @return The partition number, like 532
     */

    private static long extractPartitionNumFromTableName( String tableName )
    {
        String startsWith = "wres.timeseriestracevalue_";

        if ( !tableName.startsWith( startsWith ) )
        {
            throw new IllegalArgumentException( "Expected a table that starts with "
                                                + startsWith + " but got "
                                                + tableName );
        }

        if ( tableName.length() <= startsWith.length()  )
        {
            throw new IllegalArgumentException( "Expected a table that is longer than"
                                                + startsWith.length()
                                                + " but was "
                                                + tableName.length() );
        }

        int index = tableName.indexOf( '_' );
        String partitionNumberRaw = tableName.substring( index + 1 );

        return Long.parseLong( partitionNumberRaw );
    }


    public DatabasePartitions( SystemSettings systemSettings )
    {
        if ( !systemSettings.getDatabaseType()
                            .equalsIgnoreCase( "postgresql" ) )
        {
            throw new UnsupportedOperationException( "Only 'postgresql' database type is supported" );
        }

        this.systemSettings = systemSettings;
    }


    /**
     * Rotate the wres.TimeSeriesTraceValue partitions: create new, drop old.
     *
     * Before anything else, check if the given count of partitions have data
     * present in them. If not, does nothing because rotation is not needed.
     * Otherwise it proceeds to rotate partitions.
     *
     * First, it finds the time series and associated projects (aka datasets)
     * in the old partitions and removes the wres.Project and wres.Source rows.
     * This ensures no execution can come along and accidentally try to use old
     * data that is no longer in the database. Once the wres.Project row is gone
     * sources associated are no longer able to be seen because each retrieval
     * query has a "where project_id = ?" clause. Once the wres.Source row is
     * gone that time series is no longer able to be seen. Ingest of a time
     * series is independent of wres.Project, it only looks at wres.Source, so
     * it is most important to remove these rows prior to dropping the TST or
     * TSTV data. After wres.Project and wres.Source rows are gone, the data in
     * wres.TimeSeriesTrace and wres.TimeSeriesTraceValue are invisible to any
     * ingest or retrieval queries and are safe to remove concurrently with an
     * execution of WRES software. Therefore it is only strictly necessary to
     * lock the database instance for the delete of wres.Project and wres.Source
     * rows. Outside of that, everything else should be safe to do online aka
     * concurrently with other executions running.
     *
     * Second, truncates the old wres.TimeSeriesTraceValue partitions. The
     * wres.TimeSeriesTrace rows will have been deleted already/above via
     * foreign key constraint (and cascade) relation to wres.Source above.
     *
     * Assumes the caller obtained an exclusive advisory lock on the database.
     *
     * The root or starting place from which the total and empty partitions are
     * based is either the partition containing the maximum time series trace id
     * or a current time series trace id found by calling nextval() on the
     * sequence that generates it. From that partition forward, at least the
     * count of minimumEmptyPartitionsRequested with idCountPerPartition will be kept
     * or added. After adding/keeping/re-building the empty partitions, then we
     * go back from that one partition that already exists and keep the
     * difference between totalPartitionsRequested and
     * minimumEmptyPartitionsRequested with no regard to the idCountPerPartition
     * (which may have been based on a past argument to a past rotatepartitions
     * operation).
     */

    public void rotateTstvPartitions( int totalPartitionsRequested,
                                      int minimumEmptyPartitionsRequested,
                                      int idCountPerPartition,
                                      boolean applyChanges )
    {
        RotatePartitionsPlan plan = this.getRotatePlan( totalPartitionsRequested,
                                                        minimumEmptyPartitionsRequested,
                                                        idCountPerPartition );

        // Check that no high partitions to be removed have data.
        this.verifyHighPartitionsHaveNoData( plan );
        Pair<Long,Long> sourceIdsToDelete = this.getRangeOfSourceIdsToDelete( plan );
        this.printPlanNetEffect( plan );

        if ( !applyChanges )
        {
            LOGGER.info( "This was a dry run, no changes were applied." );
            return;
        }

        // Because these are performed in separate transactions, the order can
        // matter. Projects should be removed before sources. Sources should
        // be removed before partitions are dropped. This way, the operation is
        // safe even if only a part of the queries go through and the rest fail.
        // The creation of high partitions will re-use names from just-dropped
        // high partitions, so dropping high partitions must precede create.
        LOGGER.info( "Removing wres.Project and wres.Source data..." );
        long projectsDeleted = this.deleteFromProjectTable( sourceIdsToDelete );
        long sourcesDeleted = this.deleteFromSourceTable( sourceIdsToDelete );
        LOGGER.info( "Applying the above partition rotation changes..." );
        this.dropPartitions( plan.getLowTablesToDrop() );
        this.dropPartitions( plan.getHighTablesToDrop() );
        this.createPartitions( plan.getHighTablesToCreate() );
        long partitionsDropped = plan.getLowTablesToDrop()
                                     .size()
                                 + plan.getHighTablesToDrop()
                                       .size();
        long partitionsCreated = plan.getHighTablesToCreate()
                                     .size();
        long rowsDeleted = projectsDeleted + sourcesDeleted;
        LOGGER.info( "Completed rotate partitions successfully. Dropped {} partitions, created {} partitions, deleted {} rows.",
                     partitionsDropped, partitionsCreated, rowsDeleted );
    }


    /**
     * Builds a plan to rotate partitions based on the existing data, schema,
     * and the given arguments. This queries the database and figures out the
     * details then returns an object having tables to drop and create.
     * @param totalPartitionsRequested How many partitions there will be.
     * @param minimumEmptyPartitionsRequested Minimum empty partitions to have.
     * @param idCountPerPartition The count of TST ids per new partition.
     * @return The plan, which includes tables to drop and create.
     * @throws IllegalArgumentException When unsafe/unworkable args are passed.
     * @throws DatabasePartitionRotateFailed When queries fail for any reason.
     */

    private RotatePartitionsPlan getRotatePlan( int totalPartitionsRequested,
                                                int minimumEmptyPartitionsRequested,
                                                int idCountPerPartition )
    {

        if ( idCountPerPartition < 1 )
        {
            throw new IllegalArgumentException( "There must be at least one timeseriestrace_id per partition, not "
                                                + idCountPerPartition );
        }

        if ( minimumEmptyPartitionsRequested >= totalPartitionsRequested )
        {
            throw new IllegalArgumentException( "One may not request all partitions to be empty. "
                                                + minimumEmptyPartitionsRequested
                                                + " empty requested is >= "
                                                + totalPartitionsRequested
                                                + " total requested. If you "
                                                + "want to remove all data, use"
                                                + " cleandatabase." );
        }

        if ( minimumEmptyPartitionsRequested < 1 )
        {
            throw new IllegalArgumentException( "One must request at least one empty partition, not "
                                                + minimumEmptyPartitionsRequested );
        }

        LOGGER.info( "Total partitions requested: {}, minimum empty partitions requested: {}, TST ID count per new partition: {}",
                     totalPartitionsRequested, minimumEmptyPartitionsRequested, idCountPerPartition );
        List<DatabasePartitionInfo> partitionsPresent = this.getExistingPartitions();

        long timeSeriesTraceIdOfInterest = this.getTimeSeriesTraceIdOfInterest();

        LOGGER.info( "Found this existing partition information:tstIdOfInterest={}, partitions={}",
                     timeSeriesTraceIdOfInterest, partitionsPresent.size() );

        DatabasePartitionInfo anchorPartition = null;
        SortedMap<Long,DatabasePartitionInfo> byLowerBound = new TreeMap<>();

        // Now look within the partitions present for that id of interest,
        // postgres lower bounds are inclusive while upper are exclusive.
        // At the same time, make a map of these partitions by lower bound.
        // If the id is at a boundary, this means the next inserted row would be
        // in the higher partition, not the present partition. This is OK, this
        // code is going to ensure at least one partition above the anchor
        // partition and keep the anchor partition. The caller is responsible
        // for supplying reasonable arguments with ample capacity for more data
        // to be inserted even if this code does not precisely identify the next
        // row id here.
        for ( DatabasePartitionInfo partitionInfo : partitionsPresent )
        {
            if ( partitionInfo.getLowerBound() <= timeSeriesTraceIdOfInterest
                 && partitionInfo.getUpperBound() > timeSeriesTraceIdOfInterest )
            {
                anchorPartition = partitionInfo;
                LOGGER.info( "Found this existing partition as the anchor for changes, above which are 'high' partitions: {}",
                             anchorPartition );
            }

            byLowerBound.put( partitionInfo.getLowerBound(), partitionInfo );
        }

        if ( anchorPartition == null )
        {
            throw new DatabasePartitionRotateFailed( "Could not find a partition with boundaries including "
                                                     + timeSeriesTraceIdOfInterest );
        }

        SortedMap<Long,DatabasePartitionInfo> lowPartitions =
                byLowerBound.headMap( anchorPartition.getLowerBound() );
        SortedMap<Long,DatabasePartitionInfo> highPartitions =
                byLowerBound.tailMap( anchorPartition.getUpperBound() );

        LOGGER.info( "Found that there exist {} additional low partitions below the 'anchor' partition and {} above.",
                     lowPartitions.size(), highPartitions.size() );

        if ( !lowPartitions.isEmpty() )
        {
            LOGGER.info( "Found these as among the existing low partitions: {}, {}",
                         lowPartitions.get( lowPartitions.firstKey() ),
                         lowPartitions.get( lowPartitions.lastKey() ) );
        }

        if ( !highPartitions.isEmpty() )
        {
            LOGGER.info( "Found these as among the existing high partitions: {}, {}",
                         highPartitions.get( highPartitions.firstKey() ),
                         highPartitions.get( highPartitions.lastKey() ) );
        }

        // For low partitions, the total requested minus the empty requested
        // yields the count to keep. The anchor partition is also kept.
        // For existing low partitions, there are two possibilities: drop
        // or do not drop (leave as-is).
        int maxCountLowToKeep =  totalPartitionsRequested - minimumEmptyPartitionsRequested;

        // Includes the anchor partition
        int lowPartitionsToKeepCount;

        if ( lowPartitions.size() + 1 > maxCountLowToKeep )
        {
            // Includes the anchor partition
            lowPartitionsToKeepCount = maxCountLowToKeep;
            LOGGER.info( "There exist more low partitions than {} (including anchor). {} existing low partitions will be dropped with {} kept.",
                         maxCountLowToKeep, lowPartitions.size() - lowPartitionsToKeepCount, lowPartitionsToKeepCount );
        }
        else
        {
            // Includes the anchor partition
            lowPartitionsToKeepCount = lowPartitions.size() + 1;
            LOGGER.info( "None of the {} existing low partitions (including anchor) will be dropped.",
                         lowPartitionsToKeepCount );
        }


        // For high partitions, it is a little more complex, because there may
        // only be a handful of low partitions, and the target partition count
        // overall is the exact number of total partitions requested while the
        // empty partition count is allowed to be one fewer or many more than
        // what was requested.
        // For high/empty partitions, there are three possibilities: drop,
        // create, or leave as-is. In the "drop" case, there is a possibility of
        // the TST sequence being set to a much higher id value than the
        // detected maximum TST id (due to deletes of traces with no new traces)
        // but again it is the callers responsibility to leave ample room for
        // this possibility with a high enough total partitions request and high
        // enough empty partitions request. If data are detected in one of these
        // partitions to be dropped, it will not be dropped and an exception
        // thrown. At that point the caller can decide what to do. One example
        // would be to run an evaluation with new data such that the max TST id
        // gets updated. Another possibility would be to increase the requests.
        // Another difference between the high/empty partitions and low/full
        // partitions is this rotate routine will not attempt to modify the low
        // partitions except to drop the lowest ones to meet the total partition
        // count request whereas for high/empty partitions, a new or different
        // count of TST ids per partition may be set for these new partitions.

        int highPartitionsToDropCount = 0;
        int highPartitionsToCreateCount = 0;

        if ( highPartitions.size() + lowPartitionsToKeepCount < totalPartitionsRequested )
        {
            highPartitionsToCreateCount = totalPartitionsRequested - highPartitions.size() - lowPartitionsToKeepCount;
            LOGGER.info( "There would be fewer than {} partitions after keeping {} existing low partitions and keeping {} existing high partitions. {} new empty high partitions each with a range of {} ids each will be created.",
                         totalPartitionsRequested, lowPartitionsToKeepCount, highPartitions.size(), highPartitionsToCreateCount, idCountPerPartition );
        }
        else if ( highPartitions.size() + lowPartitionsToKeepCount > totalPartitionsRequested )
        {
            highPartitionsToDropCount = highPartitions.size() - totalPartitionsRequested + lowPartitionsToKeepCount;
            LOGGER.info( "There would be more than {} partitions when keeping {} existing empty high partitions and {} low partitions. {} existing high partitions will be dropped.",
                         totalPartitionsRequested, highPartitions.size(), lowPartitionsToKeepCount, highPartitionsToDropCount );
        }
        else
        {
            LOGGER.info( "There will be exactly {} partitions and {} or more high partitions without changing existing empty high partitions. Leaving {} high partitions as-is.",
                         totalPartitionsRequested, minimumEmptyPartitionsRequested, highPartitions.size() );
        }

        List<DatabasePartitionInfo> lowPartitionsToDrop =
                lowPartitions.values()
                             .stream()
                             // lowPartitionsToKeepCount includes anchor but
                             // collection lowPartitions does not. Add 1.
                             .limit( lowPartitions.size() + 1 - lowPartitionsToKeepCount )
                             .collect( Collectors.toList() );
        LOGGER.info( "Planning to drop these existing low partitions: {}",
                     lowPartitionsToDrop );

        // For high partitions to drop, we drop the highest: do it in reverse.
        List<DatabasePartitionInfo> highPartitions2 = new ArrayList<>( highPartitions.values() );
        List<DatabasePartitionInfo> highPartitionsToDrop = new ArrayList<>( highPartitionsToDropCount );

        for ( int i = highPartitions2.size() - 1;
              i >= highPartitions2.size() - highPartitionsToDropCount;
              i-- )
        {
            DatabasePartitionInfo toDrop = highPartitions2.get( i );
            highPartitionsToDrop.add( toDrop );
        }

        LOGGER.info( "Planning to drop these existing high partitions: {}",
                     highPartitionsToDrop );

        // Finally, create the new empty high partitions.
        // For high partitions to create, base it on the last partition to drop
        // which is lowest high partition if everything above was done orderly.
        // If there are no high partitions to drop, start at the end of the
        long lowRangeValue;
        long highRangeValue;
        long tableNum;

        if ( !highPartitionsToDrop.isEmpty() )
        {
            // The new partition replaces a partial range of the dropped one.
            DatabasePartitionInfo lowestHighPartitionToDrop;
            lowestHighPartitionToDrop = highPartitionsToDrop.get( highPartitionsToDrop.size() - 1 );
            lowRangeValue = lowestHighPartitionToDrop.getLowerBound();
            String tableName = lowestHighPartitionToDrop.getTableName();
            tableNum = DatabasePartitions.extractPartitionNumFromTableName( tableName );
        }
        else
        {
            long existingTableNum;
            // The new partition adds to the range of the existing ones (or if
            // there is no existing high one, to the range of the anchor)
            DatabasePartitionInfo existingPartition;

            if ( !highPartitions.isEmpty() )
            {
                existingPartition = highPartitions.get( highPartitions.lastKey() );
            }
            else
            {
                // This situation is unlikely when using this utility, but with
                // changes to the rules or manual changes to partitions, it can
                // happen.
                existingPartition = anchorPartition;
            }

            lowRangeValue = existingPartition.getUpperBound();
            String existingTableName = existingPartition.getTableName();
            existingTableNum = DatabasePartitions.extractPartitionNumFromTableName( existingTableName );
            tableNum = existingTableNum + 1;
        }

        highRangeValue = lowRangeValue + idCountPerPartition;
        List<DatabasePartitionInfo> highPartitionsToCreate = new ArrayList<>( highPartitionsToCreateCount );

        for ( int i = 0; i < highPartitionsToCreateCount; i++ )
        {
            String newTableName = "wres.TimeSeriesTraceValue_" + tableNum;
            DatabasePartitionInfo toCreate =
                    new DatabasePartitionInfo( newTableName,
                                               lowRangeValue,
                                               highRangeValue );
            highPartitionsToCreate.add( toCreate );
            // Increment the three counters, one for table name, two for bounds.
            tableNum++;
            lowRangeValue = highRangeValue;
            highRangeValue = lowRangeValue + idCountPerPartition;
        }

        LOGGER.info( "Planning to create these new high partitions: {}",
                     highPartitionsToCreate );

        return new RotatePartitionsPlan( lowPartitionsToDrop,
                                         highPartitionsToDrop,
                                         highPartitionsToCreate );
    }

    /**
     * Gets a list of database partitions and their boundaries.
     * @return A list of database partitions and their boundaries.
     */
    private List<DatabasePartitionInfo> getExistingPartitions()
    {
        // I cobbled this together from the sql printed by doing
        //     \set ECHO_HIDDEN 'on'
        // then
        //     \dt+ wres.timeseriestracevalue
        String query = "SELECT c.oid::pg_catalog.regclass, pg_catalog.pg_get_expr(c.relpartbound, c.oid)\n"
                       + "FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i\n"
                       + "WHERE c.oid = i.inhrelid AND i.inhparent =\n"
                       + "(\n"
                       + "    SELECT c.oid\n"
                       + "    FROM pg_catalog.pg_class c\n"
                       + "    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n"
                       + "    WHERE c.relname OPERATOR(pg_catalog.~) '^(timeseriestracevalue)$' COLLATE pg_catalog.default\n"
                       + "    AND n.nspname OPERATOR(pg_catalog.~) '^(wres)$' COLLATE pg_catalog.default\n"
                       + ")\n"
                       + "ORDER BY pg_catalog.pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT', c.oid::pg_catalog.regclass::pg_catalog.text";

        try ( Connection connection = this.getSystemSettings()
                                          .getConnectionPool()
                                          .getConnection();
              Statement statement = connection.createStatement();
              ResultSet resultSet = statement.executeQuery( query ) )
        {
            List<DatabasePartitionInfo> partitions = new ArrayList<>( 1024 );

            while( resultSet.next() )
            {
                String partitionName = resultSet.getString( 1 );
                String partitionBoundExpression = resultSet.getString( 2 );
                DatabasePartitionInfo partition =
                        new DatabasePartitionInfo( partitionName,
                                                   partitionBoundExpression );
                partitions.add( partition );
            }

            return Collections.unmodifiableList( partitions );
        }
        catch ( SQLException se )
        {
            throw new DatabasePartitionRotateFailed( "Query to get "
                                                     + "existing partitions "
                                                     + "failed.", se );
        }
    }

    /**
     * Find a time series trace id that can help locate the current partition.
     *
     * The current partition will be the "anchor" partition, it is the partition
     * at or just below the partition where new trace values are expected to
     * be inserted/copied.
     *
     * SQL queries will be used to find the maximum existing trace id or to get
     * a fresh id directly from the sequence that drives such ids (in the case
     * where the table is empty).
     * @return A time series trace id to help locate the "anchor"
     */
    private long getTimeSeriesTraceIdOfInterest()
    {
        long maxTimeSeriesTraceId = this.getMaxTimeSeriesTraceId();

        // When there are data, use the existing maximum trace id, otherwise
        // (usually in the case when on a clean or empty database) advance/get
        // the next value of the sequence. This is our starting place.
        if ( maxTimeSeriesTraceId > 0 )
        {
            return maxTimeSeriesTraceId;
        }
        else
        {
            return this.getTheNextTimeSeriesTraceIdByAdvancingIt();
        }
    }

    /**
     *
     * @return The maximum existing trace id or -1 if no rows returned.
     */
    private long getMaxTimeSeriesTraceId()
    {
        String query = "select max( timeseriestrace_id )"
                       + " from wres.TimeSeriesTrace";

        try ( Connection connection = this.getSystemSettings()
                                          .getConnectionPool()
                                          .getConnection();
              Statement statement = connection.createStatement();
              ResultSet resultSet = statement.executeQuery( query ) )
        {
            if ( resultSet.next() )
            {
                return resultSet.getLong( 1 );
            }
            else
            {
                return -1;
            }
        }
        catch ( SQLException se )
        {
            throw new DatabasePartitionRotateFailed( "Query to get max"
                                                     + " timeseriestrace_id "
                                                     + "failed.", se );
        }
    }

    private long getTheNextTimeSeriesTraceIdByAdvancingIt()
    {
        String query = "select nextval( pg_get_serial_sequence( "
                       + "'wres.timeseriestrace', 'timeseriestrace_id' ) )";
        try ( Connection connection = this.getSystemSettings()
                                          .getConnectionPool()
                                          .getConnection();
              Statement statement = connection.createStatement();
              ResultSet resultSet = statement.executeQuery( query ) )
        {
            resultSet.next();
            return resultSet.getLong( 1 );
        }
        catch ( SQLException se )
        {
            throw new DatabasePartitionRotateFailed( "Query to get the "
                                                     + "next timeseriestrace id"
                                                     + " failed.",
                                                     se );
        }
    }


    /**
     * This is an extra safety/integrity check: make sure there are no data in
     * the high partitions that are about to be removed. If there are, then
     * something went wrong and the attempt to rotate should exit exceptionally.
     * @param plan The plan to rotate partitions.
     * @throws DatabasePartitionRotateFailed When anything goes wrong.
     */
    private void verifyHighPartitionsHaveNoData( RotatePartitionsPlan plan )
    {
        List<DatabasePartitionInfo> tables = plan.getHighTablesToDrop();

        if ( tables.isEmpty() )
        {
            // No problem: there are no high tables to drop.
            return;
        }

        long low = Long.MAX_VALUE;
        long high = 0;

        for ( DatabasePartitionInfo table : tables )
        {
            if ( table.getLowerBound() < low )
            {
                low = table.getLowerBound();
            }

            if ( table.getUpperBound() > high )
            {
                high = table.getUpperBound();
            }
        }

        if ( high < low )
        {
            throw new IllegalStateException( "Something is wrong with the high "
                                             + "partitions to drop or the "
                                             + "logic above. Didn't expect low "
                                             + "value " + low + " to be greater"
                                             + " than high value " + high
                                             + "." );
        }

        String sql = "select count(*) "
                     + "from wres.TimeSeriesTraceValue "
                     + "where timeseriestrace_id >= ? "
                     + "and timeseriestrace_id <= ?";
        try ( Connection connection = this.getSystemSettings()
                                          .getConnectionPool()
                                          .getConnection();
              PreparedStatement statement = connection.prepareStatement( sql ) )
        {
            statement.setLong( 1, low );
            statement.setLong( 2, high );

            try ( ResultSet resultSet = statement.executeQuery() )
            {
                resultSet.next();
                long count = resultSet.getLong( 1 );

                if ( count > 0 )
                {
                    throw new DatabasePartitionRotateFailed(
                            "There were " + count + " unexpected rows in high "
                            + "about to be dropped." );
                }
            }
        }
        catch ( SQLException se )
        {
            throw new DatabasePartitionRotateFailed( "Failed to check if high"
                                                     + " partitions had rows.",
                                                     se );
        }
    }

    /**
     * Given the plan to rotate partitions, figure out the range of source_id to
     * be removed. This is based on the low partitions to be removed. Queries
     * the database to find the source_id range based on the TST ids planned.
     * @param plan The plan to rotate partitions.
     * @return The low source_id on the left, high source_id on the right.
     */

    private Pair<Long,Long> getRangeOfSourceIdsToDelete( RotatePartitionsPlan plan )
    {
        List<DatabasePartitionInfo> lowPartitions = plan.getLowTablesToDrop();

        if ( lowPartitions.isEmpty() )
        {
            return Pair.of( 0L, 0L );
        }

        long lowTstId = Long.MAX_VALUE;
        long highTstId = 0;

        for ( DatabasePartitionInfo partition : lowPartitions )
        {
            if ( partition.getLowerBound() < lowTstId )
            {
                lowTstId = partition.getLowerBound();
            }

            if ( partition.getUpperBound() > highTstId )
            {
                highTstId = partition.getUpperBound();
            }
        }

        LOGGER.info( "Found the lowest to highest timeseriestrace_id {} to {}",
                     lowTstId, highTstId );

        if ( highTstId < lowTstId )
        {
            throw new IllegalStateException( "Something is wrong with the lower"
                                             + " and upper bounds of the "
                                             + "partitions sent or logic above "
                                             + "because high value " + highTstId
                                             + " should not be less than low "
                                             + "value " + lowTstId + " from "
                                             + lowPartitions );
        }

        long lowSourceId;
        long highSourceId;

        String sql = "select min( source_id ), max( source_id ) "
                     + "from wres.timeseriestrace "
                     + "where timeseriestrace_id >= ? "
                     // Partition ranges are exclusive on upper end, see
                     // https://www.postgresql.org/docs/13/ddl-partitioning.html
                     + " and timeseriestrace_id < ?";

        try ( Connection connection = this.getSystemSettings()
                                          .getConnectionPool()
                                          .getConnection();
              PreparedStatement statement = connection.prepareStatement( sql ) )
        {
            statement.setLong( 1, lowTstId );
            statement.setLong( 2, highTstId );

            try ( ResultSet results = statement.executeQuery() )
            {
                results.next();
                lowSourceId = results.getLong( 1 );
                highSourceId = results.getLong( 2 );
            }
        }
        catch ( SQLException se )
        {
            throw new DatabasePartitionRotateFailed( "Failed to get source_id "
                                                     + " data from "
                                                     + "wres.TimeSeriesTrace "
                                                     + "table.", se );
        }

        LOGGER.info( "Found lowest source_id={}, highest source_id={}",
                     lowSourceId, highSourceId );
        return Pair.of( lowSourceId, highSourceId );
    }

    /**
     * Prints message(s) about the net effect of this plan. Helps the caller see
     * whether the end goal is something like the intent.
     * @param plan The plan to describe.
     */
    private void printPlanNetEffect( RotatePartitionsPlan plan )
    {

        long maxExistingTracesDeleted = 0;

        for ( DatabasePartitionInfo table : plan.getLowTablesToDrop() )
        {
            long tracesToDelete = table.getUpperBound() - table.getLowerBound();
            maxExistingTracesDeleted += tracesToDelete;
        }

        long maxTracesRoomDropped = 0;

        for ( DatabasePartitionInfo table : plan.getHighTablesToDrop() )
        {
            long roomDropped = table.getUpperBound() - table.getLowerBound();
            maxTracesRoomDropped += roomDropped;
        }

        long maxTracesRoomCreated = 0;

        for ( DatabasePartitionInfo table : plan.getHighTablesToCreate() )
        {
            long roomCreated = table.getUpperBound() - table.getLowerBound();
            maxTracesRoomCreated += roomCreated;
        }

        LOGGER.info( "The given plan would delete up to {} existing traces, drop existing room for up to {} traces, and create room for up to {} traces.",
                     maxExistingTracesDeleted, maxTracesRoomDropped, maxTracesRoomCreated );

        long net = maxTracesRoomCreated - maxTracesRoomDropped - maxExistingTracesDeleted;

        LOGGER.info( "The net effect of this plan on overall trace capacity is {}.",
                     net );
    }

    /**
     * Delete rows from wres.Project that have any references to the data about
     * to be removed via removing the wres.TimeSeriesTraceValues partitions.
     * The relation is wres.ProjectSource -> wres.TimeSeriesTrace -> [partitions]
     * @param rangeOfSourceIds The range of source ids to use to find projects.
     * @return The total count of rows deleted.
     */

    private long deleteFromProjectTable( Pair<Long,Long> rangeOfSourceIds )
    {
        if ( rangeOfSourceIds.getLeft() == 0
             && rangeOfSourceIds.getRight() == 0 )
        {
            LOGGER.info( "Skipping delete of rows from wres.Project tables." );
            return 0;
        }
        String selectProjects = "select project_id from wres.ProjectSource "
                                + "where source_id >= ? "
                                + "and source_id <= ?";
        String deleteProjects = "delete from wres.Project where project_id in "
                                + "( "
                                + selectProjects
                                + " )";
        long rowCount;

        try ( Connection connection = this.getSystemSettings()
                                          .getConnectionPool()
                                          .getConnection();
              PreparedStatement statement = connection.prepareStatement( deleteProjects ) )
        {
            statement.setLong( 1, rangeOfSourceIds.getLeft() );
            statement.setLong( 2, rangeOfSourceIds.getRight() );
            rowCount = statement.executeUpdate();
        }
        catch ( SQLException se )
        {
            throw new DatabasePartitionRotateFailed( "Failed to delete from "
                                                     + "wres.Project for range "
                                                     + rangeOfSourceIds, se );
        }

        LOGGER.info( "Successfully deleted {} rows from wres.Project",
                     rowCount );
        return rowCount;
    }


    /**
     * Delete rows from wres.Source that have any references to the data about
     * to be removed via removing the wres.TimeSeriesTraceValues partitions.
     * The relation is wres.Source -> wres.TimeSeriesTrace -> [partitions]
     * @param rangeOfSourceIds The range of source ids to delete.
     * @return The total count of wres.Source rows deleted.
     */

    private long deleteFromSourceTable( Pair<Long,Long> rangeOfSourceIds  )
    {
        if ( rangeOfSourceIds.getLeft() == 0
             && rangeOfSourceIds.getRight() == 0 )
        {
            LOGGER.info( "Skipping delete of rows from wres.Source tables." );
            return 0;
        }

        String deleteSources = "delete from wres.Source "
                               + "where source_id >= ? "
                               + "and source_id <= ?";
        long rowCount;
        try ( Connection connection = this.getSystemSettings()
                                          .getConnectionPool()
                                          .getConnection();
              PreparedStatement statement = connection.prepareStatement( deleteSources ) )
        {
            statement.setLong( 1, rangeOfSourceIds.getLeft() );
            statement.setLong( 2, rangeOfSourceIds.getRight() );
            rowCount = statement.executeUpdate();
        }
        catch ( SQLException se )
        {
            throw new DatabasePartitionRotateFailed( "Failed to delete from "
                                                     + "wres.Source for range "
                                                     + rangeOfSourceIds, se );
        }

        LOGGER.info( "Successfully deleted {} rows from wres.Source",
                     rowCount );
        return rowCount;
    }


    private void dropPartitions( List<DatabasePartitionInfo> partitionsToDrop )
    {
        try ( Connection connection = this.getSystemSettings()
                                          .getConnectionPool()
                                          .getConnection() )
        {
            for ( DatabasePartitionInfo partition : partitionsToDrop )
            {
                try ( Statement statement = connection.createStatement() )
                {
                    String query = "drop table " + partition.getTableName();
                    statement.execute( query );
                }
            }
        }
        catch ( SQLException se )
        {
            throw new DatabasePartitionRotateFailed( "Query to drop "
                                                     + "a partition failed.",
                                                     se );
        }
    }

    private void createPartitions( List<DatabasePartitionInfo> partitionsToCreate )
    {
        try ( Connection connection = this.getSystemSettings()
                                          .getConnectionPool()
                                          .getConnection() )
        {
            for ( DatabasePartitionInfo partition : partitionsToCreate )
            {
                try ( Statement statement = connection.createStatement() )
                {
                    String query = "create table " + partition.getTableName()
                                   + " partition of wres.TimeSeriesTraceValue "
                                   + "for values from ( "
                                   + partition.getLowerBound()
                                   + " ) to ( "
                                   + partition.getUpperBound()
                                   + " )";
                    statement.execute( query );
                }
            }
        }
        catch ( SQLException se )
        {
            throw new DatabasePartitionRotateFailed( "Query to create "
                                                     + "a partition failed.",
                                                     se );
        }
    }

    private SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }

    private static final class DatabasePartitionRotateFailed extends RuntimeException
    {
        DatabasePartitionRotateFailed( String message, Throwable cause )
        {
            super( message, cause );
        }

        DatabasePartitionRotateFailed( String message )
        {
            super( message );
        }
    }

    /**
     * Holds the partition/table name as well as lower and upper bounds of ids.
     */

    private static final class DatabasePartitionInfo
    {
        private static final char QUOTE = '\'';
        private final String tableName;
        private final long lowerBound;
        private final long upperBound;

        /**
         * Get the table name and long integer boundaries from a postgresql
         * bounds expression such as "FOR VALUES FROM ('111000') TO ('112000')"
         *
         * This bounds expression is what comes from the second column in the
         * method getExistingPartitions() above.
         * @param tableName The table name, including the schema.
         * @param boundsExpression The postgresql partition bounds expression.
         */
        DatabasePartitionInfo( String tableName, String boundsExpression )
        {
            this.tableName = tableName;
            int firstQuote = boundsExpression.indexOf( QUOTE, 0 );
            int secondQuote = boundsExpression.indexOf( QUOTE, firstQuote + 1 );
            int thirdQuote = boundsExpression.indexOf( QUOTE, secondQuote + 1 );
            int fourthQuote = boundsExpression.indexOf( QUOTE, thirdQuote + 1 );
            String lowerBoundRaw = boundsExpression.substring( firstQuote+1, secondQuote );
            this.lowerBound = Long.parseLong( lowerBoundRaw );
            String upperBoundRaw = boundsExpression.substring( thirdQuote+1, fourthQuote );
            this.upperBound = Long.parseLong( upperBoundRaw );
        }

        /**
         * Straightforward constructor for info of newly generated tables.
         * @param tableName The table name including schema.
         * @param lowerBound The lower bound for the partition range.
         * @param upperBound The rupper bound for the partition range.
         */
        DatabasePartitionInfo( String tableName, long lowerBound, long upperBound )
        {
            this.tableName = tableName;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        String getTableName()
        {
            return this.tableName;
        }

        long getLowerBound()
        {
            return this.lowerBound;
        }

        long getUpperBound()
        {
            return this.upperBound;
        }

        @Override
        public String toString()
        {
            return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                    .append( "tableName", tableName )
                    .append( "lowerBound", lowerBound )
                    .append( "upperBound", upperBound )
                    .toString();
        }
    }

    /**
     * Contains lists of tables to drop and create.
     */

    private static class RotatePartitionsPlan
    {
        private final List<DatabasePartitionInfo> lowTablesToDrop;
        private final List<DatabasePartitionInfo> highTablesToDrop;
        private final List<DatabasePartitionInfo> highTablesToCreate;

        RotatePartitionsPlan( List<DatabasePartitionInfo> lowTablesToDrop,
                              List<DatabasePartitionInfo> highTablesToDrop,
                              List<DatabasePartitionInfo> highTablesToCreate )
        {
            this.lowTablesToDrop = Collections.unmodifiableList( lowTablesToDrop );
            this.highTablesToDrop = Collections.unmodifiableList( highTablesToDrop );
            this.highTablesToCreate = Collections.unmodifiableList( highTablesToCreate );
        }

        List<DatabasePartitionInfo> getLowTablesToDrop()
        {
            return this.lowTablesToDrop;
        }

        List<DatabasePartitionInfo> getHighTablesToDrop()
        {
            return this.highTablesToDrop;
        }

        List<DatabasePartitionInfo> getHighTablesToCreate()
        {
            return this.highTablesToCreate;
        }
    }
}
