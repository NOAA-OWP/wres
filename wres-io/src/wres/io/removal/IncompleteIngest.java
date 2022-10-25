package wres.io.removal;

import java.sql.SQLException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.data.caching.DataSources;
import wres.io.data.details.SourceCompletedDetails;
import wres.io.data.details.SourceDetails;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;

/**
 * Deals with partial/orphaned/incomplete ingested data, both detection and
 * removal.
 */

public class IncompleteIngest
{
    private static final String WHERE_SOURCE_ID = "WHERE source_id = ?";

    private static final String WHERE_NOT_EXISTS = "WHERE NOT EXISTS (";

    private static final String SELECT_1 = "SELECT 1";

    private static final String FROM_WRES_SOURCE_S = "FROM wres.Source S";

    private static final String FROM_WRES_PROJECT_SOURCE_PS = "FROM wres.ProjectSource PS";

    private static final Logger LOGGER = LoggerFactory.getLogger( IncompleteIngest.class );

    private static final String DB_COMMUNICATION_FAILED =
            "Communication with the database failed.";


    public static boolean removeSourceDataSafely( Database database,
                                                  DataSources dataSourcesCache,
                                                  long surrogateKey,
                                                  DatabaseLockManager lockManager )
    {
        Objects.requireNonNull( lockManager );

        try
        {
            SourceDetails sourceDetails = dataSourcesCache.getSource( surrogateKey );

            if ( sourceDetails == null )
            {
                // This means a source has been removed by some Thread after the
                // call to this method but prior to getSource.
                LOGGER.warn( "Another task removed source {}, not removing.",
                             surrogateKey );
                return false;
            }
            return IncompleteIngest.removeSourceDataSafely( database,
                                                            dataSourcesCache,
                                                            sourceDetails,
                                                            lockManager );
        }
        catch ( SQLException se )
        {
            throw new IllegalStateException( DB_COMMUNICATION_FAILED, se );
        }
    }

    private static boolean removeSourceDataSafely( Database database,
                                                   DataSources dataSourcesCache,
                                                   SourceDetails source,
                                                   DatabaseLockManager lockManager )
            throws SQLException
    {
        Objects.requireNonNull( source );
        Objects.requireNonNull( source.getId() );
        Objects.requireNonNull( lockManager );

        boolean wasIngested = IncompleteIngest.wasCompletelyIngested( database,
                                                                      source );

        if ( wasIngested )
        {
            LOGGER.warn( "Source {} was fully ingested, will not remove.",
                         source );
            return false;
        }

        boolean isBeingIngested = IncompleteIngest.isBeingIngested( source,
                                                                    lockManager );
        if ( isBeingIngested )
        {
            LOGGER.warn( "Source {} is being actively ingested, will not remove.",
                         source );
            return false;
        }

        long sourceId = source.getId();

        // Simple, but slow when the partitions are not by timeseries_id:
        DataScripter timeSeriesValueScript = new DataScripter( database );
        timeSeriesValueScript.addLine( "DELETE FROM wres.TimeSeriesValue" );
        timeSeriesValueScript.addLine( "WHERE timeseries_id IN" );
        timeSeriesValueScript.addLine( "(" );
        timeSeriesValueScript.addTab().addLine( "SELECT timeseries_id" );
        timeSeriesValueScript.addTab().addLine( "FROM wres.TimeSeries" );
        timeSeriesValueScript.addTab().addLine( WHERE_SOURCE_ID );
        timeSeriesValueScript.addArgument( sourceId );
        timeSeriesValueScript.addLine( ")" );

        DataScripter referenceTimeScript = new DataScripter( database );
        referenceTimeScript.addLine( "DELETE FROM wres.TimeSeriesReferenceTime" );
        referenceTimeScript.addLine( WHERE_SOURCE_ID );
        referenceTimeScript.addArgument( sourceId );

        DataScripter timeSeriesScript = new DataScripter( database );
        timeSeriesScript.addLine( "DELETE FROM wres.TimeSeries" );
        timeSeriesScript.addLine( "WHERE timeseries_id IN" );
        timeSeriesScript.addLine( "(" );
        timeSeriesScript.addTab().addLine( "SELECT timeseries_id" );
        timeSeriesScript.addTab().addLine( "FROM wres.TimeSeries" );
        timeSeriesScript.addTab().addLine( WHERE_SOURCE_ID );
        timeSeriesScript.addArgument( sourceId );
        timeSeriesScript.addLine( ")" );

        DataScripter sourceScript = new DataScripter( database );
        sourceScript.addLine( "DELETE from wres.Source" );
        sourceScript.addLine( WHERE_SOURCE_ID );
        sourceScript.addArgument( sourceId );

        try
        {
            lockManager.lockSource( sourceId );
            int timeSeriesValuesRemoved = timeSeriesValueScript.execute();
            int referenceTimesRemoved = referenceTimeScript.execute();
            int timeSeriesRemoved = timeSeriesScript.execute();
            int sourcesRemoved = sourceScript.execute();
            LOGGER.debug( "Removed {} tsv, {} tsrt, {} ts, {} s.",
                          timeSeriesValuesRemoved,
                          referenceTimesRemoved,
                          timeSeriesRemoved,
                          sourcesRemoved );

            if ( sourcesRemoved != 1 )
            {
                LOGGER.warn( "Removed {} sources when 1 was expected.",
                             sourcesRemoved );
            }
        }
        finally
        {
            lockManager.unlockSource( sourceId );
        }

        // Invalidate caches affected by deletes above
        dataSourcesCache.invalidate( source );

        return true;
    }

    /**
     * Given a source, return true if it is currently being ingested. TODO: consider replacing with 
     * {@link DatabaseLockManager#isSourceLocked(Long)}, which already checks twice, as well as the internal cache of
     * locks.
     * @param source The source to look for, non-null and with non-null ID.
     * @param lockManager The lock manager to use.
     * @return true if the source is being actively ingested, false otherwise.
     * @throws IllegalStateException When database communication fails.
     */

    private static boolean isBeingIngested( SourceDetails source,
                                            DatabaseLockManager lockManager )
    {
        Objects.requireNonNull( source );
        Objects.requireNonNull( source.getId() );
        Long sourceId = source.getId();

        // Check twice to be more confident that no other process is currently
        // ingesting this source when the first check returns false.
        boolean isLockedCheckOne;
        boolean isLockedCheckTwo = false;

        try
        {
            isLockedCheckOne = lockManager.isSourceLocked( sourceId );

            if ( !isLockedCheckOne )
            {
                isLockedCheckTwo = lockManager.isSourceLocked( sourceId );
            }
        }
        catch ( SQLException se )
        {
            throw new IllegalStateException( DB_COMMUNICATION_FAILED, se );
        }

        return isLockedCheckOne || isLockedCheckTwo;
    }


    /**
     * Given a source, return true if it has been completely ingested.
     * @param source The source in question.
     * @return true when the source has been completely ingested, false otherwise.
     * @throws IllegalStateException When communication with the database fails.
     */

    private static boolean wasCompletelyIngested( Database database,
                                                  SourceDetails source )
    {
        SourceCompletedDetails completedDetails = new SourceCompletedDetails( database,
                                                                              source );

        try
        {
            return completedDetails.wasCompleted();
        }
        catch ( SQLException se )
        {
            throw new IllegalStateException( DB_COMMUNICATION_FAILED, se );
        }
    }

    /**
     * Checks to see if the database contains orphaned data
     * @return True if orphaned data exists within the database; false otherwise
     * @throws SQLException Thrown if the query used to detect orphaned data failed
     */
    private static boolean thereAreOrphanedValues( Database database ) throws SQLException
    {
        DataScripter scriptBuilder = new DataScripter( database );

        scriptBuilder.addLine( "SELECT EXISTS (" );
        scriptBuilder.addTab().addLine( SELECT_1 );
        scriptBuilder.addTab().addLine( FROM_WRES_SOURCE_S );
        scriptBuilder.addTab().addLine( WHERE_NOT_EXISTS );
        scriptBuilder.addTab( 2 ).addLine( SELECT_1 );
        scriptBuilder.addTab( 2 ).addLine( FROM_WRES_PROJECT_SOURCE_PS );
        scriptBuilder.addTab( 2 ).addLine( "WHERE PS.source_id = S.source_id" );
        scriptBuilder.addTab().addLine( ")" );
        scriptBuilder.addLine( ") AS orphans_exist;" );

        boolean thereAreOrphans;

        try ( DataProvider dataProvider = scriptBuilder.getData() )
        {
            thereAreOrphans = dataProvider.getBoolean( "orphans_exist" );
        }

        return thereAreOrphans;
    }


    /**
     * Removes all data from the database that isn't properly linked to a project
     * Assumes that the caller (or caller of caller) holds an exclusive lock on
     * the database instance.
     * @param database The database to use.
     * @return Whether or not values were removed
     * @throws SQLException Thrown when one of the required scripts could not complete
     */

    public static boolean removeOrphanedData( Database database ) throws SQLException
    {
        try
        {
            if ( !IncompleteIngest.thereAreOrphanedValues( database ) )
            {
                return false;
            }

            // Can't lock for mutation here because we'd also need to unlock, but this
            // operation will occur alongside other operations that need that lock.

            LOGGER.info( "Incomplete data has been detected. Incomplete data "
                         + "will now be removed to ensure that all data operated "
                         + "upon is valid." );

            Set<String> partitionTables = database.getPartitionTables();

            // We aren't actually going to collect the results so raw types are fine.
            FutureQueue removalQueue = new FutureQueue();

            for ( String partition : partitionTables )
            {
                DataScripter valueRemover = new DataScripter( database );
                valueRemover.setHighPriority( false );
                valueRemover.addLine( "DELETE FROM ", partition, " P" );
                valueRemover.addLine( WHERE_NOT_EXISTS );
                valueRemover.addTab().addLine( SELECT_1 );
                valueRemover.addTab().addLine( "FROM wres.TimeSeries TS" );
                valueRemover.addTab().addLine( "INNER JOIN wres.ProjectSource PS" );
                valueRemover.addTab( 2 ).addLine( "ON PS.source_id = TS.source_id" );
                valueRemover.addTab().addLine( "WHERE TS.timeseries_id = P.timeseries_id" );
                valueRemover.addLine( ");" );

                Future<?> timeSeriesValueRemoval = valueRemover.issue();
                removalQueue.add( timeSeriesValueRemoval );

                LOGGER.debug( "Started task to remove orphaned values in {}...", partition );
            }

            IncompleteIngest.loop( removalQueue,
                                   "Orphaned observed and forecasted values could not be removed." );

            DataScripter removeTimeSeries = new DataScripter( database );

            removeTimeSeries.addLine( "DELETE FROM wres.TimeSeries TS" );
            removeTimeSeries.addLine( WHERE_NOT_EXISTS );
            removeTimeSeries.addTab().addLine( SELECT_1 );
            removeTimeSeries.addTab().addLine( FROM_WRES_SOURCE_S );
            removeTimeSeries.addTab().addLine( "WHERE S.source_id = TS.source_id" );
            removeTimeSeries.add( ");" );

            Future<?> timeSeriesRemoval = removeTimeSeries.issue();
            removalQueue.add( timeSeriesRemoval );

            LOGGER.debug( "Added Task to remove orphaned time series..." );

            DataScripter removeReferenceTimes = new DataScripter( database );

            removeReferenceTimes.addLine( "DELETE FROM wres.TimeSeriesReferenceTime TSRT" );
            removeReferenceTimes.addLine( WHERE_NOT_EXISTS );
            removeReferenceTimes.addTab().addLine( SELECT_1 );
            removeReferenceTimes.addTab().addLine( FROM_WRES_SOURCE_S );
            removeReferenceTimes.addTab().addLine( "WHERE TSRT.source_id = S.source_id" );
            removeReferenceTimes.add( ");" );

            Future<?> referenceTimesRemoval = removeReferenceTimes.issue();
            removalQueue.add( referenceTimesRemoval );

            LOGGER.debug( "Added Task to remove orphaned reference times..." );

            DataScripter removeSources = new DataScripter( database );

            removeSources.addLine( "DELETE FROM wres.Source S" );
            removeSources.addLine( WHERE_NOT_EXISTS );
            removeSources.addTab().addLine( SELECT_1 );
            removeSources.addTab().addLine( FROM_WRES_PROJECT_SOURCE_PS );
            removeSources.addTab().addLine( "WHERE PS.source_id = S.source_id" );
            removeSources.add( ");" );

            Future<?> sourcesRemoval = removeSources.issue();
            removalQueue.add( sourcesRemoval );

            LOGGER.debug( "Added task to remove orphaned sources..." );

            DataScripter removeProjects = new DataScripter( database );

            removeProjects.addLine( "DELETE FROM wres.Project P" );
            removeProjects.addLine( WHERE_NOT_EXISTS );
            removeProjects.addTab().addLine( SELECT_1 );
            removeProjects.addTab().addLine( FROM_WRES_PROJECT_SOURCE_PS );
            removeProjects.addTab().addLine( "WHERE PS.project_id = P.project_id" );
            removeProjects.add( ");" );

            LOGGER.debug( "Added task to remove orphaned projects..." );

            Future<?> projectsRemoval = removeProjects.issue();
            removalQueue.add( projectsRemoval );

            IncompleteIngest.loop( removalQueue,
                                   "Orphaned forecast, project, and source metadata could not be removed." );

            LOGGER.info( "Incomplete data has been removed from the system." );
        }
        catch ( SQLException | ExecutionException databaseError )
        {
            throw new SQLException( "Orphaned data could not be removed", databaseError );
        }

        return true;
    }

    /**
     * Loops the tasks in the input queue.
     * @param removalQueue the queue to loop
     * @param errorMessage an error message to use when the looping fails
     * @throws SQLException if the looping fails
     */

    private static void loop( FutureQueue removalQueue, String errorMessage ) throws SQLException
    {
        try
        {
            removalQueue.loop();
        }
        catch ( ExecutionException e )
        {
            throw new SQLException( errorMessage, e );
        }
    }

    /**
     * Do not construct.
     */

    private IncompleteIngest()
    {
    }

}
