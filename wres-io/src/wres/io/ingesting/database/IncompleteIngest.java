package wres.io.ingesting.database;

import java.sql.SQLException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.data.DataProvider;
import wres.io.database.caching.DataSources;
import wres.io.database.details.SourceCompletedDetails;
import wres.io.database.details.SourceDetails;
import wres.io.database.DataScripter;
import wres.io.database.Database;

/**
 * <p>Deals with partial/orphaned/incomplete ingested data, both detection and removal. Locking semantics should be
 * handled by the caller.
 * 
 * <p>TODO: consider adding a guard to the script that remove incomplete data to select only those rows where the
 * source is marked complete because completed sources are, by definition, not within the scope of incomplete ingest
 * and no guards in code are as good as a guard within the select that chooses rows to delete.
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

    /**
     * Attempts to remove a data source from the database. The caller must handle locking semantics.
     * @param database the database
     * @param dataSourcesCache the data source cache
     * @param surrogateKey the surrogate key of the source to remove
     * @return true if the source was removed, otherwise false
     */

    public static boolean removeDataSource( Database database,
                                            DataSources dataSourcesCache,
                                            long surrogateKey )
    {
        try
        {
            SourceDetails sourceDetails = dataSourcesCache.getSource( surrogateKey );

            if ( sourceDetails == null )
            {
                // This means a source has been removed by some Thread after the
                // call to this method
                LOGGER.warn( "Another task removed source {}, not removing.",
                             surrogateKey );
                return false;
            }

            return IncompleteIngest.removeDataSource( database,
                                                      dataSourcesCache,
                                                      sourceDetails );
        }
        catch ( SQLException se )
        {
            throw new IllegalStateException( DB_COMMUNICATION_FAILED, se );
        }
    }

    /**
     * Attempts to remove the data source.
     * @param database the database
     * @param dataSourcesCache the data sources cache
     * @param source the data source/ORM
     * @return whether the source was removed
     * @throws SQLException if removal failed
     */

    private static boolean removeDataSource( Database database,
                                             DataSources dataSourcesCache,
                                             SourceDetails source )
            throws SQLException
    {
        Objects.requireNonNull( source );
        Objects.requireNonNull( source.getId() );

        boolean wasIngested = IncompleteIngest.wasCompletelyIngested( database,
                                                                      source );

        if ( wasIngested )
        {
            LOGGER.warn( "This task was asked to inspect a source for incomplete ingest but the source was "
                         + "subsequently completed by another task, so it will not be removed. The source was: {}.",
                         source );
            return false;
        }

        // If we got to this point, there should be an exclusive lock on the source that prevents other threads 
        // mutating it (e.g., removing it) and we have now checked that nothing completed it immediately before that 
        // lock was acquired, so it is truly an incomplete ingest, otherwise something went wrong. If this message is 
        // seen when one instance or multiple instances are currently trying to ingest the source, then something went 
        // wrong with the locking semantics

        LOGGER.warn( "Another task started to ingest a source but did not complete it. This source will now be removed "
                     + "from the database. The source to be removed is: {}.",
                     source );

        // Proceed to remove
        long sourceId = source.getId();

        DataScripter timeSeriesValueScript = IncompleteIngest.getTimeSeriesValueScript( database, sourceId );
        DataScripter referenceTimeScript = IncompleteIngest.getReferenceTimeScript( database, sourceId );
        DataScripter timeSeriesScript = IncompleteIngest.getTimeSeriesScript( database, sourceId );
        DataScripter sourceScript = IncompleteIngest.getTimeSeriesSourceScript( database, sourceId );

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

        // Invalidate caches affected by deletes above
        dataSourcesCache.invalidate( source );

        return true;
    }

    /**
     * @param database the database
     * @param sourceId the source id
     * @return the time-series value remover script
     */

    private static DataScripter getTimeSeriesValueScript( Database database, Long sourceId )
    {
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

        return timeSeriesValueScript;
    }

    /**
     * @param database the database
     * @param sourceId the source id
     * @return the time-series reference time remover script
     */

    private static DataScripter getReferenceTimeScript( Database database, Long sourceId )
    {
        DataScripter referenceTimeScript = new DataScripter( database );
        referenceTimeScript.addLine( "DELETE FROM wres.TimeSeriesReferenceTime" );
        referenceTimeScript.addLine( WHERE_SOURCE_ID );
        referenceTimeScript.addArgument( sourceId );

        return referenceTimeScript;
    }

    /**
     * @param database the database
     * @param sourceId the source id
     * @return the time-series remover script
     */

    private static DataScripter getTimeSeriesScript( Database database, Long sourceId )
    {
        DataScripter timeSeriesScript = new DataScripter( database );
        timeSeriesScript.addLine( "DELETE FROM wres.TimeSeries" );
        timeSeriesScript.addLine( "WHERE timeseries_id IN" );
        timeSeriesScript.addLine( "(" );
        timeSeriesScript.addTab().addLine( "SELECT timeseries_id" );
        timeSeriesScript.addTab().addLine( "FROM wres.TimeSeries" );
        timeSeriesScript.addTab().addLine( WHERE_SOURCE_ID );
        timeSeriesScript.addArgument( sourceId );
        timeSeriesScript.addLine( ")" );

        return timeSeriesScript;
    }

    /**
     * @param database the database
     * @param sourceId the source id
     * @return the time-series source remover script
     */

    private static DataScripter getTimeSeriesSourceScript( Database database, Long sourceId )
    {
        DataScripter sourceScript = new DataScripter( database );
        sourceScript.addLine( "DELETE from wres.Source" );
        sourceScript.addLine( WHERE_SOURCE_ID );
        sourceScript.addArgument( sourceId );

        return sourceScript;
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
     * @return Whether values were removed
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
