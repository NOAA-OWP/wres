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
import wres.io.data.details.TimeSeries;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;
import wres.util.FutureQueue;

/**
 * Deals with partial/orphaned/incomplete ingested data, both detection and
 * removal.
 */

public class IncompleteIngest
{
    private static Logger LOGGER = LoggerFactory.getLogger( IncompleteIngest.class );

    private static final String DB_COMMUNICATION_FAILED =
            "Communication with the database failed.";


    public static boolean removeSourceDataSafely( String sourceHash,
                                                  DatabaseLockManager lockManager )
    {
        Objects.requireNonNull( sourceHash );
        Objects.requireNonNull( lockManager );

        try
        {
            SourceDetails sourceDetails =
                    DataSources.getExistingSource( sourceHash );
            return IncompleteIngest.removeSourceDataSafely( sourceDetails,
                                                            lockManager );
        }
        catch ( SQLException se )
        {
            throw new IllegalStateException( DB_COMMUNICATION_FAILED, se );
        }
    }

    private static boolean removeSourceDataSafely( SourceDetails source,
                                                   DatabaseLockManager lockManager )
            throws SQLException
    {
        Objects.requireNonNull( source );
        Objects.requireNonNull( source.getId() );
        Objects.requireNonNull( lockManager );

        boolean wasIngested = IncompleteIngest.wasCompletelyIngested( source );

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

        int sourceId = source.getId();
        DataScripter observationsScript = new DataScripter();
        observationsScript.addLine( "DELETE FROM wres.Observation" );
        observationsScript.addLine( "WHERE source_id = ?" );
        observationsScript.addArgument( sourceId );

        // Simple, but slow when the partitions are not by timeseries_id:
        DataScripter timeSeriesValueScript = new DataScripter();
        timeSeriesValueScript.addLine( "DELETE FROM wres.TimeSeriesValue" );
        timeSeriesValueScript.addLine( "WHERE timeseries_id IN" );
        timeSeriesValueScript.addLine( "(" );
        timeSeriesValueScript.addTab().addLine( "SELECT timeseries_id" );
        timeSeriesValueScript.addTab().addLine( "FROM wres.TimeSeriesSource" );
        timeSeriesValueScript.addTab().addLine( "WHERE lead IS NULL" );
        timeSeriesValueScript.addTab().addLine( "AND source_id = ?" );
        timeSeriesValueScript.addArgument( sourceId );
        timeSeriesValueScript.addLine( ")" );

        DataScripter timeSeriesScript = new DataScripter();
        timeSeriesScript.addLine( "DELETE FROM wres.TimeSeries" );
        timeSeriesScript.addLine( "WHERE timeseries_id IN" );
        timeSeriesScript.addLine( "(" );
        timeSeriesScript.addTab().addLine( "SELECT timeseries_id" );
        timeSeriesScript.addTab().addLine( "FROM wres.TimeSeriesSource" );
        timeSeriesScript.addTab().addLine( "WHERE lead IS NULL" );
        timeSeriesScript.addTab().addLine( "AND source_id = ?" );
        timeSeriesScript.addArgument( sourceId );
        timeSeriesScript.addLine( ")" );

        DataScripter sourceScript = new DataScripter();
        sourceScript.addLine( "DELETE from wres.Source" );
        sourceScript.addLine( "WHERE source_id = ?" );
        sourceScript.addArgument( sourceId );

        try
        {
            lockManager.lockSource( sourceId );
            int observationsRemoved = observationsScript.execute();
            int timeSeriesValuesRemoved = timeSeriesValueScript.execute();
            int timeSeriesRemoved = timeSeriesScript.execute();
            int sourcesRemoved = sourceScript.execute();
            LOGGER.debug( "Removed {} obs, {} fc, {} ts, {} s.",
                          observationsRemoved,
                          timeSeriesValuesRemoved,
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
        DataSources.invalidateGlobalCache();
        TimeSeries.invalidateGlobalCache();

        return true;
    }

    /**
     * Given a source, return true if it is currently being ingested.
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
        Integer sourceId = source.getId();

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

    private static boolean wasCompletelyIngested( SourceDetails source )
    {
        SourceCompletedDetails completedDetails = new SourceCompletedDetails( source );

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
    private static boolean thereAreOrphanedValues() throws SQLException
    {
        DataScripter scriptBuilder = new DataScripter();

        scriptBuilder.addLine("SELECT EXISTS (");
        scriptBuilder.addTab().addLine("SELECT 1");
        scriptBuilder.addTab().addLine("FROM wres.Source S");
        scriptBuilder.addTab().addLine("WHERE NOT EXISTS (");
        scriptBuilder.addTab(  2  ).addLine("SELECT 1");
        scriptBuilder.addTab(  2  ).addLine("FROM wres.ProjectSource PS");
        scriptBuilder.addTab(  2  ).addLine("WHERE PS.source_id = S.source_id");
        scriptBuilder.addTab().addLine(")");
        scriptBuilder.addLine(") AS orphans_exist;");

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
     * @return Whether or not values were removed
     * @throws SQLException Thrown when one of the required scripts could not complete
     */

    @SuppressWarnings( "unchecked" )
    public static boolean removeOrphanedData() throws SQLException
    {
        try
        {
            if (!thereAreOrphanedValues())
            {
                return false;
            }

            // Can't lock for mutation here because we'd also need to unlock, but this
            // operation will occur alongside other operations that need that lock.

            LOGGER.info( "Incomplete data has been detected. Incomplete data "
                                  + "will now be removed to ensure that all data operated "
                                  + "upon is valid.");

            Set<String> partitionTables = Database.getPartitionTables();

            // We aren't actually going to collect the results so raw types are fine.
            FutureQueue removalQueue = new FutureQueue(  );

            for (String partition : partitionTables)
            {
                DataScripter valueRemover = new DataScripter();
                valueRemover.setHighPriority( false );
                valueRemover.addLine( "DELETE FROM ", partition, " P" );
                valueRemover.addLine( "WHERE NOT EXISTS (");
                valueRemover.addTab().addLine( "SELECT 1");
                valueRemover.addTab().addLine( "FROM wres.TimeSeriesSource TSS");
                valueRemover.addTab().addLine( "INNER JOIN wres.ProjectSource PS");
                valueRemover.addTab(  2  ).addLine( "ON PS.source_id = TSS.source_id");
                valueRemover.addTab().addLine( "WHERE TSS.timeseries_id = P.timeseries_id");
                valueRemover.addTab(  2  ).addLine( "AND (TSS.lead IS NULL OR TSS.lead = P.lead)");
                valueRemover.addLine( ");" );

                Future timeSeriesValueRemoval = valueRemover.issue();
                removalQueue.add( timeSeriesValueRemoval );

                LOGGER.debug( "Started task to remove orphaned values in {}...", partition);
            }

            DataScripter removeObservations = new DataScripter();
            removeObservations.setHighPriority( false );
            removeObservations.addLine( "DELETE FROM wres.Observation O" );
            removeObservations.addLine( "WHERE NOT EXISTS (" );
            removeObservations.addTab().addLine( "SELECT 1" );
            removeObservations.addTab().addLine( "FROM wres.ProjectSource PS" );
            removeObservations.addTab().addLine( "WHERE PS.source_id = O.source_id" );
            removeObservations.add( ");" );

            Future observationsRemoval = removeObservations.issue();
            removalQueue.add( observationsRemoval );

            LOGGER.debug( "Started task to remove orphaned observations...");

            try
            {
                removalQueue.loop();
            }
            catch ( ExecutionException e )
            {
                throw new SQLException( "Orphaned observed and forecasted values could not be removed.", e );
            }

            DataScripter removeTimeSeriesSource = new DataScripter();
            removeTimeSeriesSource.addLine( "DELETE FROM wres.TimeSeriesSource TSS" );
            removeTimeSeriesSource.addLine( "WHERE NOT EXISTS (" );
            removeTimeSeriesSource.addTab().addLine( "SELECT 1" );
            removeTimeSeriesSource.addTab().addLine( "FROM wres.ProjectSource PS" );
            removeTimeSeriesSource.addTab().addLine( "WHERE PS.source_id = TSS.source_id" );
            removeTimeSeriesSource.add( ");" );

            LOGGER.debug( "Removing orphaned TimeSeriesSource Links...");
            removeTimeSeriesSource.execute();

            LOGGER.debug( "Removed orphaned TimeSeriesSource Links");

            DataScripter removeTimeSeries = new DataScripter();

            removeTimeSeries.addLine( "DELETE FROM wres.TimeSeries TS" );
            removeTimeSeries.addLine( "WHERE NOT EXISTS (" );
            removeTimeSeries.addTab().addLine( "SELECT 1" );
            removeTimeSeries.addTab().addLine( "FROM wres.TimeSeriesSource TSS" );
            removeTimeSeries.addTab().addLine( "WHERE TS.timeseries_id = TS.timeseries_id" );
            removeTimeSeries.add( ");" );

            Future timeSeriesRemoval = removeTimeSeries.issue();
            removalQueue.add( timeSeriesRemoval );

            LOGGER.debug( "Added Task to remove orphaned time series...");

            DataScripter removeSources = new DataScripter();

            removeSources.addLine( "DELETE FROM wres.Source S" );
            removeSources.addLine( "WHERE NOT EXISTS (" );
            removeSources.addTab().addLine( "SELECT 1" );
            removeSources.addTab().addLine( "FROM wres.ProjectSource PS" );
            removeSources.addTab().addLine( "WHERE PS.source_id = S.source_id" );
            removeSources.add( ");" );

            Future sourcesRemoval = removeSources.issue();
            removalQueue.add( sourcesRemoval );

            LOGGER.debug( "Added task to remove orphaned sources...");

            DataScripter removeProjects = new DataScripter();

            removeProjects.addLine( "DELETE FROM wres.Project P" );
            removeProjects.addLine( "WHERE NOT EXISTS (" );
            removeProjects.addTab().addLine( "SELECT 1" );
            removeProjects.addTab().addLine( "FROM wres.ProjectSource PS" );
            removeProjects.addTab().addLine( "WHERE PS.project_id = P.project_id" );
            removeProjects.add( ");" );

            LOGGER.debug( "Added task to remove orphaned projects...");

            Future projectsRemoval = removeProjects.issue();
            removalQueue.add( projectsRemoval );

            try
            {
                removalQueue.loop();
            }
            catch ( ExecutionException e )
            {
                throw new SQLException( "Orphaned forecast, project, and source metadata could not be removed.", e );
            }

            LOGGER.info( "Incomplete data has been removed from the system.");
        }
        catch ( SQLException | ExecutionException databaseError )
        {
            throw new SQLException( "Orphaned data could not be removed", databaseError );
        }

        return true;
    }
}
