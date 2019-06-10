package wres.io.removal;

import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
import wres.util.FutureQueue;

/**
 * Deals with partial/orphaned/incomplete ingested data, both detection and
 * removal.
 */

public class IncompleteIngest
{
    private static Logger LOGGER = LoggerFactory.getLogger( IncompleteIngest.class );

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
