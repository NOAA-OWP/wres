package wres.io.data.caching;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.io.data.details.ProjectDetails;
import wres.io.reading.IngestResult;
import wres.io.utilities.Database;
import wres.util.Internal;

/**
 * Cache of available types of forecast
 */
@Internal(exclusivePackage = "wres.io")
public class Projects extends Cache<ProjectDetails, Integer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Projects.class);
    private static Projects INTERNAL_CACHE = null;
    private static final Object CACHE_LOCK = new Object();

    private static Projects getCache ()
    {
        synchronized (CACHE_LOCK)
        {
            if (INTERNAL_CACHE == null)
            {
                INTERNAL_CACHE = new Projects();
                INTERNAL_CACHE.init();
            }
            return INTERNAL_CACHE;
        }
    }

    public static ProjectDetails getProject( ProjectConfig projectConfig,
                                             List<String> leftHashes,
                                             List<String> rightHashes,
                                             List<String> baselineHashes )
            throws SQLException
    {
        ProjectDetails details = null;
        Integer inputCode = ProjectDetails.hash( projectConfig,
                                                 leftHashes,
                                                 rightHashes,
                                                 baselineHashes );

        if (Projects.getCache().hasID( inputCode ))
        {
            details = Projects.getCache().get( Projects.getCache().getID( inputCode )  );
        }

        if (details == null)
        {
            details = new ProjectDetails( projectConfig,
                                          inputCode );
            Projects.getCache().addElement( details );
        }


        return details;
    }

    @Override
    protected int getMaxDetails() {
        return 10;
    }

    @Override
    protected void init() {
        this.getDetails();

        Connection connection = null;
        ResultSet projects = null;

        try
        {
            connection = Database.getHighPriorityConnection();
            String loadScript = "SELECT *" + NEWLINE;
            loadScript += "FROM wres.project;";

            projects = Database.getResults(connection, loadScript);

            while (projects.next())
            {
                Integer projectId = projects.getInt( "project_id" );
                Integer inputCode = projects.getInt("input_code");
                this.getKeyIndex().put(
                        inputCode,
                        projectId
                );
            }

        }
        catch (SQLException error)
        {
            LOGGER.error("An error was encountered when trying to populate the Project cache.");
        }
        finally
        {
            if (projects != null)
            {
                try
                {
                    projects.close();
                }
                catch (SQLException error)
                {
                    LOGGER.error("The result set containing projects could not be closed.");
                }
            }

            if (connection != null)
            {
                Database.returnHighPriorityConnection(connection);
            }
        }
    }

    /**
     * Convert a projectConfig and a raw list of IngestResult into ProjectDetails
     * @param projectConfig the config that produced the ingest results
     * @param ingestResults the ingest results
     * @return the ProjectDetails to use
     * @throws SQLException when ProjectDetails construction goes wrong
     * @throws IllegalArgumentException when an IngestResult does not have left/right/baseline information
     */
    public static ProjectDetails getProjectFromIngest( ProjectConfig projectConfig,
                                                       List<IngestResult> ingestResults )
            throws SQLException
    {
        List<String> leftHashes = new ArrayList<>();
        List<String> rightHashes = new ArrayList<>();
        List<String> baselineHashes = new ArrayList<>();

        for ( IngestResult ingestResult : ingestResults )
        {
            if ( ingestResult.getLeftOrRightOrBaseline()
                             .equals( LeftOrRightOrBaseline.LEFT ) )
            {
                leftHashes.add( ingestResult.getHash() );
            }
            else if ( ingestResult.getLeftOrRightOrBaseline()
                                  .equals( LeftOrRightOrBaseline.RIGHT ) )
            {
                rightHashes.add( ingestResult.getHash() );
            }
            else if ( ingestResult.getLeftOrRightOrBaseline()
                                  .equals( LeftOrRightOrBaseline.BASELINE ) )
            {
                baselineHashes.add( ingestResult.getHash() );
            }
            else
            {
                throw new IllegalArgumentException( "An ingest result did not "
                                                    + "have left/right/baseline"
                                                    + " associated with it: "
                                                    + ingestResult );
            }
        }

        List<String> finalLeftHashes = Collections.unmodifiableList( leftHashes );
        List<String> finalRightHashes = Collections.unmodifiableList( rightHashes );
        List<String> finalBaselineHashes = Collections.unmodifiableList( baselineHashes );
        ProjectDetails details = Projects.getProject( projectConfig,
                                                      finalLeftHashes,
                                                      finalRightHashes,
                                                      finalBaselineHashes );

        String copyHeader = "wres.ProjectSource (project_id, source_id, member)";
        String delimiter = "|";
        StringJoiner copyStatement = new StringJoiner( NEWLINE );

        for ( IngestResult ingestResult : ingestResults )
        {
            Integer sourceID =
                DataSources.getActiveSourceID( ingestResult.getHash() );
            copyStatement.add( details.getId() + delimiter
                               + sourceID + delimiter
                               + ingestResult.getLeftOrRightOrBaseline().value() );
        }

        String allCopyValues = copyStatement.toString();
        LOGGER.trace( "Full copy statement: {}", allCopyValues );
        Database.copy( copyHeader, allCopyValues, delimiter );

        return details;
    }
}
