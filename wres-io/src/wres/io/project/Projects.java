package wres.io.data.caching;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import static wres.io.config.LeftOrRightOrBaseline.*;

import wres.io.config.LeftOrRightOrBaseline;
import wres.io.project.Project;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;

/**
 * Cache of available types of forecast
 */
// TODO: Find a way to remove the projects cache
@Deprecated(forRemoval = true) // We shouldn't cache; there will only ever be one project
public class Projects
{
    private static final String NEWLINE = System.lineSeparator();
    private static final Logger LOGGER = LoggerFactory.getLogger(Projects.class);

    private static Pair<Project,Boolean> getProject( ProjectConfig projectConfig,
                                                     List<String> leftHashes,
                                                     List<String> rightHashes,
                                                     List<String> baselineHashes )
            throws SQLException
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( leftHashes );
        Objects.requireNonNull( rightHashes );
        Objects.requireNonNull( baselineHashes );
        Integer inputCode = ConfigHelper.hashProject(
                projectConfig,
                leftHashes,
                rightHashes,
                baselineHashes
        );

        Project details = new Project( projectConfig,
                                       inputCode );
        details.save();
        boolean thisCallCausedInsert = details.performedInsert();
        LOGGER.debug( "Did the ProjectDetails created by this Thread insert into the database first? {}",
                      thisCallCausedInsert );

        return Pair.of( details, thisCallCausedInsert );
    }

    /**
     * Convert a projectConfig and a raw list of IngestResult into ProjectDetails
     * @param projectConfig the config that produced the ingest results
     * @param ingestResults the ingest results
     * @return the ProjectDetails to use
     * @throws SQLException when ProjectDetails construction goes wrong
     * @throws IllegalArgumentException when an IngestResult does not have left/right/baseline information
     * @throws IOException when a source identifier cannot be determined
     */
    public static Project getProjectFromIngest( ProjectConfig projectConfig,
                                                List<IngestResult> ingestResults )
            throws SQLException, IOException
    {
        List<String> leftHashes = new ArrayList<>();
        List<String> rightHashes = new ArrayList<>();
        List<String> baselineHashes = new ArrayList<>();

        for ( IngestResult ingestResult : ingestResults )
        {
            int countAdded = 0;

            if ( ingestResult.getLeftOrRightOrBaseline()
                             .equals( LeftOrRightOrBaseline.LEFT ) )
            {
                leftHashes.add( ingestResult.getHash() );
                countAdded++;
            }
            else if ( ingestResult.getLeftOrRightOrBaseline()
                                  .equals( LeftOrRightOrBaseline.RIGHT ) )
            {
                rightHashes.add( ingestResult.getHash() );
                countAdded++;
            }
            else if ( ingestResult.getLeftOrRightOrBaseline()
                                  .equals( LeftOrRightOrBaseline.BASELINE ) )
            {
                baselineHashes.add( ingestResult.getHash() );
                countAdded++;
            }
            else
            {
                throw new IllegalArgumentException( "An ingest result did not "
                                                    + "have left/right/baseline"
                                                    + " associated with it: "
                                                    + ingestResult );
            }

            // Additionally include links when a source is re-used in a project.
            for ( LeftOrRightOrBaseline leftOrRightOrBaseline :
                    ingestResult.getDataSource()
                                .getLinks() )
            {
                if ( leftOrRightOrBaseline.equals( LEFT ) )
                {
                    leftHashes.add( ingestResult.getHash() );
                    countAdded++;
                }
                else if ( leftOrRightOrBaseline.equals( RIGHT ) )
                {
                    rightHashes.add( ingestResult.getHash() );
                    countAdded++;
                }
                else if ( leftOrRightOrBaseline.equals( BASELINE ) )
                {
                    baselineHashes.add( ingestResult.getHash() );
                    countAdded++;
                }
            }

            LOGGER.debug( "Noticed {} has {} links.", ingestResult, countAdded );
        }

        List<String> finalLeftHashes = Collections.unmodifiableList( leftHashes );
        List<String> finalRightHashes = Collections.unmodifiableList( rightHashes );
        List<String> finalBaselineHashes = Collections.unmodifiableList( baselineHashes );

        // Check assumption that at least one left and one right source have
        // been created.
        int leftCount = finalLeftHashes.size();
        int rightCount = finalRightHashes.size();

        if ( leftCount < 1 || rightCount < 1 )
        {
            throw new IllegalStateException( "At least one source for left and "
                                             + "one source for right must be "
                                             + "linked, but left had "
                                             + leftCount + " sources and right "
                                             + "had " + rightCount
                                             + " sources." );
        }

        Pair<Project,Boolean> detailsResult =
                Projects.getProject( projectConfig,
                                     finalLeftHashes,
                                     finalRightHashes,
                                     finalBaselineHashes );
        Project details = detailsResult.getLeft();
        int detailsId = details.getId();

        if ( detailsResult.getRight() )
        {
            LOGGER.debug( "Found that this Thread is responsible for "
                          + "wres.ProjectSource rows for project {}",
                          detailsId );

            // If we just created the Project, we are responsible for relating
            // project to source. Otherwise we trust it is present.
            String copyHeader = "wres.ProjectSource (project_id, source_id, member)";
            String delimiter = "|";
            StringJoiner copyStatement = new StringJoiner( NEWLINE );

            for ( IngestResult ingestResult : ingestResults )
            {
                Integer sourceID =
                    DataSources.getActiveSourceID( ingestResult.getHash() );

                if (sourceID == null)
                {
                    throw new IngestException( "The id for source data '"
                                               + ingestResult.getHash()
                                               + "' that must be linked to "
                                               + "project "
                                               + details.getId()
                                               + " could not be determined. The"
                                               + " data ingest cannot "
                                               + "continue.");
                }

                copyStatement.add( details.getId() + delimiter
                                   + sourceID + delimiter
                                   + ingestResult.getLeftOrRightOrBaseline().value() );

                for ( LeftOrRightOrBaseline additionalLink :
                        ingestResult.getDataSource().getLinks() )
                {
                    copyStatement.add( details.getId() + delimiter
                                       + sourceID + delimiter
                                       + additionalLink.value() );
                }
            }

            String allCopyValues = copyStatement.toString();
            LOGGER.trace( "Full copy statement: {}", allCopyValues );
            Database.copy( copyHeader, allCopyValues, delimiter );
        }
        else
        {
            LOGGER.debug( "Found that this Thread is NOT responsible for "
                          + "wres.ProjectSource rows for project {}",
                          detailsId );
            DataScripter scripter = new DataScripter();
            scripter.addLine( "SELECT COUNT( source_id )" );
            scripter.addLine( "FROM wres.ProjectSource" );
            scripter.addLine( "WHERE project_id = ?" );
            scripter.addArgument( detailsId );

            // Need to wait here until the data is available. How long to wait?
            // Start with 30ish seconds, error out after that. We might actually
            // wait longer than 30 seconds.
            long startMillis = System.currentTimeMillis();
            long endMillis = startMillis + Duration.ofSeconds( 30 )
                                                   .toMillis();
            long currentMillis = startMillis;
            long sleepMillis = Duration.ofSeconds( 1 )
                                       .toMillis();
            boolean projectSourceRowsFound = false;

            while ( currentMillis < endMillis )
            {
                try ( DataProvider dataProvider = scripter.getData() )
                {
                    long count = dataProvider.getLong( "count" );

                    if ( count > 1 )
                    {
                        // We assume that the projectsource rows are made
                        // in a single transaction here. We further assume that
                        // each project will have at least a left and right
                        // member, therefore 2 or more rows (greater than 1).
                        LOGGER.debug( "wres.ProjectSource rows present for {}",
                                      detailsId );
                        projectSourceRowsFound = true;
                        break;
                    }
                    else
                    {
                        LOGGER.debug( "wres.ProjectSource rows missing for {}",
                                      detailsId );
                        Thread.sleep( sleepMillis );
                    }
                }
                catch ( InterruptedException ie )
                {
                    LOGGER.warn( "Interrupted while waiting for wres.ProjectSource rows.", ie );
                    Thread.currentThread().interrupt();
                    // No need to rethrow, the evaluation will fail.
                }

                currentMillis = System.currentTimeMillis();
            }

            if ( !projectSourceRowsFound )
            {
                throw new IngestException( "Another WRES instance failed to "
                                           + "complete ingest that this "
                                           + "evaluation depends on." );
            }
        }

        return details;
    }
}
