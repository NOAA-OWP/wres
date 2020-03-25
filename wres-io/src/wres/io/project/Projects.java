package wres.io.project;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.io.concurrency.Executor;
import wres.io.config.ConfigHelper;
import static wres.io.config.LeftOrRightOrBaseline.*;

import wres.io.config.LeftOrRightOrBaseline;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.PreIngestException;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
import wres.system.SystemSettings;

/**
 * Create or find existing wres.project rows in the database, represented by
 * Project instances.
 */
public class Projects
{
    private static final String NEWLINE = System.lineSeparator();
    private static final Logger LOGGER = LoggerFactory.getLogger(Projects.class);

    private static Pair<Project,Boolean> getProject( SystemSettings systemSettings,
                                                     Database database,
                                                     Executor executor,
                                                     ProjectConfig projectConfig,
                                                     List<String> leftHashes,
                                                     List<String> rightHashes,
                                                     List<String> baselineHashes )
            throws SQLException
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( database );
        Objects.requireNonNull( executor );
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

        Project details = new Project( systemSettings,
                                       database,
                                       executor,
                                       projectConfig,
                                       inputCode );
        details.save();
        boolean thisCallCausedInsert = details.performedInsert();
        LOGGER.debug( "Did the Project created by this Thread insert into the database first? {}",
                      thisCallCausedInsert );

        return Pair.of( details, thisCallCausedInsert );
    }

    /**
     * Convert a projectConfig and a raw list of IngestResult into ProjectDetails
     * @param systemSettings The system settings to use.
     * @param database The database to use.
     * @param executor The executor to use.
     * @param projectConfig the config that produced the ingest results
     * @param ingestResults the ingest results
     * @return the ProjectDetails to use
     * @throws SQLException when ProjectDetails construction goes wrong
     * @throws IllegalArgumentException when an IngestResult does not have left/right/baseline information
     * @throws IOException when a source identifier cannot be determined
     */
    public static Project getProjectFromIngest( SystemSettings systemSettings,
                                                Database database,
                                                Executor executor,
                                                ProjectConfig projectConfig,
                                                List<IngestResult> ingestResults )
            throws SQLException, IOException
    {
        List<Integer> leftIds = new ArrayList<>();
        List<Integer> rightIds = new ArrayList<>();
        List<Integer> baselineIds = new ArrayList<>();

        for ( IngestResult ingestResult : ingestResults )
        {
            int countAdded = 0;

            if ( ingestResult.getLeftOrRightOrBaseline()
                             .equals( LeftOrRightOrBaseline.LEFT ) )
            {
                leftIds.add( ingestResult.getSurrogateKey() );
                countAdded++;
            }
            else if ( ingestResult.getLeftOrRightOrBaseline()
                                  .equals( LeftOrRightOrBaseline.RIGHT ) )
            {
                rightIds.add( ingestResult.getSurrogateKey() );
                countAdded++;
            }
            else if ( ingestResult.getLeftOrRightOrBaseline()
                                  .equals( LeftOrRightOrBaseline.BASELINE ) )
            {
                baselineIds.add( ingestResult.getSurrogateKey() );
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
                    leftIds.add( ingestResult.getSurrogateKey() );
                    countAdded++;
                }
                else if ( leftOrRightOrBaseline.equals( RIGHT ) )
                {
                    rightIds.add( ingestResult.getSurrogateKey() );
                    countAdded++;
                }
                else if ( leftOrRightOrBaseline.equals( BASELINE ) )
                {
                    baselineIds.add( ingestResult.getSurrogateKey() );
                    countAdded++;
                }
            }

            LOGGER.debug( "Noticed {} has {} links.", ingestResult, countAdded );
        }

        List<Integer> finalLeftIds = Collections.unmodifiableList( leftIds );
        List<Integer> finalRightIds = Collections.unmodifiableList( rightIds );
        List<Integer> finalBaselineIds = Collections.unmodifiableList( baselineIds );

        Set<Integer> uniqueSourcesUsed = new HashSet<>( finalLeftIds );
        uniqueSourcesUsed.addAll( finalRightIds );
        uniqueSourcesUsed.addAll( finalBaselineIds );
        int countOfUniqueHashes = uniqueSourcesUsed.size();
        StringJoiner idJoiner = new StringJoiner( ",", "(", ");" );

        for ( Integer rawId : uniqueSourcesUsed )
        {
            String id = rawId.toString();
            idJoiner.add( id );
        }

        String query = "SELECT source_id, hash "
                       + "FROM wres.Source "
                       + "WHERE source_id in "
                       + idJoiner.toString();
        DataScripter script = new DataScripter( database, query );
        Map<Integer,String> idsToHashes = new HashMap<>( countOfUniqueHashes );

        try ( DataProvider dataProvider = script.getData() )
        {
            while ( dataProvider.next() )
            {
                Integer id = dataProvider.getInt( "source_id" );
                String hash = dataProvider.getString( "hash" );

                if ( Objects.nonNull( id )
                     && Objects.nonNull( hash ) )
                {
                    idsToHashes.put( id, hash );
                }
                else
                {
                    boolean idNull = Objects.isNull( id );
                    boolean hashNull = Objects.isNull( hash );
                    throw new PreIngestException( "Found a null value in db when expecting a value. idNull="
                                                  + idNull + " hashNull="
                                                  + hashNull );
                }
            }
        }

        // "select hash from wres.Source S inner join ( select ... ) I on S.source_id = I.source_id"
        List<String> leftHashes = new ArrayList<>( finalLeftIds.size() );
        List<String> rightHashes = new ArrayList<>( finalRightIds.size() );
        List<String> baselineHashes = new ArrayList<>( finalBaselineIds.size() );

        for ( Integer id : finalLeftIds )
        {
            String hash = idsToHashes.get( id );

            if ( Objects.nonNull( hash ) )
            {
                leftHashes.add( hash );
            }
            else
            {
                throw new PreIngestException( "Unexpected null left hash value for id="
                                              + id );
            }
        }

        for ( Integer id : finalRightIds )
        {
            String hash = idsToHashes.get( id );

            if ( Objects.nonNull( hash ) )
            {
                rightHashes.add( hash );
            }
            else
            {
                throw new PreIngestException( "Unexpected null right hash value for id="
                                              + id );
            }
        }

        for ( Integer id : finalBaselineIds )
        {
            String hash = idsToHashes.get( id );

            if ( Objects.nonNull( hash ) )
            {
                baselineHashes.add( hash );
            }
            else
            {
                throw new PreIngestException( "Unexpected null baseline hash value for id="
                                              + id );
            }
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
                Projects.getProject( systemSettings,
                                     database,
                                     executor,
                                     projectConfig,
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
                int sourceID = ingestResult.getSurrogateKey();
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
            database.copy( copyHeader, allCopyValues, delimiter );
        }
        else
        {
            LOGGER.debug( "Found that this Thread is NOT responsible for "
                          + "wres.ProjectSource rows for project {}",
                          detailsId );
            DataScripter scripter = new DataScripter( database );
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
