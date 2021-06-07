package wres.io.project;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
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
import wres.io.data.caching.Features;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(Projects.class);

    private static Pair<Project,Boolean> getProject( SystemSettings systemSettings,
                                                     Database database,
                                                     Features featuresCache,
                                                     Executor executor,
                                                     ProjectConfig projectConfig,
                                                     String[] leftHashes,
                                                     String[] rightHashes,
                                                     String[] baselineHashes )
            throws SQLException
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( database );
        Objects.requireNonNull( featuresCache );
        Objects.requireNonNull( executor );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( leftHashes );
        Objects.requireNonNull( rightHashes );
        Objects.requireNonNull( baselineHashes );
        String identity = ConfigHelper.hashProject(
                leftHashes,
                rightHashes,
                baselineHashes
        );

        Project details = new Project( systemSettings,
                                       database,
                                       featuresCache,
                                       executor,
                                       projectConfig,
                                       identity );
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
     * @throws PreIngestException if the hashes of the ingested sources cannot be determined
     * @throws IngestException if another wres instance failed to complete ingest on which this evaluation depends
     */
    public static Project getProjectFromIngest( SystemSettings systemSettings,
                                                Database database,
                                                Features featuresCache,
                                                Executor executor,
                                                ProjectConfig projectConfig,
                                                List<IngestResult> ingestResults )
            throws SQLException
    {
        long[] leftIds = Projects.getLeftIds( ingestResults );
        long[] rightIds = Projects.getRightIds( ingestResults );
        long[] baselineIds = Projects.getBaselineIds( ingestResults );
        int countOfIngestResults = ingestResults.size();

        // Check assumption that at least one left and one right source have
        // been created.
        int leftCount = leftIds.length;
        int rightCount = rightIds.length;

        if ( leftCount < 1 || rightCount < 1 )
        {
            throw new IllegalStateException( "At least one source for left and "
                                             + "one source for right must be "
                                             + "linked, but left had "
                                             + leftCount + " sources and right "
                                             + "had " + rightCount
                                             + " sources." );
        }

        // Permit the List<IngestResult> to be garbage collected here, which
        // should leave space on heap for creating collections in the following.
        return Projects.getProjectFromIngestStepTwo( systemSettings,
                                                     database,
                                                     featuresCache,
                                                     executor,
                                                     projectConfig,
                                                     leftIds,
                                                     rightIds,
                                                     baselineIds,
                                                     countOfIngestResults );
    }

    private static Project getProjectFromIngestStepTwo( SystemSettings systemSettings,
                                                        Database database,
                                                        Features featuresCache,
                                                        Executor executor,
                                                        ProjectConfig projectConfig,
                                                        long[] leftIds,
                                                        long[] rightIds,
                                                        long[] baselineIds,
                                                        int countOfIngestResults )
            throws SQLException
    {
        // We don't yet know how many unique timeseries there are. For example,
        // a baseline forecast could be the same as a right forecast. So we
        // can't as easily drop to primitive arrays because we would want to
        // know how to size them up front. The countOfIngestResults is a
        // maximum, though.
        Set<Long> uniqueSourcesUsed = new HashSet<>( countOfIngestResults );

        for ( long leftId : leftIds )
        {
            uniqueSourcesUsed.add( leftId );
        }

        for ( long rightId : rightIds )
        {
            uniqueSourcesUsed.add( rightId );
        }

        for ( long baselineId : baselineIds )
        {
            uniqueSourcesUsed.add( baselineId );
        }

        int countOfUniqueHashes = uniqueSourcesUsed.size();
        final int MAX_PARAMETER_COUNT = 999;
        Map<Long,String> idsToHashes = new HashMap<>( countOfUniqueHashes );
        Set<Long> batchOfIds = new HashSet<>( MAX_PARAMETER_COUNT );

        for ( Long rawId : uniqueSourcesUsed )
        {
            // If appending this id is <= max, add it.
            if ( batchOfIds.size() + 1 > MAX_PARAMETER_COUNT )
            {
                LOGGER.debug( "Query would exceed {} params, running it now and building a new one.",
                              MAX_PARAMETER_COUNT );
                Projects.selectIdsAndHashes( database, batchOfIds, idsToHashes );
                batchOfIds.clear();
            }

            batchOfIds.add( rawId );
        }

        // The last query with the remainder of ids.
        Projects.selectIdsAndHashes( database, batchOfIds, idsToHashes );

        // "select hash from wres.Source S inner join ( select ... ) I on S.source_id = I.source_id"
        String[] leftHashes = new String[leftIds.length];
        String[] rightHashes = new String[rightIds.length];
        String[] baselineHashes = new String[baselineIds.length];

        for ( int i = 0; i < leftIds.length; i++ )
        {
            long id = leftIds[i];
            String hash = idsToHashes.get( id );

            if ( Objects.nonNull( hash ) )
            {
                leftHashes[i] = hash;
            }
            else
            {
                throw new PreIngestException( "Unexpected null left hash value for id="
                                              + id );
            }
        }

        for ( int i = 0; i < rightIds.length; i++ )
        {
            long id = rightIds[i];
            String hash = idsToHashes.get( id );

            if ( Objects.nonNull( hash ) )
            {
                rightHashes[i] = hash;
            }
            else
            {
                throw new PreIngestException( "Unexpected null right hash value for id="
                                              + id );
            }
        }

        for ( int i = 0; i < baselineHashes.length; i++ )
        {
            long id = baselineIds[i];
            String hash = idsToHashes.get( id );

            if ( Objects.nonNull( hash ) )
            {
                baselineHashes[i] = hash;
            }
            else
            {
                throw new PreIngestException( "Unexpected null baseline hash value for id="
                                              + id );
            }
        }

        Pair<Project,Boolean> detailsResult =
                Projects.getProject( systemSettings,
                                     database,
                                     featuresCache,
                                     executor,
                                     projectConfig,
                                     leftHashes,
                                     rightHashes,
                                     baselineHashes );
        Project details = detailsResult.getLeft();
        long detailsId = details.getId();

        if ( detailsResult.getRight() )
        {
            String projectId = Long.toString( detailsId );
            LOGGER.debug( "Found that this Thread is responsible for "
                          + "wres.ProjectSource rows for project {}",
                          detailsId );

            // If we just created the Project, we are responsible for relating
            // project to source. Otherwise we trust it is present.
            String tableName = "wres.ProjectSource";
            List<String> columnNames = List.of( "project_id", "source_id", "member" );

            List<String[]> values = new ArrayList<>( leftIds.length
                                                     + rightIds.length
                                                     + baselineIds.length );

            for ( long sourceID : leftIds )
            {
                String[] row = new String[3];
                row[0] = projectId;
                row[1] = Long.toString( sourceID );
                row[2] = "left";
                values.add( row );
            }

            for ( long sourceID : rightIds )
            {
                String[] row = new String[3];
                row[0] = projectId;
                row[1] = Long.toString( sourceID );
                row[2] = "right";
                values.add( row );
            }

            for ( long sourceID : baselineIds )
            {
                String[] row = new String[3];
                row[0] = projectId;
                row[1] = Long.toString( sourceID );
                row[2] = "baseline";
                values.add( row );
            }

            // The first two columns are numbers, last one is char.
            boolean[] charColumns = { false, false, true };
            database.copy( tableName, columnNames, values, charColumns );
        }
        else
        {
            LOGGER.debug( "Found that this Thread is NOT responsible for "
                          + "wres.ProjectSource rows for project {}",
                          detailsId );
            DataScripter scripter = new DataScripter( database );
            scripter.addLine( "SELECT COUNT( source_id ) AS count" );
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


    /**
     * Select source ids and hashes and put them into the given Map.
     * @param database The database to use.
     * @param ids The ids to use for selection of hashes.
     * @param idsToHashes MUTATED by this method, results go into this Map.
     * @throws SQLException When something goes wrong related to database.
     * @throws PreIngestException When a null value is found in result set.
     */

    private static void selectIdsAndHashes( Database database,
                                            Set<Long> ids,
                                            Map<Long,String> idsToHashes )
            throws SQLException
    {
        String queryStart = "SELECT source_id, hash "
                            + "FROM wres.Source "
                            + "WHERE source_id in ";
        StringJoiner idJoiner = new StringJoiner( ",", "(", ");" );

        for ( int i = 0; i < ids.size(); i++ )
        {
            idJoiner.add( "?" );
        }

        String query = queryStart + idJoiner.toString();
        DataScripter script = new DataScripter( database, query );

        for ( Long id : ids )
        {
            script.addArgument( id );
        }

        try ( DataProvider dataProvider = script.getData() )
        {
            while ( dataProvider.next() )
            {
                Long id = dataProvider.getLong( "source_id" );
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
    }


    /**
     * Get the list of left surrogate keys from given ingest results.
     *
     * Intended to save heap by doing one dataset at a time, using primitive[]
     *
     * @param ingestResults The ingest results.
     * @return The ids for the left dataset
     */

    private static long[] getLeftIds( List<IngestResult> ingestResults )
    {
        // How big to make the array? We don't want to guess because then we
        // would need to resize, which requires more heap again. Better to get
        // it correct at the outset.
        int sizeNeeded = 0;

        for ( IngestResult ingestResult : ingestResults )
        {
            sizeNeeded += ingestResult.getLeftCount();
        }

        long[] leftIds = new long[sizeNeeded];
        int i = 0;

        for ( IngestResult ingestResult : ingestResults )
        {
            for ( short j = 0; j < ingestResult.getLeftCount(); j++ )
            {
                leftIds[i] = ingestResult.getSurrogateKey();
                i++;
            }
        }

        return leftIds;
    }


    /**
     * Get the list of right surrogate keys from given ingest results.
     *
     * Intended to save heap by doing one dataset at a time, using primitive[]
     *
     * @param ingestResults The ingest results.
     * @return The ids for the right dataset
     */

    private static long[] getRightIds( List<IngestResult> ingestResults )
    {
        // How big to make the array? We don't want to guess because then we
        // would need to resize, which requires more heap again. Better to get
        // it correct at the outset.
        int sizeNeeded = 0;

        for ( IngestResult ingestResult : ingestResults )
        {
            sizeNeeded += ingestResult.getRightCount();
        }

        long[] rightIds = new long[sizeNeeded];
        int i = 0;

        for ( IngestResult ingestResult : ingestResults )
        {
            for ( short j = 0; j < ingestResult.getRightCount(); j++ )
            {
                rightIds[i] = ingestResult.getSurrogateKey();
                i++;
            }
        }

        return rightIds;
    }


    /**
     * Get the list of baseline surrogate keys from given ingest results.
     *
     * Intended to save heap by doing one dataset at a time, using primitive[]
     *
     * @param ingestResults The ingest results.
     * @return The ids for the baseline dataset
     */

    private static long[] getBaselineIds( List<IngestResult> ingestResults )
    {
        // How big to make the array? We don't want to guess because then we
        // would need to resize, which requires more heap again. Better to get
        // it correct at the outset.
        int sizeNeeded = 0;

        for ( IngestResult ingestResult : ingestResults )
        {
            sizeNeeded += ingestResult.getBaselineCount();
        }

        long[] baselineIds = new long[sizeNeeded];
        int i = 0;

        for ( IngestResult ingestResult : ingestResults )
        {
            for ( short j = 0; j < ingestResult.getBaselineCount(); j++ )
            {
                baselineIds[i] = ingestResult.getSurrogateKey();
                i++;
            }
        }

        return baselineIds;
    }
}
