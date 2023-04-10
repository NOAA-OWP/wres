package wres.io.project;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.datamodel.time.TimeSeriesStore;
import wres.io.NoDataException;
import wres.io.data.DataProvider;
import wres.io.database.caching.DatabaseCaches;
import wres.io.database.caching.GriddedFeatures;
import wres.io.database.DataScripter;
import wres.io.database.Database;
import wres.io.ingesting.IngestException;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.PreIngestException;

/**
 * Factory class for creating various implementations of a {@link Project}, such as an in-memory project and a project
 * backed by a database.
 */
public class Projects
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( Projects.class );

    /**
     * Creates a {@link Project} backed by a database.
     * @param database The database to use
     * @param projectConfig the projectConfig to ingest
     * @param caches the database caches/ORMs
     * @param griddedFeatures the gridded features cache, if required
     * @param ingestResults the ingest results
     * @return the project
     * @throws IllegalStateException when another process already holds lock
     * @throws NullPointerException if any input is null
     * @throws IngestException when anything else goes wrong
     */
    public static Project getProject( Database database,
                                      ProjectConfig projectConfig,
                                      DatabaseCaches caches,
                                      GriddedFeatures griddedFeatures,
                                      List<IngestResult> ingestResults )
    {
        Objects.requireNonNull( database );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( caches );
        Objects.requireNonNull( ingestResults );

        try
        {
            return Projects.getDatabaseProject( database,
                                                caches,
                                                griddedFeatures,
                                                projectConfig,
                                                ingestResults );
        }
        catch ( SQLException | IngestException | PreIngestException e )
        {
            throw new IngestException( "Failed to finalize ingest.", e );
        }
    }

    /**
     * Creates a {@link Project} backed by an in-memory {@link TimeSeriesStore}.
     * @param projectConfig the projectConfig
     * @param timeSeriesStore the store of time-series data
     * @param ingestResults the ingest results
     * @return the project
     */
    public static Project getProject( ProjectConfig projectConfig,
                                      TimeSeriesStore timeSeriesStore,
                                      List<IngestResult> ingestResults )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( timeSeriesStore );
        Objects.requireNonNull( ingestResults );

        return new InMemoryProject( projectConfig, timeSeriesStore, ingestResults );
    }

    /**
     * Convert a project declaration and a raw list of ingest results into a database project.
     * @param database the database
     * @param caches the database caches
     * @param griddedFeatures the gridded features cache
     * @param projectConfig the declaration that produced the ingest results
     * @param ingestResults the ingest results
     * @return the ProjectDetails to use
     * @throws SQLException when ProjectDetails construction goes wrong
     * @throws IllegalArgumentException when an IngestResult does not have left/right/baseline information
     * @throws PreIngestException if the hashes of the ingested sources cannot be determined
     * @throws IngestException if another wres instance failed to complete ingest on which this evaluation depends
     */
    private static Project getDatabaseProject( Database database,
                                               DatabaseCaches caches,
                                               GriddedFeatures griddedFeatures,
                                               ProjectConfig projectConfig,
                                               List<IngestResult> ingestResults )
            throws SQLException
    {
        long[] leftIds = Projects.getLeftIds( ingestResults );
        long[] rightIds = Projects.getRightIds( ingestResults );
        long[] baselineIds = Projects.getBaselineIds( ingestResults );

        // Check assumption that at least one left and one right source have
        // been created.
        int leftCount = leftIds.length;
        int rightCount = rightIds.length;

        if ( leftCount < 1 || rightCount < 1 )
        {
            throw new NoDataException( "When examining the ingested data, discovered insufficient data sources to "
                                       + "proceed. At least one data source is required for the left side of the "
                                       + "evaluation and one data source for the right side, but the left side had "
                                       + leftCount
                                       + " sources and the right side had "
                                       + rightCount
                                       + " sources. There were "
                                       + baselineIds.length
                                       + " baseline sources. Please check that all intended data sources were declared "
                                       + "and that all declared data sources were ingested correctly. For example, "
                                       + "were some data sources skipped because the format was unrecognized?" );
        }

        // Permit the List<IngestResult> to be garbage collected here, which
        // should leave space on heap for creating collections in the following.
        DatabaseProject project = Projects.getDatabaseProjectStepTwo( database,
                                                                      caches,
                                                                      griddedFeatures,
                                                                      projectConfig,
                                                                      leftIds,
                                                                      rightIds,
                                                                      baselineIds );


        // Validate the saved project
        project.prepareAndValidate();

        return project;
    }

    /**
     * Continue ingest.
     *
     * @param database the database
     * @param caches the database caches
     * @param griddedFeatures the gridded features cache
     * @param projectConfig the config that produced the ingest results
     * @param leftIds the left-sided data identifiers
     * @param rightIds the right-sided data identifiers
     * @param baselineIds the baseline-sided data identifiers
     * @return the project
     * @throws SQLException if the project could not be ingested
     */
    private static DatabaseProject getDatabaseProjectStepTwo( Database database,
                                                              DatabaseCaches caches,
                                                              GriddedFeatures griddedFeatures,
                                                              ProjectConfig projectConfig,
                                                              long[] leftIds,
                                                              long[] rightIds,
                                                              long[] baselineIds )
            throws SQLException
    {
        // We don't yet know how many unique timeseries there are. For example,
        // a baseline forecast could be the same as a right forecast. So we
        // can't as easily drop to primitive arrays because we would want to
        // know how to size them up front. The countOfIngestResults is a
        // maximum, though.
        Set<Long> uniqueSourcesUsed = new HashSet<>();

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
        Map<Long, String> idsToHashes = new HashMap<>( countOfUniqueHashes );
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
        String[] leftHashes = Projects.getHashes( leftIds, idsToHashes );
        String[] rightHashes = Projects.getHashes( rightIds, idsToHashes );
        String[] baselineHashes = Projects.getHashes( baselineIds, idsToHashes );

        Pair<DatabaseProject, Boolean> detailsResult =
                Projects.getProject( database,
                                     caches,
                                     griddedFeatures,
                                     projectConfig,
                                     leftHashes,
                                     rightHashes,
                                     baselineHashes );
        DatabaseProject details = detailsResult.getLeft();

        return Projects.getDatabaseProjectStepThree( database,
                                                     details,
                                                     detailsResult.getRight(),
                                                     leftIds,
                                                     rightIds,
                                                     baselineIds );
    }

    /**
     * Continue ingest.
     *
     * @param database the database
     * @param project the database project
     * @param inserted whether the project caused an insert
     * @param leftIds the left-sided data identifiers
     * @param rightIds the right-sided data identifiers
     * @param baselineIds the baseline-sided data identifiers
     * @return the project
     * @throws SQLException if the project could not be ingested
     */
    private static DatabaseProject getDatabaseProjectStepThree( Database database,
                                                                DatabaseProject project,
                                                                boolean inserted,
                                                                long[] leftIds,
                                                                long[] rightIds,
                                                                long[] baselineIds )
            throws SQLException
    {
        long detailsId = project.getId();
        if ( Boolean.TRUE.equals( inserted ) )
        {
            String projectId = Long.toString( detailsId );
            LOGGER.debug( "Found that this Thread is responsible for "
                          + "wres.ProjectSource rows for project {}",
                          detailsId );

            // If we just created the Project, we are responsible for relating
            // project to source. Otherwise we trust it is present.
            String tableName = "wres.ProjectSource";
            List<String> columnNames = List.of( "project_id", "source_id", "member" );

            List<String[]> values = Projects.getSourceRowsFromIds( leftIds, rightIds, baselineIds, projectId );

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

        return project;
    }

    /**
     * Converts source IDs into source rows for insertion.
     * @param leftIds the left IDs
     * @param rightIds the right IDs
     * @param baselineIds the baseline IDs
     * @param projectId the project ID
     * @return the source rows to insert
     */
    private static List<String[]> getSourceRowsFromIds( long[] leftIds,
                                                        long[] rightIds,
                                                        long[] baselineIds,
                                                        String projectId )
    {
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

        return Collections.unmodifiableList( values );
    }

    /**
     * Converts the supplied IDs to source hashes using the translation map.
     * @param ids the ids to hash
     * @param idsToHashes the map of IDs to hashes
     * @return the hashes
     */
    private static String[] getHashes( long[] ids, Map<Long, String> idsToHashes )
    {
        String[] hashes = new String[ids.length];

        for ( int i = 0; i < ids.length; i++ )
        {
            long id = ids[i];
            String hash = idsToHashes.get( id );

            if ( Objects.nonNull( hash ) )
            {
                hashes[i] = hash;
            }
            else
            {
                throw new PreIngestException( "Unexpected null left hash value for id="
                                              + id );
            }
        }

        return hashes;
    }

    /**
     * Gets a project from the inputs.
     * @param database the database
     * @param caches the database ORM
     * @param griddedFeatures the gridded features, if any
     * @param projectConfig the project declaration
     * @param leftHashes the left hashes
     * @param rightHashes the right hashes
     * @param baselineHashes the baseline hashes
     * @return the project declaration and whether this project inserted into the database
     */
    private static Pair<DatabaseProject, Boolean> getProject( Database database,
                                                              DatabaseCaches caches,
                                                              GriddedFeatures griddedFeatures,
                                                              ProjectConfig projectConfig,
                                                              String[] leftHashes,
                                                              String[] rightHashes,
                                                              String[] baselineHashes )
    {
        Objects.requireNonNull( database );
        Objects.requireNonNull( caches );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( leftHashes );
        Objects.requireNonNull( rightHashes );
        Objects.requireNonNull( baselineHashes );
        String identity = getTopHashOfSources( leftHashes,
                                               rightHashes,
                                               baselineHashes );

        DatabaseProject details = new DatabaseProject( database,
                                                       caches,
                                                       griddedFeatures,
                                                       projectConfig,
                                                       identity );
        boolean thisCallCausedInsert = details.save();
        LOGGER.debug( "Did the Project created by this Thread insert into the database first? {}",
                      thisCallCausedInsert );

        return Pair.of( details, thisCallCausedInsert );
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
                                            Map<Long, String> idsToHashes )
            throws SQLException
    {
        String queryStart = "SELECT source_id, hash "
                            + "FROM wres.Source "
                            + "WHERE source_id in ";
        StringJoiner idJoiner = new StringJoiner( ",", "(", ")" );

        for ( int i = 0; i < ids.size(); i++ )
        {
            idJoiner.add( "?" );
        }

        String query = queryStart + idJoiner;
        DataScripter script = new DataScripter( database, query );

        for ( Long id : ids )
        {
            script.addArgument( id );
        }

        script.setMaxRows( ids.size() );

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
                                                  + idNull
                                                  + " hashNull="
                                                  + hashNull );
                }
            }
        }
    }

    /**
     * <p>Get the list of left surrogate keys from given ingest results.
     *
     * <p>Intended to save heap by doing one dataset at a time, using primitive[]
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
     * <p>Get the list of right surrogate keys from given ingest results.
     *
     * <p>Intended to save heap by doing one dataset at a time, using primitive[]
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
     * <p>Get the list of baseline surrogate keys from given ingest results.
     *
     * <p>Intended to save heap by doing one dataset at a time, using primitive[]
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

    /**
     * <p>Creates a hash for the indicated project configuration based on its
     * data ingested.
     *
     * @param leftHashes A collection of the hashes for the left sided
     *                           source data
     * @param rightHashes A collection of the hashes for the right sided
     *                            source data
     * @param baselineHashes A collection of hashes representing the baseline
     *                               source data
     * @return A unique hash code for the project's circumstances
     */
    private static String getTopHashOfSources( final String[] leftHashes,
                                               final String[] rightHashes,
                                               final String[] baselineHashes )
    {
        MessageDigest md5Digest;

        try
        {
            md5Digest = MessageDigest.getInstance( "MD5" );
        }
        catch ( NoSuchAlgorithmException nsae )
        {
            throw new PreIngestException( "Couldn't use MD5 algorithm.",
                                          nsae );
        }

        // Sort for deterministic hash result for same list of ingested
        Arrays.sort( leftHashes );

        for ( String leftHash : leftHashes )
        {
            DigestUtils.updateDigest( md5Digest, leftHash );
        }

        // Sort for deterministic hash result for same list of ingested
        Arrays.sort( rightHashes );

        for ( String rightHash : rightHashes )
        {
            DigestUtils.updateDigest( md5Digest, rightHash );
        }

        // Sort for deterministic hash result for same list of ingested
        Arrays.sort( baselineHashes );

        for ( String baselineHash : baselineHashes )
        {
            DigestUtils.updateDigest( md5Digest, baselineHash );
        }

        byte[] digestAsHex = md5Digest.digest();
        return Hex.encodeHexString( digestAsHex );
    }

    /**
     * Do not construct.
     */
    private Projects()
    {
    }
}
