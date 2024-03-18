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
import java.util.function.ToIntFunction;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.EvaluationDeclaration;
import wres.datamodel.time.TimeSeriesStore;
import wres.io.NoProjectDataException;
import wres.datamodel.DataProvider;
import wres.io.database.DatabaseOperations;
import wres.io.database.caching.DatabaseCaches;
import wres.reading.netcdf.grid.GriddedFeatures;
import wres.io.database.DataScripter;
import wres.io.database.Database;
import wres.io.ingesting.IngestException;
import wres.io.ingesting.IngestResult;

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
     * @param declaration the project declaration
     * @param caches the database caches/ORMs
     * @param griddedFeatures the gridded features cache, if required
     * @param ingestResults the ingest results
     * @return the project
     * @throws IllegalStateException when another process already holds lock
     * @throws NullPointerException if any input is null
     * @throws IngestException when anything else goes wrong
     */
    public static Project getProject( Database database,
                                      EvaluationDeclaration declaration,
                                      DatabaseCaches caches,
                                      GriddedFeatures griddedFeatures,
                                      List<IngestResult> ingestResults )
    {
        Objects.requireNonNull( database );
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( caches );
        Objects.requireNonNull( ingestResults );

        try
        {
            return Projects.getDatabaseProject( database,
                                                caches,
                                                griddedFeatures,
                                                declaration,
                                                ingestResults );
        }
        catch ( SQLException | IngestException e )
        {
            throw new IngestException( "Failed to finalize ingest.", e );
        }
    }

    /**
     * Creates a {@link Project} backed by an in-memory {@link TimeSeriesStore}.
     * @param declaration the project declaration
     * @param timeSeriesStore the store of time-series data
     * @param ingestResults the ingest results
     * @return the project
     */
    public static Project getProject( EvaluationDeclaration declaration,
                                      TimeSeriesStore timeSeriesStore,
                                      List<IngestResult> ingestResults )
    {
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( timeSeriesStore );
        Objects.requireNonNull( ingestResults );

        return new InMemoryProject( declaration, timeSeriesStore, ingestResults );
    }

    /**
     * Convert a project declaration and a raw list of ingest results into a database project.
     * @param database the database
     * @param caches the database caches
     * @param griddedFeatures the gridded features cache
     * @param declaration the declaration that produced the ingest results
     * @param ingestResults the ingest results
     * @return the ProjectDetails to use
     * @throws SQLException when ProjectDetails construction goes wrong
     * @throws IllegalArgumentException when an IngestResult does not have left/right/baseline information
     * @throws IngestException if another wres instance failed to complete ingest on which this evaluation depends
     */
    private static Project getDatabaseProject( Database database,
                                               DatabaseCaches caches,
                                               GriddedFeatures griddedFeatures,
                                               EvaluationDeclaration declaration,
                                               List<IngestResult> ingestResults )
            throws SQLException
    {
        long[] leftIds = Projects.getIds( ingestResults, IngestResult::getLeftCount );
        long[] rightIds = Projects.getIds( ingestResults, IngestResult::getRightCount );
        long[] baselineIds = Projects.getIds( ingestResults, IngestResult::getBaselineCount );
        long[] covariateIds = Projects.getIds( ingestResults, IngestResult::getCovariateCount );

        IngestIds ingestIds = new IngestIds( leftIds, rightIds, baselineIds, covariateIds );

        // Check assumption that at least one left and one right source have
        // been created.
        int leftCount = leftIds.length;
        int rightCount = rightIds.length;

        if ( leftCount < 1 || rightCount < 1 )
        {
            throw new NoProjectDataException(
                    "When examining the ingested data, discovered insufficient data sources to "
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
                                                                      declaration,
                                                                      ingestIds );


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
     * @param declaration the declaration that produced the ingest results
     * @param ingestIds the ingest identifiers
     * @return the project
     * @throws SQLException if the project could not be ingested
     */
    private static DatabaseProject getDatabaseProjectStepTwo( Database database,
                                                              DatabaseCaches caches,
                                                              GriddedFeatures griddedFeatures,
                                                              EvaluationDeclaration declaration,
                                                              IngestIds ingestIds )
            throws SQLException
    {
        // We don't yet know how many unique timeseries there are. For example,
        // a baseline forecast could be the same as a right forecast. So we
        // can't as easily drop to primitive arrays because we would want to
        // know how to size them up front. The countOfIngestResults is a
        // maximum, though.
        Set<Long> uniqueSourcesUsed = new HashSet<>();

        long[] leftIds = ingestIds.leftIds();
        long[] rightIds = ingestIds.rightIds();
        long[] baselineIds = ingestIds.baselineIds();
        long[] covariateIds = ingestIds.covariateIds();

        // Assemble the IDs
        Arrays.stream( leftIds )
              .forEach( uniqueSourcesUsed::add );
        Arrays.stream( rightIds )
              .forEach( uniqueSourcesUsed::add );
        Arrays.stream( baselineIds )
              .forEach( uniqueSourcesUsed::add );
        Arrays.stream( covariateIds )
              .forEach( uniqueSourcesUsed::add );

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
        String[] covariateHashes = Projects.getHashes( covariateIds, idsToHashes );

        IngestHashes hashes = new IngestHashes( leftHashes, rightHashes, baselineHashes, covariateHashes );

        Pair<DatabaseProject, Boolean> detailsResult =
                Projects.getProject( database,
                                     caches,
                                     griddedFeatures,
                                     declaration,
                                     hashes );
        DatabaseProject details = detailsResult.getLeft();

        return Projects.getDatabaseProjectStepThree( database,
                                                     details,
                                                     detailsResult.getRight(),
                                                     ingestIds );
    }

    /**
     * Continue ingest.
     *
     * @param database the database
     * @param project the database project
     * @param inserted whether the project caused an insert
     * @param ingestIds the ingest identifiers
     * @return the project
     * @throws SQLException if the project could not be ingested
     */
    private static DatabaseProject getDatabaseProjectStepThree( Database database,
                                                                DatabaseProject project,
                                                                boolean inserted,
                                                                IngestIds ingestIds )
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

            List<String[]> values = Projects.getSourceRowsFromIds( ingestIds, projectId );

            // The first two columns are numbers, last one is char.
            boolean[] charColumns = { false, false, true };
            DatabaseOperations.insertIntoDatabase( database, tableName, columnNames, values, charColumns );
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
     * @param ingestIds the ingest ids
     * @param projectId the project ID
     * @return the source rows to insert
     */
    private static List<String[]> getSourceRowsFromIds( IngestIds ingestIds,
                                                        String projectId )
    {
        List<String[]> values = new ArrayList<>();

        for ( long sourceID : ingestIds.leftIds() )
        {
            String[] row = new String[3];
            row[0] = projectId;
            row[1] = Long.toString( sourceID );
            row[2] = "left";
            values.add( row );
        }

        for ( long sourceID : ingestIds.rightIds() )
        {
            String[] row = new String[3];
            row[0] = projectId;
            row[1] = Long.toString( sourceID );
            row[2] = "right";
            values.add( row );
        }

        for ( long sourceID : ingestIds.baselineIds() )
        {
            String[] row = new String[3];
            row[0] = projectId;
            row[1] = Long.toString( sourceID );
            row[2] = "baseline";
            values.add( row );
        }

        for ( long sourceID : ingestIds.covariateIds() )
        {
            String[] row = new String[3];
            row[0] = projectId;
            row[1] = Long.toString( sourceID );
            row[2] = "covariate";
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
                throw new IngestException( "Unexpected null left hash value for id="
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
     * @param declaration the project declaration
     * @param hashes the ingest hashes
     * @return the project declaration and whether this project inserted into the database
     */
    private static Pair<DatabaseProject, Boolean> getProject( Database database,
                                                              DatabaseCaches caches,
                                                              GriddedFeatures griddedFeatures,
                                                              EvaluationDeclaration declaration,
                                                              IngestHashes hashes )
    {
        Objects.requireNonNull( database );
        Objects.requireNonNull( caches );
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( hashes );

        String identity = Projects.getTopHashOfSources( hashes );

        DatabaseProject details = new DatabaseProject( database,
                                                       caches,
                                                       griddedFeatures,
                                                       declaration,
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
     * @throws IngestException When a null value is found in result set.
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
                    throw new IngestException( "Found a null value in db when expecting a value. idNull="
                                               + idNull
                                               + " hashNull="
                                               + hashNull );
                }
            }
        }
    }

    /**
     * <p>Get the list of surrogate keys from given ingest results.
     *
     * @param ingestResults The ingest results.
     * @param count a function that returns the count of ingest results
     * @return The ids for the baseline dataset
     */

    private static long[] getIds( List<IngestResult> ingestResults,
                                  ToIntFunction<IngestResult> count )
    {
        // How big to make the array? We don't want to guess because then we
        // would need to resize, which requires more heap again. Better to get
        // it correct at the outset.
        int sizeNeeded = 0;

        for ( IngestResult ingestResult : ingestResults )
        {
            sizeNeeded += count.applyAsInt( ingestResult );
        }

        long[] ids = new long[sizeNeeded];
        int i = 0;

        for ( IngestResult ingestResult : ingestResults )
        {
            for ( short j = 0; j < count.applyAsInt( ingestResult ); j++ )
            {
                ids[i] = ingestResult.getSurrogateKey();
                i++;
            }
        }

        return ids;
    }

    /**
     * <p>Creates a hash for the indicated project configuration based on its
     * data ingested.
     *
     * @param hashes the ingest hashes
     * @return a unique hash code for the project's circumstances
     */
    private static String getTopHashOfSources( IngestHashes hashes )
    {
        MessageDigest md5Digest;

        try
        {
            md5Digest = MessageDigest.getInstance( "MD5" );
        }
        catch ( NoSuchAlgorithmException nsae )
        {
            throw new IngestException( "Couldn't use MD5 algorithm.",
                                       nsae );
        }

        // Sort for deterministic hash result for same list of ingested
        Arrays.stream( hashes.leftHashes() )
              .sorted()
              .forEach( n -> DigestUtils.updateDigest( md5Digest, n ) );
        Arrays.stream( hashes.rightHashes() )
              .sorted()
              .forEach( n -> DigestUtils.updateDigest( md5Digest, n ) );
        Arrays.stream( hashes.baselineHashes() )
              .sorted()
              .forEach( n -> DigestUtils.updateDigest( md5Digest, n ) );
        Arrays.stream( hashes.covariateHashes() )
              .sorted()
              .forEach( n -> DigestUtils.updateDigest( md5Digest, n ) );

        byte[] digestAsHex = md5Digest.digest();
        return Hex.encodeHexString( digestAsHex );
    }

    /**
     * Record of ingest identifiers.
     * @param leftIds the left identifiers
     * @param rightIds the right identifiers
     * @param baselineIds the baseline identifiers
     * @param covariateIds the covariate identifiers
     */
    private record IngestIds( long[] leftIds, long[] rightIds, long[] baselineIds, long[] covariateIds ) {} // NOSONAR

    /**
     * Record of ingest hashes.
     * @param leftHashes the left hashes
     * @param rightHashes the right hashes
     * @param baselineHashes the baseline hashes
     * @param covariateHashes the covariate hashes
     */
    private record IngestHashes( String[] leftHashes, // NOSONAR
                                 String[] rightHashes,
                                 String[] baselineHashes,
                                 String[] covariateHashes ) {}

    /**
     * Do not construct.
     */
    private Projects()
    {
    }
}
