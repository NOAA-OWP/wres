package wres.io.reading.nwm;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.InterfaceShortHand;
import wres.config.generated.ProjectConfig;
import wres.datamodel.time.TimeSeries;
import wres.io.concurrency.TimeSeriesIngester;
import wres.io.data.caching.Features;
import wres.io.data.details.FeatureDetails;
import wres.io.reading.DataSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.system.DatabaseLockManager;
import wres.system.SystemSettings;


/**
 * Creates and ingests NWMTimeSeries for each NWM dataset declared in project.
 *
 * As of 2019-10-29, only supports vector non-ensemble data, and is not enabled
 * because it is not connected to the SourceLoader. When the SourceLoader takes
 * DataSourceConfig.Source interface shorthands into account and creates one of
 * these NWMReader instances, then it will be enabled.
 *
 * Requires limits on Issued Dates aka Reference Datetimes in the declaration to
 * know where to begin looking and to limit which URIs to generate.
 *
 * Requires the interface short-hand to be an "nwm" short-hand in order to look
 * up the NWMProfile in order to read profile information to read NWM data.
 *
 * Creates a single wres.Source for each actual TimeSeries found, i.e. one per
 * location per variable per timeseries.
 *
 * Does not read the whole netCDF blob into RAM, does not transfer it to disk.
 *
 * Does the same process for file and http locations.
 *
 * Assumes the NWM dataset layout and paths and data blob names are consistent
 * with what would be seen in NWM 2.0 on NOMADS, starting with the directory
 * that contains the directories named like "nwm.20191015". The URI in the
 * source must contain everything up to that directory, including the slash, and
 * not include any directories like "nwm.20191015". The full URIs accessed
 * will be generated based on the profile specified. Assumes no missing blobs.
 *
 */

public class NWMReader implements Callable<List<IngestResult>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( NWMReader.class );
    private static final String DATES_ERROR_MESSAGE =
            "One must specify issued dates with both earliest and latest (e.g. "
            + "<issuedDates earliest=\"2019-10-25T00:00:00Z\" "
            + "latest=\"2019-11-25T00:00:00Z\" />) when using NWM data as a "
            + "source, which will be interpreted as reference datetimes. The "
            + "earliest datetime specified must be a valid reference time for "
            + "the NWM data specified, or no data will be found.";

    private final ProjectConfig projectConfig;
    private final DataSource dataSource;
    private final DatabaseLockManager lockManager;
    private final NWMProfile nwmProfile;
    private final Instant earliest;
    private final Instant latest;
    private final ThreadPoolExecutor executor;
    private final BlockingQueue<Future<List<IngestResult>>> ingests;
    private final CountDownLatch startGettingResults;

    public NWMReader( ProjectConfig projectConfig,
                      DataSource dataSource,
                      DatabaseLockManager lockManager )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( lockManager );
        Objects.requireNonNull( dataSource.getSource() );
        Objects.requireNonNull( dataSource.getSource().getInterface() );
        Objects.requireNonNull( projectConfig.getPair() );
        Objects.requireNonNull( projectConfig.getPair()
                                             .getIssuedDates(),
                                DATES_ERROR_MESSAGE );
        Objects.requireNonNull( projectConfig.getPair()
                                             .getIssuedDates()
                                             .getEarliest(),
                                DATES_ERROR_MESSAGE );
        Objects.requireNonNull( projectConfig.getPair()
                                             .getIssuedDates()
                                             .getLatest(),
                                DATES_ERROR_MESSAGE );
        this.earliest = Instant.parse( projectConfig.getPair()
                                                       .getIssuedDates()
                                                       .getEarliest() );
        this.latest = Instant.parse( projectConfig.getPair()
                                                  .getIssuedDates()
                                                  .getLatest() );

        InterfaceShortHand interfaceShortHand = dataSource.getSource()
                                                          .getInterface();

        if ( !interfaceShortHand.toString()
                                .toLowerCase()
                                .startsWith( "nwm_" ) )
        {
            throw new IllegalArgumentException( "The data source passed does "
                                                + "not appear to be a NWM "
                                                + "source because the interface"
                                                + " shorthand "
                                                + interfaceShortHand
                                                + " did not start with 'nwm_'" );
        }

        this.projectConfig = projectConfig;
        this.dataSource = dataSource;
        this.lockManager = lockManager;
        this.nwmProfile = NWMProfiles.getProfileFromShortHand( interfaceShortHand );

        ThreadFactory nwmReaderThreadFactory = new BasicThreadFactory.Builder()
                .namingPattern( "NWMReader Ingest" )
                .build();

        // See comments in WebSource class regarding the setup of the executor,
        // queue, and latch.
        BlockingQueue<Runnable>
                nwmReaderQueue = new ArrayBlockingQueue<>( SystemSettings.getMaxiumNwmIngestThreads() );
        this.executor = new ThreadPoolExecutor( SystemSettings.getMaxiumNwmIngestThreads(),
                                                SystemSettings.getMaxiumNwmIngestThreads(),
                                                SystemSettings.poolObjectLifespan(),
                                                TimeUnit.MILLISECONDS,
                                                nwmReaderQueue,
                                                nwmReaderThreadFactory );

        this.executor.setRejectedExecutionHandler( new ThreadPoolExecutor.AbortPolicy() );
        this.ingests = new ArrayBlockingQueue<>( SystemSettings.getMaxiumNwmIngestThreads() );
        this.startGettingResults = new CountDownLatch( SystemSettings.getMaxiumNwmIngestThreads() );
    }

    private ProjectConfig getProjectConfig()
    {
        return this.projectConfig;
    }

    private DatabaseLockManager getLockManager()
    {
        return this.lockManager;
    }

    private DataSource getDataSource()
    {
        return this.dataSource;
    }

    private URI getUri()
    {
        return this.getDataSource()
                   .getUri();
    }

    private NWMProfile getNwmProfile()
    {
        return this.nwmProfile;
    }

    private ThreadPoolExecutor getExecutor()
    {
        return this.executor;
    }

    @Override
    public List<IngestResult> call() throws IOException
    {
        List<IngestResult> ingestResults = new ArrayList<>();
        Set<FeatureDetails> features;

        // Calling getAllDetails limits reading to only hardcoded wres.features.
        try
        {
            features = Features.getAllDetails( this.getProjectConfig() );
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Failed to get features/locations.", se );
        }

        // A list of featureIds that will be sorted in NWM id order to be used
        // to create blocks of sequential NWM ids.
        List<Integer> featureNwmIds = new ArrayList<>( features.size() );

        // A map from featureId back to the database feature row to get names.
        Map<Integer,FeatureDetails> featuresByNwmId = new HashMap<>( features.size() );

        for ( FeatureDetails feature : features )
        {
            Integer id = feature.getComid();

            // Skip features without a comid.
            if ( !Objects.isNull( id ) )
            {
                featureNwmIds.add( id );
                featuresByNwmId.put( id, feature );
            }
        }

        featureNwmIds.sort( null );
        LOGGER.debug( "Sorted featureNwmIds: {}", featureNwmIds );

        // Chunk reads by FEATURE_READ_COUNT
        final int FEATURE_READ_COUNT = 100;
        int maxCountOfBlocks = ( featureNwmIds.size()
                                 / FEATURE_READ_COUNT )
                               + 1;
        List<int[]> featureBlocks = new ArrayList<>( maxCountOfBlocks );

        int j = 0;
        int[] block;

        // The last block is unlikely to be exactly FEATURE_READ_COUNT
        int remaining = featureNwmIds.size();

        if ( remaining < FEATURE_READ_COUNT )
        {
            block = new int[remaining];
        }
        else
        {
            block = new int[FEATURE_READ_COUNT];
        }

        for ( int i = 0; i < featureNwmIds.size(); i++ )
        {
            if ( i % FEATURE_READ_COUNT == 0 )
            {
                LOGGER.debug( "Found we are at a boundary. i={}, j={}", i, j );
                // After the first block is written, add to list.
                if ( i > 0 )
                {
                    featureBlocks.add( block );
                }

                // The last block is unlikely to be exactly FEATURE_READ_COUNT
                remaining = featureNwmIds.size() - i;

                if ( remaining < FEATURE_READ_COUNT )
                {
                    LOGGER.debug( "Creating last int[] of size {}", remaining );
                    block = new int[remaining];
                }
                else
                {
                    LOGGER.debug( "Creating full sized int[] of size {}",
                                  FEATURE_READ_COUNT );
                    block = new int[FEATURE_READ_COUNT];
                }

                j = 0;
            }

            block[j] = featureNwmIds.get( i );
            j++;
        }

        // Add the last feature block
        featureBlocks.add( block );

        if ( LOGGER.isDebugEnabled() )
        {
            for ( int[] someBlock : featureBlocks )
            {
                LOGGER.debug( "Feature block: {}", someBlock );
            }
        }

        // Find the reference datetimes for the datasets.
        Set<Instant> referenceDatetimes = this.getReferenceDatetimes( this.earliest,
                                                                      this.latest,
                                                                      this.getNwmProfile() );
        LOGGER.debug( "Reference datetimes used for NWMReader {}: {}",
                      this, referenceDatetimes );

        try
        {
            // For each reference datetime, get the dataset for all locations.
            for ( Instant referenceDatetime : referenceDatetimes )
            {
                try( NWMTimeSeries nwmTimeSeries =
                             new NWMTimeSeries( this.getNwmProfile(),
                                                referenceDatetime,
                                                this.getUri() ) )
                {
                    for ( int[] featureBlock : featureBlocks )
                    {
                        String variableName = this.getDataSource()
                                                  .getVariable()
                                                  .getValue()
                                                  .strip();
                        String unitName = nwmTimeSeries.readAttributeAsString(
                                variableName,
                                "units" );
                        Map<Integer,TimeSeries<?>> values;

                        if ( this.getNwmProfile()
                                 .getMemberCount() == 1 )
                        {
                            values = nwmTimeSeries.readTimeSerieses( featureBlock,
                                                                     variableName );
                        }
                        else if ( this.getNwmProfile()
                                      .getMemberCount() > 1 )
                        {
                            values = nwmTimeSeries.readEnsembleTimeSerieses( featureBlock,
                                                                             variableName );
                        }
                        else
                        {
                            throw new UnsupportedOperationException( "Cannot read a timeseries with "
                                                                     + this.getNwmProfile()
                                                                           .getMemberCount()
                                                                     + " members." );
                        }

                        for ( Map.Entry<Integer,TimeSeries<?>> entry : values.entrySet() )
                        {
                            // Create a uri that reflects the origin of the data
                            URI uri = this.getUri()
                                          .resolve( this.getDataSource()
                                                        .getSource()
                                                        .getInterface()
                                                        .toString()
                                                    + "/"
                                                    + entry.getKey()
                                                    + "/"
                                                    + this.getDataSource()
                                                          .getVariable()
                                                          .getValue()
                                                    + "/"
                                                    + referenceDatetime.toString() );
                            DataSource innerDataSource =
                                    DataSource.of( this.getDataSource()
                                                       .getSource(),
                                                   this.getDataSource()
                                                       .getContext(),
                                                   this.getDataSource()
                                                       .getLinks(),
                                                   uri );

                            // Find the feature name (use lid as substitute)
                            String featureName = featuresByNwmId.get( entry.getKey() )
                                                                .getLid();

                            // While wres.source table is used, it is the reader level code
                            // that must deal with the wres.source table. Use the identifier
                            // of the timeseries data as if it were a wres.source.
                            TimeSeriesIngester ingester =
                                    new TimeSeriesIngester( this.getProjectConfig(),
                                                            innerDataSource,
                                                            this.getLockManager(),
                                                            entry.getValue(),
                                                            featureName,
                                                            variableName,
                                                            unitName );
                            Future<List<IngestResult>> future =
                                    this.getExecutor().submit( ingester );
                            this.ingests.add( future );
                            this.startGettingResults.countDown();

                            if ( this.startGettingResults.getCount() <= 0 )
                            {
                                List<IngestResult> ingested =
                                        this.ingests.take()
                                                    .get();
                                ingestResults.addAll( ingested );
                            }
                        }
                    }
                }
            }

            for ( Future<List<IngestResult>> ingested : this.ingests )
            {
                ingestResults.addAll( ingested.get() );
            }
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while reading and ingesting NWM data." );
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException ee )
        {
            throw new IngestException( "While reading and ingesting NWM data:",
                                       ee );
        }

        return Collections.unmodifiableList( ingestResults );
    }


    /**
     * Get the reference datetimes for the given boundaries and NWMProfile.
     * @param earliest The earliest reference datetime.
     * @param latest The latest reference datetime.
     * @param nwmProfile The NWMProfile to use.
     * @return The Set of reference datetimes.
     */

    private Set<Instant> getReferenceDatetimes( Instant earliest,
                                                Instant latest,
                                                NWMProfile nwmProfile )
    {
        Set<Instant> datetimes = new HashSet<>();
        Duration modelRunStep = nwmProfile.getDurationBetweenReferenceDatetimes();
        datetimes.add( earliest );
        Instant additionalForecastDatetime = earliest.plus( modelRunStep );

        while ( additionalForecastDatetime.isBefore( latest )
                || additionalForecastDatetime.equals( latest ) )
        {
            datetimes.add( additionalForecastDatetime );
            additionalForecastDatetime = additionalForecastDatetime.plus( modelRunStep );
        }

        return Collections.unmodifiableSet( datetimes );
    }

}
