package wres.io.reading.nwm;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.InterfaceShortHand;
import wres.config.generated.ProjectConfig;
import wres.datamodel.time.TimeSeries;
import wres.io.concurrency.TimeSeriesIngester;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.reading.DataSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;
import wres.system.SystemSettings;


/**
 * Creates and ingests NWMTimeSeries for each NWM dataset declared in project.
 *
 * As of 2019-12-11, supports most NWM vector single-valued and ensemble data.
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
 * will be generated based on the profile specified.
 *
 * Considers non-dense forecasts (e.g. missing blobs) to be an error, although
 * this probably can and should be relaxed to allow for one or more of:
 * 1. Skipping an empty NWM dataset, e.g. zero blobs exist for a reference time.
 * 2. Allowing a partial NWM timeseries if values for first N steps exist.
 * 3. Allowing a partial NWM timeseries if more than half of the data exist.
 *
 * Another improvement would be to allow reading of NWM feature ids not present
 * in the wres.Features table. The NWMTimeSeries class used to read the NWM data
 * has no assumption of connection to the wres.Features table.
 *
 * One-shot callable. Closes internal executor at end of first call().
 */

public class NWMReader implements Callable<List<IngestResult>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( NWMReader.class );
    private static final String DATES_ERROR_MESSAGE =
            "One must specify issued datetimes with both earliest and latest "
            + "(e.g. <issuedDates earliest=\"2019-10-25T00:00:00Z\" "
            + "latest=\"2019-11-25T00:00:00Z\" />) when using NWM forecast data"
            + " as a source, which will be interpreted as reference datetimes. "
            + "The earliest datetime specified must be a valid reference time "
            + "for the NWM data specified, or no data will be found. One must "
            + "specify valid datetimes with both earliest and latest "
            + "(e.g. <dates earliest=\"2019-12-11T00:00:00Z\" "
            + "latest=\"2019-12-12T00:00:00Z\" />) when using NWM analysis data"
            + " as a source, which will be used to find data falling within "
            +"those valid datetimes.";

    private final SystemSettings systemSettings;
    private final Database database;
    private final Features featuresCache;
    private final Variables variablesCache;
    private final Ensembles ensemblesCache;
    private final MeasurementUnits measurementUnitsCache;
    private final ProjectConfig projectConfig;
    private final DataSource dataSource;
    private final DatabaseLockManager lockManager;
    private final NWMProfile nwmProfile;
    private final Instant earliest;
    private final Instant latest;
    private final ThreadPoolExecutor executor;
    private final BlockingQueue<Future<List<IngestResult>>> ingests;
    private final CountDownLatch startGettingResults;
    private final URI baseUri;

    public NWMReader( SystemSettings systemSettings,
                      Database database,
                      Features featuresCache,
                      Variables variablesCache,
                      Ensembles ensemblesCache,
                      MeasurementUnits measurementUnitsCache,
                      ProjectConfig projectConfig,
                      DataSource dataSource,
                      DatabaseLockManager lockManager )
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( database );
        Objects.requireNonNull( featuresCache );
        Objects.requireNonNull( variablesCache );
        Objects.requireNonNull( ensemblesCache );
        Objects.requireNonNull( measurementUnitsCache );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( lockManager );
        Objects.requireNonNull( dataSource.getSource() );
        Objects.requireNonNull( dataSource.getSource().getInterface() );
        Objects.requireNonNull( dataSource.getSource().getValue() );

        this.systemSettings = systemSettings;
        this.database = database;
        this.featuresCache = featuresCache;
        this.variablesCache = variablesCache;
        this.ensemblesCache = ensemblesCache;
        this.measurementUnitsCache = measurementUnitsCache;

        URI literalUri = dataSource.getSource()
                                   .getValue();

        if ( literalUri.isAbsolute() )
        {
            this.baseUri = literalUri;
        }
        else
        {
            URI resolvedUri = systemSettings.getDataDirectory()
                                            .resolve( literalUri.getPath() )
                                            .toUri();
            LOGGER.debug( "Transformed relative URI {} to URI {}.",
                          literalUri,
                          resolvedUri );
            this.baseUri = resolvedUri;
        }

        InterfaceShortHand interfaceShortHand = dataSource.getSource()
                                                          .getInterface();
        this.nwmProfile = NWMProfiles.getProfileFromShortHand( interfaceShortHand );
        Objects.requireNonNull( projectConfig.getPair() );

        if ( this.nwmProfile.getTimeLabel()
                            .equals( NWMProfile.TimeLabel.f ) )
        {
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
        }
        else if ( this.nwmProfile.getTimeLabel()
                                 .equals( NWMProfile.TimeLabel.tm ) )
        {
            Objects.requireNonNull( projectConfig.getPair()
                                                 .getDates(),
                                    DATES_ERROR_MESSAGE );
            Objects.requireNonNull( projectConfig.getPair()
                                                 .getDates()
                                                 .getEarliest(),
                                    DATES_ERROR_MESSAGE );
            Objects.requireNonNull( projectConfig.getPair()
                                                 .getDates()
                                                 .getLatest(),
                                    DATES_ERROR_MESSAGE );
        }

        // When we have analysis data, extend out the reference datetimes in
        // order to include data by valid datetime, kind of like observations.
        if ( this.nwmProfile.getTimeLabel()
                            .equals( NWMProfile.TimeLabel.f ) )
        {
            this.earliest = Instant.parse( projectConfig.getPair()
                                                        .getIssuedDates()
                                                        .getEarliest() );
            this.latest = Instant.parse( projectConfig.getPair()
                                                      .getIssuedDates()
                                                      .getLatest() );
        }
        else if ( this.nwmProfile.getTimeLabel()
                                 .equals( NWMProfile.TimeLabel.tm ) )
        {
            Instant earliestValidDatetime =
                    Instant.parse( projectConfig.getPair()
                                                .getDates()
                                                .getEarliest() );
            Instant latestValidDatetime =
                    Instant.parse( projectConfig.getPair()
                                                .getDates()
                                                .getLatest() );
            Pair<Instant,Instant> referenceBounds =
                    getReferenceBoundsByValidBounds( this.nwmProfile,
                                                     earliestValidDatetime,
                                                     latestValidDatetime );
            this.earliest = referenceBounds.getLeft();
            this.latest = referenceBounds.getRight();
        }
        else
        {
            throw new UnsupportedOperationException( "Unable to read NWM data with label "
                                                     + this.nwmProfile.getTimeLabel() );
        }

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

        ThreadFactory nwmReaderThreadFactory = new BasicThreadFactory.Builder()
                .namingPattern( "NWMReader Ingest %d" )
                .build();

        // See comments in WebSource class regarding the setup of the executor,
        // queue, and latch.
        BlockingQueue<Runnable>
                nwmReaderQueue = new ArrayBlockingQueue<>( systemSettings.getMaxiumNwmIngestThreads() );
        this.executor = new ThreadPoolExecutor( systemSettings.getMaxiumNwmIngestThreads(),
                                                systemSettings.getMaxiumNwmIngestThreads(),
                                                systemSettings.poolObjectLifespan(),
                                                TimeUnit.MILLISECONDS,
                                                nwmReaderQueue,
                                                nwmReaderThreadFactory );

        this.executor.setRejectedExecutionHandler( new ThreadPoolExecutor.AbortPolicy() );
        this.ingests = new ArrayBlockingQueue<>( systemSettings.getMaxiumNwmIngestThreads() );
        this.startGettingResults = new CountDownLatch( systemSettings.getMaxiumNwmIngestThreads() );
    }

    private SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }

    private Database getDatabase()
    {
        return this.database;
    }

    private Features getFeaturesCache()
    {
        return this.featuresCache;
    }

    private Variables getVariablesCache()
    {
        return this.variablesCache;
    }

    private Ensembles getEnsemblesCache()
    {
        return this.ensemblesCache;
    }

    private MeasurementUnits getMeasurementUnitsCache()
    {
        return this.measurementUnitsCache;
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
        return this.baseUri;
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
        try
        {
            return this.ingest();
        }
        finally
        {
            this.shutdownNow();
        }
    }

    private List<IngestResult> ingest() throws IngestException
    {
        List<IngestResult> ingestResults = new ArrayList<>();
        Set<String> features = ConfigHelper.getFeatureNamesForSource( this.getProjectConfig(),
                                                                      this.getDataSource()
                                                                          .getContext() );

        // A list of featureIds that will be sorted in NWM id order to be used
        // to create blocks of sequential NWM ids.
        List<Integer> featureNwmIds = new ArrayList<>( features.size() );

        for ( String feature : features )
        {
            try
            {
                Integer id = Integer.parseUnsignedInt( feature );
                featureNwmIds.add( id );
            }
            catch ( NumberFormatException nfe )
            {
                LOGGER.warn( "Skipping non-integer NWM feature ID {} due to {}",
                             feature, nfe.getMessage() );
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
                             new NWMTimeSeries( this.getSystemSettings(),
                                                this.getNwmProfile(),
                                                referenceDatetime,
                                                this.getUri() ) )
                {
                    if ( nwmTimeSeries.countOfNetcdfFiles() <= 0 )
                    {
                        LOGGER.debug( "Found an empty TimeSeries, skipping {}",
                                      nwmTimeSeries );
                        continue;
                    }

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
                                                                     variableName,
                                                                     unitName );
                        }
                        else if ( this.getNwmProfile()
                                      .getMemberCount() > 1 )
                        {
                            values = nwmTimeSeries.readEnsembleTimeSerieses( featureBlock,
                                                                             variableName,
                                                                             unitName );
                        }
                        else
                        {
                            throw new UnsupportedOperationException( "Cannot read a timeseries with "
                                                                     + this.getNwmProfile()
                                                                           .getMemberCount()
                                                                     + " members." );
                        }

                        // Skip ingest steps when resulting timeseries is empty.
                        if ( values.isEmpty() )
                        {
                            LOGGER.debug( "Found an empty TimeSeries for NWM features {}, skipping {}",
                                          featureBlock,
                                          nwmTimeSeries );
                            continue;
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

                            // While wres.source table is used, it is the reader level code
                            // that must deal with the wres.source table. Use the identifier
                            // of the timeseries data as if it were a wres.source.
                            TimeSeriesIngester ingester =
                                    TimeSeriesIngester.of( this.getSystemSettings(),
                                                           this.getDatabase(),
                                                           this.getFeaturesCache(),
                                                           this.getVariablesCache(),
                                                           this.getEnsemblesCache(),
                                                           this.getMeasurementUnitsCache(),
                                                           this.getProjectConfig(),
                                                           innerDataSource,
                                                           this.getLockManager(),
                                                           entry.getValue() );
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
     *
     * As of 2020-08-04, there are datasets that have reference datetimes one
     * duration after T00Z but another duration between reference datetimes.
     * The profile now distinguishes between these two.
     *
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
        Duration issuedStep = nwmProfile.getDurationBetweenReferenceDatetimes();
        
        //Simply truncate earliest to be at time 0 for the earliest
        //day, that should be a good starting point for finding the first 
        //NWM forecast reference strictly after the provided earliest Instant.
        // Then add the duration past midnight of the first forecast.
        Instant forecastDatetime = earliest.truncatedTo( ChronoUnit.DAYS )
                                           .plus( nwmProfile.getDurationPastMidnight() );

        while ( !forecastDatetime.isAfter( earliest ) )
        {
            forecastDatetime = forecastDatetime.plus( issuedStep );
        }

        LOGGER.debug( "Resolved earliest {} issued datetime given to first forecast at {}",
                      earliest, forecastDatetime );
        datetimes.add( forecastDatetime );
        Instant additionalForecastDatetime = forecastDatetime.plus( issuedStep );

        while ( !additionalForecastDatetime.isAfter( latest ) )
        {
            datetimes.add( additionalForecastDatetime );
            additionalForecastDatetime = additionalForecastDatetime.plus( issuedStep );
        }

        return Collections.unmodifiableSet( datetimes );
    }


    /**
     * Creates the values needed to call getReferenceDatetimes when wanting to
     * ingest all the possible data with valid datetimes within given bounds.
     *
     * Useful for ingesting analysis data based on the valid datetimes rather
     * than on reference datetime ranges.
     *
     * @param nwmProfile A profile to use to calculate the new ranges.
     * @param earliestValidDatetime The earliest valid datetime to include.
     * @param latestValidDatetime The latest valid datetime to include.
     * @return The boundaries to be used to filter by reference datetimes.
     */
    private Pair<Instant,Instant> getReferenceBoundsByValidBounds( NWMProfile nwmProfile,
                                                                   Instant earliestValidDatetime,
                                                                   Instant latestValidDatetime )
    {
        Instant earliestReferenceDatetime = earliestValidDatetime;
        Instant latestReferenceDatetime = latestValidDatetime;
        Duration betweenBlobs = nwmProfile.getDurationBetweenValidDatetimes();
        int maxBlobsAwayFromReference = nwmProfile.getBlobCount();

        // Figure the furthest away from the reference datetime based on count.
        Duration toExtend = betweenBlobs.multipliedBy( maxBlobsAwayFromReference );

        // When the time label is "f", assume the blobs extend into future,
        // therefore we need to include earlier reference datetimes to get
        // all the valid datetimes.
        if ( nwmProfile.getTimeLabel().equals( NWMProfile.TimeLabel.f ) )
        {
            earliestReferenceDatetime = earliestValidDatetime.minus( toExtend );
        }
        else if ( nwmProfile.getTimeLabel().equals( NWMProfile.TimeLabel.tm ) )
        {
            // When the time label is "tm", assume the blobs extend into past,
            // therefore we need to include later reference datetimes to get
            // all the valid datetimes.
            latestReferenceDatetime = latestValidDatetime.plus( toExtend );
        }
        else
        {
            // In case another time label arrives, tolerate it by extending
            // in both directions, past and future, ingesting more data but
            // having a better chance of including all the desired data.
            earliestReferenceDatetime = earliestValidDatetime.minus( toExtend );
            latestReferenceDatetime = latestValidDatetime.plus( toExtend );
        }

        return Pair.of( earliestReferenceDatetime, latestReferenceDatetime );
    }

    /**
     * Shut down this instance's executor(s) to prevent Thread leaks.
     */

    private void shutdownNow()
    {
        List<Runnable> incompleteTasks = this.executor.shutdownNow();

        // An exception should already be propagating if the following is true.
        if ( !incompleteTasks.isEmpty() && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "Failed to complete {} ingest tasks associated with {}",
                         incompleteTasks.size(),
                         this.getDataSource().getUri() );
        }
    }
}
