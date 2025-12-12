package wres.io.ingesting.database;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.components.DataType;
import wres.datamodel.types.Ensemble;
import wres.datamodel.MissingValues;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.io.database.DatabaseOperations;
import wres.io.database.caching.DatabaseCaches;
import wres.io.database.caching.DataSources;
import wres.io.database.caching.Ensembles;
import wres.io.database.caching.Features;
import wres.io.database.caching.MeasurementUnits;
import wres.io.database.details.SourceCompletedDetails;
import wres.io.database.details.SourceDetails;
import wres.io.database.Database;
import wres.io.ingesting.IngestException;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.database.caching.TimeScales;
import wres.reading.DataSource;
import wres.reading.TimeSeriesTuple;
import wres.io.database.locking.DatabaseLockManager;
import wres.system.SystemSettings;
import wres.reading.netcdf.Netcdf;

import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * <p>Ingests given {@link TimeSeries} data if not already ingested.
 *
 * <p>As of 2019-10-29, only supports data with one or more reference datetime.
 * <p>As of 2019-10-29, uses {@link TimeSeries#toString()} to identify data.
 *
 * <p>Identifies data for convenience (and parallelism).
 *
 * <p>Different source types have different optimization strategies for identifying data and avoiding ingest,
 * therefore those can avoid ingest at a higher level using different strategies than this code. This class always
 * checks if an individual timeseries has been ingested prior to ingesting it, regardless of higher level checks.
 *
 * <p>Does not link data, leaves that to higher level code to do later. In this context, link means to associate a
 * dataset with all of the sides/orientations in which it appears. For example, a left-ish dataset may be used to
 * generate a baseline time-series and, therefore, appear in two contexts, left and baseline.
 *
 * <p>Does not preclude the skipping of ingest at a higher level.
 *
 * @author James Brown
 * @author Jesse Bickel
 */

public class DatabaseTimeSeriesIngester implements TimeSeriesIngester
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger( DatabaseTimeSeriesIngester.class );

    /** The maximum number of ingest retries. */
    private static final int MAXIMUM_RETRIES = 100;

    /** The number of ingest retries after which to begin backoff. */
    private static final int BACKOFF_RETRIES = 10;

    /** The default key value for a data source when not discovered in the cache. */
    private static final long KEY_NOT_FOUND = Long.MIN_VALUE;

    /** Level of patience in waiting for ingest to be marked complete. */
    private static final Duration PATIENCE_LEVEL = Duration.ofMinutes( 30 );

    /** System settings. */
    private final SystemSettings systemSettings;

    /** Database. */
    private final Database database;

    /** Database ORMs/caches. */
    private final DatabaseCaches caches;

    /** Database lock manager. */
    private final DatabaseLockManager lockManager;

    /** A thread pool to process ingests. */
    private final ExecutorService executor;

    /**
     * Builds an instance incrementally.
     */

    public static class Builder
    {
        private SystemSettings systemSettings;
        private Database database;
        private DatabaseCaches caches;
        private DatabaseLockManager lockManager;
        private ExecutorService executor;

        /**
         * @param systemSettings the system settings to set
         * @return the builder
         */
        public Builder setSystemSettings( SystemSettings systemSettings )
        {
            this.systemSettings = systemSettings;
            return this;
        }

        /**
         * @param database the database to set
         * @return the builder
         */
        public Builder setDatabase( Database database )
        {
            this.database = database;
            return this;
        }

        /**
         * @param caches the caches to set
         * @return the builder
         */
        public Builder setCaches( DatabaseCaches caches )
        {
            this.caches = caches;
            return this;
        }

        /**
         * @param lockManager the lock manager to set
         * @return the builder
         */
        public Builder setLockManager( DatabaseLockManager lockManager )
        {
            this.lockManager = lockManager;
            return this;
        }

        /**
         * @param executor the executor used to ingest data
         * @return the builder
         */
        public Builder setIngestExecutor( ExecutorService executor )
        {
            this.executor = executor;
            return this;
        }

        /**
         * @return a time-series ingester instance
         */
        public DatabaseTimeSeriesIngester build()
        {
            return new DatabaseTimeSeriesIngester( this );
        }
    }

    @Override
    public List<IngestResult> ingest( Stream<TimeSeriesTuple> timeSeriesTuple, DataSource outerSource )
    {
        Objects.requireNonNull( timeSeriesTuple );
        Objects.requireNonNull( outerSource );

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Ingesting time-series from source {}.", outerSource );
        }

        // If this is a gridded dataset, it has special treatment, inserting the sources only. This will disappear
        // if/when gridded ingest is normalized with other ingest: see #51232.
        if ( outerSource.isGridded() )
        {
            return this.ingestGriddedData( outerSource );
        }

        // A queue of ingest tasks
        BlockingQueue<Future<List<IngestResult>>> ingestQueue =
                new ArrayBlockingQueue<>( this.systemSettings.getMaximumIngestThreads() );

        // Start to get results before the ingest queue overflows
        CountDownLatch startGettingResults = new CountDownLatch( this.getSystemSettings()
                                                                     .getMaximumIngestThreads() );

        List<IngestResult> finalResults = new ArrayList<>();

        // Read one time-series into memory at a time, closing the stream on completion
        try ( timeSeriesTuple )
        {
            Iterator<TimeSeriesTuple> tupleIterator = timeSeriesTuple.iterator();
            int timeSeriesCount = 0;

            while ( tupleIterator.hasNext() )
            {
                TimeSeriesTuple nextTuple = tupleIterator.next();
                DataSource innerSource = nextTuple.getDataSource();

                // Log the read
                this.logRead( innerSource.uri() );

                // Single-valued time-series?
                if ( nextTuple.hasSingleValuedTimeSeries() )
                {
                    TimeSeries<Double> nextSeries =
                            this.checkForEmptySeriesAndAddReferenceTimeIfRequired( nextTuple.getSingleValuedTimeSeries(),
                                                                                   innerSource.uri() );
                    String timeSeriesId = this.identifyTimeSeries( nextSeries );

                    Future<List<IngestResult>> innerResults =
                            this.getExecutor()
                                .submit( () -> this.ingestSingleValuedTimeSeriesWithRetries( nextSeries,
                                                                                             timeSeriesId,
                                                                                             innerSource ) );

                    // Add the future ingest results to the ingest queue
                    ingestQueue.add( innerResults );
                    startGettingResults.countDown();
                    timeSeriesCount++;
                }

                // Ensemble time-series?
                if ( nextTuple.hasEnsembleTimeSeries() )
                {
                    TimeSeries<Ensemble> nextSeries =
                            this.checkForEmptySeriesAndAddReferenceTimeIfRequired( nextTuple.getEnsembleTimeSeries(),
                                                                                   innerSource.uri() );
                    String timeSeriesId = this.identifyTimeSeries( nextSeries );
                    Future<List<IngestResult>> innerResults =
                            this.getExecutor()
                                .submit( () -> this.ingestEnsembleTimeSeriesWithRetries( nextSeries,
                                                                                         timeSeriesId,
                                                                                         innerSource ) );

                    // Add the future ingest results to the ingest queue
                    ingestQueue.add( innerResults );
                    startGettingResults.countDown();
                    timeSeriesCount++;
                }

                // Start to get ingest results?
                if ( startGettingResults.getCount() <= 0 )
                {
                    Future<List<IngestResult>> futureResult = ingestQueue.poll();
                    if ( Objects.nonNull( futureResult ) )
                    {
                        finalResults.addAll( futureResult.get() );
                    }
                }
            }

            // Get any remaining results
            for ( Future<List<IngestResult>> next : ingestQueue )
            {
                List<IngestResult> nextResults = next.get();
                finalResults.addAll( nextResults );
            }

            // Log the count of series attempted
            this.logTimeSeriesCount( timeSeriesCount, outerSource );

            return Collections.unmodifiableList( finalResults );
        }
        catch ( ExecutionException e )
        {
            throw new IngestException( "Failed to get ingest results.", e );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread()
                  .interrupt();
            throw new IngestException( "Interrupted while getting ingest results.", e );
        }
    }

    /**
     * Logs the number of time-series from a data source for which ingest was attempted.
     * @param timeSeriesCount the time-series count
     * @param outerSource the data source
     */
    private void logTimeSeriesCount( int timeSeriesCount, DataSource outerSource )
    {
        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Attempted to ingest {} timeseries from {} into the database.",
                          timeSeriesCount,
                          outerSource );
        }
    }

    /**
     * Logs a read.
     * @param uri the URI that was read.
     */

    private void logRead( URI uri )
    {
        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Read a time-series from {}.", uri );
        }
    }

    /**
     * Database ingest currently requires at least one reference time. If none exists, add a default one. Also, check 
     * that the time-series is not empty and throw an exception if it is empty.
     * @param timeSeries the time-series to check
     * @param uri the source URI to help with error messaging
     * @return a time-series with at least one reference time
     * @throws IngestException if the time-series is empty
     */

    private <T> TimeSeries<T> checkForEmptySeriesAndAddReferenceTimeIfRequired( TimeSeries<T> timeSeries, URI uri )
    {
        if ( TimeSeriesSlicer.canInferValidTimeFromReferenceTimeAndLeadDuration( timeSeries.getReferenceTimes()
                                                                                           .keySet() ) )
        {
            return timeSeries;
        }

        if ( timeSeries.getEvents()
                       .isEmpty() )
        {
            throw new IngestException( "While ingesting source " + uri
                                       + ", discovered an empty time-series, which cannot be ingested. The empty "
                                       + "time-series is: "
                                       + timeSeries
                                       + "." );
        }

        ReferenceTimeType type = ReferenceTimeType.UNKNOWN;
        Instant value = timeSeries.getEvents()
                                  .last()
                                  .getTime();

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Discovered a time-series with zero reference times. Adding a default reference time of type "
                          + "{} with value {} for database ingest. The time-series metadata was: {}.",
                          type,
                          value,
                          timeSeries.getMetadata() );
        }

        // Add an arbitrary reference time of unknown type until the database schema accepts zero or more times
        Map<ReferenceTimeType, Instant> defaultReferenceTime = Map.of( type, value );

        TimeSeriesMetadata adjusted = timeSeries.getMetadata()
                                                .toBuilder()
                                                .setReferenceTimes( defaultReferenceTime )
                                                .build();

        return TimeSeries.of( adjusted, timeSeries.getEvents() );
    }

    /**
     * Ingests a time-series whose events are {@link Double}. Retries, as necessary, using exponential back-off, up to
     * the {@link #MAXIMUM_RETRIES}.
     *
     * @param timeSeries the time-series to ingest, not null
     * @param timeSeriesId the time-series identity or content hash
     * @param dataSource the data source
     * @return the ingest results
     */

    private List<IngestResult> ingestSingleValuedTimeSeriesWithRetries( TimeSeries<Double> timeSeries,
                                                                        String timeSeriesId,
                                                                        DataSource dataSource )
    {
        long sleepMillis = 1000;
        for ( int i = 0; i <= DatabaseTimeSeriesIngester.MAXIMUM_RETRIES; i++ )
        {
            List<IngestResult> results = this.ingestSingleValuedTimeSeries( timeSeries, timeSeriesId, dataSource );

            // Success
            if ( this.isIngestComplete( results ) )
            {
                if ( i > 0 )
                {
                    LOGGER.debug( "Successfully ingested a time-series from {} on attempt {} of {} in thread '{}'.",
                                  dataSource,
                                  i + 1,
                                  DatabaseTimeSeriesIngester.MAXIMUM_RETRIES,
                                  Thread.currentThread()
                                        .getName() );
                }

                LOGGER.trace( "Successfully ingested a time-series from {} in thread '{}'.",
                              dataSource,
                              Thread.currentThread()
                                    .getName() );

                return results;
            }

            sleepMillis = this.sleepBetweenRetries( sleepMillis, i, dataSource, timeSeries, timeSeriesId );
        }

        throw new IngestException( "Failed to ingest a time-series after " + DatabaseTimeSeriesIngester.MAXIMUM_RETRIES
                                   + " attempts. The time-series metadata was: "
                                   + timeSeries.getMetadata()
                                   + "." );
    }

    /**
     * Ingests a time-series whose events are {@link Double}.
     * @param timeSeries the time-series to ingest, not null
     * @param timeSeriesId the time-series identity or content hash
     * @param dataSource the data source
     * @return the ingest results
     */

    private List<IngestResult> ingestSingleValuedTimeSeries( TimeSeries<Double> timeSeries,
                                                             String timeSeriesId,
                                                             DataSource dataSource )
    {
        Objects.requireNonNull( timeSeries );
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( timeSeries.getMetadata() );
        Objects.requireNonNull( timeSeries.getMetadata()
                                          .getFeature() );
        Objects.requireNonNull( timeSeries.getMetadata()
                                          .getVariableName() );
        Objects.requireNonNull( timeSeries.getMetadata()
                                          .getUnit() );

        List<IngestResult> results;

        LOGGER.debug( "Ingesting a single-valued time-series from source {}.", dataSource.uri() );

        // Try to insert a row into wres.Source for the time-series
        SourceDetails source = this.saveTimeSeriesSource( timeSeries, timeSeriesId, dataSource.uri() );

        DataType dataType = TimeSeriesSlicer.getDataType( timeSeries );

        // The source identifier may be null if this thread did not perform the insert AND another thread deleted the
        // existing source in the interim, as the source is not yet locked. See #116551. To mitigate, return a retry
        // request
        if ( Objects.isNull( source.getId() ) )
        {
            LOGGER.debug( "Discovered a source {} without a source_id. This may occur if another thread deleted the "
                          + "source while this thread was attempting ingest it. Returning a retry request.",
                          source.getHash() );

            // Return a retry request with a fake surrogate key
            return IngestResult.singleItemListFrom( dataSource,
                                                    dataType,
                                                    0,
                                                    true,
                                                    true,
                                                    TimeSeriesSlicer.hasNonMissingValues( timeSeries ) );
        }

        DatabaseLockManager innerLockManager = this.getLockManager();

        // Inserted?
        if ( source.performedInsert() )
        {
            // Try to lock source with an advisory lock
            if ( this.lockSource( source, timeSeries, innerLockManager ) )
            {
                try
                {
                    // Advisory locked, so go ahead and create
                    results = this.createNewSingleValuedSource( timeSeries, source, dataSource );
                }
                // Lock succeeded, so unlock
                finally
                {
                    this.unlockSource( source, innerLockManager );
                }
            }
            else
            {
                LOGGER.debug( "Detected a data source that is locked in another task, {}. Will retry later.",
                              dataSource );

                // Busy, retry again later
                results = IngestResult.singleItemListFrom( dataSource,
                                                           dataType,
                                                           source.getId(),
                                                           true,
                                                           true,
                                                           TimeSeriesSlicer.hasNonMissingValues( timeSeries ) );
            }
        }
        // Source was not inserted by this thread, so this is an existing source. But was it completed or abandoned?
        else
        {
            results = this.finalizeExistingSource( source,
                                                   dataType,
                                                   dataSource,
                                                   TimeSeriesSlicer.hasNonMissingValues( timeSeries ) );
        }

        return results;
    }

    /**
     * Inserts a single-valued time-series into the database. The caller is responsible for locking.
     * @param timeSeries the time-series
     * @param source the source/ORM
     * @param dataSource the raw data source
     * @return the ingest results
     */

    private List<IngestResult> createNewSingleValuedSource( TimeSeries<Double> timeSeries,
                                                            SourceDetails source,
                                                            DataSource dataSource )
    {
        DataType dataType = TimeSeriesSlicer.getDataType( timeSeries );

        Set<Pair<CountDownLatch, CountDownLatch>> latches =
                this.insertSingleValuedTimeSeries( this.getSystemSettings(),
                                                   this.getDatabase(),
                                                   this.getCaches()
                                                       .getEnsemblesCache(),
                                                   timeSeries,
                                                   source.getId() );

        // Finalize, which marks the source complete
        return this.finalizeNewSource( source,
                                       dataType,
                                       latches,
                                       dataSource,
                                       TimeSeriesSlicer.hasNonMissingValues( timeSeries ) );
    }

    /**
     * Ingests a time-series whose events are {@link Ensemble}. Retries, as necessary, using exponential back-off, up to
     * the {@link #MAXIMUM_RETRIES}.
     *
     * @param timeSeries the time-series to ingest, not null
     * @param timeSeriesId the time-series identity or content hash
     * @param dataSource the data source
     * @return the ingest results
     */

    private List<IngestResult> ingestEnsembleTimeSeriesWithRetries( TimeSeries<Ensemble> timeSeries,
                                                                    String timeSeriesId,
                                                                    DataSource dataSource )
    {
        long sleepMillis = 1000;
        for ( int i = 0; i <= DatabaseTimeSeriesIngester.MAXIMUM_RETRIES; i++ )
        {
            List<IngestResult> result = this.ingestEnsembleTimeSeries( timeSeries, timeSeriesId, dataSource );

            // Success
            if ( this.isIngestComplete( result ) )
            {
                if ( i > 0 )
                {
                    LOGGER.debug( "Successfully ingested a time-series from {} on attempt {} of {} in thread '{}'.",
                                  dataSource,
                                  i + 1,
                                  DatabaseTimeSeriesIngester.MAXIMUM_RETRIES,
                                  Thread.currentThread()
                                        .getName() );
                }

                LOGGER.trace( "Successfully ingested a time-series from {} in thread '{}'.",
                              dataSource,
                              Thread.currentThread()
                                    .getName() );

                return result;
            }

            sleepMillis = this.sleepBetweenRetries( sleepMillis, i, dataSource, timeSeries, timeSeriesId );
        }

        throw new IngestException( "Failed to ingest a time-series after " + DatabaseTimeSeriesIngester.MAXIMUM_RETRIES
                                   + " attempts. The time-series metadata was: "
                                   + timeSeries.getMetadata()
                                   + "." );
    }

    /**
     * Ingests a time-series whose events are {@link Ensemble}.
     * @param timeSeries the time-series to ingest, not null
     * @param timeSeriesId the time-series identity or content hash
     * @param dataSource the data source
     * @return the ingest results
     */

    private List<IngestResult> ingestEnsembleTimeSeries( TimeSeries<Ensemble> timeSeries,
                                                         String timeSeriesId,
                                                         DataSource dataSource )
    {
        Objects.requireNonNull( timeSeries );
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( timeSeries.getMetadata() );
        Objects.requireNonNull( timeSeries.getMetadata()
                                          .getFeature() );
        Objects.requireNonNull( timeSeries.getMetadata()
                                          .getVariableName() );
        Objects.requireNonNull( timeSeries.getMetadata()
                                          .getUnit() );

        List<IngestResult> results;

        LOGGER.debug( "Ingesting an ensemble time-series from source {}.", dataSource.uri() );

        // Try to insert a row into wres.Source for the time-series
        SourceDetails source = this.saveTimeSeriesSource( timeSeries, timeSeriesId, dataSource.uri() );

        DataType dataType = TimeSeriesSlicer.getDataType( timeSeries );

        // The source identifier may be null if this thread did not perform the insert AND another thread deleted the
        // existing source in the interim, as the source is not yet locked. See #116551. To mitigate, return a retry
        // request
        if ( Objects.isNull( source.getId() ) )
        {
            LOGGER.debug( "Discovered a source {} without a source_id. This may occur if another thread deleted the "
                          + "source while this thread was attempting to ingest it. Returning a retry request.",
                          source.getHash() );

            // Return a retry request with a fake surrogate key
            return IngestResult.singleItemListFrom( dataSource,
                                                    dataType,
                                                    0,
                                                    true,
                                                    true,
                                                    TimeSeriesSlicer.hasNonMissingEnsembleMemberValues( timeSeries ) );
        }

        DatabaseLockManager innerLockManager = this.getLockManager();

        // Inserted?
        if ( source.performedInsert() )
        {
            // Try to lock source with an advisory lock
            if ( this.lockSource( source, timeSeries, innerLockManager ) )
            {
                try
                {
                    // Advisory locked, so go ahead and create
                    results = this.createNewEnsembleSource( timeSeries, source, dataSource );
                }
                // Lock succeeded, so unlock
                finally
                {
                    this.unlockSource( source, innerLockManager );
                }
            }
            else
            {
                LOGGER.debug( "Detected a data source that is locked in another task, {}. Will retry later.",
                              dataSource );

                // Busy, retry again later
                boolean nonMissingValues = TimeSeriesSlicer.hasNonMissingEnsembleMemberValues( timeSeries );
                results = IngestResult.singleItemListFrom( dataSource,
                                                           dataType,
                                                           source.getId(),
                                                           true,
                                                           true,
                                                           nonMissingValues );
            }
        }
        // Source was not inserted by this thread, so this is an existing source. But was it completed or abandoned?
        else
        {
            results = this.finalizeExistingSource( source,
                                                   dataType,
                                                   dataSource,
                                                   TimeSeriesSlicer.hasNonMissingEnsembleMemberValues( timeSeries ) );
        }

        return results;
    }

    /**
     * Inserts a new ensemble time-series into the database. The caller is responsible for locking.
     * @param timeSeries the time-series
     * @param source the source/ORM
     * @param dataSource the raw data source
     * @return the ingest results
     */

    private List<IngestResult> createNewEnsembleSource( TimeSeries<Ensemble> timeSeries,
                                                        SourceDetails source,
                                                        DataSource dataSource )
    {
        DataType dataType = TimeSeriesSlicer.getDataType( timeSeries );

        Set<Pair<CountDownLatch, CountDownLatch>> latches =
                this.insertEnsembleTimeSeries( this.getSystemSettings(),
                                               this.getDatabase(),
                                               this.getCaches()
                                                   .getEnsemblesCache(),
                                               timeSeries,
                                               source.getId() );

        boolean nonMissing = TimeSeriesSlicer.hasNonMissingEnsembleMemberValues( timeSeries );

        // Finalize, which marks the source complete
        return this.finalizeNewSource( source, dataType, latches, dataSource, nonMissing );
    }

    /**
     * @return whether {@link IngestResult#requiresRetry()} returns {@code false} for all ingest results.
     */

    private boolean isIngestComplete( List<IngestResult> results )
    {
        return results.stream()
                      .noneMatch( IngestResult::requiresRetry );
    }

    /**
     * Sleeps between ingest retries.
     * @param sleepMillis the number of milliseconds to sleep
     * @param retryNumber the retry number
     * @param dataSource the data source
     * @param timeSeries the time-series
     * @param timeSeriesId the time-series identity or content hash
     * @return the adjusted sleep time for the next retry
     * @param <T> the time-series data type
     */
    private <T> long sleepBetweenRetries( long sleepMillis,
                                          int retryNumber,
                                          DataSource dataSource,
                                          TimeSeries<T> timeSeries,
                                          String timeSeriesId )
    {
        if ( retryNumber <= BACKOFF_RETRIES )
        {
            LOGGER.debug( "Retrying without backoff as the retry number is {}, which is less than or equal to the "
                          + "number of retries after which backoff should begin ({}).", retryNumber,
                          BACKOFF_RETRIES );

            return sleepMillis;
        }

        LOGGER.warn( "Encountered an unusually large number of retries while attempting to ingest a time-series into "
                     + "the database. This is retry {} of {} for time-series {}. The remaining retries will experience "
                     + "exponential backoff, which may slow down ingest.",
                     retryNumber,
                     MAXIMUM_RETRIES,
                     timeSeriesId );

        long newMillis = sleepMillis;
        try
        {
            Thread.sleep( sleepMillis );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread()
                  .interrupt();

            throw new IngestException( "Interrupted while attempting to ingest a time-series with metadata: "
                                       + timeSeries.getMetadata()
                                       + "." );
        }

        // Exponential back-off
        newMillis *= 2;

        LOGGER.debug( "Failed to ingest a time-series from {} on attempt {} of {} in thread '{}'. Continuing "
                      + "to retry until the maximum retry count of {} is reached. There are {} attempts "
                      + "remaining.",
                      dataSource,
                      retryNumber,
                      DatabaseTimeSeriesIngester.MAXIMUM_RETRIES,
                      Thread.currentThread()
                            .getName(),
                      DatabaseTimeSeriesIngester.MAXIMUM_RETRIES,
                      DatabaseTimeSeriesIngester.MAXIMUM_RETRIES - retryNumber );

        return newMillis;
    }

    /**
     * Ingests a gridded data source, which involves inserting the source reference only, not the time-series data. See
     * ticket #51232.
     *
     * @param dataSource the data source
     * @return the ingest results
     */

    private List<IngestResult> ingestGriddedData( DataSource dataSource )
    {
        try
        {
            String hash = Netcdf.getGriddedUniqueIdentifier( dataSource.uri(),
                                                             dataSource.getVariable()
                                                                       .name() );
            DataSources dataSources = this.getCaches()
                                          .getDataSourcesCache();
            SourceDetails sourceDetails = dataSources.getSource( hash );

            if ( Objects.nonNull( sourceDetails )
                 && Files.exists( Paths.get( sourceDetails.getSourcePath() ) ) )
            {
                SourceCompletedDetails completedDetails =
                        new SourceCompletedDetails( this.getDatabase(), sourceDetails );
                boolean completed = completedDetails.wasCompleted();


                // Cannot know the data type until retrieval/read time. See #51232
                return IngestResult.singleItemListFrom( dataSource,
                                                        null,
                                                        sourceDetails.getId(),
                                                        false,
                                                        !completed,
                                                        // Assume some data present until proven otherwise at read time
                                                        true );
            }

            // Not present, so save it
            GriddedMetadataSaver saver = new GriddedMetadataSaver( this.getSystemSettings(),
                                                                   this.getDatabase(),
                                                                   this.getCaches(),
                                                                   dataSource,
                                                                   hash );

            return saver.call();
        }
        catch ( IOException | SQLException e )
        {
            throw new IngestException( "Could not check to see if gridded data is already present in the database.",
                                       e );
        }
    }

    /**
     * Attempts to lock a source aka time-series.
     * @param <T> the type of time-series event value
     * @param source the source to lock
     * @param timeSeries the time-series
     * @param lockManager the lock manager
     * @return whether the lock was acquired
     */

    private <T> boolean lockSource( SourceDetails source,
                                    TimeSeries<T> timeSeries,
                                    DatabaseLockManager lockManager )
    {
        if ( LOGGER.isDebugEnabled()
             && Objects.nonNull( timeSeries ) )
        {
            LOGGER.debug( "{} is responsible for source {}", this, source.getId() );
        }

        try
        {
            LOGGER.debug( "Preparing to lock source {} in thread {}.",
                          source.getId(),
                          Thread.currentThread().getName() );

            boolean locked = lockManager.lockSource( source.getId() );

            LOGGER.debug( "Locked source {} in thread {}.",
                          source.getId(),
                          Thread.currentThread().getName() );

            return locked;
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Failed to lock for source id "
                                       + source.getId(),
                                       se );
        }
    }

    /**
     * Attempts to unlock a source aka time-series.
     * @param source the source to unlock
     * @param lockManager the lock manager
     */

    private void unlockSource( SourceDetails source, DatabaseLockManager lockManager )
    {
        try
        {
            boolean unlocked = lockManager.unlockSource( source.getId() );

            if ( unlocked && LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Unlocked source {} in thread {}.",
                              source.getId(),
                              Thread.currentThread().getName() );
            }
            else if ( !unlocked && LOGGER.isWarnEnabled() )
            {
                LOGGER.debug( "Failed to unlock source {} in thread {}. Some other thread probably unlocked this "
                              + "source.",
                              source.getId(),
                              Thread.currentThread().getName() );
            }
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Failed to unlock for source id "
                                       + source.getId(),
                                       se );
        }
    }

    /**
     * Attempts to save the source information associated with the time-series.
     * @param <T> the type of time-series event value
     * @param timeSeries the time-series whose source information should be saved
     * @param timeSeriesId the time-series identity or content hash
     * @param uri the data source uri
     * @return the source details
     */

    private <T> SourceDetails saveTimeSeriesSource( TimeSeries<T> timeSeries, String timeSeriesId, URI uri )
    {
        Database innerDatabase = this.getDatabase();
        SourceDetails source = this.createSourceDetails( timeSeriesId );

        // Lead column is only for raster data as of 2022-01
        source.setLead( null );
        source.setIsPointData( true );
        source.setSourcePath( uri );
        TimeSeriesMetadata metadata = timeSeries.getMetadata();
        String measurementUnit = metadata.getUnit();
        Feature feature = metadata.getFeature();
        TimeScaleOuter timeScale = metadata.getTimeScale();

        String variableName = metadata.getVariableName();
        source.setVariableName( variableName );

        try
        {
            long measurementUnitId = this.getMeasurementUnitId( measurementUnit );
            long featureId = this.getFeatureId( feature );
            Long timeScaleId = null;

            if ( timeScale != null )
            {
                timeScaleId = this.getTimeScaleId( timeScale );
            }

            source.setMeasurementUnitId( measurementUnitId );
            source.setFeatureId( featureId );
            source.setTimeScaleId( timeScaleId );
            source.save( innerDatabase );
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Source metadata about '"
                                       + uri
                                       + "' could not be stored or retrieved from the database.",
                                       se );
        }

        LOGGER.debug( "Found {}? {}", source, !source.performedInsert() );

        return source;
    }

    /**
     * Completes a new source.  Requires that locking semantics are handled by the caller.
     *
     * @param source the source details
     * @param dataType the data type
     * @param latches the latches used to coordinate with other ingest tasks
     * @param dataSource the data source
     * @param nonMissingValuesPresent whether the source has some valid data
     * @return the ingest results
     */

    private List<IngestResult> finalizeNewSource( SourceDetails source,
                                                  DataType dataType,
                                                  Set<Pair<CountDownLatch, CountDownLatch>> latches,
                                                  DataSource dataSource,
                                                  boolean nonMissingValuesPresent )
    {
        /* Mark the given source completed because the caller was in charge of
         * ingest and needs to mark it so.
         *
         * Ensure that ingest of a given sourceId is complete, either by verifying
         * that another task has finished it or by finishing it right here and now.
         *
         * Due to #64922 (empty WRDS AHPS data sources) and #65049 (empty CSV
         * data sources), this class tolerates an empty Set of latches and logs a
         * warning (prior behavior was to throw IllegalArgumentException).
         */

        // Make sure the ingest is actually complete by sending
        // a signal that we sit and await the ingest of values prior to
        // marking them complete.
        for ( Pair<CountDownLatch, CountDownLatch> latchPair : latches )
        {
            // Say "I am about to sit here and wait, y'all..."
            latchPair.getLeft()
                     .countDown();
        }

        if ( !latches.isEmpty() )
        {
            try
            {
                this.flush( latches, source );
            }
            catch ( InterruptedException ie )
            {
                String message =
                        "Interrupted while waiting for another task to ingest data for source "
                        + source
                        + ".";
                Thread.currentThread()
                      .interrupt();
                // Additionally throw exception to ensure we don't accidentally mark
                // this source as completed a few lines down.
                throw new IngestException( message, ie );
            }
        }
        else
        {
            LOGGER.warn( "A data source with no data may have been found. Please check your dataset. "
                         + "(Technical info: source={}.)",
                         source );
        }

        SourceCompletedDetails completedDetails =
                new SourceCompletedDetails( this.database, source.getId() );

        try
        {
            completedDetails.markCompleted();

            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Successfully marked source {} as completed.",
                              source.getId() );
            }
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Failed to mark source "
                                       + source.getId()
                                       + " as completed.",
                                       se );
        }

        return IngestResult.singleItemListFrom( dataSource,
                                                dataType,
                                                source.getId(),
                                                false,
                                                false,
                                                nonMissingValuesPresent );
    }

    /**
     * Creates a list of ingest results for a completed source or an abandoned source that should be retried. If an 
     * abandoned source is detected, the source is locked and removal in preparation for a retry. The caller does not
     * need to handle locking semantics because locking is only required on source removal.
     *
     * @param source the source details
     * @param dataType the data type
     * @param dataSource the data source
     * @param nonMissingValuesPresent whether the source has some valid data present
     * @return the ingest results
     */

    private List<IngestResult> finalizeExistingSource( SourceDetails source,
                                                       DataType dataType,
                                                       DataSource dataSource,
                                                       boolean nonMissingValuesPresent )
    {
        // Completed, no retries needed
        if ( this.isSourceComplete( source ) )
        {
            LOGGER.trace( "Already ingested and completed data source {}.", dataSource );

            // Already present and completed
            return IngestResult.singleItemListFrom( dataSource,
                                                    dataType,
                                                    source.getId(),
                                                    true,
                                                    false,
                                                    nonMissingValuesPresent );
        }
        // Started, but not completed (unless in the interim) and not in progress, so remove and retry later
        else
        {
            // Check again for completion as it may have happened in the interim
            if ( this.isSourceComplete( source ) )
            {
                LOGGER.trace( "Already ingested and completed data source {}.", dataSource );

                return IngestResult.singleItemListFrom( dataSource,
                                                        dataType,
                                                        source.getId(),
                                                        true,
                                                        false,
                                                        nonMissingValuesPresent );
            }

            long surrogateKey = this.getSurrogateKey( source.getHash(), dataSource.uri() );

            // First, try to safely remove it, which requires an exclusive lock
            boolean removed = false;
            if ( surrogateKey != KEY_NOT_FOUND
                 && this.lockSource( source, null, this.getLockManager() ) )
            {
                try
                {
                    // There is a final check below that some other thread did not complete in between the last check 
                    // for completion and the exclusive lock being acquired above
                    removed = IncompleteIngest.removeDataSource( this.getDatabase(),
                                                                 this.getCaches()
                                                                     .getDataSourcesCache(),
                                                                 surrogateKey );
                }
                finally
                {
                    this.unlockSource( source, this.getLockManager() );
                }
            }

            // Log status
            if ( LOGGER.isDebugEnabled() )
            {
                // Removed incomplete source, try to ingest later
                if ( removed )
                {
                    LOGGER.debug( "Successfully removed an abandoned data source, {}, identified by '{}'. This source "
                                  + "will be retried at the next opportunity.",
                                  dataSource,
                                  surrogateKey );
                }
                // Not removed, inspect and retry again later
                else
                {
                    LOGGER.debug( "Failed to remove data source {}, identified by '{}'. This source will be reexamined "
                                  + "at the next opportunity.",
                                  dataSource,
                                  surrogateKey );
                }
            }

            // Retry ingest later
            return IngestResult.singleItemListFrom( dataSource,
                                                    dataType,
                                                    source.getId(),
                                                    true,
                                                    true,
                                                    nonMissingValuesPresent );
        }
    }

    /**
     * @param source the source to check
     * @return true if completed, false otherwise
     */

    private boolean isSourceComplete( SourceDetails source )
    {
        SourceCompletedDetails completedDetails = this.createSourceCompletedDetails( source );
        try
        {
            return completedDetails.wasCompleted();
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Failed to determine if source "
                                       + source
                                       + " was already ingested.",
                                       se );
        }
    }

    /**
     * Attempts to flush ingested data to the database.
     *
     * @param latches the latches
     * @param source the source
     * @throws InterruptedException if the flush was interrupted
     */

    private void flush( Set<Pair<CountDownLatch, CountDownLatch>> latches, SourceDetails source )
            throws InterruptedException
    {
        Duration eachWait = Duration.ofMillis( 1 );

        for ( Pair<CountDownLatch, CountDownLatch> latchPair : latches )
        {
            // Wait a moment for another task to save my data before
            // doing it myself.
            boolean dataFinished = latchPair.getRight()
                                            .await( eachWait.toMillis(),
                                                    TimeUnit.MILLISECONDS );
            if ( !dataFinished )
            {
                LOGGER.debug( "Sick of waiting for another task, saving data myself! {}, {}",
                              source,
                              latchPair );
                boolean thisFlushed = IngestedValues.flush( this.database,
                                                            latchPair );

                // It is still necessary to double-check that the data has
                // actually been written, because even if we call flush(),
                // there is no guarantee that this was the task that wrote.
                // However, because we called flush, we or someone must be
                // doing the write at this point. Wait indefinitely for it.
                // On the other hand, if the other task died while
                // attempting the copy, we cannot sit here and wait forever.
                // If we truly completed it, await() call would return
                // immediately. If we did not successfully complete, then we
                // should be ready to give up after a time to break
                // deadlock.
                if ( !thisFlushed )
                {
                    boolean done = latchPair.getRight()
                                            .await( PATIENCE_LEVEL.toMillis(),
                                                    TimeUnit.MILLISECONDS );

                    // This is uncomfortable for sure, and a better way
                    // should be found than making an assumption that the
                    // other task failed. The thing we are working around is
                    // that get() may be called on this task prior to get()
                    // on the task responsible for marking completed, while
                    // that other task has had an exception that does not
                    // propagate.
                    if ( !done )
                    {
                        throw new IngestException(
                                "Another task did not "
                                + "ingest and complete "
                                + latchPair
                                + " within "
                                + PATIENCE_LEVEL
                                + ", therefore assuming "
                                + "it failed." );
                    }
                }
            }
        }
    }

    /**
     * @param hash the hash
     * @param uri the uri to help with messaging
     * @return the surrogate key or {@link #KEY_NOT_FOUND}
     */
    private long getSurrogateKey( String hash, URI uri )
    {
        // Get the surrogate key if it exists
        Long dataSourceKey = null;
        if ( Objects.nonNull( hash ) )
        {
            try
            {
                DataSources dataSources = this.getCaches()
                                              .getDataSourcesCache();
                dataSourceKey = dataSources.getSourceId( hash );
            }
            catch ( SQLException se )
            {
                throw new IngestException( "While determining if source '"
                                           + uri
                                           + "' should be ingested, "
                                           + "failed to translate natural key '"
                                           + hash
                                           + "' to surrogate key.",
                                           se );
            }
        }

        if ( Objects.nonNull( dataSourceKey ) )
        {
            return dataSourceKey;
        }

        return KEY_NOT_FOUND;
    }

    /**
     * @param systemSettings the system settings
     * @param database the database
     * @param ensemblesCache the ensembles cache
     * @param timeSeries the time-series to insert
     * @param sourceId the source identifier
     * @return a set of latch pairs, the left of which indicates "waiting" and the right indicates "completed"
     */

    private Set<Pair<CountDownLatch, CountDownLatch>> insertSingleValuedTimeSeries( SystemSettings systemSettings,
                                                                                    Database database,
                                                                                    Ensembles ensemblesCache,
                                                                                    TimeSeries<Double> timeSeries,
                                                                                    long sourceId )
    {
        if ( timeSeries.getEvents()
                       .isEmpty() )
        {
            throw new IllegalArgumentException( "TimeSeries must not be empty." );
        }

        TimeSeriesMetadata metadata = timeSeries.getMetadata();
        this.insertReferenceTimeRows( database, sourceId, metadata.getReferenceTimes() );

        long timeSeriesId = this.insertTimeSeriesRow( database,
                                                      ensemblesCache,
                                                      timeSeries,
                                                      sourceId );
        Set<Pair<CountDownLatch, CountDownLatch>> innerLatches = this.insertTimeSeriesValuesRows( systemSettings,
                                                                                                  database,
                                                                                                  timeSeriesId,
                                                                                                  timeSeries );

        Set<Pair<CountDownLatch, CountDownLatch>> latches = new HashSet<>( innerLatches );

        return Collections.unmodifiableSet( latches );
    }

    /**
     * @param systemSettings the system settings
     * @param database the database
     * @param ensemblesCache the ensembles cache
     * @param timeSeries the time-series to insert
     * @param sourceId the source identifier
     * @return a set of latch pairs, the left of which indicates "waiting" and the right indicates "completed"
     */

    private Set<Pair<CountDownLatch, CountDownLatch>> insertEnsembleTimeSeries( SystemSettings systemSettings,
                                                                                Database database,
                                                                                Ensembles ensemblesCache,
                                                                                TimeSeries<Ensemble> timeSeries,
                                                                                long sourceId )
    {
        if ( timeSeries.getEvents()
                       .isEmpty() )
        {
            throw new IllegalArgumentException( "TimeSeries must not be empty." );
        }

        Set<Pair<CountDownLatch, CountDownLatch>> latches = new HashSet<>();

        TimeSeriesMetadata metadata = timeSeries.getMetadata();
        this.insertReferenceTimeRows( database, sourceId, metadata.getReferenceTimes() );

        for ( Map.Entry<Object, SortedSet<Event<Double>>> trace : TimeSeriesSlicer.decomposeWithLabels( timeSeries )
                                                                                  .entrySet() )
        {
            LOGGER.debug( "TimeSeries trace: {}", trace );
            String ensembleName = trace.getKey()
                                       .toString();

            long ensembleId = this.insertOrGetEnsembleId( ensemblesCache,
                                                          ensembleName );
            long timeSeriesId = this.insertTimeSeriesRowForEnsembleTrace( database,
                                                                          timeSeries,
                                                                          ensembleId,
                                                                          sourceId );
            Set<Pair<CountDownLatch, CountDownLatch>> nextLatches = this.insertEnsembleTrace( systemSettings,
                                                                                              database,
                                                                                              timeSeries,
                                                                                              timeSeriesId,
                                                                                              trace.getValue() );

            latches.addAll( nextLatches );
        }

        return Collections.unmodifiableSet( latches );
    }

    /**
     * @param database the database
     * @param sourceId the source identifier
     * @param referenceTimes the reference times
     */

    private void insertReferenceTimeRows( Database database,
                                          long sourceId,
                                          Map<ReferenceTimeType, Instant> referenceTimes )
    {
        int rowCount = referenceTimes.size();

        if ( rowCount == 0 )
        {
            throw new IngestException( "At least one reference datetime is required until we refactor TSV to "
                                       + "permit zero." );
        }

        List<String[]> rows = new ArrayList<>( rowCount );

        for ( Map.Entry<ReferenceTimeType, Instant> referenceTime : referenceTimes.entrySet() )
        {
            String[] row = new String[3];
            row[0] = Long.toString( sourceId );

            // Reference time (instant)
            row[1] = referenceTime.getValue()
                                  .toString();

            // Reference time type
            row[2] = referenceTime.getKey()
                                  .toString();

            rows.add( row );
        }

        List<String> columns = List.of( "source_id",
                                        "reference_time",
                                        "reference_time_type" );
        boolean[] quotedColumns = { false, true, true };
        DatabaseOperations.insertIntoDatabase( database, "wres.TimeSeriesReferenceTime",
                                               columns,
                                               rows,
                                               quotedColumns );
    }

    /**
     * @param ensembleName Name of the ensemble trace
     * @return Raw surrogate key from db
     * @throws IngestException When any query involved fails
     */

    private long insertOrGetEnsembleId( Ensembles ensemblesCache,
                                        String ensembleName )
    {
        long id;

        try
        {
            id = ensemblesCache.getOrCreateEnsembleId( ensembleName );
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Failed to get Ensemble info for "
                                       + ensembleName,
                                       se );
        }

        return id;
    }

    /**
     * @param systemSettings the system settings
     * @param database the database
     * @param originalTimeSeries the time-series to insert
     * @param timeSeriesIdForTrace the trace identifier
     * @param ensembleTrace the trace values
     * @return a set of latch pairs, the left of which indicates "waiting" and the right indicates "completed"
     */

    private Set<Pair<CountDownLatch, CountDownLatch>> insertEnsembleTrace( SystemSettings systemSettings,
                                                                           Database database,
                                                                           TimeSeries<Ensemble> originalTimeSeries,
                                                                           long timeSeriesIdForTrace,
                                                                           SortedSet<Event<Double>> ensembleTrace )
    {
        Instant referenceDatetime = this.getReferenceDatetime( originalTimeSeries );

        Set<Pair<CountDownLatch, CountDownLatch>> latches = new HashSet<>();

        for ( Event<Double> event : ensembleTrace )
        {
            Pair<CountDownLatch, CountDownLatch> nextLatch = this.insertTimeSeriesValuesRow( systemSettings,
                                                                                             database,
                                                                                             timeSeriesIdForTrace,
                                                                                             referenceDatetime,
                                                                                             event );
            latches.add( nextLatch );
        }

        return Collections.unmodifiableSet( latches );
    }

    /**
     * @param systemSettings the system settings
     * @param database the database
     * @param timeSeriesId the trace identifier
     * @param timeSeries the time-series to ingest
     * @return a set of latch pairs, the left of which indicates "waiting" and the right indicates "completed"
     * @throws IngestException if ingest failed for any reason
     */

    private Set<Pair<CountDownLatch, CountDownLatch>> insertTimeSeriesValuesRows( SystemSettings systemSettings,
                                                                                  Database database,
                                                                                  long timeSeriesId,
                                                                                  TimeSeries<Double> timeSeries )
    {
        Instant referenceDatetime = this.getReferenceDatetime( timeSeries );

        Set<Pair<CountDownLatch, CountDownLatch>> latches = new HashSet<>();

        for ( Event<Double> event : timeSeries.getEvents() )
        {
            Pair<CountDownLatch, CountDownLatch> nextLatch = this.insertTimeSeriesValuesRow( systemSettings,
                                                                                             database,
                                                                                             timeSeriesId,
                                                                                             referenceDatetime,
                                                                                             event );
            latches.add( nextLatch );
        }

        return Collections.unmodifiableSet( latches );
    }

    /**
     * @param systemSettings the system settings
     * @param database the database
     * @param timeSeriesId the trace identifier
     * @param referenceDatetime the reference time
     * @param valueAndValidDateTime the event
     * @return a pair of latches, the left of which indicates "waiting" and the right indicates "completed"
     */

    private Pair<CountDownLatch, CountDownLatch> insertTimeSeriesValuesRow( SystemSettings systemSettings,
                                                                            Database database,
                                                                            long timeSeriesId,
                                                                            Instant referenceDatetime,
                                                                            Event<Double> valueAndValidDateTime )
    {
        Instant validDatetime = valueAndValidDateTime.getTime();
        Duration leadDuration = Duration.between( referenceDatetime, validDatetime );
        Double valueToAdd = valueAndValidDateTime.getValue();

        // When the Java-land value matches WRES Missing Value, use NULL in DB.
        if ( MissingValues.isMissingValue( valueToAdd ) )
        {
            valueToAdd = null;
        }

        return IngestedValues.addTimeSeriesValue( systemSettings,
                                                  database,
                                                  timeSeriesId,
                                                  ( int ) leadDuration.toMinutes(),
                                                  valueToAdd );
    }

    /**
     * @param database the database
     * @param ensemblesCache the ensemble cache
     * @param timeSeries the time-series
     * @param sourceId the source identifier
     * @return the time-series identity
     */

    private long insertTimeSeriesRow( Database database,
                                      Ensembles ensemblesCache,
                                      TimeSeries<Double> timeSeries,
                                      long sourceId )
    {
        wres.io.database.details.TimeSeries databaseTimeSeries;

        try
        {
            databaseTimeSeries = this.getDatabaseTimeSeries( database,
                                                             ensemblesCache,
                                                             sourceId );
            // The following indirectly calls save:
            return databaseTimeSeries.getTimeSeriesID();
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Failed to get trace info for "
                                       + "timeSeries="
                                       + timeSeries
                                       + " source="
                                       + sourceId,
                                       se );
        }
    }

    /**
     * @param database the database
     * @param timeSeries the time-series
     * @param ensembleId the ensemble identifier
     * @param sourceId the source identifier
     * @return the time-series identity
     */

    private long insertTimeSeriesRowForEnsembleTrace( Database database,
                                                      TimeSeries<Ensemble> timeSeries,
                                                      long ensembleId,
                                                      long sourceId )
    {
        wres.io.database.details.TimeSeries databaseTimeSeries;

        try
        {
            databaseTimeSeries = this.getDatabaseTimeSeriesForEnsembleTrace( database,
                                                                             ensembleId,
                                                                             sourceId );
            // The following indirectly calls save:
            return databaseTimeSeries.getTimeSeriesID();
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Failed to get trace info for "
                                       + "timeSeries="
                                       + timeSeries
                                       + " source="
                                       + sourceId
                                       + " ensembleId="
                                       + ensembleId,
                                       se );
        }
    }

    /**
     * @param database the database
     * @param ensembleId the ensemble identifier
     * @param sourceId the source identifier
     * @return a time-series for an ensemble trace
     */

    private wres.io.database.details.TimeSeries getDatabaseTimeSeriesForEnsembleTrace( Database database,
                                                                                       long ensembleId,
                                                                                       long sourceId )
    {
        return new wres.io.database.details.TimeSeries( database,
                                                        ensembleId,
                                                        sourceId );
    }

    /**
     * @param database the database
     * @param ensemblesCache the ensemble cache
     * @param sourceId the source identifier
     * @return a time-series
     * @throws SQLException if the time-series could not be created
     */

    private wres.io.database.details.TimeSeries
    getDatabaseTimeSeries( Database database,
                           Ensembles ensemblesCache,
                           long sourceId )
            throws SQLException
    {
        return new wres.io.database.details.TimeSeries( database,
                                                        ensemblesCache.getDefaultEnsembleId(),
                                                        sourceId );
    }

    /**
     * @param <T> the type of time-series event value
     * @param timeSeries the time-series
     * @return the first reference time
     */

    private <T> Instant getReferenceDatetime( TimeSeries<T> timeSeries )
    {
        if ( timeSeries.getReferenceTimes()
                       .isEmpty() )
        {
            throw new IllegalStateException( "Data must have at least one reference datetime: "
                                             + timeSeries );
        }

        // Use the first reference datetime found until the database allows
        // storage of all the reference datetimes associated with a forecast.
        Map.Entry<ReferenceTimeType, Instant> entry =
                timeSeries.getReferenceTimes()
                          .entrySet()
                          .iterator()
                          .next();

        LOGGER.debug( "Using the first reference datetime type found: {}",
                      entry );
        return entry.getValue();
    }

    /**
     * @param measurementUnit the measurement unit
     * @return the measurement unit identity
     * @throws SQLException if the measurement unit identity could not be determined
     */

    private long getMeasurementUnitId( String measurementUnit ) throws SQLException
    {
        MeasurementUnits innerMeasurementUnitsCache = this.getCaches()
                                                          .getMeasurementUnitsCache();
        return innerMeasurementUnitsCache.getOrCreateMeasurementUnitId( measurementUnit );
    }

    /**
     * @param featureKey the the feature key
     * @return the feature identity
     * @throws SQLException if the feature identity could not be determined
     */

    private long getFeatureId( Feature featureKey ) throws SQLException
    {
        Features innerFeaturesCache = this.getCaches()
                                          .getFeaturesCache();
        return innerFeaturesCache.getOrCreateFeatureId( featureKey );
    }

    /**
     * @param timeScale the time scale
     * @return the time scale identity
     * @throws SQLException if the identity could not be determined
     */

    private long getTimeScaleId( TimeScaleOuter timeScale ) throws SQLException
    {
        TimeScales innerTimeScalesCache = this.getCaches()
                                              .getTimeScalesCache();
        return innerTimeScalesCache.getOrCreateTimeScaleId( timeScale );
    }

    /**
     * @param timeSeries the time-series
     * @return the time-series MD5 checksum as a hex string
     */

    private String identifyTimeSeries( TimeSeries<?> timeSeries )
    {
        MessageDigest md5Name;

        try
        {
            md5Name = MessageDigest.getInstance( "MD5" );
        }
        catch ( NoSuchAlgorithmException nsae )
        {
            throw new IngestException( "Couldn't use MD5 algorithm.",
                                       nsae );
        }

        DigestUtils digestUtils = new DigestUtils( md5Name );

        // Here assuming that the toString represents all state of a timeseries.
        byte[] hash = digestUtils.digest( timeSeries.toString() );

        if ( hash.length < 16 )
        {
            throw new IngestException( "The MD5 sum of " + timeSeries
                                       + " was shorter than expected." );
        }

        return Hex.encodeHexString( hash, false );
    }

    /**
     * This method facilitates testing, Pattern 1 at
     * <a href="https://github.com/mockito/mockito/wiki/Mocking-Object-Creation">Mocking</a>
     * @param sourceKey the first arg to SourceDetails
     * @return a SourceDetails
     */

    private SourceDetails createSourceDetails( String sourceKey )
    {
        return new SourceDetails( sourceKey );
    }

    /**
     * This method facilitates testing, Pattern 1 at
     * <a href="https://github.com/mockito/mockito/wiki/Mocking-Object-Creation">Mocking</a>
     * @param sourceDetails the first arg to SourceCompletedDetails
     * @return a SourceCompleter
     */

    private SourceCompletedDetails createSourceCompletedDetails( SourceDetails sourceDetails )
    {
        return new SourceCompletedDetails( this.getDatabase(), sourceDetails );
    }

    /**
     * @return the database
     */

    private Database getDatabase()
    {
        return this.database;
    }

    /**
     * @return the system settings
     */

    private SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }

    /**
     * @return the caches
     */

    private DatabaseCaches getCaches()
    {
        return this.caches;
    }

    /**
     * @return the thread pool executor
     */

    private ExecutorService getExecutor()
    {
        return this.executor;
    }

    /**
     * @return the database lock manager
     */

    private DatabaseLockManager getLockManager()
    {
        return this.lockManager;
    }

    /**
     * Creates an instance.
     * @param builder the builder
     */

    private DatabaseTimeSeriesIngester( Builder builder )
    {
        this.systemSettings = builder.systemSettings;
        this.database = builder.database;
        this.caches = builder.caches;
        this.lockManager = builder.lockManager;
        this.executor = builder.executor;

        Objects.requireNonNull( this.systemSettings );
        Objects.requireNonNull( this.database );
        Objects.requireNonNull( this.caches );
        Objects.requireNonNull( this.lockManager );
        Objects.requireNonNull( this.executor );
    }
}
