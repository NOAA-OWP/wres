package wres.io.ingesting.database;

import java.io.Closeable;
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
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.datamodel.Ensemble;
import wres.datamodel.MissingValues;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.io.data.caching.DatabaseCaches;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.details.SourceCompletedDetails;
import wres.io.data.details.SourceDetails;
import wres.io.database.Database;
import wres.io.ingesting.IngestException;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.PreIngestException;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.data.caching.TimeScales;
import wres.io.reading.DataSource;
import wres.io.reading.TimeSeriesTuple;
import wres.system.DatabaseLockManager;
import wres.system.SystemSettings;
import wres.util.NetCDF;

import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Ingests given {@link TimeSeries} data if not already ingested.
 *
 * As of 2019-10-29, only supports data with one or more reference datetime.
 * As of 2019-10-29, uses {@link TimeSeries#toString()} to identify data.
 *
 * Identifies data for convenience (and parallelism).
 *
 * Different source types have different optimization strategies for identifying
 * data and avoiding ingest, therefore those can avoid ingest at a higher level
 * using different strategies than this code. This class always checks if an
 * individual timeseries has been ingested prior to ingesting it, regardless of
 * higher level checks.
 *
 * Does not link data, leaves that to higher level code to do later. In this
 * context, link means to associate a dataset with all of the sides/orientations
 * in which it appears. For example, a left-ish dataset may be used to generate
 * a baseline time-series and, therefore, appear in two contexts, left and 
 * baseline.
 *
 * Does not preclude the skipping of ingest at a higher level.
 * 
 * @author James Brown
 * @author Jesse Bickel
 */

public class DatabaseTimeSeriesIngester implements TimeSeriesIngester, Closeable
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger( DatabaseTimeSeriesIngester.class );

    /** The maximum number of ingest retries. */
    private static final int MAXIMUM_RETRIES = 10;

    /** The default key value for a data source when not discovered in the cache. */
    private static final long KEY_NOT_FOUND = Long.MIN_VALUE;

    private final SystemSettings systemSettings;
    private final Database database;
    private final DatabaseCaches caches;
    private final ProjectConfig projectConfig;
    private final DatabaseLockManager lockManager;

    /** A thread pool to process ingests. */
    private final ThreadPoolExecutor executor;

    /**
     * Builds an instance incrementally.
     */

    public static class Builder
    {
        private SystemSettings systemSettings;
        private Database database;
        private DatabaseCaches caches;
        private ProjectConfig projectConfig;
        private DatabaseLockManager lockManager;

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
         * @param projectConfig the project declaration to set
         * @return the builder
         */
        public Builder setProjectConfig( ProjectConfig projectConfig )
        {
            this.projectConfig = projectConfig;
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
         * @return a time-series ingester instance
         */
        public DatabaseTimeSeriesIngester build()
        {
            return new DatabaseTimeSeriesIngester( this );
        }
    }

    @Override
    public List<IngestResult> ingest( Stream<TimeSeriesTuple> timeSeriesTuple,
                                      DataSource outerSource )
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

        // A queue of tasks
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
            while ( tupleIterator.hasNext() )
            {
                TimeSeriesTuple nextTuple = tupleIterator.next();

                DataSource innerSource = nextTuple.getDataSource();

                // Single-valued time-series?
                if ( nextTuple.hasSingleValuedTimeSeries() )
                {
                    TimeSeries<Double> nextSeries =
                            this.checkForEmptySeriesAndAddReferenceTimeIfRequired( nextTuple.getSingleValuedTimeSeries(),
                                                                                   innerSource.getUri() );

                    Future<List<IngestResult>> innerResults = this.getExecutor()
                                                                  .submit( () -> this.ingestSingleValuedTimeSeriesWithRetries( nextSeries,
                                                                                                                               innerSource ) );

                    // Add the future ingest results to the ingest queue
                    ingestQueue.add( innerResults );
                    startGettingResults.countDown();
                }

                // Ensemble time-series?
                if ( nextTuple.hasEnsembleTimeSeries() )
                {
                    TimeSeries<Ensemble> nextSeries =
                            this.checkForEmptySeriesAndAddReferenceTimeIfRequired( nextTuple.getEnsembleTimeSeries(),
                                                                                   innerSource.getUri() );

                    Future<List<IngestResult>> innerResults = this.getExecutor()
                                                                  .submit( () -> this.ingestEnsembleTimeSeriesWithRetries( nextSeries,
                                                                                                                           innerSource ) );

                    // Add the future ingest results to the ingest queue
                    ingestQueue.add( innerResults );
                    startGettingResults.countDown();
                }

                // Start to get ingest results?
                if ( startGettingResults.getCount() <= 0 )
                {
                    List<IngestResult> result = ingestQueue.poll()
                                                           .get();
                    finalResults.addAll( result );
                }
            }

            // Get any remaining results
            for ( Future<List<IngestResult>> next : ingestQueue )
            {
                List<IngestResult> nextResults = next.get();
                finalResults.addAll( nextResults );
            }

            return Collections.unmodifiableList( finalResults );
        }
        catch ( ExecutionException e )
        {
            throw new IngestException( "Failed to get ingest results.", e );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new IngestException( "Interrupted while getting ingest results.", e );
        }
    }

    @Override
    public void close() throws IOException
    {
        this.getExecutor()
            .shutdownNow();
    }

    /**
     * Database ingest currently requires at least one reference time. If none exists, add a default one. Also, check 
     * that the time-series is not empty and throw an exception if it is empty.
     * @param timeSeries the time-series to check
     * @param uri the source URI to help with error messaging
     * @return a time-series with at least one reference time
     * @throws PreIngestException if the time-series is empty
     */

    private <T> TimeSeries<T> checkForEmptySeriesAndAddReferenceTimeIfRequired( TimeSeries<T> timeSeries, URI uri )
    {
        if ( !timeSeries.getReferenceTimes().isEmpty() )
        {
            return timeSeries;
        }

        if ( timeSeries.getEvents()
                       .isEmpty() )
        {
            throw new PreIngestException( "While ingesting source " + uri
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
     * the {@link MAXIMUM_RETRIES}.
     *  
     * @param timeSeries the time-series to ingest, not null
     * @param dataSource the data source
     * @return the ingest results
     */

    private List<IngestResult> ingestSingleValuedTimeSeriesWithRetries( TimeSeries<Double> timeSeries,
                                                                        DataSource dataSource )
    {
        long sleepMillis = 1000;
        for ( int i = 0; i <= DatabaseTimeSeriesIngester.MAXIMUM_RETRIES; i++ )
        {
            if ( i > 0 )
            {
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
                sleepMillis *= 2;

                LOGGER.warn( "Failed to ingest a time-series on attempt {} of {}. Continuing to retry until the "
                             + "maximum retry count of {} is reached. There are {} attempts remaining.",
                             i,
                             DatabaseTimeSeriesIngester.MAXIMUM_RETRIES,
                             DatabaseTimeSeriesIngester.MAXIMUM_RETRIES,
                             DatabaseTimeSeriesIngester.MAXIMUM_RETRIES - i );
            }

            List<IngestResult> results = this.ingestSingleValuedTimeSeries( timeSeries, dataSource );

            // Success
            if ( this.isIngestComplete( results ) )
            {
                if ( i > 0 )
                {
                    LOGGER.info( "Successfully ingested a time-series from {} on attempt {} of {}.",
                                 dataSource,
                                 i + 1,
                                 DatabaseTimeSeriesIngester.MAXIMUM_RETRIES );
                }

                LOGGER.trace( "Successfully ingested a time-series with metadata: {}.", timeSeries.getMetadata() );

                return results;
            }
        }

        throw new IngestException( "Failed to ingest a time-series after " + DatabaseTimeSeriesIngester.MAXIMUM_RETRIES
                                   + " attempts. The time-series metadata was: "
                                   + timeSeries.getMetadata()
                                   + "." );
    }

    /**
     * Ingests a time-series whose events are {@link Double}.
     * @param timeSeries the time-series to ingest, not null
     * @param dataSource the data source
     * @return the ingest results
     */

    private List<IngestResult> ingestSingleValuedTimeSeries( TimeSeries<Double> timeSeries, DataSource dataSource )
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

        LOGGER.debug( "Ingesting a single-valued time-series from source {}.", dataSource.getUri() );

        // Try to insert a row into wres.Source for the time-series
        SourceDetails source = this.saveTimeSeriesSource( timeSeries, dataSource.getUri() );

        // Source row was inserted, so this is a new time-series
        if ( source.performedInsert() )
        {
            // Lock source with an advisory lock. Unlocked by a SourceCompleter.
            this.lockSource( source, timeSeries );

            // Ready to insert the time-series, source row was inserted and is (advisory) locked.
            Set<Pair<CountDownLatch, CountDownLatch>> latches =
                    this.insertSingleValuedTimeSeries( this.getSystemSettings(),
                                                       this.getDatabase(),
                                                       this.getCaches()
                                                           .getEnsemblesCache(),
                                                       timeSeries,
                                                       source.getId() );

            // Finalize, which marks the source complete and unlocks it
            results = this.finalizeNewSource( source, latches, dataSource );
        }
        // Source was not inserted, so must be in progress, abandoned or completed. Try to finalize it.
        else
        {
            results = this.finalizeExistingSource( source, dataSource );
        }

        return results;
    }

    /**
     * Ingests a time-series whose events are {@link Ensemble}. Retries, as necessary, using exponential back-off, up to
     * the {@link MAXIMUM_RETRIES}.
     *  
     * @param timeSeries the time-series to ingest, not null
     * @param dataSource the data source
     * @return the ingest results
     */

    private List<IngestResult> ingestEnsembleTimeSeriesWithRetries( TimeSeries<Ensemble> timeSeries,
                                                                    DataSource dataSource )
    {
        long sleepMillis = 1000;
        for ( int i = 0; i <= DatabaseTimeSeriesIngester.MAXIMUM_RETRIES; i++ )
        {
            if ( i > 0 )
            {
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
                sleepMillis *= 2;

                LOGGER.warn( "Failed to ingest a time-series from {} on attempt {} of {}. Continuing to retry until "
                             + "the maximum retry count of {} is reached. There are {} attempts remaining.",
                             dataSource,
                             i,
                             DatabaseTimeSeriesIngester.MAXIMUM_RETRIES,
                             DatabaseTimeSeriesIngester.MAXIMUM_RETRIES,
                             DatabaseTimeSeriesIngester.MAXIMUM_RETRIES - i );
            }

            List<IngestResult> result = this.ingestEnsembleTimeSeries( timeSeries, dataSource );

            // Success
            if ( this.isIngestComplete( result ) )
            {
                if ( i > 0 )
                {
                    LOGGER.info( "Successfully ingested a time-series from {} on attempt {} of {}.",
                                 dataSource,
                                 i + 1,
                                 DatabaseTimeSeriesIngester.MAXIMUM_RETRIES );
                }

                LOGGER.trace( "Successfully ingested a time-series with metadata: {}.", timeSeries.getMetadata() );

                return result;
            }
        }

        throw new IngestException( "Failed to ingest a time-series after " + DatabaseTimeSeriesIngester.MAXIMUM_RETRIES
                                   + " attempts. The time-series metadata was: "
                                   + timeSeries.getMetadata()
                                   + "." );
    }

    /**
     * Ingests a time-series whose events are {@link Ensemble}.
     * @param timeSeries the time-series to ingest, not null
     * @param dataSource the data source
     * @return the ingest results
     */

    private List<IngestResult> ingestEnsembleTimeSeries( TimeSeries<Ensemble> timeSeries, DataSource dataSource )
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

        LOGGER.debug( "Ingesting an ensemble time-series from source {}.", dataSource.getUri() );

        SourceDetails source = this.saveTimeSeriesSource( timeSeries, dataSource.getUri() );

        // Not found
        if ( source.performedInsert() )
        {
            // Try to lock source with an advisory lock. Unlocked by a SourceCompleter. See #110218
            if ( this.lockSource( source, timeSeries ) )
            {
                // Ready to ingest, source row was inserted and is (advisory) locked.
                Set<Pair<CountDownLatch, CountDownLatch>> latches =
                        this.insertEnsembleTimeSeries( this.getSystemSettings(),
                                                       this.getDatabase(),
                                                       this.getCaches()
                                                           .getEnsemblesCache(),
                                                       timeSeries,
                                                       source.getId() );

                // Finalize, which marks the source complete and unlocks it using a SourceCompleter
                results = this.finalizeNewSource( source, latches, dataSource );
            }
            else
            {
                LOGGER.debug( "Detected a data source that is locked in another task, {}. Will retry later.",
                              dataSource );

                // Busy, retry again later
                results = IngestResult.singleItemListFrom( dataSource,
                                                           source.getId(),
                                                           true,
                                                           true );
            }
        }
        // Source was not inserted, so this is an existing source. But was it completed or abandoned?
        else
        {
            results = this.finalizeExistingSource( source, dataSource );
        }

        return results;
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
     * Ingests a gridded data source, which involves inserting the source reference only, not the time-series data. See
     * ticket #51232.
     * 
     * @param dataSource
     * @return
     */

    private List<IngestResult> ingestGriddedData( DataSource dataSource )
    {
        try
        {
            String hash = NetCDF.getGriddedUniqueIdentifier( dataSource.getUri(),
                                                             dataSource.getVariable()
                                                                       .getValue() );
            DataSources dataSources = this.getCaches()
                                          .getDataSourcesCache();
            SourceDetails sourceDetails = dataSources.getSource( hash );

            if ( Objects.nonNull( sourceDetails ) && Files.exists( Paths.get( sourceDetails.getSourcePath() ) ) )
            {
                SourceCompletedDetails completedDetails =
                        new SourceCompletedDetails( this.getDatabase(), sourceDetails );
                boolean completed = completedDetails.wasCompleted();
                return IngestResult.singleItemListFrom( dataSource,
                                                        sourceDetails.getId(),
                                                        false,
                                                        !completed );
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
            throw new PreIngestException( "Could not check to see if gridded data is already present in the database.",
                                          e );
        }
    }

    /**
     * Attempts to lock a source aka time-series. Unlocking is managed by a {@link SourceCompleter}.
     * @param <T> the type of time-series event value
     * @param source the source to lock
     * @param timeSeries the time-series
     * @return whether the lock was acquired
     */

    private <T> boolean lockSource( SourceDetails source, TimeSeries<T> timeSeries )
    {
        if ( LOGGER.isDebugEnabled() )
        {
            byte[] rawHash = this.identifyTimeSeries( timeSeries, "" );
            String hash = Hex.encodeHexString( rawHash, false );
            LOGGER.debug( "{} is responsible for source {}", this, hash );
        }

        try
        {
            if ( !this.lockManager.isSourceLocked( source.getId() ) )
            {
                this.lockManager.lockSource( source.getId() );

                return true;
            }

            return false;
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Failed to lock for source id "
                                       + source.getId(),
                                       se );
        }
    }

    /**
     * Attempts to save the source information associated with the time-series.
     * @param <T> the type of time-series event value
     * @param time-series whose source information should be saved
     * @param uri the data source uri
     * @return the source details
     */

    private <T> SourceDetails saveTimeSeriesSource( TimeSeries<T> timeSeries, URI uri )
    {
        byte[] rawHash = this.identifyTimeSeries( timeSeries, "" );
        String hash = Hex.encodeHexString( rawHash, false );

        Database innerDatabase = this.getDatabase();
        SourceDetails source = this.createSourceDetails( hash );

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
     * Creates a list of ingest results for a completed source or an abandoned source that should be retried. If an 
     * abandoned source is detected, the source is removed safely in preparation for a retry.
     *  
     * @param source the source details
     * @param dataSource the data source
     * @return the ingest results
     */

    private List<IngestResult> finalizeExistingSource( SourceDetails source,
                                                       DataSource dataSource )
    {
        // Completed, no retries needed
        if ( this.isSourceComplete( source ) )
        {
            LOGGER.trace( "Already ingested and completed data source {}.", dataSource );

            // Already present and completed
            return IngestResult.singleItemListFrom( dataSource,
                                                    source.getId(),
                                                    true,
                                                    false );
        }
        // Source is already locked for mutation, try again later. See #110218
        else if ( this.isSourceLocked( this.lockManager, source.getId() ) )
        {
            LOGGER.debug( "Detected a data source that is locked in another task, {}. Will retry later.",
                          dataSource );

            // Retry later
            return IngestResult.singleItemListFrom( dataSource,
                                                    source.getId(),
                                                    true,
                                                    true );
        }
        // Started, but not completed (unless in the interim) and not in progress, so remove and retry later
        else
        {
            // Check again for completion as it may have happened in the interim
            if ( this.isSourceComplete( source ) )
            {
                LOGGER.trace( "Already ingested and completed data source {}.", dataSource );

                return IngestResult.singleItemListFrom( dataSource,
                                                        source.getId(),
                                                        true,
                                                        false );
            }

            // Check again whether source is locked for mutation and, if locked, try again later. See #110218
            if ( this.isSourceLocked( this.lockManager, source.getId() ) )
            {
                LOGGER.trace( "Discovered a lock on data source {}, retrying later.", dataSource );

                return IngestResult.singleItemListFrom( dataSource,
                                                        source.getId(),
                                                        true,
                                                        true );
            }

            // First, try to safely remove it, which will again check for completion and only remove if safe
            long surrogateKey = this.getSurrogateKey( source.getHash(), dataSource.getUri() );

            LOGGER.warn( "Another instance started to ingest data source, {}, identified by '{}' but did not finish. "
                         + "Cleaning up...",
                         dataSource,
                         surrogateKey );

            boolean removed = IncompleteIngest.removeSourceDataSafely( this.getDatabase(),
                                                                       this.getCaches()
                                                                           .getDataSourcesCache(),
                                                                       surrogateKey,
                                                                       this.getLockManager() );

            // Log status
            if ( LOGGER.isDebugEnabled() )
            {
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
                                                    source.getId(),
                                                    true,
                                                    true );
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
            throw new PreIngestException( "Failed to determine if source "
                                          + source
                                          + " was already ingested.",
                                          se );
        }
    }

    /**
     * Determines if the indicated data is currently being ingested or removed by another task.
     * 
     * @param sourceId the source identifier
     * @return true if a task is detected to be ingesting, false otherwise
     * @throws PreIngestException when communication with the database fails
     */
    private boolean isSourceLocked( DatabaseLockManager lockManager, Long sourceId )
    {
        try
        {
            LOGGER.debug( "Checking source lock for source {}.", sourceId );

            return lockManager.isSourceLocked( sourceId );
        }
        catch ( SQLException e )
        {
            throw new PreIngestException( "Failed to determine whether the data source with identity "
                                          + sourceId
                                          + " was currently being ingested." );
        }
    }

    /**
     * Completes a new source.
     * @param source the source details
     * @param latches the latches
     * @param dataSource the data source
     * @return the ingest results
     */

    private List<IngestResult> finalizeNewSource( SourceDetails source,
                                                  Set<Pair<CountDownLatch, CountDownLatch>> latches,
                                                  DataSource dataSource )
    {
        List<IngestResult> results;

        // Mark complete
        SourceCompleter completer = this.createSourceCompleter( source.getId(),
                                                                this.lockManager );
        completer.complete( latches );
        results = IngestResult.singleItemListFrom( dataSource,
                                                   source.getId(),
                                                   false,
                                                   false );
        return results;
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
                throw new PreIngestException( "While determining if source '"
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

        Set<Pair<CountDownLatch, CountDownLatch>> latches = new HashSet<>();

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

        latches.addAll( innerLatches );

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
            throw new PreIngestException( "At least one reference datetime is required until we refactor TSV to "
                                          + "permit zero." );
        }

        if ( rowCount != 1 )
        {
            throw new PreIngestException( "Exactly one reference datetime is required until we allow callers to "
                                          + "declare which one to use at evaluation time." );
            // TODO: add post-ingest pre-retrieval validation and/or
            // optional declaration to disambiguate, then remove this block.
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
        database.copy( "wres.TimeSeriesReferenceTime",
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
     * @param referenceDateTime the reference time
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
                                                  (int) leadDuration.toMinutes(),
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
        wres.io.data.details.TimeSeries databaseTimeSeries;

        try
        {
            databaseTimeSeries = this.getDbTimeSeries( database,
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
        wres.io.data.details.TimeSeries databaseTimeSeries;

        try
        {
            databaseTimeSeries = this.getDbTimeSeriesForEnsembleTrace( database,
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

    private wres.io.data.details.TimeSeries getDbTimeSeriesForEnsembleTrace( Database database,
                                                                             long ensembleId,
                                                                             long sourceId )
    {
        return new wres.io.data.details.TimeSeries( database,
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

    private wres.io.data.details.TimeSeries getDbTimeSeries( Database database,
                                                             Ensembles ensemblesCache,
                                                             long sourceId )
            throws SQLException
    {
        return new wres.io.data.details.TimeSeries( database,
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
        if ( timeSeries.getReferenceTimes().isEmpty() )
        {
            throw new IllegalStateException( "Data must have at least one reference datetime: "
                                             + timeSeries.toString() );
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
     * @param additionalIdentifiers additional identifiers
     * @return the time-series MD5 checksum
     */

    private byte[] identifyTimeSeries( TimeSeries<?> timeSeries,
                                       String additionalIdentifiers )
    {
        MessageDigest md5Name;

        try
        {
            md5Name = MessageDigest.getInstance( "MD5" );
        }
        catch ( NoSuchAlgorithmException nsae )
        {
            throw new PreIngestException( "Couldn't use MD5 algorithm.",
                                          nsae );
        }

        DigestUtils digestUtils = new DigestUtils( md5Name );

        // Here assuming that the toString represents all state of a timeseries.
        byte[] hash = digestUtils.digest( timeSeries.toString()
                                          + additionalIdentifiers );

        if ( hash.length < 16 )
        {
            throw new PreIngestException( "The MD5 sum of " + timeSeries
                                          + " was shorter than expected." );
        }

        return hash;
    }

    /**
     * This method facilitates testing, Pattern 1 at
     * https://github.com/mockito/mockito/wiki/Mocking-Object-Creation
     * @param sourceKey the first arg to SourceDetails
     * @return a SourceDetails
     */

    SourceDetails createSourceDetails( String sourceKey )
    {
        return new SourceDetails( sourceKey );
    }


    /**
     * This method facilitates testing, Pattern 1 at
     * https://github.com/mockito/mockito/wiki/Mocking-Object-Creation
     * @param sourceId the first arg to SourceCompleter
     * @param lockManager the second arg to SourceCompleter
     * @return a SourceCompleter
     */

    SourceCompleter createSourceCompleter( long sourceId,
                                           DatabaseLockManager lockManager )
    {
        return new SourceCompleter( this.getDatabase(), sourceId, lockManager );
    }


    /**
     * This method facilitates testing, Pattern 1 at
     * https://github.com/mockito/mockito/wiki/Mocking-Object-Creation
     * @param sourceDetails the first arg to SourceCompletedDetails
     * @return a SourceCompleter
     */

    SourceCompletedDetails createSourceCompletedDetails( SourceDetails sourceDetails )
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

    private ThreadPoolExecutor getExecutor()
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
        this.projectConfig = builder.projectConfig;
        this.lockManager = builder.lockManager;

        Objects.requireNonNull( this.systemSettings );
        Objects.requireNonNull( this.database );
        Objects.requireNonNull( this.caches );
        Objects.requireNonNull( this.projectConfig );
        Objects.requireNonNull( this.lockManager );

        ThreadFactory threadFactoryWithNaming =
                new BasicThreadFactory.Builder().namingPattern( "Ingesting Thread %d" )
                                                .build();

        ThreadPoolExecutor executorInner =
                new ThreadPoolExecutor( this.systemSettings.getMaximumIngestThreads(),
                                        this.systemSettings.getMaximumIngestThreads(),
                                        this.systemSettings.poolObjectLifespan(),
                                        TimeUnit.MILLISECONDS,
                                        // Queue should be large enough to allow
                                        // join() call below to be reached with
                                        // zero or few rejected submissions to
                                        // the executor service.
                                        new ArrayBlockingQueue<>( this.systemSettings.getMaximumIngestThreads() ),
                                        threadFactoryWithNaming );
        executorInner.setRejectedExecutionHandler( new ThreadPoolExecutor.CallerRunsPolicy() );
        this.executor = executorInner;
    }
}
