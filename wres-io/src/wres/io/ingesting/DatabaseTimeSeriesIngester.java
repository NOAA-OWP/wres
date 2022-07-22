package wres.io.ingesting;

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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.datamodel.Ensemble;
import wres.datamodel.MissingValues;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.io.data.caching.Caches;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.details.SourceCompletedDetails;
import wres.io.data.details.SourceDetails;
import wres.io.data.caching.TimeScales;
import wres.io.reading.DataSource;
import wres.io.reading.ZippedSource;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;
import wres.system.SystemSettings;

/**
 * Ingests given TimeSeries data if not already ingested.
 *
 * As of 2019-10-29, only supports data with one or more reference datetime.
 * As of 2019-10-29, uses toString() representation to identify data.
 * As of 2020-02-18, used by (raw) NWM reader, WRDS NWM reader, NWIS reader.
 *
 * Identifies data for convenience (and parallelism).
 *
 * Different source types have different optimization strategies for identifying
 * data and avoiding ingest, therefore those can avoid ingest at a higher level
 * using different strategies than this code. This class always checks if an
 * individual timeseries has been ingested prior to ingesting it, regardless of
 * higher level checks.
 *
 * Does not link data, leaves that to higher level code to do later.
 *
 * Separates the tasks of reading from ingesting. Assumes that reading has
 * already taken place and the result of the reading is a TimeSeries.
 *
 * Does not preclude the skipping of ingest at a higher level, e.g. when a full
 * PI-XML stream is identified and marked as ingested in wres.source. This class
 * could replace the use of wres.source table with a natural identifier column
 * in the wres.timeseries table. Then sources would not be started/completed,
 * but timeseries would be started/completed. This would present some difficulty
 * in optimizing the case of already-ingested bytestreams but relieves the
 * difficulty where a TimeSeries is not contained in a single bytestream: NWM.
 */

public class DatabaseTimeSeriesIngester implements TimeSeriesIngester
{  
    private static final Logger LOGGER =
            LoggerFactory.getLogger( DatabaseTimeSeriesIngester.class );

    private final SystemSettings systemSettings;
    private final Database database;
    private final Caches caches;
    private final ProjectConfig projectConfig;
    private final DatabaseLockManager lockManager;
    
    /**
     * Creates an instance.
     */
    
    public static class Builder
    {
        private SystemSettings systemSettings;
        private Database database;
        private Caches caches;
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
        public Builder setCaches( Caches caches )
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
    }


    /**
     * Ingests a time-series whose events are {@link Double}.
     * @param timeSeries the time-series to ingest, not null
     * @param dataSource the data source
     * @return the ingest results
     */

    @Override
    public List<IngestResult> ingestSingleValuedTimeSeries( TimeSeries<Double> timeSeries, DataSource dataSource )
    {
        Objects.requireNonNull( timeSeries );
        Objects.requireNonNull( timeSeries.getMetadata() );
        Objects.requireNonNull( timeSeries.getMetadata()
                                          .getFeature() );
        Objects.requireNonNull( timeSeries.getMetadata()
                                          .getVariableName() );
        Objects.requireNonNull( timeSeries.getMetadata()
                                          .getUnit() );

        List<IngestResult> results;

        SourceDetails source = this.saveSource( timeSeries, dataSource.getUri() );

        // Not found 
        if ( source.performedInsert() )
        {
            this.lockSource( source, timeSeries );

            // Ready to ingest, source row was inserted and is (advisory) locked.
            Set<Pair<CountDownLatch, CountDownLatch>> latches =
                    this.insertSingleValuedTimeSeries( this.getSystemSettings(),
                                                       this.getDatabase(),
                                                       this.getCaches()
                                                           .getEnsemblesCache(),
                                                       timeSeries,
                                                       source.getId() );

            // Mark complete
            results = this.completeSource( source, latches, dataSource );
        }
        // Found
        else
        {
            results = this.completeSource( source, dataSource );
        }

        return results;
    }
    
    /**
     * Ingests a time-series whose events are {@link Ensemble}.
     * @param timeSeries the time-series to ingest, not null
     * @param dataSource the data source
     * @return the ingest results
     */

    @Override
    public List<IngestResult> ingestEnsembleTimeSeries( TimeSeries<Ensemble> timeSeries, DataSource dataSource )
    {
        Objects.requireNonNull( timeSeries );
        Objects.requireNonNull( timeSeries.getMetadata() );
        Objects.requireNonNull( timeSeries.getMetadata()
                                          .getFeature() );
        Objects.requireNonNull( timeSeries.getMetadata()
                                          .getVariableName() );
        Objects.requireNonNull( timeSeries.getMetadata()
                                          .getUnit() );

        List<IngestResult> results;

        SourceDetails source = this.saveSource( timeSeries, dataSource.getUri() );

        // Not found
        if ( source.performedInsert() )
        {
            this.lockSource( source, timeSeries );

            // Ready to ingest, source row was inserted and is (advisory) locked.
            Set<Pair<CountDownLatch, CountDownLatch>> latches =
                    this.insertEnsembleTimeSeries( this.getSystemSettings(),
                                                   this.getDatabase(),
                                                   this.getCaches()
                                                       .getEnsemblesCache(),
                                                   timeSeries,
                                                   source.getId() );

            // Mark complete
            results = this.completeSource( source, latches, dataSource );
        }
        // Found
        else
        {
            results = this.completeSource( source, dataSource );
        }

        return results;
    }

    /**
     * Attempts to lock a source aka time-series.
     * @param <T> the type of time-series event value
     * @param source the source to lock
     * @param timeSeries the time-series
     */

    private <T> void lockSource( SourceDetails source, TimeSeries<T> timeSeries )
    {
        if ( LOGGER.isDebugEnabled() )
        {
            byte[] rawHash = this.identifyTimeSeries( timeSeries, "" );
            String hash = Hex.encodeHexString( rawHash, false );
            LOGGER.debug( "{} is responsible for source {}", this, hash );
        }

        try
        {
            this.lockManager.lockSource( source.getId() );
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Failed to lock for source id "
                                       + source.getId(),
                                       se );
        }
    }

    /**
     * Saves the source information associated with the time-series.
     * @param <T> the type of time-series event value
     * @param time-series whose source information should be saved
     * @param uri the data source uri
     * @return the source details
     */

    private <T> SourceDetails saveSource( TimeSeries<T> timeSeries, URI uri )
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
        FeatureKey feature = metadata.getFeature();
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
            throw new IngestException( "Source metadata about '" + uri
                                       +
                                       "' could not be stored or retrieved from the database.",
                                       se );
        }

        LOGGER.debug( "Found {}? {}", source, !source.performedInsert() );

        return source;
    }

    /**
     * Completes a source.
     * @param source the source details
     * @param dataSource the data source
     * @return the ingest results
     */

    private List<IngestResult> completeSource( SourceDetails source,
                                               DataSource dataSource )
    {
        List<IngestResult> results;
        SourceCompletedDetails completedDetails = this.createSourceCompletedDetails( source );
        boolean completed;
        try
        {
            completed = completedDetails.wasCompleted();
        }
        catch ( SQLException se )
        {
            throw new PreIngestException( "Failed to determine if source "
                                          + source
                                          + " was already ingested.",
                                          se );
        }

        if ( completed )
        {
            // Already present and completed
            results = IngestResult.singleItemListFrom( dataSource,
                                                       source.getId(),
                                                       true,
                                                       false );

            // When successfully completed, remove temp files associated.
            URI uri = dataSource.getUri();

            if ( uri.toString()
                    .contains( ZippedSource.TEMP_FILE_PREFIX ) )
            {
                try
                {
                    Files.delete( Paths.get( uri ) );
                }
                catch ( IOException ioe )
                {
                    LOGGER.warn( "Could not remove temp file {}", uri );
                }
            }
        }
        else
        {
            results = IngestResult.singleItemListFrom( dataSource,
                                                       source.getId(),
                                                       true,
                                                       true );
        }

        return results;
    }

    /**
     * Completes a source.
     * @param source the source details
     * @param latches the latches
     * @param dataSource the data source
     * @return the ingest results
     */

    private List<IngestResult> completeSource( SourceDetails source,
                                               Set<Pair<CountDownLatch, CountDownLatch>> latches,
                                               DataSource dataSource )
    {
        List<IngestResult> results;

        // Mark complete
        SourceCompleter completer = createSourceCompleter( source.getId(),
                                                           this.lockManager );
        completer.complete( latches );
        results = IngestResult.singleItemListFrom( dataSource,
                                                   source.getId(),
                                                   false,
                                                   false );
        return results;
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

    private void insertReferenceTimeRows( Database database,
                                          long sourceId,
                                          Map<ReferenceTimeType, Instant> referenceTimes )
    {
        int rowCount = referenceTimes.size();

        if ( rowCount == 0 )
        {
            throw new PreIngestException( "At least one reference datetime is required until we refactor TSV to permit zero." );
        }

        if ( rowCount != 1 )
        {
            throw new PreIngestException( "Exactly one reference datetime is required until we allow callers to declare which one to use at evaluation time." );
            // TODO: add post-ingest pre-retrieval validation and/or
            //     optional declaration to disambiguate, then remove this block.
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
     *
     * @param ensembleName Name of the ensemble trace.
     * @return Raw surrogate key from db
     * @throws IngestException When any query involved fails.
     */
    private long insertOrGetEnsembleId( Ensembles ensemblesCache,
                                        String ensembleName )
    {
        long id;

        try
        {
            id = ensemblesCache.getEnsembleID( ensembleName );
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
        if ( valueToAdd.equals( MissingValues.DOUBLE ) )
        {
            valueToAdd = null;
        }

        return IngestedValues.addTimeSeriesValue( systemSettings,
                                                  database,
                                                  timeSeriesId,
                                                  (int) leadDuration.toMinutes(),
                                                  valueToAdd );
    }

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


    private wres.io.data.details.TimeSeries getDbTimeSeriesForEnsembleTrace( Database database,
                                                                             long ensembleId,
                                                                             long sourceId )
    {
        return new wres.io.data.details.TimeSeries( database,
                                                    ensembleId,
                                                    sourceId );
    }

    private wres.io.data.details.TimeSeries getDbTimeSeries( Database database,
                                                             Ensembles ensemblesCache,
                                                             long sourceId )
            throws SQLException
    {
        return new wres.io.data.details.TimeSeries( database,
                                                    ensemblesCache.getDefaultEnsembleID(),
                                                    sourceId );
    }

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


    private long getMeasurementUnitId( String measurementUnit ) throws SQLException
    {
        MeasurementUnits innerMeasurementUnitsCache = this.getCaches()
                                                          .getMeasurementUnitsCache();
        return innerMeasurementUnitsCache.getOrCreateMeasurementUnitId( measurementUnit );
    }


    private long getFeatureId( FeatureKey featureKey ) throws SQLException
    {
        Features innerFeaturesCache = this.getCaches()
                                          .getFeaturesCache();
        return innerFeaturesCache.getOrCreateFeatureId( featureKey );
    }

    private long getTimeScaleId( TimeScaleOuter timeScale ) throws SQLException
    {
        TimeScales innerTimeScalesCache = this.getCaches()
                                              .getTimeScalesCache();
        return innerTimeScalesCache.getOrCreateTimeScaleId( timeScale );
    }

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

    private Database getDatabase()
    {
        return this.database;
    }

    private SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }

    private Caches getCaches()
    {
        return this.caches;
    }
}
