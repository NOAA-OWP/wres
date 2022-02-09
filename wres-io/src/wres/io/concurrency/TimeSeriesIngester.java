package wres.io.concurrency;

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.Callable;
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
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.details.SourceCompletedDetails;
import wres.io.data.details.SourceDetails;
import wres.io.data.caching.TimeScales;
import wres.io.reading.DataSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.IngestedValues;
import wres.io.reading.PreIngestException;
import wres.io.reading.SourceCompleter;
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

public class TimeSeriesIngester implements Callable<List<IngestResult>>
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger( TimeSeriesIngester.class );

    private final SystemSettings systemSettings;
    private final Database database;
    private final Features featuresCache;
    private final TimeScales timeScalesCache;
    private final Ensembles ensemblesCache;
    private final MeasurementUnits measurementUnitsCache;
    private final ProjectConfig projectConfig;
    private final DataSource dataSource;
    private final DatabaseLockManager lockManager;
    private final TimeSeries<?> timeSeries;
    private final Set<Pair<CountDownLatch, CountDownLatch>> latches = new HashSet<>();

    public static TimeSeriesIngester of( SystemSettings systemSettings,
                                         Database database,
                                         Features featuresCache,
                                         TimeScales timeScalesCache,
                                         Ensembles ensemblesCache,
                                         MeasurementUnits measurementUnitsCache,
                                         ProjectConfig projectConfig,
                                         DataSource dataSource,
                                         DatabaseLockManager databaseLockManager,
                                         TimeSeries<?> timeSeries )
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( database );
        Objects.requireNonNull( timeSeries );
        Objects.requireNonNull( timeSeries.getMetadata() );
        Objects.requireNonNull( timeSeries.getMetadata()
                                          .getFeature() );
        Objects.requireNonNull( timeSeries.getMetadata()
                                          .getVariableName() );
        Objects.requireNonNull( timeSeries.getMetadata()
                                          .getUnit() );
        Objects.requireNonNull( ensemblesCache );
        Objects.requireNonNull( featuresCache );
        Objects.requireNonNull( timeScalesCache );
        Objects.requireNonNull( measurementUnitsCache );

        return new TimeSeriesIngester( systemSettings,
                                       database,
                                       featuresCache,
                                       timeScalesCache,
                                       ensemblesCache,
                                       measurementUnitsCache,
                                       projectConfig,
                                       dataSource,
                                       databaseLockManager,
                                       timeSeries );
    }

    private TimeSeriesIngester( SystemSettings systemSettings,
                                Database database,
                                Features featuresCache,
                                TimeScales timeScalesCache,
                                Ensembles ensemblesCache,
                                MeasurementUnits measurementUnitsCache,
                                ProjectConfig projectConfig,
                                DataSource dataSource,
                                DatabaseLockManager lockManager,
                                TimeSeries<?> timeSeries )
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( database );
        Objects.requireNonNull( featuresCache );
        Objects.requireNonNull( timeScalesCache );
        Objects.requireNonNull( measurementUnitsCache );
        Objects.requireNonNull( ensemblesCache );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( lockManager );
        Objects.requireNonNull( timeSeries );
        Objects.requireNonNull( timeSeries.getMetadata() );

        this.systemSettings = systemSettings;
        this.database = database;
        this.featuresCache = featuresCache;
        this.timeScalesCache = timeScalesCache;
        this.ensemblesCache = ensemblesCache;
        this.measurementUnitsCache = measurementUnitsCache;
        this.projectConfig = projectConfig;
        this.dataSource = dataSource;
        this.lockManager = lockManager;
        this.timeSeries = timeSeries;
    }


    @Override
    public List<IngestResult> call()
    {
        List<IngestResult> results;
        URI location = this.getLocation();

        byte[] rawHash = this.identifyTimeSeries( this.getTimeSeries(), "" );
        String hash = Hex.encodeHexString( rawHash, false );

        boolean foundAlready;
        boolean completed;
        SourceCompletedDetails completedDetails;
        Database database = this.getDatabase();
        SourceDetails source = this.createSourceDetails( hash );

        // Lead column is only for raster data as of 2022-01
        source.setLead( null );
        source.setIsPointData( true );
        source.setSourcePath( location );
        TimeSeriesMetadata metadata = this.getTimeSeries()
                                          .getMetadata();
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
            source.save( database );
            foundAlready = !source.performedInsert();
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Source metadata about '" + location +
                                       "' could not be stored or retrieved from the database.",
                                       se );
        }

        LOGGER.debug( "Found {}? {}", source, foundAlready );

        if ( !foundAlready )
        {
            LOGGER.debug( "{} is responsible for source {}", this, hash );

            try
            {
                this.lockManager.lockSource( source.getId() );
            }
            catch ( SQLException se )
            {
                throw new IngestException( "Failed to lock for source id "
                                           + source.getId(), se );
            }

            // Ready to ingest, source row was inserted and is (advisory) locked.
            this.insertEverything( this.getSystemSettings(),
                                   this.getDatabase(),
                                   this.getEnsemblesCache(),
                                   this.getTimeSeries(),
                                   source.getId() );

            // Mark complete
            SourceCompleter completer = createSourceCompleter( source.getId(),
                                                               this.lockManager );
            completer.complete( this.latches );
            results = IngestResult.singleItemListFrom( this.projectConfig,
                                                       this.dataSource,
                                                       source.getId(),
                                                       false,
                                                       false );
        }
        else
        {
            completedDetails = this.createSourceCompletedDetails( source );

            try
            {
                completed = completedDetails.wasCompleted();
            }
            catch ( SQLException se )
            {
                throw new PreIngestException( "Failed to determine if source "
                                              + source
                                              + " was already ingested.", se );
            }

            if ( completed )
            {
                // Already present and completed
                results = IngestResult.singleItemListFrom( this.projectConfig,
                                                           this.dataSource,
                                                           source.getId(),
                                                           true,
                                                           false );

                // When successfully completed, remove temp files associated.
                URI uri = this.dataSource.getUri();

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
                // Already started but not completed, include the TimeSeries.
                DataSource dataSourceWithTimeSeries =
                        DataSource.of( this.dataSource.getDisposition(),
                                       this.dataSource.getSource(),
                                       this.dataSource.getContext(),
                                       this.dataSource.getLinks(),
                                       this.dataSource.getUri(),
                                       this.timeSeries );
                results = IngestResult.singleItemListFrom( this.projectConfig,
                                                           dataSourceWithTimeSeries,
                                                           source.getId(),
                                                           true,
                                                           true );
            }
        }

        return results;
    }

    private void insertEverything( SystemSettings systemSettings,
                                   Database database,
                                   Ensembles ensemblesCache,
                                   TimeSeries<?> timeSeries,
                                   long sourceId )
    {
        SortedSet<? extends Event<?>> events = timeSeries.getEvents();

        if ( timeSeries.getEvents()
                       .isEmpty() )
        {
            throw new IllegalArgumentException( "TimeSeries must not be empty." );
        }

        Event<?> event = events.iterator()
                               .next();

        TimeSeriesMetadata metadata = timeSeries.getMetadata();
        this.insertReferenceTimeRows( database, sourceId, metadata.getReferenceTimes() );

        if ( event.getValue() instanceof Ensemble )
        {
            LOGGER.debug( "Found a TimeSeries<Ensemble>" );
            for ( Map.Entry<Object,SortedSet<Event<Double>>> trace :
                    TimeSeriesSlicer.decomposeWithLabels( (TimeSeries<Ensemble>) timeSeries )
                                    .entrySet() )
            {
                LOGGER.debug( "TimeSeries trace: {}", trace );
                String ensembleName = trace.getKey()
                                           .toString();

                long ensembleId = this.insertOrGetEnsembleId( ensemblesCache,
                                                              ensembleName );
                long timeSeriesId = this.insertTimeSeriesRowForEnsembleTrace(
                        database,
                        (TimeSeries<Ensemble>) timeSeries,
                        ensembleId,
                        sourceId );
                this.insertEnsembleTrace( systemSettings,
                                          database,
                                          (TimeSeries<Ensemble>) timeSeries,
                                          timeSeriesId,
                                          trace.getValue() );
            }
        }
        else if ( event.getValue() instanceof Double )
        {
            long timeSeriesId = this.insertTimeSeriesRow( database,
                                                          ensemblesCache,
                                                          (TimeSeries<Double>) timeSeries,
                                                          sourceId );
            this.insertTimeSeriesValuesRows( systemSettings,
                                             database,
                                             timeSeriesId,
                                             (TimeSeries<Double>) timeSeries );
        }
        else
        {
            throw new UnsupportedOperationException( "Unable to ingest value "
                                                     + event.getValue() );
        }
    }

    private void insertReferenceTimeRows( Database database,
                                          long sourceId,
                                          Map<ReferenceTimeType,Instant> referenceTimes )
    {
        int rowCount = referenceTimes.size();

        if ( rowCount == 0 )
        {
            throw new PreIngestException( "At least one reference datetime is required until we refactor TSV to permit zero." );

            // TODO: refactor wres.TimeSeriesValue to use valid datetimes, then:
            //return;
        }

        if ( rowCount != 1 )
        {
            throw new PreIngestException( "Exactly one reference datetime is required until we allow callers to declare which one to use at evaluation time." );
            // TODO: add post-ingest pre-retrieval validation and/or
            //     optional declaration to disambiguate, then remove this block.
        }

        List<String[]> rows = new ArrayList<>( rowCount );

        for ( Map.Entry<ReferenceTimeType,Instant> referenceTime : referenceTimes.entrySet() )
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
            throws IngestException
    {
        long id;

        try
        {
            id = ensemblesCache.getEnsembleID( ensembleName );
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Failed to get Ensemble info for "
                                       + ensembleName, se );
        }

        return id;
    }

    private void insertEnsembleTrace( SystemSettings systemSettings,
                                      Database database,
                                      TimeSeries<Ensemble> originalTimeSeries,
                                      long timeSeriesIdForTrace,
                                      SortedSet<Event<Double>> ensembleTrace )
            throws IngestException
    {
        Instant referenceDatetime = this.getReferenceDatetime( originalTimeSeries );

        for ( Event<Double> event : ensembleTrace )
        {
            this.insertTimeSeriesValuesRow( systemSettings,
                                            database,
                                            timeSeriesIdForTrace,
                                            referenceDatetime,
                                            event );
        }
    }

    private void insertTimeSeriesValuesRows( SystemSettings systemSettings,
                                             Database database,
                                             long timeSeriesId,
                                             TimeSeries<Double> timeSeries )
            throws IngestException
    {
        Instant referenceDatetime = this.getReferenceDatetime( timeSeries );

        for ( Event<Double> event : timeSeries.getEvents() )
        {
            this.insertTimeSeriesValuesRow( systemSettings,
                                            database,
                                            timeSeriesId,
                                            referenceDatetime,
                                            event );
        }
    }

    private void insertTimeSeriesValuesRow( SystemSettings systemSettings,
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

        Pair<CountDownLatch, CountDownLatch> latchPair =
                IngestedValues.addTimeSeriesValue( systemSettings,
                                                   database,
                                                   timeSeriesId,
                                                   (int) leadDuration.toMinutes(),
                                                   valueToAdd );
        this.latches.add( latchPair );
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
                                       + "timeSeries=" + timeSeries
                                       + " source=" + sourceId,
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
                                       + "timeSeries=" + timeSeries
                                       + " source=" + sourceId
                                       + " ensembleId=" + ensembleId,
                                       se );
        }
    }


    private wres.io.data.details.TimeSeries getDbTimeSeriesForEnsembleTrace(
            Database database,
            long ensembleId,
            long sourceId )
            throws SQLException
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

    private Instant getReferenceDatetime( TimeSeries<?> timeSeries )
    {
        if ( timeSeries.getReferenceTimes().isEmpty() )
        {
            throw new IllegalStateException( "Data must have at least one reference datetime: "
                                             + timeSeries.toString() );
        }

        // Use the first reference datetime found until the database allows
        // storage of all the reference datetimes associated with a forecast.
        Map.Entry<ReferenceTimeType,Instant> entry =
                timeSeries.getReferenceTimes()
                          .entrySet()
                          .iterator()
                          .next();
        {
            LOGGER.debug( "Using the first reference datetime type found: {}",
                          entry );
            return entry.getValue();
        }
    }


    private long getMeasurementUnitId( String measurementUnit ) throws SQLException
    {
        MeasurementUnits measurementUnitsCache = this.getMeasurementUnitsCache();
        return measurementUnitsCache.getOrCreateMeasurementUnitId( measurementUnit );
    }


    private long getFeatureId( FeatureKey featureKey ) throws SQLException
    {
        Features featuresCache = this.getFeaturesCache();
        return featuresCache.getOrCreateFeatureId( featureKey );
    }

    private long getTimeScaleId( TimeScaleOuter timeScale ) throws SQLException
    {
        TimeScales timeScalesCache = this.getTimeScalesCache();
        return timeScalesCache.getOrCreateTimeScaleId( timeScale );
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

    private Ensembles getEnsemblesCache()
    {
        return this.ensemblesCache;
    }

    private Features getFeaturesCache()
    {
        return this.featuresCache;
    }

    private TimeScales getTimeScalesCache()
    {
        return this.timeScalesCache;
    }

    private MeasurementUnits getMeasurementUnitsCache()
    {
        return this.measurementUnitsCache;
    }

    private TimeSeries<?> getTimeSeries()
    {
        return this.timeSeries;
    }

    private URI getLocation()
    {
        return this.dataSource.getUri();
    }
}
