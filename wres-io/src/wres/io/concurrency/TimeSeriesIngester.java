package wres.io.concurrency;

import java.io.IOException;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
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

import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.datamodel.Ensemble;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.data.details.FeatureDetails;
import wres.io.data.details.SourceCompletedDetails;
import wres.io.data.details.SourceDetails;
import wres.io.reading.DataSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.IngestedValues;
import wres.io.reading.PreIngestException;
import wres.io.reading.SourceCompleter;
import wres.system.DatabaseLockManager;

/**
 * Ingests given TimeSeries data if not already ingested.
 *
 * As of 2019-10-29, only used by NWMReader, which as of 2019-10-29 is disabled.
 * As of 2019-10-29, only supports data with one or more reference datetime.
 * As of 2019-10-29, uses toString() representation to identify data.
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

    private final ProjectConfig projectConfig;
    private final DataSource dataSource;
    private final DatabaseLockManager lockManager;
    private final String locationName;
    private final String variableName;
    private final String measurementUnit;
    private final TimeSeries<?> timeSeries;
    private final Set<Pair<CountDownLatch, CountDownLatch>> latches = new HashSet<>();

    public TimeSeriesIngester( ProjectConfig projectConfig,
                               DataSource dataSource,
                               DatabaseLockManager lockManager,
                               TimeSeries<?> timeSeries,
                               String locationName,
                               String variableName,
                               String measurementUnit )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( lockManager );
        Objects.requireNonNull( timeSeries );
        Objects.requireNonNull( locationName );
        Objects.requireNonNull( variableName );
        Objects.requireNonNull( measurementUnit );

        this.projectConfig = projectConfig;
        this.dataSource = dataSource;
        this.lockManager = lockManager;
        this.timeSeries = timeSeries;
        this.locationName = locationName;
        this.variableName = variableName;
        this.measurementUnit = measurementUnit;
    }


    @Override
    public List<IngestResult> call() throws IOException
    {
        URI location = this.getLocation();

        Instant now = Instant.now();
        byte[] rawHash = this.identifyTimeSeries( this.getTimeSeries() );
        String hash = Hex.encodeHexString( rawHash, false );

        SourceDetails.SourceKey sourceKey =
                new SourceDetails.SourceKey( location,
                                             now.toString(),
                                             null,
                                             hash );

        boolean foundAlready;
        boolean completed;
        SourceDetails source;
        SourceCompletedDetails completedDetails;

        try
        {
            source = this.createSourceDetails( sourceKey );
            source.save();
            foundAlready = !source.performedInsert();
        }
        catch ( SQLException e )
        {
            throw new IngestException( "Source metadata about '" + location +
                                       "' could not be stored or retrieved from the database." );
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
            this.insertEverything( this.getTimeSeries(),
                                   source.getId(),
                                   this.getMeasurementUnit() );

            // Mark complete
            SourceCompleter completer = createSourceCompleter( source.getId(),
                                                               this.lockManager );
            completer.complete( this.latches );
            return IngestResult.singleItemListFrom( this.projectConfig,
                                                    this.dataSource,
                                                    hash,
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
            // Already present.
            return IngestResult.singleItemListFrom( this.projectConfig,
                                                    this.dataSource,
                                                    hash,
                                                    true,
                                                    !completed );
        }
    }

    private void insertEverything( TimeSeries<?> timeSeries,
                                   int sourceId,
                                   String measurementUnit )
            throws IOException
    {
        SortedSet<? extends Event<?>> events = timeSeries.getEvents();

        if ( timeSeries.getEvents()
                       .isEmpty() )
        {
            throw new IllegalArgumentException( "TimeSeries must not be empty." );
        }

        Event<?> event = events.iterator()
                               .next();

        if ( event.getValue() instanceof Ensemble )
        {
            LOGGER.debug( "Found a TimeSeries<Ensemble>" );
            for ( Map.Entry<Object,SortedSet<Event<Double>>> trace :
                    TimeSeriesSlicer.decomposeWithLabels( (TimeSeries<Ensemble>) timeSeries )
                                    .entrySet() )
            {
                LOGGER.info( "TimeSeries trace: {}", trace );
                Object ensembleName = trace.getKey();
                int ensembleId = this.insertOrGetEnsembleId( ensembleName );
                int timeSeriesId = this.insertTimeSeriesRowForEnsembleTrace(
                        (TimeSeries<Ensemble>) timeSeries,
                        ensembleId,
                        sourceId,
                        measurementUnit );
                this.insertEnsembleTrace( (TimeSeries<Ensemble>) timeSeries,
                                          timeSeriesId,
                                          trace.getValue() );
            }
        }
        else if ( event.getValue() instanceof Double )
        {
            int timeSeriesId = this.insertTimeSeriesRow( (TimeSeries<Double>) timeSeries,
                                                         sourceId,
                                                         measurementUnit );
            this.insertTimeSeriesValuesRows( timeSeriesId,
                                             (TimeSeries<Double>) timeSeries );
        }
        else
        {
            throw new UnsupportedOperationException( "Unable to ingest value "
                                                     + event.getValue() );
        }
    }

    /**
     *
     * @param ensembleName Name of the ensemble trace.
     * @return Raw surrogate key from db
     * @throws IngestException When any query involved fails.
     */
    private int insertOrGetEnsembleId( Object ensembleName )
            throws IngestException
    {
        int id;

        Integer ensembleNumber = null;

        if ( ensembleName instanceof Integer )
        {
            ensembleNumber = (Integer) ensembleName;
        }

        try
        {
            id = Ensembles.getEnsembleID( ensembleName.toString(),
                                          ensembleNumber,
                                          ensembleName.toString() );
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Failed to get Ensemble info for "
                                       + ensembleName, se );
        }

        return id;
    }

    private void insertEnsembleTrace( TimeSeries<Ensemble> originalTimeSeries,
                                      int timeSeriesIdForTrace,
                                      SortedSet<Event<Double>> ensembleTrace )
            throws IngestException
    {
        Instant referenceDatetime = this.getReferenceDatetime( originalTimeSeries );

        for ( Event<Double> event : ensembleTrace )
        {
            this.insertTimeSeriesValuesRow( timeSeriesIdForTrace,
                                            referenceDatetime,
                                            event );
        }
    }

    private void insertTimeSeriesValuesRows( int timeSeriesId,
                                             TimeSeries<Double> timeSeries )
            throws IngestException
    {
        Instant referenceDatetime = this.getReferenceDatetime( timeSeries );

        for ( Event<Double> event : timeSeries.getEvents() )
        {
            this.insertTimeSeriesValuesRow( timeSeriesId,
                                            referenceDatetime,
                                            event );
        }
    }

    private void insertTimeSeriesValuesRow( int timeSeriesId,
                                            Instant referenceDatetime,
                                            Event<Double> valueAndValidDateTime )
            throws IngestException
    {

        Instant validDatetime = valueAndValidDateTime.getTime();
        Duration leadDuration = Duration.between( referenceDatetime, validDatetime );
        Pair<CountDownLatch, CountDownLatch> latchPair =
                IngestedValues.addTimeSeriesValue( timeSeriesId,
                                                   (int) leadDuration.toMinutes(),
                                                   valueAndValidDateTime.getValue() );
        this.latches.add( latchPair );
    }

    private int insertTimeSeriesRow( TimeSeries<Double> timeSeries,
                                     int sourceId,
                                     String measurementUnit )
            throws IngestException
    {

        wres.io.data.details.TimeSeries databaseTimeSeries;
        try
        {
            databaseTimeSeries = this.getDbTimeSeries( timeSeries,
                                                       sourceId,
                                                       measurementUnit );
            // The following indirectly calls save:
            return databaseTimeSeries.getTimeSeriesID();
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Failed to get TimeSeries info for "
                                       + "timeSeries=" + timeSeries
                                       + " source=" + sourceId
                                       + " measurementUnit=" + measurementUnit,
                                       se );
        }
    }

    private int insertTimeSeriesRowForEnsembleTrace( TimeSeries<Ensemble> timeSeries,
                                                     int ensembleId,
                                                     int sourceId,
                                                     String measurementUnit )
            throws IngestException
    {

        wres.io.data.details.TimeSeries databaseTimeSeries;
        try
        {
            databaseTimeSeries = this.getDbTimeSeriesForEnsembleTrace( timeSeries,
                                                                       ensembleId,
                                                                       sourceId,
                                                                       measurementUnit );
            // The following indirectly calls save:
            return databaseTimeSeries.getTimeSeriesID();
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Failed to get TimeSeries info for "
                                       + "timeSeries=" + timeSeries
                                       + " source=" + sourceId
                                       + " measurementUnit=" + measurementUnit,
                                       se );
        }
    }


    private wres.io.data.details.TimeSeries getDbTimeSeriesForEnsembleTrace(
            TimeSeries<Ensemble> ensembleTimeSeries,
            int ensembleId,
            int sourceId,
            String measurementUnit )
            throws SQLException
    {
        Instant referenceDatetime = this.getReferenceDatetime( ensembleTimeSeries );
        wres.io.data.details.TimeSeries databaseTimeSeries =
                new wres.io.data.details.TimeSeries( sourceId,
                                                     referenceDatetime.toString() );
        databaseTimeSeries.setEnsembleID( ensembleId );
        int measurementUnitId = this.getMeasurementUnitId( measurementUnit );
        databaseTimeSeries.setMeasurementUnitID( measurementUnitId );
        int variableFeatureId = this.getVariableFeatureId( this.getLocationName(),
                                                           this.getVariableName() );
        databaseTimeSeries.setVariableFeatureID( variableFeatureId );
        databaseTimeSeries.setTimeScale( ensembleTimeSeries.getTimeScale() );
        return databaseTimeSeries;
    }

    private wres.io.data.details.TimeSeries getDbTimeSeries( TimeSeries<Double> timeSeries,
                                                             int sourceId,
                                                             String measurementUnit )
            throws SQLException
    {
        Instant referenceDatetime = this.getReferenceDatetime( timeSeries );
        wres.io.data.details.TimeSeries databaseTimeSeries =
                new wres.io.data.details.TimeSeries( sourceId,
                                                     referenceDatetime.toString() );
        databaseTimeSeries.setEnsembleID( Ensembles.getDefaultEnsembleID() );
        int measurementUnitId = this.getMeasurementUnitId( measurementUnit );
        databaseTimeSeries.setMeasurementUnitID( measurementUnitId );
        int variableFeatureId = this.getVariableFeatureId( this.getLocationName(),
                                                           this.getVariableName() );
        databaseTimeSeries.setVariableFeatureID( variableFeatureId );
        databaseTimeSeries.setTimeScale( timeSeries.getTimeScale() );
        return databaseTimeSeries;
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
            LOGGER.warn( "Using the first reference datetime type found: {}",
                          entry );
            return entry.getValue();
        }
    }


    private int getMeasurementUnitId( String measurementUnit ) throws SQLException
    {
        return MeasurementUnits.getMeasurementUnitID( measurementUnit );
    }

    private byte[] identifyTimeSeries ( TimeSeries<?> timeSeries )
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
        byte[] hash = digestUtils.digest( timeSeries.toString() );

        if ( hash.length < 16 )
        {
            throw new PreIngestException( "The MD5 sum of " + timeSeries
                                          + " was shorter than expected." );
        }

        return hash;
    }


    private int getVariableFeatureId( String featureName, String variableName )
            throws SQLException
    {
        Feature feature = new Feature( null,
                                       null,
                                       null,
                                       null,
                                       null,
                                       featureName,
                                       null,
                                       null,
                                       null,
                                       null,
                                       null,
                                       null,
                                       null );
        FeatureDetails details = Features.getDetails( feature );
        int variableId = Variables.getVariableID( variableName );
        return Features.getVariableFeatureByFeature( details, variableId );
    }

    /**
     * This method facilitates testing, Pattern 1 at
     * https://github.com/mockito/mockito/wiki/Mocking-Object-Creation
     * @param sourceKey the first arg to SourceDetails
     * @return a SourceDetails
     */

    SourceDetails createSourceDetails( SourceDetails.SourceKey sourceKey )
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

    SourceCompleter createSourceCompleter( int sourceId,
                                           DatabaseLockManager lockManager )
    {
        return new SourceCompleter( sourceId, lockManager );
    }


    /**
     * This method facilitates testing, Pattern 1 at
     * https://github.com/mockito/mockito/wiki/Mocking-Object-Creation
     * @param sourceDetails the first arg to SourceCompletedDetails
     * @return a SourceCompleter
     */

    SourceCompletedDetails createSourceCompletedDetails( SourceDetails sourceDetails )
    {
        return new SourceCompletedDetails( sourceDetails );
    }


    private TimeSeries<?> getTimeSeries()
    {
        return this.timeSeries;
    }

    private URI getLocation()
    {
        return this.dataSource.getUri();
    }

    private String getMeasurementUnit()
    {
        return this.measurementUnit;
    }

    private String getLocationName()
    {
        return this.locationName;
    }

    private String getVariableName()
    {
        return this.variableName;
    }
}