package wres.io.reading.wrds;

import java.io.IOException;
import java.io.InputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import wres.config.generated.ProjectConfig;
import wres.datamodel.metadata.TimeScale;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.data.details.FeatureDetails;
import wres.io.data.details.SourceCompletedDetails;
import wres.io.data.details.SourceDetails;
import wres.io.data.details.TimeSeries;
import wres.io.reading.DataSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.IngestedValues;
import wres.io.reading.PreIngestException;
import wres.io.reading.SourceCompleter;
import wres.system.DatabaseLockManager;
import wres.util.TimeHelper;

public class ReadValueManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ReadValueManager.class );

    // TODO: inject http client in constructor without changing much else #60281
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private static final String MD5SUM_OF_EMPTY_STRING = "68b329da9893e34099c7d8ad5cb9c940";

    private final ProjectConfig projectConfig;
    private final DataSource dataSource;
    private final DatabaseLockManager lockManager;
    private final Set<Pair<CountDownLatch,CountDownLatch>> latches = new HashSet<>();

    ReadValueManager( final ProjectConfig projectConfig,
                      final DataSource datasource,
                      DatabaseLockManager lockManager )
    {
        this.projectConfig = projectConfig;
        this.dataSource = datasource;
        this.lockManager = lockManager;
    }

    public List<IngestResult> save() throws IOException
    {
        InputStream forecastData;
        Instant now = Instant.now();
        URI location = this.getLocation();

        if ( location.getScheme()
                     .equals( "file" ) )
        {
            forecastData = this.getFromFile( location );
        }
        else if ( location.getScheme()
                          .toLowerCase()
                          .startsWith( "http" ) )
        {
            Pair<Integer,InputStream> response = this.getFromWeb( location );
            int httpStatus = response.getLeft();

            if ( httpStatus >= 400 && httpStatus < 500 )
            {
                LOGGER.warn( "Treating HTTP response code {} as no data found from URI {}",
                             httpStatus,
                             location );

                try
                {
                    // Cannot trust the DataSources.get() method to accurately
                    // report performedInsert(). Use other means here.
                    SourceDetails.SourceKey sourceKey =
                            new SourceDetails.SourceKey( location,
                                                         now.toString(),
                                                         null,
                                                         MD5SUM_OF_EMPTY_STRING.toUpperCase() );

                    SourceDetails details = this.createSourceDetails( sourceKey );
                    details.save();
                    boolean foundAlready = !details.performedInsert();

                    LOGGER.debug( "Found {}? {}", details, foundAlready );

                    if ( !foundAlready )
                    {
                        this.lockManager.lockSource( details.getId() );
                        SourceCompletedDetails completedDetails =
                                createSourceCompletedDetails( details );
                        completedDetails.markCompleted();
                        // A special case here, where we don't use
                        // source completer because we know there are no data
                        // rows to be inserted, therefore there will be no
                        // coordination with the use of synchronizers/latches.
                        // Therefore, plain lock and unlock here.
                        this.lockManager.unlockSource( details.getId() );

                        LOGGER.debug( "Empty source id {} marked complete.",
                                     details.getId() );
                    }

                    return IngestResult.singleItemListFrom(
                            this.projectConfig,
                            this.dataSource,
                            MD5SUM_OF_EMPTY_STRING.toUpperCase(),
                            foundAlready,
                            false
                    );
                }
                catch ( SQLException e )
                {
                    throw new IngestException( "Source metadata for '"
                                               + location +
                                               "' could not be stored in or retrieved from the database.",
                                               e );
                }
            }
            else
            {
                forecastData = response.getRight();
            }
        }
        else
        {
            throw new UnsupportedOperationException( "Only file and http(s) "
                                                     + "are supported. Got: "
                                                     + location );
        }

        // It is conceivable that we could tee/pipe the data to both
        // the md5sum and the parser at the same time, but this involves
        // more complexity and may not be worth it. For now assume that we are
        // not going to exhaust our heap by including the whole forecast
        // here in memory temporarily.
        byte[] rawForecast = IOUtils.toByteArray( forecastData );

        MessageDigest md5Name;

        try
        {
            md5Name = MessageDigest.getInstance( "MD5" );
        }
        catch ( NoSuchAlgorithmException nsae )
        {
            throw new PreIngestException( "Couldn't use MD5 algorithm.", nsae );
        }

        DigestUtils digestUtils = new DigestUtils( md5Name );
        String hash = digestUtils.digestAsHex( rawForecast )
                                 .toUpperCase();

        boolean foundAlready;
        boolean completed;
        boolean dataSaved = false;
        SourceDetails source;
        SourceCompletedDetails completedDetails;

        try
        {
            source = DataSources.get( location, now.toString(), null, hash );
            foundAlready = !source.performedInsert();
        }
        catch ( SQLException e )
        {
            throw new IngestException( "Source metadata about '" + location +
                                       "' could not be stored or retrieved from the database." );
        }

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

            ObjectMapper mapper = new ObjectMapper();

            try
            {
                ForecastResponse response = mapper.readValue( rawForecast,
                                                              ForecastResponse.class );

                for ( Forecast forecast : response.getForecasts() )
                {
                    LOGGER.debug( "Parsing {}", forecast );
                    boolean saved = this.read( forecast, source.getId() );
                    dataSaved = dataSaved || saved;
                }
            }
            catch ( JsonMappingException jme )
            {
                throw new PreIngestException( "Failed to parse the response body"
                                              + " from WRDS url "
                                              + location,
                                              jme );
            }
            catch ( IngestException e )
            {
                throw new IngestException( "Values from WRDS url "
                                           + location
                                           + " could not be ingested.",
                                           e );
            }

            SourceCompleter completer = createSourceCompleter( source.getId(),
                                                               this.lockManager );

            // See #64922, there are two special cases where "read()" will
            // return before any data is saved. Tolerate that situation.
            if ( dataSaved )
            {
                completer.complete( this.latches );
            }
            else if ( this.latches.isEmpty() )
            {
                Pair<CountDownLatch,CountDownLatch> fakeLatches
                        = Pair.of( new CountDownLatch( 0 ),
                                   new CountDownLatch( 0 ) );
                // Satisfy the completer when there is no data saved by passing
                // fake latches.
                Set<Pair<CountDownLatch,CountDownLatch>> fakeSet
                        = Set.of( fakeLatches );
                completer.complete( fakeSet );
            }
            else
            {
                throw new IllegalStateException( "When no data was saved, no "
                                                 + "coordinating latches should"
                                                 + " exist." );
            }

            completed = true;
        }
        else
        {
            LOGGER.debug( "{} yields for source {}", this, hash );

            completedDetails = new SourceCompletedDetails( source.getId() );

            try
            {
                completed = completedDetails.wasCompleted();
            }
            catch ( SQLException se )
            {
                throw new IngestException( "Unable to ask if source with url "
                                           + this.dataSource.getUri() + " and source id "
                                           + source.getId() + " was completed.",
                                           se );
            }
        }

        return IngestResult.singleItemListFrom(
                this.projectConfig,
                this.dataSource,
                hash,
                foundAlready,
                !completed
        );
    }

    /**
     *
     * @param forecast
     * @param sourceId
     * @return true when data was saved, false otherwise.
     * @throws IngestException
     */
    private boolean read( Forecast forecast, int sourceId ) throws IngestException
    {
        URI location = this.getLocation();
        List<DataPoint> dataPointsList;

        if ( forecast.getMembers() != null
             && forecast.getMembers().length > 0
             && forecast.getMembers()[0].getDataPointsList().size() > 0 )
        {
            dataPointsList = forecast.getMembers()[0].getDataPointsList().get( 0 );
        }
        else
        {
            LOGGER.warn( "The forecast '{}' from '{}' did not have data to save.",
                         forecast, location );
            return false;
        }

        if ( dataPointsList.size() < 2 )
        {
            LOGGER.warn( "Fewer than two values present in the first forecast '{}' from '{}'.",
                         forecast, location );
            return false;
        }

        Duration timeDuration = Duration.between( dataPointsList.get( 0 ).getTime(),
                                                  dataPointsList.get( 1 ).getTime() );

        OffsetDateTime startTime = this.getStartTime( forecast, timeDuration );

        // Get the time scale information, if available
        TimeScale timeScale = TimeScaleFromParameterCodes.getTimeScale( forecast.getParameterCodes(), location );

        TimeSeries timeSeries;
        try
        {
            timeSeries = this.getTimeSeries( forecast, sourceId, startTime, timeScale );
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Failed to get TimeSeries info for "
                                       + "forecast=" + forecast
                                       + " source=" + sourceId
                                       + " startTime=" + startTime
                                       + " timeScale=" + timeScale,
                                       se );
        }

        // Before ingest, validate the timeseries as being a timeseries in the
        // sense that a timeseries is a sequence of values in time.
        this.validateTimeseries( dataPointsList );

        for (DataPoint dataPoint : dataPointsList)
        {
            Duration between = Duration.between( startTime, dataPoint.getTime());
            int lead = ( int ) TimeHelper.durationToLongUnits( between, TimeHelper.LEAD_RESOLUTION );

            try
            {
                Pair<CountDownLatch,CountDownLatch> synchronizer =
                        IngestedValues.addTimeSeriesValue( timeSeries.getTimeSeriesID(),
                                                           lead,
                                                           dataPoint.getValue() );
                this.latches.add( synchronizer );
            }
            catch ( SQLException se )
            {
                throw new IngestException( "Failed to ingest values when adding "
                                           + "timeSeries=" + timeSeries
                                           + " lead=" + lead
                                           + " dataPoint=" + dataPoint,
                                           se );
            }
        }
        return true;
    }


    /**
     * Validate a timeseries. Return if valid, else throw PreIngestException.
     *
     * A timeseries according to this method is a sequence of values in time.
     * Therefore a list of data with duplicate values for any given datetime is
     * invalid and will cause a PreIngestException.
     *
     * @param dataPointsList the WRDS-formatted timeseries data points
     * @throws wres.io.reading.PreIngestException when invalid timeseries found
     */

    private void validateTimeseries( List<DataPoint> dataPointsList )
    {
        Objects.requireNonNull( dataPointsList );

        // Put each datetime in a set. We can compare the set size to the list
        // size and if they are identical: all good.
        Set<OffsetDateTime> dateTimes = new HashSet<>( dataPointsList.size() );

        // For error message purposes, track the exact datetimes that had more
        // than one value.
        Set<OffsetDateTime> multipleValues = new TreeSet<>();

        for ( DataPoint wrdsDataPoint : dataPointsList )
        {
            OffsetDateTime dateTimeForOneValue = wrdsDataPoint.getTime();
            boolean added = dateTimes.add( dateTimeForOneValue );

            if ( !added )
            {
                multipleValues.add( dateTimeForOneValue );
            }
        }

        // Check the size of the datetimes set vs the size of the list
        if ( dataPointsList.size() != dateTimes.size() )
        {
            String message = "Invalid timeseries data encountered. Multiple data"
                             + " found for each of the following datetimes in "
                             + "a forecast from " + this.getLocation()
                             + " : " + multipleValues;
            throw new PreIngestException( message );
        }
    }

    private int getVariableFeatureId(final Forecast forecast) throws SQLException
    {
        LocationNames locationDescription = forecast.getLocation().getNames();

        FeatureDetails details = new FeatureDetails(  );
        details.setFeatureName( locationDescription.getNwsName() );

        // Tolerate missing comid
        if ( !locationDescription.getComId().isBlank() )
        {
            details.setComid( Integer.parseInt( locationDescription.getComId() ) );
        }

        details.setGageID( locationDescription.getUsgsSiteCode() );
        details.setLid( locationDescription.getNwsLid() );
        details.save();

        // Use the Physical Element code as the variable name because AHPS
        // forecasts have QR vs QI vs HG which represent different variables.
        // See redmine issue #61535 for details.
        int variableId = Variables.getVariableID( forecast.getParameterCodes()
                                                          .getPhysicalElement() );

        return Features.getVariableFeatureByFeature( details, variableId );
    }

    private TimeSeries getTimeSeries(
            final Forecast forecast,
            final int sourceId,
            final OffsetDateTime startDate,
            final TimeScale timeScale
    ) throws SQLException
    {
        String startTime = TimeHelper.convertDateToString( startDate );

        TimeSeries timeSeries = new TimeSeries( sourceId, startTime);
        timeSeries.setEnsembleID( Ensembles.getDefaultEnsembleID() );
        timeSeries.setMeasurementUnitID( this.getMeasurementUnitId( forecast ) );
        timeSeries.setVariableFeatureID( this.getVariableFeatureId( forecast ) );
        timeSeries.setTimeScale( timeScale );
        return timeSeries;
    }

    private OffsetDateTime getStartTime(final Forecast forecast, Duration timeStep)
    {
        if (forecast.getBasisTime() != null)
        {
            return forecast.getBasisTime().minus( timeStep );
        }

        return forecast.getIssuedTime();
    }

    private int getMeasurementUnitId(final Forecast forecast) throws SQLException
    {
        return MeasurementUnits.getMeasurementUnitID(forecast.getUnits().getUnitName());
    }

    private InputStream getFromFile( URI uri ) throws FileNotFoundException
    {
        if ( !uri.getScheme().equals( "file" ) )
        {
            throw new IllegalArgumentException(
                    "Must pass a file uri, got " + uri );
        }

        Path forecastPath = Paths.get( uri );
        File forecastFile = forecastPath.toFile();
        return new FileInputStream( forecastFile );
    }

    private Pair<Integer,InputStream> getFromWeb( URI uri ) throws IOException
    {
        if ( !uri.getScheme().startsWith( "http" ) )
        {
            throw new IllegalArgumentException(
                    "Must pass an http uri, got " + uri );
        }

        LOGGER.debug( "getFromWeb {}", uri );

        try
        {
            HttpRequest request = HttpRequest.newBuilder()
                                             .uri( uri )
                                             .build();
            HttpResponse<InputStream> httpResponse =
                    HTTP_CLIENT.send( request,
                                      HttpResponse.BodyHandlers.ofInputStream() );

            int httpStatus = httpResponse.statusCode();

            if ( httpStatus >= 400 && httpStatus < 500 )
            {
                return Pair.of( httpStatus, InputStream.nullInputStream() );
            }
            else if ( httpStatus >= 500 )
            {
                throw new IngestException( "Failed to get data from "
                                           + uri
                                           + " due to status code "
                                           + httpStatus );
            }

            LOGGER.debug( "Successfully retrieved data from {}", uri );
            return Pair.of( httpStatus, httpResponse.body() );
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while getting data from {}", uri, ie );
            Thread.currentThread().interrupt();
            return Pair.of( -1, InputStream.nullInputStream() );
        }
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

    private URI getLocation()
    {
        return this.dataSource.getUri();
    }
}
