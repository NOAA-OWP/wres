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
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.metadata.TimeScale;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.data.details.FeatureDetails;
import wres.io.data.details.SourceDetails;
import wres.io.data.details.TimeSeries;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.IngestedValues;
import wres.io.reading.PreIngestException;
import wres.util.Strings;
import wres.util.TimeHelper;

public class ReadValueManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ReadValueManager.class );

    // TODO: inject http client in constructor without changing much else #60281
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    private final URI location;
    private final ProjectConfig projectConfig;
    private final DataSourceConfig dataSourceConfig;

    ReadValueManager( final ProjectConfig projectConfig,
                      final DataSourceConfig datasourceConfig,
                      final URI location )
    {
        this.location = location;
        this.projectConfig = projectConfig;
        this.dataSourceConfig = datasourceConfig;
    }

    public List<IngestResult> save() throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();
        Instant now = Instant.now();
        String hash = Strings.getMD5Checksum( this.location.toURL().getFile().getBytes());

        boolean foundAlready;
        SourceDetails source;

        try
        {
            source = DataSources.get( this.location, now.toString(), null, hash );
            foundAlready = !source.performedInsert();
        }
        catch ( SQLException e )
        {
            throw new IngestException( "Source metadata about '" + this.location +
                                       "' could not be stored or retrieved from the database." );
        }

        // If this was the application that performed the insert into the source records,
        // this needs to perform the ingest
        if ( !foundAlready )
        {
            try
            {
                InputStream forecastData;

                if ( this.location.getScheme().equals( "file" ) )
                {
                    forecastData = this.getFromFile( this.location );
                }
                else if ( this.location.getScheme().startsWith( "http" ) )
                {
                    Pair<Integer,InputStream> response = this.getFromWeb( this.location );
                    int httpStatus = response.getLeft();

                    if ( httpStatus >= 400 && httpStatus < 500 )
                    {
                        LOGGER.warn( "Treating HTTP response code {} as no data found from URI {}",
                                     httpStatus,
                                     this.location );

                        // Mark this data as already having been ingested even
                        // if it is no data found. See #60289 for why.
                        return IngestResult.singleItemListFrom(
                                this.projectConfig,
                                this.dataSourceConfig,
                                hash,
                                this.location,
                                false
                        );
                    }

                    forecastData = response.getRight();
                }
                else
                {
                    throw new UnsupportedOperationException("Only file and http(s) "
                            + "are supported. Got: "
                            + this.location );
                }

                ForecastResponse response = mapper.readValue( forecastData,
                                                              ForecastResponse.class );

                for ( Forecast forecast : response.getForecasts() )
                {
                    LOGGER.debug("Parsing {}", forecast);
                    this.read( forecast, source.getId() );
                }
            }
            catch ( SQLException e )
            {
                throw new IngestException(
                        "Values from WRDS could not be ingested.",
                        e );
            }
        }

        return IngestResult.singleItemListFrom(
                this.projectConfig,
                this.dataSourceConfig,
                hash,
                this.location,
                foundAlready
        );
    }

    private void read(final Forecast forecast, final int sourceId) throws SQLException
    {

        List<DataPoint> dataPointsList;

        if ( forecast.getMembers() != null
             && forecast.getMembers().length > 0
             && forecast.getMembers()[0].getDataPointsList().size() > 0 )
        {
            dataPointsList = forecast.getMembers()[0].getDataPointsList().get( 0 );
        }
        else
        {
            LOGGER.warn("The forecast '{}' from '{}' did not have data to save.", forecast, this.location);
            return;
        }

        if ( dataPointsList.size() < 2 )
        {
            LOGGER.warn( "Fewer than two values present in the first forecast '{}' from '{}'.", forecast, this.location );
            return;
        }

        Duration timeDuration = Duration.between( dataPointsList.get( 0 ).getTime(),
                                                  dataPointsList.get( 1 ).getTime() );

        OffsetDateTime startTime = this.getStartTime( forecast, timeDuration );
        
        // Get the time scale information, if available
        TimeScale timeScale = TimeScaleFromParameterCodes.getTimeScale( forecast.getParameterCodes(), this.location );

        TimeSeries timeSeries = this.getTimeSeries( forecast, sourceId, startTime, timeScale );

        // Before ingest, validate the timeseries as being a timeseries in the
        // sense that a timeseries is a sequence of values in time.
        this.validateTimeseries( dataPointsList );

        for (DataPoint dataPoint : dataPointsList)
        {
            Duration between = Duration.between( startTime, dataPoint.getTime());

            int lead = ( int ) TimeHelper.durationToLongUnits( between, TimeHelper.LEAD_RESOLUTION );
            IngestedValues.addTimeSeriesValue( timeSeries.getTimeSeriesID(), lead, dataPoint.getValue() );
        }
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

    private URI getLocation()
    {
        return this.location;
    }
}
