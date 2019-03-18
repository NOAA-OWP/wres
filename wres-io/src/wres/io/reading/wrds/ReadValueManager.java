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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
import wres.util.Strings;
import wres.util.TimeHelper;

public class ReadValueManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ReadValueManager.class );
    private static final Set<Integer> foundTimeSeries = new HashSet<>();
    private static final Map<Integer, List<Duration>> foundLeads = new HashMap<>(  );

    // TODO: inject http client in constructor without changing much else #60281
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

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

        Integer foundTimeSeriesHash = null;

        // If we're debugging, we want to have an easily identifiable code for a time series that
        // might have repetitive data
        if (LOGGER.isDebugEnabled())
        {
            foundTimeSeriesHash = Objects.hash( timeSeries );
        }

        int index = 0;
        int foundAtIndex = -1;

        for (DataPoint dataPoint : dataPointsList)
        {
            Duration between = Duration.between( startTime, dataPoint.getTime());

            // If we're debugging, we want to check to see if repetitive leads for forecasts are being added
            if ( LOGGER.isDebugEnabled() )
            {
                // If we haven't seen a repetitive value yet...
                if ( foundAtIndex == -1 )
                {

                    synchronized ( foundLeads )
                    {
                        if ( !foundLeads.containsKey( foundTimeSeriesHash ) )
                        {
                            foundLeads.put( foundTimeSeriesHash, new ArrayList<>() );
                        }

                        if ( foundLeads.get( foundTimeSeriesHash ).contains( between ) )
                        {
                            foundAtIndex = index;
                            LOGGER.debug( "Found {} in {} again at index {}!",
                                          between, forecast, foundAtIndex );
                        }
                        else
                        {
                            foundLeads.get( foundTimeSeriesHash ).add( between );
                        }
                    }
                }
            }

            int lead = ( int ) TimeHelper.durationToLongUnits( between, TimeHelper.LEAD_RESOLUTION );
            IngestedValues.addTimeSeriesValue( timeSeries.getTimeSeriesID(), lead, dataPoint.getValue() );
            index++;
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

        int variableId = Variables.getVariableID(this.dataSourceConfig);

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

    private final URI location;
    private final ProjectConfig projectConfig;
    private final DataSourceConfig dataSourceConfig;
}
