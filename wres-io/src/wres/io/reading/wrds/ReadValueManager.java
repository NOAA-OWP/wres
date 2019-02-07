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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.ProjectConfig;
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

    private final HttpClient httpClient;

    ReadValueManager( final ProjectConfig projectConfig,
                      final DataSourceConfig datasourceConfig,
                      final URI location )
    {
        this.location = location;
        this.projectConfig = projectConfig;
        this.dataSourceConfig = datasourceConfig;
        this.httpClient = HttpClient.newHttpClient();
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
        if (!foundAlready)
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
                    forecastData = this.getFromWeb( this.location );
                }
		else
                {
		    throw new UnsupportedOperationException( "Only file and http(s) "
							     + "are supported. Got: "
							     + this.location );
		}

                ForecastResponse response = mapper.readValue( forecastData,
                                                              ForecastResponse.class );

                for ( Forecast forecast : response.getForecasts() )
                {
                    this.read( forecast, source.getId() );
                }
            }
            catch(SQLException e)
            {
                throw new IngestException( "Values from WRDS could not be ingested.", e );
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

        long timeStep = TimeHelper.durationToLongUnits(
                timeDuration, TimeHelper.LEAD_RESOLUTION
        );

        OffsetDateTime startTime = this.getStartTime( forecast, timeDuration );
        TimeSeries timeSeries = this.getTimeSeries( forecast, sourceId, startTime );

        for (DataPoint dataPoint : dataPointsList)
        {
            Duration between = Duration.between( startTime, dataPoint.getTime());
            int lead = ( int ) TimeHelper.durationToLongUnits( between, TimeHelper.LEAD_RESOLUTION );
            IngestedValues.addTimeSeriesValue( timeSeries.getTimeSeriesID(), lead, dataPoint.getValue() );
        }
    }

    private int getVariableFeatureId(final Forecast forecast) throws SQLException
    {
        LocationNames locationDescription = forecast.getLocation().getNames();

        FeatureDetails details = new FeatureDetails(  );
        details.setFeatureName( locationDescription.getNwsName() );
        details.setComid( Integer.parseInt(locationDescription.getComId()) );
        details.setGageID( locationDescription.getUsgsSiteCode() );
        details.setLid( locationDescription.getNwsLid() );
        details.save();

        int variableId = Variables.getVariableID(this.dataSourceConfig);

        return Features.getVariableFeatureByFeature( details, variableId );
    }

    private TimeSeries getTimeSeries(
            final Forecast forecast,
            final int sourceId,
            final OffsetDateTime startDate
    ) throws SQLException
    {
        String startTime = TimeHelper.convertDateToString( startDate );

        TimeSeries timeSeries = new TimeSeries( sourceId, startTime);
        timeSeries.setEnsembleID( Ensembles.getDefaultEnsembleID() );
        timeSeries.setMeasurementUnitID( this.getMeasurementUnitId( forecast ) );
        timeSeries.setVariableFeatureID( this.getVariableFeatureId( forecast ) );

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
	    throw new IllegalArgumentException( "Must pass a file uri, got " + uri );
	}
	
	Path forecastPath = Paths.get( uri );
	File forecastFile = forecastPath.toFile();
	return new FileInputStream( forecastFile );
    }

    private InputStream getFromWeb( URI uri ) throws IOException
    {
	if ( !uri.getScheme().startsWith( "http" ) )
        {
	    throw new IllegalArgumentException( "Must pass an http uri, got " + uri );
	}

	try
        {
	    HttpRequest request = HttpRequest.newBuilder()
                                             .uri( uri )
                                             .build();
	    HttpResponse<InputStream> httpResponse =
		this.httpClient.send( request,
				      HttpResponse.BodyHandlers.ofInputStream() );

	    int httpStatus = httpResponse.statusCode();

	    if ( httpStatus >= 400 && httpStatus < 500 )
            {
		LOGGER.warn( "Could not retrieve data from {} due to status code {}",
                             uri,
                             httpResponse.statusCode() );
		InputStream.nullInputStream();
	    }
	    else if ( httpStatus >= 500 )
	    {
		throw new IngestException( "Failed to get data from "
					   + uri
					   + " due to status code "
					   + httpStatus );
	    }

	    return httpResponse.body();
	}
	catch ( InterruptedException ie )
        {
	    LOGGER.warn( "Interrupted while getting data from {}", uri, ie );
	    Thread.currentThread().interrupt();
	    return InputStream.nullInputStream();
        }
    }

    private final URI location;
    private final ProjectConfig projectConfig;
    private final DataSourceConfig dataSourceConfig;
}
