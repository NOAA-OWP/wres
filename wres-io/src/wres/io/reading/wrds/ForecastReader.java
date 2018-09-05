package wres.io.reading.wrds;

import java.io.IOException;
import java.net.URL;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import wres.io.reading.TimeSeriesValues;
import wres.util.Strings;
import wres.util.TimeHelper;

public class ForecastReader
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ForecastReader.class );

    ForecastReader( final ProjectConfig projectConfig,
                    final DataSourceConfig datasourceConfig,
                    final URL location )
    {
        this.location = location;
        this.projectConfig = projectConfig;
        this.dataSourceConfig = datasourceConfig;
    }

    public List<IngestResult> save() throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();
        Instant now = Instant.now();
        String hash = Strings.getMD5Checksum( this.location.getFile().getBytes());

        boolean foundAlready;
        SourceDetails source;

        try
        {
            source = DataSources.get(this.location.toString(), now.toString(), null, hash);
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
                ForecastResponse response = mapper.readValue( this.location, ForecastResponse.class );

                if (response.getStatusCode() >= 400)
                {
                    throw new IngestException( response.getMesssage() );
                }

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
                this.location.toString(),
                foundAlready
        );
    }

    private void read(final Forecast forecast, final int sourceId) throws SQLException
    {
        TimeSeries timeSeries = this.getTimeSeries( forecast, sourceId );
        OffsetDateTime startTime = this.getStartTime( forecast );

        DataPoint[] dataPointsList;

        if (forecast.getMembers() != null && forecast.getMembers().length > 0)
        {
            dataPointsList = forecast.getMembers()[0].getDataPointsList();
        }
        else if (forecast.getDataPointsList() != null && forecast.getDataPointsList().length > 0)
        {
            dataPointsList = forecast.getDataPointsList();
        }
        else
        {
            LOGGER.warn("The forecast '{}' from '{}' did not have data to save.", forecast, this.location);
            return;
        }

        long timeStep = TimeHelper.durationToLeadUnits(
                Duration.between( dataPointsList[0].getTime(), dataPointsList[1].getTime() )
        );

        timeSeries.setTimeStep( (int)timeStep );

        for (DataPoint dataPoint : dataPointsList)
        {
            Duration between = Duration.between( startTime, dataPoint.getTime());
            int lead = ( int ) TimeHelper.durationToLeadUnits( between );
            TimeSeriesValues.add( timeSeries.getTimeSeriesID(), lead, dataPoint.getValue() );
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

    private TimeSeries getTimeSeries(final Forecast forecast, final int sourceId) throws SQLException
    {
        String startTime = TimeHelper.convertDateToString( this.getStartTime( forecast ) );

        TimeSeries timeSeries = new TimeSeries( sourceId, startTime);
        timeSeries.setEnsembleID( Ensembles.getDefaultEnsembleID() );
        timeSeries.setMeasurementUnitID( this.getMeasurementUnitId( forecast ) );
        timeSeries.setVariableFeatureID( this.getVariableFeatureId( forecast ) );

        return timeSeries;
    }

    private OffsetDateTime getStartTime(final Forecast forecast)
    {
        if (forecast.getBasisTime() != null)
        {
            return forecast.getBasisTime();
        }

        return forecast.getIssuedTime();
    }

    private int getMeasurementUnitId(final Forecast forecast) throws SQLException
    {
        return MeasurementUnits.getMeasurementUnitID(forecast.getUnits().getUnitName());
    }

    private final URL location;
    private final ProjectConfig projectConfig;
    private final DataSourceConfig dataSourceConfig;
}
